/*
===============================================================================
Warehouse Reconciliation Audit Layer
===============================================================================

Purpose:
- Prove trusted Gold/SCD2 staging outputs match PostgreSQL warehouse facts.
- Prove warehouse fact totals match BI/reporting marts.
- Write every check to audit.reconciliation_report with PASS/FAIL status.
- Keep the output human-readable and interview-defensible.

Run order:
1. scripts/load_gold_to_postgres_staging.py
2. sql/warehouse/create_warehouse_tables.sql
3. sql/warehouse/create_marts.sql
4. sql/warehouse/reconciliation_report.sql

Notes:
- Source side means trusted Gold/SCD2 staging tables loaded from data/gold.
- Target side means warehouse or marts objects.
- A tolerance of 0.01 is used for money checks to avoid decimal noise.
===============================================================================
*/

BEGIN;

-- This file is one step of the warehouse build. It refuses to run anywhere but
-- in the build transaction that sql/warehouse/begin_generation.sql started
-- (which sets p2.build_txid for that transaction only), so it can never drop or
-- rebuild the live tables on its own.
DO $$
BEGIN
    IF current_setting('p2.build_txid', true) IS DISTINCT FROM txid_current()::TEXT THEN
        RAISE EXCEPTION 'reconciliation_report.sql runs only inside the warehouse build transaction started by sql/warehouse/begin_generation.sql. Run the whole build: bash scripts/run_airflow_task.sh build_warehouse';
    END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS audit;

DROP VIEW IF EXISTS audit.v_latest_reconciliation_report;
DROP TABLE IF EXISTS audit.reconciliation_report CASCADE;

CREATE TABLE audit.reconciliation_report (
    reconciliation_id BIGSERIAL PRIMARY KEY,
    reconciliation_run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_category TEXT NOT NULL,
    source_object TEXT,
    target_object TEXT,
    metric_name TEXT NOT NULL,
    source_count NUMERIC(38, 6),
    target_count NUMERIC(38, 6),
    count_difference NUMERIC(38, 6),
    source_amount NUMERIC(38, 6),
    target_amount NUMERIC(38, 6),
    amount_difference NUMERIC(38, 6),
    tolerance NUMERIC(38, 6) NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    details TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_reconciliation_run_check UNIQUE (reconciliation_run_id, check_name)
);

COMMENT ON TABLE audit.reconciliation_report IS
'Warehouse reconciliation audit table. Stores PASS/FAIL checks proving Gold/SCD2 staging, warehouse facts, and marts remain aligned.';

COMMENT ON COLUMN audit.reconciliation_report.reconciliation_run_id IS
'Logical run identifier shared by all checks from one execution of reconciliation_report.sql.';

COMMENT ON COLUMN audit.reconciliation_report.check_name IS
'Human-readable reconciliation check name.';

COMMENT ON COLUMN audit.reconciliation_report.status IS
'PASS when source and target match within tolerance; FAIL when drift is detected.';

COMMENT ON COLUMN audit.reconciliation_report.checked_at IS
'Timestamp when the reconciliation check was written.';

CREATE INDEX idx_reconciliation_report_checked_at
ON audit.reconciliation_report (checked_at DESC);

CREATE INDEX idx_reconciliation_report_run_status
ON audit.reconciliation_report (reconciliation_run_id, status);

CREATE INDEX idx_reconciliation_report_check_category
ON audit.reconciliation_report (check_category);

CREATE OR REPLACE FUNCTION audit.first_existing_table(VARIADIC table_names TEXT[])
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        IF to_regclass(table_name) IS NOT NULL THEN
            RETURN table_name;
        END IF;
    END LOOP;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION audit.record_reconciliation_check(
    p_reconciliation_run_id TEXT,
    p_check_name TEXT,
    p_check_category TEXT,
    p_source_object TEXT,
    p_target_object TEXT,
    p_metric_name TEXT,
    p_metric_type TEXT,
    p_source_value NUMERIC,
    p_target_value NUMERIC,
    p_tolerance NUMERIC,
    p_checked_at TIMESTAMPTZ,
    p_details TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    value_difference NUMERIC(38, 6);
    resolved_status TEXT;
BEGIN
    IF p_source_value IS NULL OR p_target_value IS NULL THEN
        value_difference := NULL;
        resolved_status := 'FAIL';
    ELSE
        value_difference := ROUND((p_source_value - p_target_value)::NUMERIC, 6);
        resolved_status := CASE
            WHEN ABS(value_difference) <= COALESCE(p_tolerance, 0) THEN 'PASS'
            ELSE 'FAIL'
        END;
    END IF;

    INSERT INTO audit.reconciliation_report (
        reconciliation_run_id,
        check_name,
        check_category,
        source_object,
        target_object,
        metric_name,
        source_count,
        target_count,
        count_difference,
        source_amount,
        target_amount,
        amount_difference,
        tolerance,
        status,
        details,
        checked_at
    )
    VALUES (
        p_reconciliation_run_id,
        p_check_name,
        p_check_category,
        p_source_object,
        p_target_object,
        p_metric_name,
        CASE WHEN upper(p_metric_type) = 'COUNT' THEN p_source_value END,
        CASE WHEN upper(p_metric_type) = 'COUNT' THEN p_target_value END,
        CASE WHEN upper(p_metric_type) = 'COUNT' THEN value_difference END,
        CASE WHEN upper(p_metric_type) = 'AMOUNT' THEN p_source_value END,
        CASE WHEN upper(p_metric_type) = 'AMOUNT' THEN p_target_value END,
        CASE WHEN upper(p_metric_type) = 'AMOUNT' THEN value_difference END,
        COALESCE(p_tolerance, 0),
        resolved_status,
        p_details,
        p_checked_at
    );
END;
$$;

DO $$
DECLARE
    run_id TEXT := NULLIF(current_setting('p2.run_id', true), '');
    checked_time TIMESTAMPTZ := CURRENT_TIMESTAMP;

    source_table TEXT;
    invalid_reference RECORD;
    scd2_dimension RECORD;
    source_value NUMERIC(38, 6);
    target_value NUMERIC(38, 6);
    order_count_excluded NUMERIC(38, 6);
BEGIN
    /*
    ---------------------------------------------------------------------------
    Gold order count vs warehouse fact_orders count
    ---------------------------------------------------------------------------
    */

    source_table := audit.first_existing_table(
        'staging.stg_gold_fact_orders_scd2',
        'staging.stg_gold_fact_orders'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.fact_orders;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold order count equals warehouse fact_orders count',
        'Gold-to-warehouse count',
        COALESCE(source_table, 'missing Gold order staging table'),
        'warehouse.fact_orders',
        'order_count',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        CASE WHEN source_table IS NULL THEN 'Expected staging.stg_gold_fact_orders_scd2 or staging.stg_gold_fact_orders.' END
    );

    /*
    ---------------------------------------------------------------------------
    Gold order item count vs warehouse fact_order_items count
    ---------------------------------------------------------------------------
    */

    source_table := audit.first_existing_table(
        'staging.stg_gold_fact_order_items_scd2',
        'staging.stg_gold_fact_order_items'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.fact_order_items;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold order item count equals warehouse fact_order_items count',
        'Gold-to-warehouse count',
        COALESCE(source_table, 'missing Gold order item staging table'),
        'warehouse.fact_order_items joined to warehouse.dim_product excluding UNKNOWN product',
        'order_item_count',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        CASE WHEN source_table IS NULL THEN 'Expected staging.stg_gold_fact_order_items_scd2 or staging.stg_gold_fact_order_items.' END
    );

    /*
    ---------------------------------------------------------------------------
    Gold revenue vs warehouse revenue
    ---------------------------------------------------------------------------
    */

    source_table := audit.first_existing_table(
        'staging.stg_gold_fact_orders_scd2',
        'staging.stg_gold_fact_orders'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format($sql$
            SELECT ROUND(
                COALESCE(
                    SUM(
                        COALESCE(
                            warehouse.safe_numeric(to_jsonb(src)->>'net_revenue'),
                            warehouse.safe_numeric(to_jsonb(src)->>'revenue'),
                            warehouse.safe_numeric(to_jsonb(src)->>'total_revenue'),
                            warehouse.safe_numeric(to_jsonb(src)->>'cart_value'),
                            0
                        )
                    ),
                    0
                )::NUMERIC,
                2
            )
            FROM %s src
        $sql$, source_table)
        INTO source_value;
    END IF;

    SELECT ROUND(COALESCE(SUM(net_revenue), 0)::NUMERIC, 2)
    INTO target_value
    FROM warehouse.fact_orders;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold revenue equals warehouse fact_orders net revenue',
        'Gold-to-warehouse amount',
        COALESCE(source_table, 'missing Gold order staging table'),
        'warehouse.fact_orders',
        'net_revenue',
        'AMOUNT',
        source_value,
        target_value,
        0.01,
        checked_time,
        'Compares Gold order revenue fields against warehouse.fact_orders.net_revenue.'
    );

    /*
    ---------------------------------------------------------------------------
    Gold campaign spend vs warehouse campaign spend
    ---------------------------------------------------------------------------
    */

    source_table := audit.first_existing_table(
        'staging.stg_gold_fact_campaign_spend_scd2',
        'staging.stg_gold_fact_ad_spend'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format($sql$
            SELECT ROUND(
                COALESCE(
                    SUM(
                        COALESCE(
                            warehouse.safe_numeric(to_jsonb(src)->>'spend_amount'),
                            warehouse.safe_numeric(to_jsonb(src)->>'ad_spend'),
                            warehouse.safe_numeric(to_jsonb(src)->>'cost'),
                            0
                        )
                    ),
                    0
                )::NUMERIC,
                2
            )
            FROM %s src
        $sql$, source_table)
        INTO source_value;
    END IF;

    SELECT ROUND(COALESCE(SUM(spend_amount), 0)::NUMERIC, 2)
    INTO target_value
    FROM warehouse.fact_campaign_spend;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold campaign spend equals warehouse campaign spend',
        'Gold-to-warehouse amount',
        COALESCE(source_table, 'missing Gold campaign spend staging table'),
        'warehouse.fact_campaign_spend',
        'campaign_spend',
        'AMOUNT',
        source_value,
        target_value,
        0.01,
        checked_time,
        'Compares Gold campaign/ad spend against warehouse.fact_campaign_spend.spend_amount.'
    );

    /*
    ---------------------------------------------------------------------------
    Staging row counts vs warehouse row counts
    ---------------------------------------------------------------------------
    */

    source_table := audit.first_existing_table(
        'staging.stg_gold_dim_customers_scd2',
        'staging.stg_gold_dim_customers'
    );

    source_value := NULL;

    -- Distinct natural keys on both sides, so SCD2 history (several
    -- versions per key) does not inflate the warehouse count.
    IF source_table IS NOT NULL THEN
        EXECUTE format(
            'SELECT COUNT(DISTINCT customer_id::TEXT)::NUMERIC FROM %s WHERE customer_id IS NOT NULL',
            source_table
        )
        INTO source_value;
    END IF;


    SELECT COUNT(DISTINCT customer_id)::NUMERIC
    INTO target_value
    FROM warehouse.dim_customer
    WHERE customer_sk NOT IN (0, -1)
      AND customer_id NOT IN ('UNKNOWN', 'INVALID');

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold customer dimension key count equals warehouse dim_customer key count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing customer staging table'),
        'warehouse.dim_customer',
        'customer_dimension_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Distinct customer_id on both sides, excluding the unknown member (customer_sk = 0) and the invalid-reference member (customer_sk = -1). No member is invented per key: a fact whose customer is not in the dimension is kept on the invalid-reference member.'
    );

    source_table := audit.first_existing_table(
        'staging.stg_gold_dim_products_scd2',
        'staging.stg_gold_dim_products'
    );

    source_value := NULL;

    -- Distinct natural keys on both sides, so SCD2 history (several
    -- versions per key) does not inflate the warehouse count.
    IF source_table IS NOT NULL THEN
        EXECUTE format(
            'SELECT COUNT(DISTINCT product_id::TEXT)::NUMERIC FROM %s WHERE product_id IS NOT NULL',
            source_table
        )
        INTO source_value;
    END IF;


    SELECT COUNT(DISTINCT product_id)::NUMERIC
    INTO target_value
    FROM warehouse.dim_product
    WHERE product_sk NOT IN (0, -1)
      AND product_id NOT IN ('UNKNOWN', 'INVALID');

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold product dimension key count equals warehouse dim_product key count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing product staging table'),
        'warehouse.dim_product',
        'product_dimension_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Distinct product_id on both sides, excluding the unknown member (product_sk = 0) and the invalid-reference member (product_sk = -1). No member is invented per key: a fact whose product is not in the dimension is kept on the invalid-reference member.'
    );

    source_table := audit.first_existing_table(
        'staging.stg_gold_dim_campaigns_scd2',
        'staging.stg_gold_dim_campaigns'
    );

    source_value := NULL;

    -- Distinct natural keys on both sides, so SCD2 history (several
    -- versions per key) does not inflate the warehouse count.
    IF source_table IS NOT NULL THEN
        EXECUTE format(
            'SELECT COUNT(DISTINCT campaign_id::TEXT)::NUMERIC FROM %s WHERE campaign_id IS NOT NULL',
            source_table
        )
        INTO source_value;
    END IF;


    SELECT COUNT(DISTINCT campaign_id)::NUMERIC
    INTO target_value
    FROM warehouse.dim_campaign
    WHERE campaign_sk NOT IN (0, -1)
      AND campaign_id NOT IN ('UNKNOWN', 'INVALID');

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold campaign dimension key count equals warehouse dim_campaign key count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing campaign staging table'),
        'warehouse.dim_campaign',
        'campaign_dimension_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Distinct campaign_id on both sides, excluding the unknown member (campaign_sk = 0) and the invalid-reference member (campaign_sk = -1). No member is invented per key: a fact whose campaign is not in the dimension is kept on the invalid-reference member.'
    );

    source_table := audit.first_existing_table(
        'staging.stg_gold_fact_campaign_spend_scd2',
        'staging.stg_gold_fact_ad_spend'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.fact_campaign_spend;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Staging campaign spend row count equals warehouse fact_campaign_spend row count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing campaign spend staging table'),
        'warehouse.fact_campaign_spend',
        'campaign_spend_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        NULL
    );

    source_table := audit.first_existing_table(
        'staging.stg_gold_fact_web_events'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    -- Every staging web event is a warehouse fact: none is rejected or dropped
    -- for a bad reference (it loads on the invalid-reference member instead).
    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.fact_web_events;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Staging web event row count equals warehouse fact_web_events row count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing web event staging table'),
        'warehouse.fact_web_events',
        'web_event_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'No web event is rejected for a customer, product or campaign key without a valid dimension row; such a key is the invalid-reference member (-1).'
    );

    /*
    ---------------------------------------------------------------------------
    Invalid references, per fact and dimension
    ---------------------------------------------------------------------------
    A fact is never dropped because its dimension reference is bad. A key that
    is present but has no valid dimension row (quarantined in Silver, or never
    delivered) is the invalid-reference member (-1); a missing key is the
    unknown member (0). Each check counts both sides independently: the facts
    whose natural ID is in no version of the dimension, and the facts on
    member -1. The count itself is the number of invalid references, and it
    reaches data_quality_summary.csv. The fact keeps its natural ID, so it
    resolves to the real member once the dimension row is valid.
    */

    FOR invalid_reference IN
        SELECT * FROM (VALUES
            ('fact_orders', 'customer'),
            ('fact_orders', 'campaign'),
            ('fact_order_items', 'product'),
            ('fact_campaign_spend', 'campaign'),
            ('fact_web_events', 'customer'),
            ('fact_web_events', 'product'),
            ('fact_web_events', 'campaign')
        ) AS pairs(fact_name, dimension_name)
    LOOP
        EXECUTE format($sql$
            SELECT
                COUNT(*) FILTER (WHERE NOT EXISTS (
                    SELECT 1 FROM warehouse.dim_%2$s d WHERE d.%2$s_id = f.%2$s_id))::NUMERIC,
                COUNT(*) FILTER (WHERE f.%2$s_sk = -1)::NUMERIC
            FROM warehouse.%1$s f
        $sql$, invalid_reference.fact_name, invalid_reference.dimension_name)
        INTO source_value, target_value;

        PERFORM audit.record_reconciliation_check(
            run_id,
            format('Invalid %s references in %s are on the invalid-reference member',
                   invalid_reference.dimension_name, invalid_reference.fact_name),
            'Invalid references',
            format('warehouse.%s rows whose %s_id is in no warehouse.dim_%s version',
                   invalid_reference.fact_name, invalid_reference.dimension_name, invalid_reference.dimension_name),
            format('warehouse.%s rows with %s_sk = -1', invalid_reference.fact_name, invalid_reference.dimension_name),
            format('invalid_%s_references_in_%s', invalid_reference.dimension_name, invalid_reference.fact_name),
            'COUNT',
            source_value,
            target_value,
            0,
            checked_time,
            format('%s %s rows reference a %s with no valid dimension row. They are kept, with their %s_id, on the invalid-reference member.',
                   target_value, invalid_reference.fact_name, invalid_reference.dimension_name,
                   invalid_reference.dimension_name)
        );
    END LOOP;

    /*
    ---------------------------------------------------------------------------
    SCD2 versions: Gold vs warehouse
    ---------------------------------------------------------------------------
    The key-count checks above compare distinct natural keys. These compare the
    number of versions: every Gold SCD2 version is one warehouse dimension row,
    apart from the warehouse's own unknown (0) and invalid-reference (-1)
    members.
    */

    FOR scd2_dimension IN
        SELECT * FROM (VALUES
            ('customer', 'staging.stg_gold_dim_customers_scd2'),
            ('product', 'staging.stg_gold_dim_products_scd2'),
            ('campaign', 'staging.stg_gold_dim_campaigns_scd2')
        ) AS dims(dimension_name, staging_table)
    LOOP
        source_value := NULL;
        IF to_regclass(scd2_dimension.staging_table) IS NOT NULL THEN
            EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', scd2_dimension.staging_table) INTO source_value;
        END IF;
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM warehouse.dim_%1$s WHERE %1$s_sk NOT IN (0, -1)',
                       scd2_dimension.dimension_name)
        INTO target_value;

        PERFORM audit.record_reconciliation_check(
            run_id,
            format('Gold %s SCD2 version count equals warehouse dim_%s version count',
                   scd2_dimension.dimension_name, scd2_dimension.dimension_name),
            'Staging-to-warehouse row count',
            scd2_dimension.staging_table,
            format('warehouse.dim_%s', scd2_dimension.dimension_name),
            format('%s_dimension_versions', scd2_dimension.dimension_name),
            'COUNT',
            source_value,
            target_value,
            0,
            checked_time,
            'Rows (SCD2 versions) on both sides, excluding the unknown member (0) and the invalid-reference member (-1).'
        );
    END LOOP;

    /*
    ---------------------------------------------------------------------------
    Ad spend: regional rows vs the campaign-day total
    ---------------------------------------------------------------------------
    The ad feed has one row per campaign, day and region, and each row repeats
    the platform's total for the campaign-day (campaign_day_spend, kept in
    staging only: it is not additive). The warehouse rows of a campaign-day
    must sum to that total, and a region appears once per campaign-day.
    */

    EXECUTE $sql$
        SELECT COUNT(*)::NUMERIC, (SELECT COUNT(*) FROM (
                   SELECT 1 FROM warehouse.fact_campaign_spend
                   GROUP BY campaign_id, spend_date, region_sk HAVING COUNT(*) > 1) duplicated)::NUMERIC
        FROM (
            SELECT f.campaign_id, f.spend_date
            FROM warehouse.fact_campaign_spend f
            JOIN staging.stg_gold_fact_campaign_spend_scd2 s ON s.spend_id::TEXT = f.campaign_spend_nk
            GROUP BY f.campaign_id, f.spend_date
            HAVING SUM(f.spend_amount) <> MAX(s.campaign_day_spend)::NUMERIC(18, 2)
                OR MIN(s.campaign_day_spend) <> MAX(s.campaign_day_spend)
        ) mismatched
    $sql$
    INTO target_value, source_value;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Regional ad spend rows sum to the campaign-day total',
        'Warehouse integrity',
        'expected zero campaign-days whose regional spend does not sum to campaign_day_spend, and zero repeated regions',
        'warehouse.fact_campaign_spend',
        'campaign_days_not_summing_to_their_total',
        'COUNT',
        0,
        target_value + source_value,
        0,
        checked_time,
        format('Checked %s campaign-days over %s regional rows; %s repeated campaign-day-region rows.',
               (SELECT COUNT(*) FROM (SELECT 1 FROM warehouse.fact_campaign_spend GROUP BY campaign_id, spend_date) d),
               (SELECT COUNT(*) FROM warehouse.fact_campaign_spend), source_value)
    );

    /*
    ---------------------------------------------------------------------------
    Order total
    ---------------------------------------------------------------------------
    What the customer was charged is net revenue plus tax plus shipping, to the
    cent, for every order. net_revenue stays the sum of the lines.
    */

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.fact_orders
    WHERE total_amount <> net_revenue + tax_amount + shipping_amount;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Order total equals net revenue plus tax plus shipping for every order',
        'Warehouse integrity',
        'expected zero orders where total_amount <> net_revenue + tax_amount + shipping_amount',
        'warehouse.fact_orders',
        'order_total_mismatched_orders',
        'COUNT',
        0,
        target_value,
        0,
        checked_time,
        format('Checked %s orders.', (SELECT COUNT(*) FROM warehouse.fact_orders))
    );

    /*
    ---------------------------------------------------------------------------
    Null foreign keys
    ---------------------------------------------------------------------------
    */

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM (
        SELECT order_id::TEXT AS fact_key
        FROM warehouse.fact_orders
        WHERE customer_sk IS NULL
           OR campaign_sk IS NULL
           OR order_date_sk IS NULL
           OR region_sk IS NULL
           OR channel_sk IS NULL

        UNION ALL

        SELECT order_item_nk::TEXT AS fact_key
        FROM warehouse.fact_order_items
        WHERE order_sk IS NULL
           OR customer_sk IS NULL
           OR product_sk IS NULL
           OR order_date_sk IS NULL
           OR region_sk IS NULL
           OR channel_sk IS NULL

        UNION ALL

        SELECT campaign_spend_nk::TEXT AS fact_key
        FROM warehouse.fact_campaign_spend
        WHERE campaign_sk IS NULL
           OR spend_date_sk IS NULL
           OR region_sk IS NULL
           OR channel_sk IS NULL

        UNION ALL

        SELECT event_id::TEXT AS fact_key
        FROM warehouse.fact_web_events
        WHERE customer_sk IS NULL
           OR product_sk IS NULL
           OR campaign_sk IS NULL
           OR event_date_sk IS NULL
           OR region_sk IS NULL
           OR channel_sk IS NULL

        UNION ALL

        SELECT conversion_nk::TEXT AS fact_key
        FROM warehouse.fact_conversions
        WHERE customer_sk IS NULL
           OR product_sk IS NULL
           OR campaign_sk IS NULL
           OR conversion_date_sk IS NULL
           OR region_sk IS NULL
           OR channel_sk IS NULL
    ) null_fk_rows;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'No null foreign keys exist in warehouse fact tables',
        'Warehouse integrity',
        'expected zero null foreign keys',
        'warehouse fact tables',
        'null_foreign_key_rows',
        'COUNT',
        0,
        target_value,
        0,
        checked_time,
        'Checks fact_orders, fact_order_items, fact_campaign_spend, fact_web_events, and fact_conversions.'
    );

    /*
    ---------------------------------------------------------------------------
    Source-provided web-event natural keys must resolve to real surrogate keys
    ---------------------------------------------------------------------------
    This check intentionally uses the same direct validation query used during
    manual debugging.

    It fails only if a source-provided customer_id/user_id, product_id, or
    campaign_id in staging.stg_gold_fact_web_events still maps to surrogate key
    0 in warehouse.fact_web_events.

    Rows where the source did not provide a natural key are allowed to remain
    on Unknown members. Source-provided keys are not.
    ---------------------------------------------------------------------------
    */

    SELECT
        (
            COUNT(*) FILTER (
                WHERE warehouse.normalized_natural_key(
                    COALESCE(src.r->>'customer_id', src.r->>'user_id')
                ) IS NOT NULL
                  AND fwe.customer_sk = 0
            )
            +
            COUNT(*) FILTER (
                WHERE warehouse.normalized_natural_key(src.r->>'product_id') IS NOT NULL
                  AND fwe.product_sk = 0
            )
            +
            COUNT(*) FILTER (
                WHERE warehouse.normalized_natural_key(src.r->>'campaign_id') IS NOT NULL
                  AND fwe.campaign_sk = 0
            )
        )::NUMERIC
    INTO target_value
    FROM (
        SELECT to_jsonb(s) AS r
        FROM staging.stg_gold_fact_web_events s
    ) src
    JOIN warehouse.fact_web_events fwe
        ON fwe.event_id = NULLIF(src.r->>'event_id', '');

    PERFORM audit.record_reconciliation_check(
        run_id,
        'No source-provided web-event natural keys resolve to unknown surrogate keys',
        'Warehouse integrity',
        'source-provided web-event customer/product/campaign natural keys',
        'warehouse.fact_web_events',
        'unresolved_web_event_source_natural_key_rows',
        'COUNT',
        0,
        target_value,
        0,
        checked_time,
        'PASS means every source-provided web-event customer/product/campaign natural key resolves to a real warehouse dimension surrogate key. Rows with no source natural key may still use Unknown members.'
    );

    /*
    ---------------------------------------------------------------------------
    Duplicate fact keys
    ---------------------------------------------------------------------------
    */

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM (
        SELECT order_id::TEXT AS duplicate_key
        FROM warehouse.fact_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1

        UNION ALL

        SELECT order_item_nk::TEXT AS duplicate_key
        FROM warehouse.fact_order_items
        GROUP BY order_item_nk
        HAVING COUNT(*) > 1

        UNION ALL

        SELECT campaign_spend_nk::TEXT AS duplicate_key
        FROM warehouse.fact_campaign_spend
        GROUP BY campaign_spend_nk
        HAVING COUNT(*) > 1

        UNION ALL

        SELECT event_id::TEXT AS duplicate_key
        FROM warehouse.fact_web_events
        GROUP BY event_id
        HAVING COUNT(*) > 1

        UNION ALL

        SELECT conversion_nk::TEXT AS duplicate_key
        FROM warehouse.fact_conversions
        GROUP BY conversion_nk
        HAVING COUNT(*) > 1
    ) duplicate_fact_keys;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'No duplicate natural fact keys exist in warehouse facts',
        'Warehouse integrity',
        'expected zero duplicate fact keys',
        'warehouse fact tables',
        'duplicate_fact_key_count',
        'COUNT',
        0,
        target_value,
        0,
        checked_time,
        'Checks order_id, order_item_nk, campaign_spend_nk, event_id, and conversion_nk.'
    );

    /*
    ---------------------------------------------------------------------------
    Mart revenue vs fact revenue
    ---------------------------------------------------------------------------
    */

    -- Sales are the orders that were not cancelled (on their order date);
    -- cancelled orders are in the mart's own column, compared in the same check.
    SELECT ROUND(COALESCE(SUM(net_revenue), 0)::NUMERIC, 2)
    INTO source_value
    FROM warehouse.fact_orders;

    SELECT ROUND(COALESCE(SUM(net_revenue + cancelled_amount), 0)::NUMERIC, 2)
    INTO target_value
    FROM marts.mart_revenue_daily;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Mart daily revenue equals warehouse fact_orders revenue',
        'Mart-to-fact amount',
        'warehouse.fact_orders',
        'marts.mart_revenue_daily',
        'net_revenue',
        'AMOUNT',
        source_value,
        target_value,
        0.01,
        checked_time,
        'Compares the net value of every order in fact_orders to mart_revenue_daily net_revenue (orders that were not cancelled) + cancelled_amount: no order is lost from the mart, whatever its status.'
    );

    SELECT ROUND((COALESCE(SUM(foi.line_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0)
                  - COALESCE(SUM(foi.refund_amount), 0))::NUMERIC, 2)
    INTO source_value
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo
        ON fo.order_sk = foi.order_sk;

    SELECT ROUND(COALESCE(SUM(product_revenue), 0)::NUMERIC, 2)
    INTO target_value
    FROM marts.mart_product_sales;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Mart product revenue equals warehouse fact_order_items revenue',
        'Mart-to-fact amount',
        'warehouse.fact_order_items',
        'marts.mart_product_sales',
        'product_revenue',
        'AMOUNT',
        source_value,
        target_value,
        0.01,
        checked_time,
        'Compares fact_order_items.line_revenue to mart_product_sales.product_revenue, for the items of orders that were not cancelled, less the item refunds. The mart has a row for the unknown and for the invalid-reference product member, so it ties out to the fact in full.'
    );

    /*
    ---------------------------------------------------------------------------
    Refunds
    ---------------------------------------------------------------------------
    A refund is an amount on a returned order. The point-in-time job splits
    it over the order's items; for every order with loaded items the item
    refunds must sum to exactly the order's refund. And the refunds the daily
    mart shows (on their return dates) are the refunds of the fact.
    */

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM (
        SELECT fo.order_sk
        FROM warehouse.fact_orders fo
        JOIN warehouse.fact_order_items foi ON foi.order_sk = fo.order_sk
        GROUP BY fo.order_sk, fo.refund_amount
        HAVING SUM(foi.refund_amount) <> fo.refund_amount
    ) mismatched;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Item refunds sum to their order refund',
        'Warehouse integrity',
        'expected zero orders whose item refunds do not sum to the order refund',
        'warehouse.fact_order_items',
        'orders_with_item_refunds_not_summing',
        'COUNT',
        0,
        target_value,
        0,
        checked_time,
        format('Checked %s returned orders with loaded items.',
               (SELECT COUNT(DISTINCT fo.order_sk) FROM warehouse.fact_orders fo
                JOIN warehouse.fact_order_items foi ON foi.order_sk = fo.order_sk WHERE fo.order_status = 'returned'))
    );

    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC INTO source_value FROM warehouse.fact_orders;
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC INTO target_value FROM marts.mart_revenue_daily;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Mart refunds equal warehouse fact_orders refunds',
        'Mart-to-fact amount',
        'warehouse.fact_orders',
        'marts.mart_revenue_daily',
        'refund_amount',
        'AMOUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Refunds are recognised on the return date. The daily mart total equals the fact total exactly; the customer, campaign and product marts are compared in the mart validation.'
    );

    /*
    ---------------------------------------------------------------------------
    Order gross minus discount equals net revenue
    ---------------------------------------------------------------------------
    gross_amount and discount_amount are sums over the order's loaded items. For
    a complete order (its loaded lines add up to its net revenue) gross minus
    discount must equal net revenue exactly. An order that lost items to Silver
    quarantine or point-in-time rejection is excluded and counted, as in the
    point-in-time order totals check.
    */

    SELECT
        COUNT(*) FILTER (
            WHERE o.line_total = o.net_revenue
              AND o.gross_amount - o.discount_amount <> o.net_revenue
        )::NUMERIC,
        COUNT(*) FILTER (WHERE o.line_total = o.net_revenue)::NUMERIC,
        COUNT(*) FILTER (WHERE o.line_total IS DISTINCT FROM o.net_revenue)::NUMERIC
    INTO target_value, source_value, order_count_excluded
    FROM (
        SELECT fo.net_revenue, fo.gross_amount, fo.discount_amount, items.line_total
        FROM warehouse.fact_orders fo
        LEFT JOIN (
            SELECT order_sk, SUM(line_revenue) AS line_total
            FROM warehouse.fact_order_items
            GROUP BY order_sk
        ) items
            ON items.order_sk = fo.order_sk
    ) o;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Order gross minus discount equals net revenue for every complete order',
        'Warehouse integrity',
        'expected zero complete orders where gross_amount - discount_amount <> net_revenue',
        'warehouse.fact_orders',
        'gross_discount_net_mismatched_orders',
        'COUNT',
        0,
        target_value,
        0,
        checked_time,
        format(
            'Checked %s complete orders; excluded %s incomplete orders (items quarantined or rejected, or none loaded).',
            source_value::BIGINT, order_count_excluded::BIGINT
        )
    );
END $$;

CREATE OR REPLACE VIEW audit.v_latest_reconciliation_report AS
WITH latest_run AS (
    SELECT reconciliation_run_id
    FROM audit.reconciliation_report
    ORDER BY checked_at DESC, reconciliation_id DESC
    LIMIT 1
)
SELECT
    reconciliation_id,
    reconciliation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    tolerance,
    status,
    details,
    checked_at
FROM audit.reconciliation_report
WHERE reconciliation_run_id = (SELECT reconciliation_run_id FROM latest_run)
ORDER BY
    CASE status WHEN 'FAIL' THEN 0 ELSE 1 END,
    check_category,
    check_name;

SELECT
    check_name,
    source_object,
    target_object,
    metric_name,
    COALESCE(source_count, source_amount) AS source_value,
    COALESCE(target_count, target_amount) AS target_value,
    COALESCE(count_difference, amount_difference) AS difference,
    status,
    checked_at
FROM audit.v_latest_reconciliation_report
ORDER BY
    CASE status WHEN 'FAIL' THEN 0 ELSE 1 END,
    check_name;

-- ---------------------------------------------------------------------------
-- Quality gate, before the commit: a build that fails a check is never
-- published. Every check of this run in the three reports must be PASS, and
-- each report must have checks for this run. Otherwise the failing checks are
-- written to the log (one NOTICE each, and one P2_FAILED_BUILD_REPORT line of
-- JSON that scripts/run_warehouse_sql.sh saves as
-- logs/warehouse/failed_build_<timestamp>.json), and the exception rolls the
-- whole transaction back: the previous warehouse stays live and untouched, and
-- the report tables of the failed build are gone with it.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    current_run TEXT := NULLIF(current_setting('p2.run_id', true), '');
    report RECORD;
    check_count BIGINT;
    failed JSONB;
    item JSONB;
    failing JSONB := '[]'::JSONB;
BEGIN
    FOR report IN SELECT * FROM (VALUES
        ('reconciliation_report', 'reconciliation_run_id'),
        ('scd2_validation_results', 'validation_run_id'),
        ('mart_validation_report', 'validation_run_id')
    ) AS reports(table_name, run_column)
    LOOP
        EXECUTE format(
            'SELECT count(*), coalesce(jsonb_agg(to_jsonb(t)) FILTER (WHERE t.status IS DISTINCT FROM ''PASS''), ''[]''::JSONB) '
            'FROM audit.%I t WHERE %I = $1', report.table_name, report.run_column)
        INTO check_count, failed USING current_run;

        IF check_count = 0 THEN
            failing := failing || jsonb_build_object(
                'report', 'audit.' || report.table_name,
                'check_name', '(no checks recorded for this run)',
                'expected', 'at least one check',
                'actual', '0 checks',
                'details', format('audit.%s has no row for run %s: its checks did not run.', report.table_name, current_run));
        END IF;

        FOR item IN SELECT value FROM jsonb_array_elements(failed) ORDER BY value->>'check_name' LOOP
            failing := failing || jsonb_build_object(
                'report', 'audit.' || report.table_name,
                'check_name', item->>'check_name',
                'status', item->>'status',
                -- What the check compares: the source side is what was expected.
                'expected', coalesce(item->>'source_count', item->>'source_amount', item->>'source_value',
                                     CASE WHEN item ? 'failed_row_count' THEN '0' END),
                'actual', coalesce(item->>'target_count', item->>'target_amount', item->>'target_value',
                                   item->>'failed_row_count'),
                'difference', coalesce(item->>'count_difference', item->>'amount_difference', item->>'difference'),
                'metric', coalesce(item->>'metric_name', CASE WHEN item ? 'failed_row_count' THEN 'failed_row_count' END),
                'expected_from', coalesce(item->>'source_object', item->>'checked_entity'),
                'actual_from', item->>'target_object',
                -- A check without its own text still says what was compared.
                'details', coalesce(item->>'details', format(
                    '%s: %s has %s, %s has %s (difference %s).',
                    coalesce(item->>'metric_name', 'failed_row_count'),
                    coalesce(item->>'source_object', item->>'checked_entity', 'expected'),
                    coalesce(item->>'source_count', item->>'source_amount', item->>'source_value', '0'),
                    coalesce(item->>'target_object', 'actual'),
                    coalesce(item->>'target_count', item->>'target_amount', item->>'target_value', item->>'failed_row_count', '?'),
                    coalesce(item->>'count_difference', item->>'amount_difference', item->>'difference', '?'))));
        END LOOP;
    END LOOP;

    IF jsonb_array_length(failing) > 0 THEN
        FOR item IN SELECT value FROM jsonb_array_elements(failing) LOOP
            RAISE NOTICE 'FAILED CHECK % | % | expected % | actual % | %',
                item->>'report', item->>'check_name', coalesce(item->>'expected', '-'),
                coalesce(item->>'actual', '-'), coalesce(item->>'details', '');
        END LOOP;
        RAISE NOTICE 'P2_FAILED_BUILD_REPORT %', jsonb_build_object(
            'run_id', current_run,
            'failed_at', to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'outcome', 'rolled back: nothing of this build was published, the previous warehouse is unchanged',
            'failing_check_count', jsonb_array_length(failing),
            'failing_checks', failing)::TEXT;
        RAISE EXCEPTION 'Quality gate failed: % failing check(s) in run %. The build is rolled back; the previous warehouse stays live.',
            jsonb_array_length(failing), current_run;
    END IF;
END $$;

COMMIT;
