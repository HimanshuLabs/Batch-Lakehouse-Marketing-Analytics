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
    run_id TEXT := to_char(clock_timestamp(), 'YYYYMMDDHH24MISSUS') || '-' || substr(md5(random()::TEXT), 1, 8);
    checked_time TIMESTAMPTZ := CURRENT_TIMESTAMP;

    source_table TEXT;
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
                            warehouse.safe_numeric(to_jsonb(src)->>'total_amount'),
                            warehouse.safe_numeric(to_jsonb(src)->>'total_revenue'),
                            warehouse.safe_numeric(to_jsonb(src)->>'order_total'),
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
    WHERE customer_sk <> 0
      AND customer_id <> 'UNKNOWN';

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
        'Distinct customer_id on both sides, excluding the unknown member customer_sk = 0. No member is invented: a web event whose customer is not in the dimension is rejected into audit.fact_web_events_rejects.'
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
    WHERE product_sk <> 0
      AND product_id <> 'UNKNOWN';

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
        'Distinct product_id on both sides, excluding the unknown member product_sk = 0. No member is invented: a web event whose product is not in the dimension is rejected into audit.fact_web_events_rejects.'
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
    WHERE campaign_sk <> 0
      AND campaign_id <> 'UNKNOWN';

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
        'Distinct campaign_id on both sides, excluding the unknown member campaign_sk = 0. No member is invented: a web event whose campaign is not in the dimension is rejected into audit.fact_web_events_rejects.'
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

    -- Every staging web event is a warehouse fact or a reject, never dropped.
    SELECT
        (SELECT COUNT(*) FROM warehouse.fact_web_events)
        + (SELECT COUNT(*) FROM audit.fact_web_events_rejects)
    INTO target_value;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Staging web event row count equals warehouse fact_web_events plus rejected web events',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing web event staging table'),
        'warehouse.fact_web_events + audit.fact_web_events_rejects',
        'web_event_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        format(
            'fact_web_events %s + rejected %s (a source customer/product/campaign key with no covering dimension version).',
            (SELECT COUNT(*) FROM warehouse.fact_web_events),
            (SELECT COUNT(*) FROM audit.fact_web_events_rejects)
        )
    );

    /*
    ---------------------------------------------------------------------------
    Web-event rejects, recomputed from staging and the dimensions
    ---------------------------------------------------------------------------
    A staging web event must be rejected exactly when one of its source keys
    (customer, product, campaign) is present and no version of that dimension
    covers the event time. A NULL key is the unknown member, not a reject.
    */

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format($sql$
            SELECT COUNT(*)::NUMERIC
            FROM (
                SELECT
                    warehouse.safe_timestamp(coalesce(r->>'event_timestamp', r->>'event_time')) AS event_ts,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', '')) AS customer_id,
                    nullif(r->>'product_id', '') AS product_id,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', '')) AS campaign_id
                FROM (SELECT to_jsonb(s) AS r FROM %s s) src
            ) e
            WHERE (e.customer_id IS NOT NULL AND NOT EXISTS (
                      SELECT 1 FROM warehouse.dim_customer d
                      WHERE d.customer_id = e.customer_id
                        AND e.event_ts >= d.effective_from AND e.event_ts < d.effective_to))
               OR (e.product_id IS NOT NULL AND NOT EXISTS (
                      SELECT 1 FROM warehouse.dim_product d
                      WHERE d.product_id = e.product_id
                        AND e.event_ts >= d.effective_from AND e.event_ts < d.effective_to))
               OR (e.campaign_id IS NOT NULL AND NOT EXISTS (
                      SELECT 1 FROM warehouse.dim_campaign d
                      WHERE d.campaign_id = e.campaign_id
                        AND e.event_ts >= d.effective_from AND e.event_ts < d.effective_to))
        $sql$, source_table)
        INTO source_value;
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM audit.fact_web_events_rejects;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Rejected web events equal staging events with a source key missing from its dimension',
        'Warehouse integrity',
        COALESCE(source_table, 'missing web event staging table') || ' with warehouse dimensions',
        'audit.fact_web_events_rejects',
        'rejected_web_event_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'A NULL key resolves to the unknown member (0); a present key with no covering dimension version is rejected, never given an invented member.'
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

    SELECT ROUND(COALESCE(SUM(net_revenue), 0)::NUMERIC, 2)
    INTO source_value
    FROM warehouse.fact_orders;

    SELECT ROUND(COALESCE(SUM(net_revenue), 0)::NUMERIC, 2)
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
        'Compares fact_orders.net_revenue to mart_revenue_daily.net_revenue.'
    );

    SELECT ROUND(COALESCE(SUM(foi.line_revenue), 0)::NUMERIC, 2)
    INTO source_value
    FROM warehouse.fact_order_items foi
    JOIN warehouse.dim_product dp
        ON foi.product_sk = dp.product_sk
    WHERE dp.product_id <> 'UNKNOWN';

    SELECT ROUND(COALESCE(SUM(product_revenue), 0)::NUMERIC, 2)
    INTO target_value
    FROM marts.mart_product_sales;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Mart product revenue equals BI-visible warehouse fact_order_items revenue',
        'Mart-to-fact amount',
        'warehouse.fact_order_items',
        'marts.mart_product_sales',
        'product_revenue',
        'AMOUNT',
        source_value,
        target_value,
        0.01,
        checked_time,
        'Compares BI-visible fact_order_items.line_revenue to mart_product_sales.product_revenue, excluding UNKNOWN product rows because the product mart is business-facing.'
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

COMMIT;

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
