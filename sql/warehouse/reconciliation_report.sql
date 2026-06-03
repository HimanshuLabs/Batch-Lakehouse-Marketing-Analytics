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
        'warehouse.fact_order_items',
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

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    IF to_regclass('staging.stg_gold_fact_web_events') IS NOT NULL THEN
        SELECT COALESCE(source_value, 0) + COUNT(*)::NUMERIC
        INTO source_value
        FROM (
            SELECT DISTINCT
                warehouse.normalized_natural_key(
                    COALESCE(to_jsonb(src)->>'customer_id', to_jsonb(src)->>'user_id')
                ) AS customer_id_norm
            FROM staging.stg_gold_fact_web_events src
        ) web_keys
        WHERE customer_id_norm IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM warehouse.dim_customer dc
              WHERE dc.scd_hash NOT LIKE 'INFERRED_WEB_EVENT_CUSTOMER|%%'
                AND dc.customer_sk <> 0
                AND warehouse.normalized_natural_key(dc.customer_id) = web_keys.customer_id_norm
          );
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.dim_customer
    WHERE customer_sk <> 0;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold plus inferred customer dimension row count equals warehouse dim_customer row count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing customer staging table') || ' + inferred web-event customer keys',
        'warehouse.dim_customer',
        'customer_dimension_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Excludes warehouse unknown customer member customer_sk = 0 and includes inferred late-arriving web-event customer keys.'
    );

    source_table := audit.first_existing_table(
        'staging.stg_gold_dim_products_scd2',
        'staging.stg_gold_dim_products'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    IF to_regclass('staging.stg_gold_fact_web_events') IS NOT NULL THEN
        SELECT COALESCE(source_value, 0) + COUNT(*)::NUMERIC
        INTO source_value
        FROM (
            SELECT DISTINCT
                warehouse.normalized_natural_key(to_jsonb(src)->>'product_id') AS product_id_norm
            FROM staging.stg_gold_fact_web_events src
        ) web_keys
        WHERE product_id_norm IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM warehouse.dim_product dp
              WHERE dp.scd_hash NOT LIKE 'INFERRED_WEB_EVENT_PRODUCT|%%'
                AND dp.product_sk <> 0
                AND warehouse.normalized_natural_key(dp.product_id) = web_keys.product_id_norm
          );
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.dim_product
    WHERE product_sk <> 0;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold plus inferred product dimension row count equals warehouse dim_product row count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing product staging table') || ' + inferred web-event product keys',
        'warehouse.dim_product',
        'product_dimension_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Excludes warehouse unknown product member product_sk = 0 and includes inferred late-arriving web-event product keys.'
    );

    source_table := audit.first_existing_table(
        'staging.stg_gold_dim_campaigns_scd2',
        'staging.stg_gold_dim_campaigns'
    );

    source_value := NULL;

    IF source_table IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*)::NUMERIC FROM %s', source_table)
        INTO source_value;
    END IF;

    IF to_regclass('staging.stg_gold_fact_web_events') IS NOT NULL THEN
        SELECT COALESCE(source_value, 0) + COUNT(*)::NUMERIC
        INTO source_value
        FROM (
            SELECT DISTINCT
                warehouse.normalized_natural_key(to_jsonb(src)->>'campaign_id') AS campaign_id_norm
            FROM staging.stg_gold_fact_web_events src
        ) web_keys
        WHERE campaign_id_norm IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM warehouse.dim_campaign dc
              WHERE dc.scd_hash NOT LIKE 'INFERRED_WEB_EVENT_CAMPAIGN|%%'
                AND dc.campaign_sk <> 0
                AND warehouse.normalized_natural_key(dc.campaign_id) = web_keys.campaign_id_norm
          );
    END IF;

    SELECT COUNT(*)::NUMERIC
    INTO target_value
    FROM warehouse.dim_campaign
    WHERE campaign_sk <> 0;

    PERFORM audit.record_reconciliation_check(
        run_id,
        'Gold plus inferred campaign dimension row count equals warehouse dim_campaign row count',
        'Staging-to-warehouse row count',
        COALESCE(source_table, 'missing campaign staging table') || ' + inferred web-event campaign keys',
        'warehouse.dim_campaign',
        'campaign_dimension_rows',
        'COUNT',
        source_value,
        target_value,
        0,
        checked_time,
        'Excludes warehouse unknown campaign member campaign_sk = 0 and includes inferred late-arriving web-event campaign keys.'
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
        NULL
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

    SELECT ROUND(COALESCE(SUM(line_revenue), 0)::NUMERIC, 2)
    INTO source_value
    FROM warehouse.fact_order_items;

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
        'Compares fact_order_items.line_revenue to mart_product_sales.product_revenue.'
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
