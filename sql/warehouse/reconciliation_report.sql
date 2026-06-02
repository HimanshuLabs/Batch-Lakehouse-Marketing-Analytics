-- Warehouse reconciliation checks.
-- Purpose: prove warehouse totals match trusted Gold/SCD2 staging outputs.

CREATE TABLE IF NOT EXISTS audit.reconciliation_report (
    check_name TEXT PRIMARY KEY,
    source_count NUMERIC(18,2),
    target_count NUMERIC(18,2),
    count_difference NUMERIC(18,2),
    source_amount NUMERIC(18,2),
    target_amount NUMERIC(18,2),
    amount_difference NUMERIC(18,2),
    status TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

TRUNCATE TABLE audit.reconciliation_report;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'order_count_staging_vs_warehouse',
    src.source_count,
    tgt.target_count,
    src.source_count - tgt.target_count,
    NULL,
    NULL,
    NULL,
    CASE WHEN src.source_count = tgt.target_count THEN 'PASS' ELSE 'FAIL' END
FROM
    (SELECT COUNT(*)::NUMERIC(18,2) AS source_count FROM staging.stg_gold_fact_orders_scd2) src,
    (SELECT COUNT(*)::NUMERIC(18,2) AS target_count FROM warehouse.fact_orders) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'revenue_staging_vs_warehouse',
    NULL,
    NULL,
    NULL,
    src.source_amount,
    tgt.target_amount,
    src.source_amount - tgt.target_amount,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END
FROM
    (
        SELECT ROUND(COALESCE(SUM(total_amount), 0)::NUMERIC, 2)::NUMERIC(18,2) AS source_amount
        FROM staging.stg_gold_fact_orders_scd2
    ) src,
    (
        SELECT ROUND(COALESCE(SUM(net_revenue), 0)::NUMERIC, 2)::NUMERIC(18,2) AS target_amount
        FROM warehouse.fact_orders
    ) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'campaign_spend_count_staging_vs_warehouse',
    src.source_count,
    tgt.target_count,
    src.source_count - tgt.target_count,
    NULL,
    NULL,
    NULL,
    CASE WHEN src.source_count = tgt.target_count THEN 'PASS' ELSE 'FAIL' END
FROM
    (SELECT COUNT(*)::NUMERIC(18,2) AS source_count FROM staging.stg_gold_fact_campaign_spend_scd2) src,
    (SELECT COUNT(*)::NUMERIC(18,2) AS target_count FROM warehouse.fact_campaign_spend) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'campaign_spend_amount_staging_vs_warehouse',
    NULL,
    NULL,
    NULL,
    src.source_amount,
    tgt.target_amount,
    src.source_amount - tgt.target_amount,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END
FROM
    (
        SELECT ROUND(COALESCE(SUM(spend_amount), 0)::NUMERIC, 2)::NUMERIC(18,2) AS source_amount
        FROM staging.stg_gold_fact_campaign_spend_scd2
    ) src,
    (
        SELECT ROUND(COALESCE(SUM(spend_amount), 0)::NUMERIC, 2)::NUMERIC(18,2) AS target_amount
        FROM warehouse.fact_campaign_spend
    ) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'order_item_count_staging_vs_warehouse',
    src.source_count,
    tgt.target_count,
    src.source_count - tgt.target_count,
    NULL,
    NULL,
    NULL,
    CASE WHEN src.source_count = tgt.target_count THEN 'PASS' ELSE 'FAIL' END
FROM
    (SELECT COUNT(*)::NUMERIC(18,2) AS source_count FROM staging.stg_gold_fact_order_items_scd2) src,
    (SELECT COUNT(*)::NUMERIC(18,2) AS target_count FROM warehouse.fact_order_items) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'order_item_revenue_staging_vs_warehouse',
    NULL,
    NULL,
    NULL,
    src.source_amount,
    tgt.target_amount,
    src.source_amount - tgt.target_amount,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END
FROM
    (
        SELECT ROUND(COALESCE(SUM(line_amount), 0)::NUMERIC, 2)::NUMERIC(18,2) AS source_amount
        FROM staging.stg_gold_fact_order_items_scd2
    ) src,
    (
        SELECT ROUND(COALESCE(SUM(line_revenue), 0)::NUMERIC, 2)::NUMERIC(18,2) AS target_amount
        FROM warehouse.fact_order_items
    ) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'web_event_count_staging_vs_warehouse',
    src.source_count,
    tgt.target_count,
    src.source_count - tgt.target_count,
    NULL,
    NULL,
    NULL,
    CASE WHEN src.source_count = tgt.target_count THEN 'PASS' ELSE 'FAIL' END
FROM
    (SELECT COUNT(*)::NUMERIC(18,2) AS source_count FROM staging.stg_gold_fact_web_events) src,
    (SELECT COUNT(*)::NUMERIC(18,2) AS target_count FROM warehouse.fact_web_events) tgt;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'null_customer_keys_in_fact_orders',
    0,
    COUNT(*)::NUMERIC(18,2),
    0 - COUNT(*)::NUMERIC(18,2),
    NULL,
    NULL,
    NULL,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.fact_orders
WHERE customer_sk IS NULL;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'null_product_keys_in_fact_order_items',
    0,
    COUNT(*)::NUMERIC(18,2),
    0 - COUNT(*)::NUMERIC(18,2),
    NULL,
    NULL,
    NULL,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.fact_order_items
WHERE product_sk IS NULL;

INSERT INTO audit.reconciliation_report (
    check_name,
    source_count,
    target_count,
    count_difference,
    source_amount,
    target_amount,
    amount_difference,
    status
)
SELECT
    'duplicate_order_ids_in_warehouse',
    0,
    COUNT(*)::NUMERIC(18,2),
    0 - COUNT(*)::NUMERIC(18,2),
    NULL,
    NULL,
    NULL,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT order_id
    FROM warehouse.fact_orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
) duplicate_orders;

SELECT *
FROM audit.reconciliation_report
ORDER BY status, check_name;
