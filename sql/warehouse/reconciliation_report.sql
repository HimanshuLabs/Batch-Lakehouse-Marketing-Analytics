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
    (SELECT COUNT(*) FROM staging.gold_fact_orders_scd2),
    (SELECT COUNT(*) FROM warehouse.fact_orders),
    (SELECT COUNT(*) FROM staging.gold_fact_orders_scd2) - (SELECT COUNT(*) FROM warehouse.fact_orders),
    NULL,
    NULL,
    NULL,
    CASE
        WHEN (SELECT COUNT(*) FROM staging.gold_fact_orders_scd2) = (SELECT COUNT(*) FROM warehouse.fact_orders)
        THEN 'PASS'
        ELSE 'FAIL'
    END;

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
    COALESCE((SELECT SUM(net_revenue) FROM staging.gold_fact_orders_scd2), 0),
    COALESCE((SELECT SUM(net_revenue) FROM warehouse.fact_orders), 0),
    COALESCE((SELECT SUM(net_revenue) FROM staging.gold_fact_orders_scd2), 0)
      - COALESCE((SELECT SUM(net_revenue) FROM warehouse.fact_orders), 0),
    CASE
        WHEN COALESCE((SELECT SUM(net_revenue) FROM staging.gold_fact_orders_scd2), 0)
           = COALESCE((SELECT SUM(net_revenue) FROM warehouse.fact_orders), 0)
        THEN 'PASS'
        ELSE 'FAIL'
    END;

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
    'campaign_spend_staging_vs_warehouse',
    NULL,
    NULL,
    NULL,
    COALESCE((SELECT SUM(spend_amount) FROM staging.gold_fact_campaign_spend_scd2), 0),
    COALESCE((SELECT SUM(spend_amount) FROM warehouse.fact_campaign_spend), 0),
    COALESCE((SELECT SUM(spend_amount) FROM staging.gold_fact_campaign_spend_scd2), 0)
      - COALESCE((SELECT SUM(spend_amount) FROM warehouse.fact_campaign_spend), 0),
    CASE
        WHEN COALESCE((SELECT SUM(spend_amount) FROM staging.gold_fact_campaign_spend_scd2), 0)
           = COALESCE((SELECT SUM(spend_amount) FROM warehouse.fact_campaign_spend), 0)
        THEN 'PASS'
        ELSE 'FAIL'
    END;

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
    COUNT(*),
    0 - COUNT(*),
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
    'duplicate_order_ids_in_warehouse',
    0,
    COUNT(*),
    0 - COUNT(*),
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
