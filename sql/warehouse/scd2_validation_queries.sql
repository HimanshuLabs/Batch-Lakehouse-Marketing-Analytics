-- SCD2 warehouse validation checks.
-- These checks protect point-in-time correctness for customer, product, and campaign history.

CREATE TABLE IF NOT EXISTS audit.scd2_validation_results (
    check_name TEXT PRIMARY KEY,
    issue_count BIGINT,
    status TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

TRUNCATE TABLE audit.scd2_validation_results;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'customer_one_current_row_per_natural_key',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT customer_id
    FROM warehouse.dim_customer
    WHERE is_current = TRUE
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) issues;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'product_one_current_row_per_natural_key',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT product_id
    FROM warehouse.dim_product
    WHERE is_current = TRUE
    GROUP BY product_id
    HAVING COUNT(*) > 1
) issues;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'campaign_one_current_row_per_natural_key',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT campaign_id
    FROM warehouse.dim_campaign
    WHERE is_current = TRUE
    GROUP BY campaign_id
    HAVING COUNT(*) > 1
) issues;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'customer_invalid_effective_date_ranges',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.dim_customer
WHERE effective_end_date IS NOT NULL
  AND effective_end_date <= effective_start_date;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'product_invalid_effective_date_ranges',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.dim_product
WHERE effective_end_date IS NOT NULL
  AND effective_end_date <= effective_start_date;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'campaign_invalid_effective_date_ranges',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.dim_campaign
WHERE effective_end_date IS NOT NULL
  AND effective_end_date <= effective_start_date;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'fact_orders_missing_customer_dimension',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.fact_orders fo
LEFT JOIN warehouse.dim_customer dc
    ON fo.customer_sk = dc.customer_sk
WHERE fo.customer_sk IS NOT NULL
  AND dc.customer_sk IS NULL;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'fact_order_items_missing_product_dimension',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.fact_order_items foi
LEFT JOIN warehouse.dim_product dp
    ON foi.product_sk = dp.product_sk
WHERE foi.product_sk IS NOT NULL
  AND dp.product_sk IS NULL;

INSERT INTO audit.scd2_validation_results (check_name, issue_count, status)
SELECT
    'fact_campaign_spend_missing_campaign_dimension',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM warehouse.fact_campaign_spend fcs
LEFT JOIN warehouse.dim_campaign dc
    ON fcs.campaign_sk = dc.campaign_sk
WHERE fcs.campaign_sk IS NOT NULL
  AND dc.campaign_sk IS NULL;

SELECT *
FROM audit.scd2_validation_results
ORDER BY status, check_name;
