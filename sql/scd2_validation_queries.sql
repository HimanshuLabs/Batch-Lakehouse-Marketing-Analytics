-- Project 2 — SCD Type 2 Validation Queries
-- These queries are written for PostgreSQL/DuckDB-style SQL adaptation.
-- They document the validation logic used by spark-batch/scd2_dimensions.py.

-- ============================================================
-- 1. One current customer row per natural key
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS current_record_count
FROM dim_customers_scd2
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 2. One current product row per natural key
-- ============================================================

SELECT
    product_id,
    COUNT(*) AS current_record_count
FROM dim_products_scd2
WHERE is_current = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 3. One current campaign row per natural key
-- ============================================================

SELECT
    campaign_id,
    COUNT(*) AS current_record_count
FROM dim_campaigns_scd2
WHERE is_current = TRUE
GROUP BY campaign_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 4. Invalid customer date ranges
-- ============================================================

SELECT *
FROM dim_customers_scd2
WHERE effective_to <= effective_from;


-- ============================================================
-- 5. Invalid product date ranges
-- ============================================================

SELECT *
FROM dim_products_scd2
WHERE effective_to <= effective_from;


-- ============================================================
-- 6. Invalid campaign date ranges
-- ============================================================

SELECT *
FROM dim_campaigns_scd2
WHERE effective_to <= effective_from;


-- ============================================================
-- 7. Customer history depth
-- Shows which customers have more than one historical version.
-- ============================================================

SELECT
    customer_id,
    COUNT(*) AS version_count,
    MIN(effective_from) AS first_seen_at,
    MAX(effective_from) AS latest_seen_at
FROM dim_customers_scd2
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC, customer_id;


-- ============================================================
-- 8. Product history depth
-- ============================================================

SELECT
    product_id,
    COUNT(*) AS version_count,
    MIN(effective_from) AS first_seen_at,
    MAX(effective_from) AS latest_seen_at
FROM dim_products_scd2
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC, product_id;


-- ============================================================
-- 9. Campaign history depth
-- ============================================================

SELECT
    campaign_id,
    COUNT(*) AS version_count,
    MIN(effective_from) AS first_seen_at,
    MAX(effective_from) AS latest_seen_at
FROM dim_campaigns_scd2
GROUP BY campaign_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC, campaign_id;
