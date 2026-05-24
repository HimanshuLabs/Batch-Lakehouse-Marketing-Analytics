-- Project 2 — Point-in-Time Fact Join Validation Queries
-- Purpose:
-- Validate that fact tables are joined to the correct SCD2 surrogate keys.

-- ============================================================
-- 1. fact_orders should have valid customer_sk
-- ============================================================

SELECT
    COUNT(*) AS missing_customer_sk_count
FROM fact_orders_scd2
WHERE customer_sk IS NULL;


-- ============================================================
-- 2. fact_orders should have valid campaign_sk
-- ============================================================

SELECT
    COUNT(*) AS missing_campaign_sk_count
FROM fact_orders_scd2
WHERE campaign_sk IS NULL;


-- ============================================================
-- 3. fact_order_items should have valid product_sk
-- ============================================================

SELECT
    COUNT(*) AS missing_product_sk_count
FROM fact_order_items_scd2
WHERE product_sk IS NULL;


-- ============================================================
-- 4. fact_campaign_spend should have valid campaign_sk
-- ============================================================

SELECT
    COUNT(*) AS missing_campaign_sk_count
FROM fact_campaign_spend_scd2
WHERE campaign_sk IS NULL;


-- ============================================================
-- 5. No duplicate order records
-- ============================================================

SELECT
    order_id,
    COUNT(*) AS row_count
FROM fact_orders_scd2
GROUP BY order_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 6. No duplicate order item records
-- ============================================================

SELECT
    order_item_id,
    COUNT(*) AS row_count
FROM fact_order_items_scd2
GROUP BY order_item_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 7. No duplicate campaign spend records
-- ============================================================

SELECT
    spend_id,
    COUNT(*) AS row_count
FROM fact_campaign_spend_scd2
GROUP BY spend_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 8. Point-in-time customer join audit
-- ============================================================

SELECT
    f.order_id,
    f.customer_id,
    f.customer_sk,
    f.order_date,
    d.effective_from,
    d.effective_to,
    d.is_current
FROM fact_orders_scd2 f
JOIN dim_customers_scd2 d
    ON f.customer_sk = d.customer_sk
WHERE NOT (
    f.order_date >= d.effective_from
    AND f.order_date < d.effective_to
);


-- ============================================================
-- 9. Point-in-time product join audit
-- ============================================================

SELECT
    f.order_item_id,
    f.product_id,
    f.product_sk,
    f.order_date,
    d.effective_from,
    d.effective_to,
    d.is_current
FROM fact_order_items_scd2 f
JOIN dim_products_scd2 d
    ON f.product_sk = d.product_sk
WHERE NOT (
    f.order_date >= d.effective_from
    AND f.order_date < d.effective_to
);


-- ============================================================
-- 10. Point-in-time campaign join audit for orders
-- ============================================================

SELECT
    f.order_id,
    f.campaign_id,
    f.campaign_sk,
    f.order_date,
    d.effective_from,
    d.effective_to,
    d.is_current
FROM fact_orders_scd2 f
JOIN dim_campaigns_scd2 d
    ON f.campaign_sk = d.campaign_sk
WHERE NOT (
    f.order_date >= d.effective_from
    AND f.order_date < d.effective_to
);


-- ============================================================
-- 11. Point-in-time campaign join audit for spend
-- ============================================================

SELECT
    f.spend_id,
    f.campaign_id,
    f.campaign_sk,
    f.spend_date,
    d.effective_from,
    d.effective_to,
    d.is_current
FROM fact_campaign_spend_scd2 f
JOIN dim_campaigns_scd2 d
    ON f.campaign_sk = d.campaign_sk
WHERE NOT (
    f.spend_date >= d.effective_from
    AND f.spend_date < d.effective_to
);
