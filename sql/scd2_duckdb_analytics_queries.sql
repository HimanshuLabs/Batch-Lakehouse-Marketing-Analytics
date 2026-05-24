-- Project 2 — SCD2-Aware DuckDB Analytics Query Pack
-- Purpose:
-- Business-facing analytical queries over SCD2-aware Gold marts.

-- ============================================================
-- 1. Top campaigns by ROAS
-- ============================================================

SELECT
    campaign_id,
    campaign_name,
    channel,
    target_segment,
    campaign_status,
    total_spend,
    total_revenue,
    orders_count,
    customers_count,
    roas,
    cost_per_click,
    cost_per_acquisition
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet')
WHERE total_spend > 0
ORDER BY roas DESC
LIMIT 20;


-- ============================================================
-- 2. Campaign spend vs revenue
-- ============================================================

SELECT
    campaign_id,
    campaign_name,
    channel,
    total_spend,
    total_revenue,
    total_revenue - total_spend AS net_return,
    roas,
    orders_count
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet')
ORDER BY net_return DESC;


-- ============================================================
-- 3. Highest lifetime value customers
-- ============================================================

SELECT
    customer_id,
    customer_name,
    city,
    state,
    country,
    customer_segment,
    loyalty_tier,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    customer_lifetime_days
FROM read_parquet('data/gold/mart_customer_lifetime_value_scd2/*.parquet')
ORDER BY total_revenue DESC
LIMIT 50;


-- ============================================================
-- 4. Customer value by segment and loyalty tier
-- ============================================================

SELECT
    COALESCE(customer_segment, 'unknown') AS customer_segment,
    COALESCE(loyalty_tier, 'unknown') AS loyalty_tier,
    COUNT(*) AS customer_count,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue,
    AVG(avg_order_value) AS avg_order_value,
    AVG(customer_lifetime_days) AS avg_customer_lifetime_days
FROM read_parquet('data/gold/mart_customer_lifetime_value_scd2/*.parquet')
GROUP BY
    COALESCE(customer_segment, 'unknown'),
    COALESCE(loyalty_tier, 'unknown')
ORDER BY total_revenue DESC;


-- ============================================================
-- 5. Best product performers
-- ============================================================

SELECT
    product_id,
    product_name,
    category,
    brand,
    status,
    units_sold,
    total_revenue,
    avg_selling_price,
    revenue_per_unit,
    order_count
FROM read_parquet('data/gold/mart_product_performance_scd2/*.parquet')
ORDER BY total_revenue DESC
LIMIT 50;


-- ============================================================
-- 6. Product revenue by category and brand
-- ============================================================

SELECT
    COALESCE(category, 'unknown') AS category,
    COALESCE(brand, 'unknown') AS brand,
    COUNT(*) AS product_count,
    SUM(units_sold) AS units_sold,
    SUM(total_revenue) AS total_revenue,
    AVG(avg_selling_price) AS avg_selling_price
FROM read_parquet('data/gold/mart_product_performance_scd2/*.parquet')
GROUP BY
    COALESCE(category, 'unknown'),
    COALESCE(brand, 'unknown')
ORDER BY total_revenue DESC;


-- ============================================================
-- 7. Funnel conversion by campaign/channel
-- ============================================================

SELECT
    campaign_id,
    campaign_name,
    channel,
    target_segment,
    campaign_status,
    sessions_count,
    users_count,
    page_views,
    product_views,
    add_to_cart,
    checkout,
    purchases,
    view_to_cart_rate,
    cart_to_purchase_rate,
    overall_conversion_rate
FROM read_parquet('data/gold/mart_marketing_funnel_scd2/*.parquet')
ORDER BY purchases DESC, overall_conversion_rate DESC;


-- ============================================================
-- 8. Funnel conversion by channel
-- ============================================================

SELECT
    channel,
    SUM(sessions_count) AS sessions_count,
    SUM(users_count) AS users_count,
    SUM(page_views) AS page_views,
    SUM(product_views) AS product_views,
    SUM(add_to_cart) AS add_to_cart,
    SUM(checkout) AS checkout,
    SUM(purchases) AS purchases,
    CASE
        WHEN SUM(page_views) = 0 THEN 0
        ELSE SUM(add_to_cart)::DOUBLE / SUM(page_views)
    END AS view_to_cart_rate,
    CASE
        WHEN SUM(add_to_cart) = 0 THEN 0
        ELSE SUM(purchases)::DOUBLE / SUM(add_to_cart)
    END AS cart_to_purchase_rate,
    CASE
        WHEN SUM(page_views) = 0 THEN 0
        ELSE SUM(purchases)::DOUBLE / SUM(page_views)
    END AS overall_conversion_rate
FROM read_parquet('data/gold/mart_marketing_funnel_scd2/*.parquet')
GROUP BY channel
ORDER BY purchases DESC;


-- ============================================================
-- 9. Revenue reconciliation across SCD2 marts
-- ============================================================

SELECT
    'fact_orders_scd2' AS object_name,
    SUM(total_amount) AS amount
FROM read_parquet('data/gold/fact_orders_scd2/**/*.parquet')

UNION ALL

SELECT
    'mart_campaign_performance_scd2' AS object_name,
    SUM(total_revenue) AS amount
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet')

UNION ALL

SELECT
    'mart_customer_lifetime_value_scd2' AS object_name,
    SUM(total_revenue) AS amount
FROM read_parquet('data/gold/mart_customer_lifetime_value_scd2/*.parquet');


-- ============================================================
-- 10. Product revenue reconciliation
-- ============================================================

SELECT
    'fact_order_items_scd2' AS object_name,
    SUM(line_amount) AS amount
FROM read_parquet('data/gold/fact_order_items_scd2/**/*.parquet')

UNION ALL

SELECT
    'mart_product_performance_scd2' AS object_name,
    SUM(total_revenue) AS amount
FROM read_parquet('data/gold/mart_product_performance_scd2/*.parquet');


-- ============================================================
-- 11. Campaign spend reconciliation
-- ============================================================

SELECT
    'fact_campaign_spend_scd2' AS object_name,
    SUM(spend_amount) AS amount
FROM read_parquet('data/gold/fact_campaign_spend_scd2/**/*.parquet')

UNION ALL

SELECT
    'mart_campaign_performance_scd2' AS object_name,
    SUM(total_spend) AS amount
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet');


-- ============================================================
-- 12. Quarantined SCD2 fact rejects summary
-- ============================================================

SELECT
    'fact_orders_scd2_rejects' AS reject_table,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_orders_scd2_rejects/**/*.parquet')

UNION ALL

SELECT
    'fact_order_items_scd2_rejects' AS reject_table,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_order_items_scd2_rejects/**/*.parquet')

UNION ALL

SELECT
    'fact_campaign_spend_scd2_rejects' AS reject_table,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_campaign_spend_scd2_rejects/**/*.parquet');
