-- Project 2 — SCD2 Gold Mart Validation Queries
-- These queries validate marts rebuilt from SCD2-aware Gold facts.

-- 1. Campaign mart revenue reconciles to fact_orders_scd2

SELECT
    'fact_orders_scd2' AS source_name,
    SUM(total_amount) AS total_revenue
FROM fact_orders_scd2

UNION ALL

SELECT
    'mart_campaign_performance_scd2' AS source_name,
    SUM(total_revenue) AS total_revenue
FROM mart_campaign_performance_scd2;


-- 2. CLV mart revenue reconciles to fact_orders_scd2

SELECT
    'fact_orders_scd2' AS source_name,
    SUM(total_amount) AS total_revenue
FROM fact_orders_scd2

UNION ALL

SELECT
    'mart_customer_lifetime_value_scd2' AS source_name,
    SUM(total_revenue) AS total_revenue
FROM mart_customer_lifetime_value_scd2;


-- 3. Product mart revenue reconciles to fact_order_items_scd2

SELECT
    'fact_order_items_scd2' AS source_name,
    SUM(line_amount) AS total_revenue
FROM fact_order_items_scd2

UNION ALL

SELECT
    'mart_product_performance_scd2' AS source_name,
    SUM(total_revenue) AS total_revenue
FROM mart_product_performance_scd2;


-- 4. Campaign spend reconciles to fact_campaign_spend_scd2

SELECT
    'fact_campaign_spend_scd2' AS source_name,
    SUM(spend_amount) AS total_spend
FROM fact_campaign_spend_scd2

UNION ALL

SELECT
    'mart_campaign_performance_scd2' AS source_name,
    SUM(total_spend) AS total_spend
FROM mart_campaign_performance_scd2;


-- 5. Best campaigns by ROAS

SELECT
    campaign_id,
    campaign_name,
    channel,
    total_spend,
    total_revenue,
    orders_count,
    roas,
    cost_per_acquisition
FROM mart_campaign_performance_scd2
WHERE total_spend > 0
ORDER BY roas DESC;


-- 6. Highest lifetime value customers

SELECT
    customer_id,
    customer_name,
    customer_segment,
    loyalty_tier,
    city,
    state,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date
FROM mart_customer_lifetime_value_scd2
ORDER BY total_revenue DESC;


-- 7. Best product performers

SELECT
    product_id,
    product_name,
    category,
    brand,
    units_sold,
    total_revenue,
    avg_selling_price,
    order_count
FROM mart_product_performance_scd2
ORDER BY total_revenue DESC;


-- 8. Funnel conversion by campaign/channel

SELECT
    campaign_id,
    campaign_name,
    channel,
    sessions_count,
    page_views,
    product_views,
    add_to_cart,
    checkout,
    purchases,
    view_to_cart_rate,
    cart_to_purchase_rate,
    overall_conversion_rate
FROM mart_marketing_funnel_scd2
ORDER BY purchases DESC;
