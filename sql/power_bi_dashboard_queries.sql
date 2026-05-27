-- Power BI Dashboard Query Pack
-- Source: SCD2-aware Gold marts and facts.

-- 1. Executive KPI Summary
SELECT
    SUM(total_revenue) AS total_revenue,
    SUM(total_spend) AS total_spend,
    CASE WHEN SUM(total_spend) = 0 THEN 0 ELSE SUM(total_revenue) / SUM(total_spend) END AS roas,
    SUM(orders_count) AS total_orders,
    SUM(customers_count) AS total_customers
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet');

-- 2. Campaign Performance
SELECT
    campaign_id,
    campaign_name,
    channel,
    target_segment,
    campaign_status,
    total_spend,
    total_revenue,
    roas,
    cost_per_click,
    cost_per_acquisition,
    orders_count,
    customers_count
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet')
ORDER BY roas DESC;

-- 3. Customer Value
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
    customer_lifetime_days
FROM read_parquet('data/gold/mart_customer_lifetime_value_scd2/*.parquet')
ORDER BY total_revenue DESC;

-- 4. Product Performance
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
ORDER BY total_revenue DESC;

-- 5. Funnel Analytics
SELECT
    campaign_id,
    campaign_name,
    channel,
    target_segment,
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
ORDER BY purchases DESC;

-- 6. Data Quality Summary
SELECT
    'orders_rejects' AS reject_type,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_orders_scd2_rejects/**/*.parquet')

UNION ALL

SELECT
    'order_items_rejects' AS reject_type,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_order_items_scd2_rejects/**/*.parquet')

UNION ALL

SELECT
    'campaign_spend_rejects' AS reject_type,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_campaign_spend_scd2_rejects/**/*.parquet');
