-- Dashboard Query Pack
-- BI tools and interview demos should read from marts and warehouse schemas only.

-- Daily revenue trend
SELECT
    calendar_date,
    orders,
    gross_revenue,
    discount_amount,
    net_revenue,
    average_order_value
FROM marts.mart_revenue_daily
ORDER BY calendar_date;

-- Revenue by region
SELECT
    dr.country,
    dr.state,
    dr.city,
    SUM(fo.net_revenue) AS net_revenue,
    COUNT(DISTINCT fo.order_id) AS orders
FROM warehouse.fact_orders fo
LEFT JOIN warehouse.dim_region dr
    ON fo.region_sk = dr.region_sk
GROUP BY
    dr.country,
    dr.state,
    dr.city
ORDER BY net_revenue DESC;

-- Top customers by lifetime value
SELECT
    customer_id,
    customer_name,
    membership_tier,
    customer_segment,
    total_orders,
    lifetime_revenue,
    average_order_value
FROM marts.mart_customer_360
ORDER BY lifetime_revenue DESC
LIMIT 25;

-- Campaign ROAS
SELECT
    campaign_id,
    campaign_name,
    channel,
    traffic_source,
    total_spend,
    attributed_revenue,
    roas
FROM marts.mart_campaign_performance
ORDER BY roas DESC NULLS LAST;

-- Product category performance
SELECT
    category,
    SUM(units_sold) AS units_sold,
    SUM(product_revenue) AS product_revenue
FROM marts.mart_product_sales
GROUP BY category
ORDER BY product_revenue DESC;

-- Customer segment performance
SELECT
    customer_segment,
    COUNT(*) AS customers,
    SUM(total_orders) AS total_orders,
    SUM(lifetime_revenue) AS lifetime_revenue,
    AVG(average_order_value) AS average_order_value
FROM marts.mart_customer_360
GROUP BY customer_segment
ORDER BY lifetime_revenue DESC;

-- Marketing funnel conversion
SELECT
    channel,
    user_journey_stage,
    event_type,
    event_count,
    sessions,
    customers,
    avg_engagement_score,
    avg_purchase_probability
FROM marts.mart_marketing_funnel
ORDER BY
    channel,
    user_journey_stage,
    event_type;

-- Repeat purchase rate
SELECT
    COUNT(*) FILTER (WHERE total_orders > 1)::NUMERIC
        / NULLIF(COUNT(*), 0) AS repeat_purchase_rate
FROM marts.mart_customer_360;

-- Average order value
SELECT
    AVG(net_revenue) AS average_order_value
FROM warehouse.fact_orders;

-- Revenue reconciliation status
SELECT
    check_name,
    source_amount,
    target_amount,
    amount_difference,
    status,
    checked_at
FROM audit.reconciliation_report
WHERE check_name = 'revenue_staging_vs_warehouse';
