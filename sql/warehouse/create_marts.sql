-- BI-ready warehouse marts.
-- Views are used so the reporting layer remains easy to validate and iterate.

CREATE OR REPLACE VIEW marts.mart_customer_360 AS
SELECT
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.membership_tier,
    dc.customer_segment,
    dc.home_city,
    dc.home_state,
    dc.country,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COALESCE(SUM(fo.net_revenue), 0) AS lifetime_revenue,
    COALESCE(AVG(fo.net_revenue), 0) AS average_order_value,
    MAX(fo.order_timestamp) AS last_order_timestamp
FROM warehouse.dim_customer dc
LEFT JOIN warehouse.fact_orders fo
    ON dc.customer_sk = fo.customer_sk
WHERE dc.is_current = TRUE
GROUP BY
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.membership_tier,
    dc.customer_segment,
    dc.home_city,
    dc.home_state,
    dc.country;

CREATE OR REPLACE VIEW marts.mart_campaign_performance AS
WITH campaign_spend AS (
    SELECT
        campaign_sk,
        SUM(spend_amount) AS total_spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(attributed_revenue) AS spend_attributed_revenue
    FROM warehouse.fact_campaign_spend
    GROUP BY campaign_sk
),
campaign_orders AS (
    SELECT
        campaign_sk,
        COUNT(DISTINCT order_id) AS orders,
        SUM(net_revenue) AS attributed_revenue
    FROM warehouse.fact_orders
    GROUP BY campaign_sk
)
SELECT
    dc.campaign_id,
    dc.campaign_name,
    dc.channel,
    dc.traffic_source,
    COALESCE(cs.total_spend, 0) AS total_spend,
    COALESCE(cs.impressions, 0) AS impressions,
    COALESCE(cs.clicks, 0) AS clicks,
    COALESCE(cs.conversions, 0) AS conversions,
    COALESCE(co.orders, 0) AS orders,
    COALESCE(co.attributed_revenue, 0) AS attributed_revenue,
    CASE
        WHEN COALESCE(cs.total_spend, 0) = 0 THEN NULL
        ELSE ROUND(COALESCE(co.attributed_revenue, 0) / NULLIF(cs.total_spend, 0), 4)
    END AS roas
FROM warehouse.dim_campaign dc
LEFT JOIN campaign_spend cs
    ON dc.campaign_sk = cs.campaign_sk
LEFT JOIN campaign_orders co
    ON dc.campaign_sk = co.campaign_sk
WHERE dc.is_current = TRUE;

CREATE OR REPLACE VIEW marts.mart_product_sales AS
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    COALESCE(SUM(foi.quantity), 0) AS units_sold,
    COALESCE(SUM(foi.line_revenue), 0) AS product_revenue,
    COALESCE(AVG(foi.unit_price), 0) AS average_selling_price
FROM warehouse.dim_product dp
LEFT JOIN warehouse.fact_order_items foi
    ON dp.product_sk = foi.product_sk
WHERE dp.is_current = TRUE
GROUP BY
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand;

CREATE OR REPLACE VIEW marts.mart_revenue_daily AS
SELECT
    dd.full_date,
    dd.year_number,
    dd.quarter_number,
    dd.month_number,
    COUNT(DISTINCT fo.order_id) AS orders,
    COALESCE(SUM(fo.gross_amount), 0) AS gross_revenue,
    COALESCE(SUM(fo.discount_amount), 0) AS discount_amount,
    COALESCE(SUM(fo.net_revenue), 0) AS net_revenue,
    COALESCE(AVG(fo.net_revenue), 0) AS average_order_value
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd
    ON fo.order_date_sk = dd.date_sk
GROUP BY
    dd.full_date,
    dd.year_number,
    dd.quarter_number,
    dd.month_number;

CREATE OR REPLACE VIEW marts.mart_marketing_funnel AS
SELECT
    COALESCE(dc.channel, 'unknown') AS channel,
    fwe.user_journey_stage,
    fwe.event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT fwe.session_id) AS sessions,
    COUNT(DISTINCT fwe.customer_sk) AS customers,
    COALESCE(AVG(fwe.engagement_score), 0) AS avg_engagement_score,
    COALESCE(AVG(fwe.purchase_probability), 0) AS avg_purchase_probability
FROM warehouse.fact_web_events fwe
LEFT JOIN warehouse.dim_campaign dc
    ON fwe.campaign_sk = dc.campaign_sk
GROUP BY
    COALESCE(dc.channel, 'unknown'),
    fwe.user_journey_stage,
    fwe.event_type;
