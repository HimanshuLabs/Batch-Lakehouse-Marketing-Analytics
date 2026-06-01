-- BI-ready warehouse marts.
-- Views are used first so the reporting layer remains easy to validate and iterate.

CREATE OR REPLACE VIEW marts.mart_customer_360 AS
SELECT
    dc.customer_id,
    dc.customer_name,
    dc.email,
    dc.membership_tier,
    dc.customer_segment,
    dc.city,
    dc.state,
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
    dc.city,
    dc.state,
    dc.country;

CREATE OR REPLACE VIEW marts.mart_campaign_performance AS
SELECT
    dc.campaign_id,
    dc.campaign_name,
    dc.channel,
    dc.traffic_source,
    COALESCE(SUM(fcs.spend_amount), 0) AS total_spend,
    COALESCE(SUM(fcs.impressions), 0) AS impressions,
    COALESCE(SUM(fcs.clicks), 0) AS clicks,
    COALESCE(SUM(fcs.conversions), 0) AS conversions,
    COALESCE(SUM(fo.net_revenue), 0) AS attributed_revenue,
    CASE
        WHEN COALESCE(SUM(fcs.spend_amount), 0) = 0 THEN NULL
        ELSE ROUND(SUM(fo.net_revenue) / NULLIF(SUM(fcs.spend_amount), 0), 4)
    END AS roas
FROM warehouse.dim_campaign dc
LEFT JOIN warehouse.fact_campaign_spend fcs
    ON dc.campaign_sk = fcs.campaign_sk
LEFT JOIN warehouse.fact_orders fo
    ON dc.campaign_sk = fo.campaign_sk
WHERE dc.is_current = TRUE
GROUP BY
    dc.campaign_id,
    dc.campaign_name,
    dc.channel,
    dc.traffic_source;

CREATE OR REPLACE VIEW marts.mart_product_sales AS
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    SUM(foi.quantity) AS units_sold,
    SUM(foi.line_revenue) AS product_revenue,
    AVG(foi.unit_price) AS average_selling_price
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
    dd.calendar_date,
    dd.calendar_year,
    dd.calendar_quarter,
    dd.calendar_month,
    COUNT(DISTINCT fo.order_id) AS orders,
    SUM(fo.gross_revenue) AS gross_revenue,
    SUM(fo.discount_amount) AS discount_amount,
    SUM(fo.net_revenue) AS net_revenue,
    AVG(fo.net_revenue) AS average_order_value
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd
    ON fo.order_date_sk = dd.date_sk
GROUP BY
    dd.calendar_date,
    dd.calendar_year,
    dd.calendar_quarter,
    dd.calendar_month;

CREATE OR REPLACE VIEW marts.mart_marketing_funnel AS
SELECT
    COALESCE(dc.channel, 'unknown') AS channel,
    fwe.user_journey_stage,
    fwe.event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT fwe.session_id) AS sessions,
    COUNT(DISTINCT fwe.customer_sk) AS customers,
    AVG(fwe.engagement_score) AS avg_engagement_score,
    AVG(fwe.purchase_probability) AS avg_purchase_probability
FROM warehouse.fact_web_events fwe
LEFT JOIN warehouse.dim_campaign dc
    ON fwe.campaign_sk = dc.campaign_sk
GROUP BY
    COALESCE(dc.channel, 'unknown'),
    fwe.user_journey_stage,
    fwe.event_type;
