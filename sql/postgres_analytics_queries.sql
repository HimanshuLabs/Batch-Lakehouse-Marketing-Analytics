-- ============================================================
-- Project 2: Batch Data Lakehouse for Marketing Analytics
-- PostgreSQL Analytical Query Pack
-- Layer: Gold Serving Layer
-- Schema: gold
-- ============================================================

-- ------------------------------------------------------------
-- 01. Top campaigns by revenue
-- Business question:
-- Which campaigns generated the highest revenue?
-- ------------------------------------------------------------

SELECT
    campaign_id,
    campaign_name,
    traffic_source,
    total_ad_spend,
    total_revenue,
    roas,
    conversion_rate,
    total_orders,
    total_sessions
FROM gold.mart_campaign_performance
ORDER BY total_revenue DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 02. Best ROAS campaigns
-- Business question:
-- Which campaigns returned the most revenue per rupee spent?
-- ------------------------------------------------------------

SELECT
    campaign_id,
    campaign_name,
    traffic_source,
    total_ad_spend,
    total_revenue,
    roas,
    ctr,
    cpc
FROM gold.mart_campaign_performance
WHERE total_ad_spend > 0
ORDER BY roas DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 03. Campaigns wasting spend
-- Business question:
-- Which campaigns spent money but produced weak revenue?
-- ------------------------------------------------------------

SELECT
    campaign_id,
    campaign_name,
    traffic_source,
    total_ad_spend,
    total_revenue,
    roas,
    total_orders,
    conversion_rate
FROM gold.mart_campaign_performance
WHERE total_ad_spend > 0
ORDER BY roas ASC, total_ad_spend DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 04. Top products by revenue
-- Business question:
-- Which products generated the most sales revenue?
-- ------------------------------------------------------------

SELECT
    product_id,
    product_name,
    category,
    total_units_sold,
    total_product_revenue,
    product_views,
    add_to_cart_events,
    purchase_events,
    view_to_cart_rate,
    cart_to_purchase_rate
FROM gold.mart_product_performance
ORDER BY total_product_revenue DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 05. Category performance
-- Business question:
-- Which product categories perform best commercially?
-- ------------------------------------------------------------

SELECT
    category,
    COUNT(DISTINCT product_id) AS product_count,
    SUM(total_units_sold) AS total_units_sold,
    ROUND(SUM(total_product_revenue)::numeric, 2) AS total_revenue,
    SUM(product_views) AS product_views,
    SUM(add_to_cart_events) AS add_to_cart_events,
    SUM(purchase_events) AS purchase_events,
    ROUND(AVG(view_to_cart_rate)::numeric, 6) AS avg_view_to_cart_rate,
    ROUND(AVG(cart_to_purchase_rate)::numeric, 6) AS avg_cart_to_purchase_rate
FROM gold.mart_product_performance
GROUP BY category
ORDER BY total_revenue DESC;


-- ------------------------------------------------------------
-- 06. Customer lifetime value leaders
-- Business question:
-- Which customers are most valuable?
-- ------------------------------------------------------------

SELECT
    customer_id,
    user_name,
    email,
    membership_tier,
    user_segment,
    is_prime_user,
    total_orders,
    customer_lifetime_value,
    avg_order_value,
    total_sessions,
    avg_engagement_score
FROM gold.mart_customer_value
ORDER BY customer_lifetime_value DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 07. High-engagement customers with low purchase value
-- Business question:
-- Which users engage heavily but are not converting enough?
-- ------------------------------------------------------------

SELECT
    customer_id,
    user_name,
    email,
    membership_tier,
    user_segment,
    total_sessions,
    total_events,
    avg_engagement_score,
    avg_purchase_probability,
    customer_lifetime_value,
    total_orders
FROM gold.mart_customer_value
WHERE total_sessions > 0
ORDER BY avg_engagement_score DESC, customer_lifetime_value ASC
LIMIT 20;


-- ------------------------------------------------------------
-- 08. Marketing funnel by traffic source
-- Business question:
-- Which acquisition channels convert best?
-- ------------------------------------------------------------

SELECT
    traffic_source,
    SUM(total_events) AS total_events,
    SUM(total_sessions) AS total_sessions,
    SUM(page_views) AS page_views,
    SUM(product_views) AS product_views,
    SUM(add_to_cart_events) AS add_to_cart_events,
    SUM(checkout_events) AS checkout_events,
    SUM(purchase_events) AS purchase_events,
    ROUND(
        CASE
            WHEN SUM(total_sessions) > 0
            THEN SUM(purchase_events)::numeric / SUM(total_sessions)
            ELSE 0
        END,
        6
    ) AS session_conversion_rate
FROM gold.mart_marketing_funnel
GROUP BY traffic_source
ORDER BY session_conversion_rate DESC;


-- ------------------------------------------------------------
-- 09. Device performance
-- Business question:
-- Which device type gives better conversion and performance?
-- ------------------------------------------------------------

SELECT
    device_type,
    SUM(total_sessions) AS total_sessions,
    SUM(product_views) AS product_views,
    SUM(add_to_cart_events) AS add_to_cart_events,
    SUM(checkout_events) AS checkout_events,
    SUM(purchase_events) AS purchase_events,
    ROUND(AVG(avg_api_latency_ms)::numeric, 2) AS avg_api_latency_ms,
    ROUND(AVG(avg_page_load_time_ms)::numeric, 2) AS avg_page_load_time_ms,
    ROUND(AVG(session_conversion_rate)::numeric, 6) AS avg_session_conversion_rate
FROM gold.mart_marketing_funnel
GROUP BY device_type
ORDER BY avg_session_conversion_rate DESC;


-- ------------------------------------------------------------
-- 10. A/B test performance
-- Business question:
-- Which experiment group performs best?
-- ------------------------------------------------------------

SELECT
    ab_test_group,
    SUM(total_events) AS total_events,
    SUM(total_sessions) AS total_sessions,
    SUM(purchase_events) AS purchase_events,
    ROUND(
        CASE
            WHEN SUM(total_sessions) > 0
            THEN SUM(purchase_events)::numeric / SUM(total_sessions)
            ELSE 0
        END,
        6
    ) AS session_conversion_rate,
    ROUND(AVG(avg_engagement_score)::numeric, 4) AS avg_engagement_score
FROM gold.mart_marketing_funnel
GROUP BY ab_test_group
ORDER BY session_conversion_rate DESC;


-- ------------------------------------------------------------
-- 11. Revenue by payment method
-- Business question:
-- Which payment methods drive the most revenue?
-- ------------------------------------------------------------

SELECT
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(order_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(order_amount)::numeric, 2) AS avg_order_value
FROM gold.fact_orders
GROUP BY payment_method
ORDER BY total_revenue DESC;


-- ------------------------------------------------------------
-- 12. Fraud-risk order review
-- Business question:
-- Which high-value orders carry high fraud risk?
-- ------------------------------------------------------------

SELECT
    order_id,
    customer_id,
    campaign_id,
    order_timestamp,
    payment_method,
    order_amount,
    fraud_score,
    city,
    country
FROM gold.fact_orders
WHERE fraud_score >= 0.80
ORDER BY fraud_score DESC, order_amount DESC
LIMIT 25;


-- ------------------------------------------------------------
-- 13. Daily revenue trend
-- Business question:
-- How is revenue moving by order date?
-- ------------------------------------------------------------

SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(order_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(order_amount)::numeric, 2) AS avg_order_value
FROM gold.fact_orders
GROUP BY order_date
ORDER BY order_date;


-- ------------------------------------------------------------
-- 14. Daily event trend
-- Business question:
-- How much behavioral activity happens each day?
-- ------------------------------------------------------------

SELECT
    event_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
    SUM(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS checkout_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events
FROM gold.fact_web_events
GROUP BY event_date
ORDER BY event_date;


-- ------------------------------------------------------------
-- 15. Prime vs non-prime customer value
-- Business question:
-- Are prime users more valuable?
-- ------------------------------------------------------------

SELECT
    is_prime_user,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(total_orders) AS total_orders,
    ROUND(SUM(customer_lifetime_value)::numeric, 2) AS total_customer_value,
    ROUND(AVG(customer_lifetime_value)::numeric, 2) AS avg_customer_lifetime_value,
    ROUND(AVG(avg_order_value)::numeric, 2) AS avg_order_value
FROM gold.mart_customer_value
GROUP BY is_prime_user
ORDER BY avg_customer_lifetime_value DESC;


-- ------------------------------------------------------------
-- 16. Membership tier value
-- Business question:
-- Which membership tier is commercially strongest?
-- ------------------------------------------------------------

SELECT
    membership_tier,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(total_orders) AS total_orders,
    ROUND(SUM(customer_lifetime_value)::numeric, 2) AS total_customer_value,
    ROUND(AVG(customer_lifetime_value)::numeric, 2) AS avg_customer_lifetime_value,
    ROUND(AVG(avg_engagement_score)::numeric, 4) AS avg_engagement_score
FROM gold.mart_customer_value
GROUP BY membership_tier
ORDER BY avg_customer_lifetime_value DESC;


-- ------------------------------------------------------------
-- 17. Campaign publish audit
-- Business question:
-- What did we publish to PostgreSQL?
-- ------------------------------------------------------------

SELECT
    table_name,
    row_count,
    published_at,
    postgres_schema,
    source_path
FROM gold.gold_publish_audit
ORDER BY table_name;
