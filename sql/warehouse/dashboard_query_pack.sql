/*
===============================================================================
Warehouse Dashboard Query Pack
===============================================================================

Purpose:
    BI/demo/interview-ready SQL queries for the PostgreSQL warehouse reporting
    layer.

Rule:
    Build and validate one dashboard query at a time.
    Use marts first.
    Use warehouse only when marts do not expose the required metric.
===============================================================================
*/


/*
===============================================================================
1. Daily Revenue Trend
===============================================================================

Business question:
    How is revenue trending day by day?

Primary BI use:
    Line chart by revenue date.

Source:
    marts.mart_revenue_daily

Expected output columns:
    - revenue_date
    - order_count
    - purchasing_customer_count
    - repeat_customer_count
    - item_count
    - gross_revenue
    - discount_amount
    - net_revenue
    - average_order_value
    - repeat_customer_rate_pct
===============================================================================
*/

SELECT
    revenue_date,
    order_count,
    purchasing_customer_count,
    repeat_customer_count,
    item_count,
    gross_revenue,
    discount_amount,
    net_revenue,
    average_order_value,
    repeat_customer_rate_pct
FROM marts.mart_revenue_daily
ORDER BY revenue_date;


/*
===============================================================================
2. Revenue by State
===============================================================================

Business question:
    Which customer home states generate the most revenue?

Primary BI use:
    Bar chart / map-style regional revenue view.

Source:
    marts.mart_customer_360

Expected output columns:
    - country
    - home_state
    - customer_count
    - order_count
    - total_revenue
    - average_revenue_per_customer
    - average_order_value
===============================================================================
*/

SELECT
    COALESCE(country, 'Unknown') AS country,
    COALESCE(home_state, 'Unknown') AS home_state,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(total_order_count) AS order_count,
    SUM(lifetime_value) AS total_revenue,
    ROUND(
        SUM(lifetime_value)::numeric
        / NULLIF(COUNT(DISTINCT customer_id), 0),
        2
    ) AS average_revenue_per_customer,
    ROUND(
        SUM(lifetime_value)::numeric
        / NULLIF(SUM(total_order_count), 0),
        2
    ) AS average_order_value
FROM marts.mart_customer_360
GROUP BY
    COALESCE(country, 'Unknown'),
    COALESCE(home_state, 'Unknown')
ORDER BY total_revenue DESC;


/*
===============================================================================
3. Top Customers by Lifetime Value
===============================================================================

Business question:
    Who are the highest-value customers?

Primary BI use:
    Customer leaderboard / drill-down table.

Source:
    marts.mart_customer_360

Expected output columns:
    - customer_id
    - customer_name
    - email
    - membership_tier
    - customer_segment
    - customer_value_segment
    - home_city
    - home_state
    - country
    - total_order_count
    - lifetime_value
    - average_order_value
    - total_units_purchased
    - first_order_timestamp
    - last_order_timestamp
    - days_since_last_order
    - repeat_purchase_signal
===============================================================================
*/

SELECT
    customer_id,
    customer_name,
    email,
    COALESCE(membership_tier, 'Unknown') AS membership_tier,
    COALESCE(customer_segment, 'Unknown') AS customer_segment,
    COALESCE(customer_value_segment, 'Unknown') AS customer_value_segment,
    COALESCE(home_city, 'Unknown') AS home_city,
    COALESCE(home_state, 'Unknown') AS home_state,
    COALESCE(country, 'Unknown') AS country,
    total_order_count,
    lifetime_value,
    average_order_value,
    total_units_purchased,
    first_order_timestamp,
    last_order_timestamp,
    days_since_last_order,
    repeat_purchase_signal
FROM marts.mart_customer_360
ORDER BY lifetime_value DESC, total_order_count DESC
LIMIT 25;


/*
===============================================================================
4. Campaign ROAS
===============================================================================

Business question:
    Which campaigns generate the best return on ad spend?

Primary BI use:
    Campaign performance table / ROAS ranking chart.

Source:
    marts.mart_campaign_performance

Expected output columns:
    - campaign_id
    - campaign_name
    - channel
    - traffic_source
    - target_segment
    - campaign_status
    - ab_test_group
    - campaign_budget
    - total_spend
    - budget_remaining
    - impressions
    - clicks
    - warehouse_conversion_count
    - order_count
    - purchasing_customer_count
    - attributed_revenue
    - average_order_value
    - roas
    - click_through_rate_pct
    - cost_per_click
    - cost_per_order
    - session_to_order_conversion_rate_pct
===============================================================================
*/

SELECT
    campaign_id,
    campaign_name,
    COALESCE(channel, 'Unknown') AS channel,
    COALESCE(traffic_source, 'Unknown') AS traffic_source,
    COALESCE(target_segment, 'Unknown') AS target_segment,
    campaign_status,
    ab_test_group,
    campaign_budget,
    total_spend,
    budget_remaining,
    impressions,
    clicks,
    warehouse_conversion_count,
    order_count,
    purchasing_customer_count,
    attributed_revenue,
    average_order_value,
    roas,
    click_through_rate_pct,
    cost_per_click,
    cost_per_order,
    session_to_order_conversion_rate_pct
FROM marts.mart_campaign_performance
ORDER BY roas DESC NULLS LAST, attributed_revenue DESC;


/*
===============================================================================
5. Product Category Performance
===============================================================================

Business question:
    Which product categories drive the most revenue, order volume, and units sold?

Primary BI use:
    Product/category performance bar chart and summary table.

Source:
    marts.mart_product_sales

Expected output columns:
    - category
    - product_count
    - order_count
    - purchasing_customer_count
    - units_sold
    - product_revenue
    - average_selling_price
    - average_revenue_per_product
    - average_units_per_product
===============================================================================
*/

SELECT
    COALESCE(category, 'Unknown') AS category,
    COUNT(DISTINCT product_id) AS product_count,
    SUM(order_count) AS order_count,
    SUM(purchasing_customer_count) AS purchasing_customer_count,
    SUM(units_sold) AS units_sold,
    SUM(product_revenue) AS product_revenue,
    ROUND(AVG(average_selling_price)::numeric, 2) AS average_selling_price,
    ROUND(
        SUM(product_revenue)::numeric
        / NULLIF(COUNT(DISTINCT product_id), 0),
        2
    ) AS average_revenue_per_product,
    ROUND(
        SUM(units_sold)::numeric
        / NULLIF(COUNT(DISTINCT product_id), 0),
        2
    ) AS average_units_per_product
FROM marts.mart_product_sales
GROUP BY COALESCE(category, 'Unknown')
ORDER BY product_revenue DESC;


/*
===============================================================================
6. Customer Segment Performance
===============================================================================

Business question:
    Which customer segments generate the strongest revenue, retention, and engagement?

Primary BI use:
    Customer segment comparison chart and KPI table.

Source:
    marts.mart_customer_360

Expected output columns:
    - customer_segment
    - customer_count
    - order_count
    - total_revenue
    - average_customer_lifetime_value
    - average_order_value
    - repeat_customer_count
    - repeat_customer_rate_pct
    - avg_engagement_score
    - avg_purchase_probability
===============================================================================
*/

SELECT
    COALESCE(customer_segment, 'Unknown') AS customer_segment,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(total_order_count) AS order_count,
    SUM(lifetime_value) AS total_revenue,
    ROUND(AVG(lifetime_value)::numeric, 2) AS average_customer_lifetime_value,
    ROUND(
        SUM(lifetime_value)::numeric
        / NULLIF(SUM(total_order_count), 0),
        2
    ) AS average_order_value,
    COUNT(*) FILTER (WHERE repeat_purchase_signal IS TRUE) AS repeat_customer_count,
    ROUND(
        (
            COUNT(*) FILTER (WHERE repeat_purchase_signal IS TRUE)
        )::numeric
        / NULLIF(COUNT(DISTINCT customer_id), 0)
        * 100,
        2
    ) AS repeat_customer_rate_pct,
    ROUND(AVG(avg_engagement_score)::numeric, 2) AS avg_engagement_score,
    ROUND(AVG(avg_purchase_probability)::numeric, 4) AS avg_purchase_probability
FROM marts.mart_customer_360
GROUP BY COALESCE(customer_segment, 'Unknown')
ORDER BY total_revenue DESC;


/*
===============================================================================
7. Marketing Funnel Conversion
===============================================================================

Business question:
    Where do customers drop off across the marketing funnel?

Primary BI use:
    Funnel chart / conversion scorecards by date, channel, and campaign.

Source:
    marts.mart_marketing_funnel

Expected output columns:
    - funnel_date
    - channel_name
    - channel_group
    - campaign_id
    - campaign_name
    - traffic_source
    - total_event_count
    - session_count
    - engaged_customer_count
    - product_view_event_count
    - cart_event_count
    - checkout_event_count
    - purchase_event_count
    - purchase_session_count
    - funnel_conversion_rate_pct
    - avg_engagement_score
    - avg_purchase_probability
    - avg_cart_abandonment_probability
===============================================================================
*/

SELECT
    event_date AS funnel_date,
    COALESCE(channel_name, 'Unknown') AS channel_name,
    COALESCE(channel_group, 'Unknown') AS channel_group,
    campaign_id,
    campaign_name,
    COALESCE(traffic_source, 'Unknown') AS traffic_source,
    total_event_count,
    session_count,
    engaged_customer_count,
    product_view_event_count,
    cart_event_count,
    checkout_event_count,
    purchase_event_count,
    purchase_session_count,
    funnel_conversion_rate_pct,
    avg_engagement_score,
    avg_purchase_probability,
    avg_cart_abandonment_probability
FROM marts.mart_marketing_funnel
ORDER BY funnel_date, channel_name, campaign_id;


/*
===============================================================================
8. Repeat Purchase Rate
===============================================================================

Business question:
    What percentage of purchasing customers placed more than one order?

Primary BI use:
    Retention KPI card and repeat-customer summary table.

Source:
    marts.mart_customer_360

Expected output columns:
    - purchasing_customers
    - repeat_customers
    - one_time_customers
    - total_orders
    - total_revenue
    - repeat_customer_revenue
    - repeat_purchase_rate_pct
    - average_orders_per_customer
===============================================================================
*/

SELECT
    COUNT(DISTINCT customer_id) FILTER (WHERE total_order_count > 0) AS purchasing_customers,
    COUNT(DISTINCT customer_id) FILTER (WHERE total_order_count > 1) AS repeat_customers,
    COUNT(DISTINCT customer_id) FILTER (WHERE total_order_count = 1) AS one_time_customers,
    SUM(total_order_count) AS total_orders,
    SUM(lifetime_value) AS total_revenue,
    SUM(lifetime_value) FILTER (WHERE total_order_count > 1) AS repeat_customer_revenue,
    ROUND(
        (
            COUNT(DISTINCT customer_id) FILTER (WHERE total_order_count > 1)
        )::numeric
        / NULLIF(
            COUNT(DISTINCT customer_id) FILTER (WHERE total_order_count > 0),
            0
        )
        * 100,
        2
    ) AS repeat_purchase_rate_pct,
    ROUND(
        SUM(total_order_count)::numeric
        / NULLIF(COUNT(DISTINCT customer_id) FILTER (WHERE total_order_count > 0), 0),
        2
    ) AS average_orders_per_customer
FROM marts.mart_customer_360;


/*
===============================================================================
9. Average Order Value
===============================================================================

Business question:
    What is the average revenue generated per order?

Primary BI use:
    Executive KPI card and revenue efficiency metric.

Source:
    marts.mart_revenue_daily

Expected output columns:
    - total_orders
    - gross_revenue
    - discount_amount
    - net_revenue
    - average_order_value
===============================================================================
*/

SELECT
    SUM(order_count) AS total_orders,
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(net_revenue) AS net_revenue,
    ROUND(
        SUM(net_revenue)::numeric
        / NULLIF(SUM(order_count), 0),
        2
    ) AS average_order_value
FROM marts.mart_revenue_daily;


/*
===============================================================================
10. Revenue Reconciliation
===============================================================================

Business question:
    Do the warehouse revenue numbers reconcile with the trusted audit layer?

Primary BI use:
    Data trust table for dashboard/demo validation.

Source:
    audit.reconciliation_report

Expected output columns:
    - check_name
    - source_count
    - target_count
    - status
    - checked_at
===============================================================================
*/

SELECT
    check_name,
    source_count,
    target_count,
    status,
    checked_at
FROM audit.reconciliation_report
WHERE LOWER(check_name) LIKE '%revenue%'
   OR LOWER(check_name) LIKE '%order%'
ORDER BY
    checked_at DESC,
    check_name;


/*
===============================================================================
Ad spend and return on ad spend by channel
===============================================================================
Source:
- warehouse.fact_campaign_spend (one row per campaign, day and region)
- warehouse.fact_orders (completed orders only are revenue)

platform_roas is what the ad platform reports for its own spend;
warehouse_roas is the completed order revenue of the channel's campaigns over
the same spend.
===============================================================================
*/

WITH spend AS (
    SELECT
        ch.channel_name,
        SUM(fcs.spend_amount) AS total_spend,
        SUM(fcs.impressions) AS impressions,
        SUM(fcs.clicks) AS clicks,
        SUM(fcs.conversions) AS ad_platform_conversions,
        SUM(fcs.attributed_revenue) AS platform_attributed_revenue
    FROM warehouse.fact_campaign_spend fcs
    JOIN warehouse.dim_channel ch
        ON fcs.channel_sk = ch.channel_sk
    GROUP BY ch.channel_name
),
revenue AS (
    SELECT
        ch.channel_name,
        SUM(fo.net_revenue) AS completed_revenue
    FROM warehouse.fact_orders fo
    JOIN warehouse.dim_channel ch
        ON fo.channel_sk = ch.channel_sk
    WHERE fo.order_status = 'completed'
    GROUP BY ch.channel_name
)
SELECT
    s.channel_name,
    ROUND(s.total_spend, 2) AS total_spend,
    s.impressions,
    s.clicks,
    s.ad_platform_conversions,
    ROUND(s.platform_attributed_revenue / NULLIF(s.total_spend, 0), 4) AS platform_roas,
    ROUND(COALESCE(r.completed_revenue, 0) / NULLIF(s.total_spend, 0), 4) AS warehouse_roas
FROM spend s
LEFT JOIN revenue r
    ON s.channel_name = r.channel_name
ORDER BY total_spend DESC;


/*
===============================================================================
Ad spend and return on ad spend by region
===============================================================================
Source:
- warehouse.fact_campaign_spend and warehouse.dim_region (the state the ad
  platform reports the spend for)
- warehouse.fact_orders and warehouse.dim_region (the state the order was
  placed from; completed orders only)
===============================================================================
*/

WITH spend AS (
    SELECT
        dr.state,
        SUM(fcs.spend_amount) AS total_spend,
        SUM(fcs.clicks) AS clicks,
        SUM(fcs.conversions) AS ad_platform_conversions,
        SUM(fcs.attributed_revenue) AS platform_attributed_revenue
    FROM warehouse.fact_campaign_spend fcs
    JOIN warehouse.dim_region dr
        ON fcs.region_sk = dr.region_sk
    GROUP BY dr.state
),
revenue AS (
    SELECT
        dr.state,
        SUM(fo.net_revenue) AS completed_revenue
    FROM warehouse.fact_orders fo
    JOIN warehouse.dim_region dr
        ON fo.region_sk = dr.region_sk
    WHERE fo.order_status = 'completed'
    GROUP BY dr.state
)
SELECT
    s.state,
    ROUND(s.total_spend, 2) AS total_spend,
    s.clicks,
    s.ad_platform_conversions,
    ROUND(s.platform_attributed_revenue / NULLIF(s.total_spend, 0), 4) AS platform_roas,
    ROUND(COALESCE(r.completed_revenue, 0) / NULLIF(s.total_spend, 0), 4) AS warehouse_roas
FROM spend s
LEFT JOIN revenue r
    ON s.state = r.state
ORDER BY total_spend DESC;
