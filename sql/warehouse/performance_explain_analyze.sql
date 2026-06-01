-- Performance tuning examples for warehouse reporting queries.
-- Run after tables, indexes, and sample data exist.

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    dd.calendar_month,
    dc.membership_tier,
    SUM(fo.net_revenue) AS net_revenue
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd
    ON fo.order_date_sk = dd.date_sk
JOIN warehouse.dim_customer dc
    ON fo.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE
GROUP BY
    dd.calendar_month,
    dc.membership_tier
ORDER BY
    dd.calendar_month,
    dc.membership_tier;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    dp.category,
    SUM(foi.quantity) AS units_sold,
    SUM(foi.line_revenue) AS product_revenue
FROM warehouse.fact_order_items foi
JOIN warehouse.dim_product dp
    ON foi.product_sk = dp.product_sk
WHERE dp.is_current = TRUE
GROUP BY dp.category
ORDER BY product_revenue DESC;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    dc.channel,
    SUM(fcs.spend_amount) AS spend,
    SUM(fcs.clicks) AS clicks,
    SUM(fcs.conversions) AS conversions
FROM warehouse.fact_campaign_spend fcs
JOIN warehouse.dim_campaign dc
    ON fcs.campaign_sk = dc.campaign_sk
WHERE dc.is_current = TRUE
GROUP BY dc.channel
ORDER BY spend DESC;
