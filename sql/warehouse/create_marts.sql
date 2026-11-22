/* ===============================================================================
PostgreSQL BI-Ready Warehouse Marts
===============================================================================

Purpose:
- Build business-friendly marts from warehouse facts and dimensions only.
- Keep BI/reporting away from Bronze, Silver, and raw lakehouse internals.
- Expose dashboard-ready metrics for customer, campaign, product, revenue,
  and marketing funnel reporting.
- Add audit validation checks proving marts reconcile with warehouse facts.

Run order:
1. scripts/load_gold_to_postgres_staging.py
2. sql/warehouse/create_warehouse_tables.sql
3. sql/warehouse/create_marts.sql
4. sql/warehouse/reconciliation_report.sql

Design:
- Views are used for local portfolio reproducibility and easy BI iteration.
- All marts are built from warehouse.* tables.
- SCD2 correctness is preserved because warehouse facts already carry
  point-in-time surrogate keys.

=============================================================================== */

BEGIN;

-- This file is one step of the warehouse build. It refuses to run anywhere but
-- in the build transaction that sql/warehouse/begin_generation.sql started
-- (which sets p2.build_txid for that transaction only), so it can never drop or
-- rebuild the live tables on its own.
DO $$
BEGIN
    IF current_setting('p2.build_txid', true) IS DISTINCT FROM txid_current()::TEXT THEN
        RAISE EXCEPTION 'create_marts.sql runs only inside the warehouse build transaction started by sql/warehouse/begin_generation.sql. Run the whole build: bash scripts/run_airflow_task.sh build_warehouse';
    END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;

DROP TABLE IF EXISTS marts.mart_customer_daily_snapshot, marts.mart_product_daily_snapshot, marts.mart_campaign_daily_snapshot CASCADE;
DROP VIEW IF EXISTS marts.mart_marketing_funnel CASCADE;
DROP VIEW IF EXISTS marts.mart_revenue_daily CASCADE;
DROP VIEW IF EXISTS marts.mart_product_sales CASCADE;
DROP VIEW IF EXISTS marts.mart_campaign_performance CASCADE;
DROP VIEW IF EXISTS marts.mart_customer_360 CASCADE;

-- =============================================================================
-- Customer 360 Mart
-- =============================================================================
-- Business use:
-- - Customer profile reporting
-- - Lifetime value analysis
-- - Repeat purchase identification
-- - Customer segment and loyalty performance
-- - CRM / retention dashboard source

CREATE OR REPLACE VIEW marts.mart_customer_360 AS
WITH current_customers AS (
    SELECT
        customer_id,
        customer_name,
        email,
        gender,
        age,
        membership_tier,
        loyalty_points,
        preferred_language,
        customer_segment,
        is_prime_user,
        home_city,
        home_state,
        country
    FROM warehouse.dim_customer
    WHERE is_current = TRUE
),
orders_by_customer AS (
    SELECT
        dc.customer_id,
        -- A sale is every order that was not cancelled, recognised when it is
        -- placed. A returned order stays a sale and its refund is subtracted:
        -- lifetime_value = net_sales - refund_amount (net revenue after refunds).
        -- Cancelled and returned orders are also counted in columns of their own.
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status <> 'cancelled') AS total_order_count,
        COALESCE(SUM(fo.gross_amount) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS gross_revenue,
        COALESCE(SUM(fo.discount_amount) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS total_discount_amount,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) - COALESCE(SUM(fo.refund_amount), 0) AS lifetime_value,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS net_sales,
        COALESCE(SUM(fo.refund_amount), 0) AS refund_amount,
        COALESCE(AVG(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS average_order_value,
        MIN(fo.order_timestamp) FILTER (WHERE fo.order_status <> 'cancelled') AS first_order_timestamp,
        MAX(fo.order_timestamp) FILTER (WHERE fo.order_status <> 'cancelled') AS last_order_timestamp,
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'cancelled') AS cancelled_order_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'cancelled'), 0) AS cancelled_amount,
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'returned') AS returned_order_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'returned'), 0) AS returned_amount,
        -- 1 = the third of the buying customers with the highest lifetime value.
        NTILE(3) OVER (
            PARTITION BY (COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) - COALESCE(SUM(fo.refund_amount), 0) > 0
                          AND dc.customer_id NOT IN ('UNKNOWN', 'INVALID'))
            ORDER BY COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) - COALESCE(SUM(fo.refund_amount), 0) DESC, dc.customer_id
        ) AS value_third
    FROM warehouse.fact_orders fo
    JOIN warehouse.dim_customer dc
        ON fo.customer_sk = dc.customer_sk
    GROUP BY dc.customer_id
),
items_by_customer AS (
    SELECT
        dc.customer_id,
        COALESCE(SUM(foi.quantity), 0) AS total_units_purchased,
        COUNT(DISTINCT dp.category) FILTER (WHERE dp.category IS NOT NULL) AS product_categories_purchased,
        COUNT(DISTINCT dp.product_id) FILTER (WHERE dp.product_id NOT IN ('UNKNOWN', 'INVALID')) AS distinct_products_purchased
    FROM warehouse.fact_order_items foi
    -- Items of completed orders only.
    JOIN warehouse.fact_orders fo
        ON fo.order_sk = foi.order_sk
       AND fo.order_status <> 'cancelled'
    JOIN warehouse.dim_customer dc
        ON foi.customer_sk = dc.customer_sk
    LEFT JOIN warehouse.dim_product dp
        ON foi.product_sk = dp.product_sk
    GROUP BY dc.customer_id
),
web_by_customer AS (
    SELECT
        dc.customer_id,
        COUNT(*) AS web_event_count,
        COUNT(DISTINCT fwe.session_id) FILTER (WHERE fwe.session_id IS NOT NULL) AS session_count,
        COUNT(*) FILTER (
            WHERE lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
               OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
        ) AS purchase_intent_event_count,
        COUNT(DISTINCT fwe.session_id) FILTER (
            WHERE fwe.session_id IS NOT NULL
              AND (
                    lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
                 OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
              )
        ) AS purchase_session_count,
        COALESCE(AVG(fwe.engagement_score), 0) AS avg_engagement_score,
        COALESCE(AVG(fwe.purchase_probability), 0) AS avg_purchase_probability,
        COALESCE(AVG(fwe.cart_abandonment_probability), 0) AS avg_cart_abandonment_probability
    FROM warehouse.fact_web_events fwe
    JOIN warehouse.dim_customer dc
        ON fwe.customer_sk = dc.customer_sk
    GROUP BY dc.customer_id
)
SELECT
    cc.customer_id,
    -- 'known' for a real member. The marts also carry one row each for the
    -- unknown member (facts with no key) and the invalid-reference member
    -- (facts whose key has no valid dimension row), so every mart ties out to
    -- the fact totals; filter member_type = 'known' for the real members only.
    CASE cc.customer_id WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' ELSE 'known' END AS member_type,
    cc.customer_name,
    cc.email,
    cc.gender,
    cc.age,
    cc.membership_tier,
    cc.loyalty_points,
    cc.preferred_language,
    cc.customer_segment,
    cc.is_prime_user,
    cc.home_city,
    cc.home_state,
    cc.country,

    COALESCE(o.total_order_count, 0)::BIGINT AS total_order_count,
    ROUND(COALESCE(o.gross_revenue, 0), 2) AS gross_revenue,
    ROUND(COALESCE(o.total_discount_amount, 0), 2) AS total_discount_amount,
    ROUND(COALESCE(o.lifetime_value, 0), 2) AS lifetime_value,
    ROUND(COALESCE(o.net_sales, 0), 2) AS net_sales,
    ROUND(COALESCE(o.refund_amount, 0), 2) AS refund_amount,
    ROUND(COALESCE(o.average_order_value, 0), 2) AS average_order_value,

    COALESCE(o.cancelled_order_count, 0)::BIGINT AS cancelled_order_count,
    ROUND(COALESCE(o.cancelled_amount, 0), 2) AS cancelled_amount,
    COALESCE(o.returned_order_count, 0)::BIGINT AS returned_order_count,
    ROUND(COALESCE(o.returned_amount, 0), 2) AS returned_amount,

    COALESCE(i.total_units_purchased, 0)::BIGINT AS total_units_purchased,
    COALESCE(i.product_categories_purchased, 0)::BIGINT AS product_categories_purchased,
    COALESCE(i.distinct_products_purchased, 0)::BIGINT AS distinct_products_purchased,

    o.first_order_timestamp,
    o.last_order_timestamp,

    CASE
        WHEN o.last_order_timestamp IS NULL THEN NULL
        -- Both sides are business dates (Asia/Kolkata), like the fact date keys.
        ELSE (SELECT MAX(warehouse.business_date(order_timestamp)) FROM warehouse.fact_orders)
             - warehouse.business_date(o.last_order_timestamp)
    END AS days_since_last_order,

    (COALESCE(o.total_order_count, 0) >= 2) AS repeat_purchase_signal,

    -- Thirds of the customers who have bought, by lifetime value: the
    -- boundaries come from the data, not from fixed amounts.
    CASE
        WHEN COALESCE(o.lifetime_value, 0) <= 0 THEN 'No Purchase Yet'
        WHEN o.value_third = 1 THEN 'High Value'
        WHEN o.value_third = 2 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_value_segment,

    COALESCE(w.web_event_count, 0)::BIGINT AS web_event_count,
    COALESCE(w.session_count, 0)::BIGINT AS session_count,
    COALESCE(w.purchase_intent_event_count, 0)::BIGINT AS purchase_intent_event_count,

    ROUND(
        CASE
            WHEN COALESCE(w.session_count, 0) = 0 THEN 0
            ELSE LEAST(
                    COALESCE(w.purchase_session_count, 0),
                    COALESCE(w.session_count, 0)
                 )::NUMERIC
                 / NULLIF(w.session_count, 0) * 100
        END,
        2
    ) AS customer_conversion_rate_pct,

    ROUND(COALESCE(w.avg_engagement_score, 0), 4) AS avg_engagement_score,
    ROUND(COALESCE(w.avg_purchase_probability, 0), 4) AS avg_purchase_probability,
    ROUND(COALESCE(w.avg_cart_abandonment_probability, 0), 4) AS avg_cart_abandonment_probability

FROM current_customers cc
LEFT JOIN orders_by_customer o
    ON cc.customer_id = o.customer_id
LEFT JOIN items_by_customer i
    ON cc.customer_id = i.customer_id
LEFT JOIN web_by_customer w
    ON cc.customer_id = w.customer_id;

COMMENT ON VIEW marts.mart_customer_360 IS
'BI-ready customer profile mart combining current customer attributes, lifetime revenue, order behavior, repeat purchase signal, and web engagement metrics.';

-- =============================================================================
-- Campaign Performance Mart
-- =============================================================================
-- Business use:
-- - Campaign ROI / ROAS dashboard
-- - Spend efficiency analysis
-- - Channel performance reporting
-- - Campaign attribution reporting

CREATE OR REPLACE VIEW marts.mart_campaign_performance AS
WITH current_campaigns AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_type,
        channel,
        traffic_source,
        target_segment,
        campaign_status,
        ab_test_group,
        campaign_start_date,
        campaign_end_date,
        budget
    FROM warehouse.dim_campaign
    WHERE is_current = TRUE
),
spend_by_campaign AS (
    SELECT
        dc.campaign_id,
        COALESCE(SUM(fcs.spend_amount), 0) AS total_spend,
        COALESCE(SUM(fcs.impressions), 0) AS impressions,
        COALESCE(SUM(fcs.clicks), 0) AS clicks,
        -- What the ad platform itself reports, beside the warehouse's own figures.
        COALESCE(SUM(fcs.conversions), 0) AS ad_platform_conversions,
        COALESCE(SUM(fcs.attributed_revenue), 0) AS platform_attributed_revenue
    FROM warehouse.fact_campaign_spend fcs
    JOIN warehouse.dim_campaign dc
        ON fcs.campaign_sk = dc.campaign_sk
    GROUP BY dc.campaign_id
),
orders_by_campaign AS (
    SELECT
        dc.campaign_id,
        -- Completed orders only (see mart_customer_360).
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status <> 'cancelled') AS order_count,
        COUNT(DISTINCT fo.customer_id) FILTER (WHERE fo.order_status <> 'cancelled') AS purchasing_customer_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) - COALESCE(SUM(fo.refund_amount), 0) AS warehouse_attributed_revenue,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS net_sales,
        COALESCE(SUM(fo.refund_amount), 0) AS refund_amount,
        COALESCE(AVG(fo.net_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS average_order_value,
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'cancelled') AS cancelled_order_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'cancelled'), 0) AS cancelled_amount,
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'returned') AS returned_order_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'returned'), 0) AS returned_amount
    FROM warehouse.fact_orders fo
    JOIN warehouse.dim_campaign dc
        ON fo.campaign_sk = dc.campaign_sk
    GROUP BY dc.campaign_id
),
web_by_campaign AS (
    SELECT
        dc.campaign_id,
        COUNT(*) AS web_event_count,
        COUNT(DISTINCT fwe.session_id) FILTER (WHERE fwe.session_id IS NOT NULL) AS session_count,
        -- People by their natural key: a customer with several SCD versions
        -- (customer_sk) is one customer.
        COUNT(DISTINCT fwe.customer_id) AS engaged_customer_count,
        COUNT(*) FILTER (
            WHERE lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
               OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
        ) AS purchase_event_count,
        COALESCE(AVG(fwe.engagement_score), 0) AS avg_engagement_score,
        COALESCE(AVG(fwe.purchase_probability), 0) AS avg_purchase_probability
    FROM warehouse.fact_web_events fwe
    JOIN warehouse.dim_campaign dc
        ON fwe.campaign_sk = dc.campaign_sk
    GROUP BY dc.campaign_id
),
conversions_by_campaign AS (
    SELECT
        dc.campaign_id,
        COUNT(*) AS warehouse_conversion_count,
        COALESCE(SUM(fc.conversion_value), 0) AS warehouse_conversion_value
    FROM warehouse.fact_conversions fc
    JOIN warehouse.dim_campaign dc
        ON fc.campaign_sk = dc.campaign_sk
    GROUP BY dc.campaign_id
)
SELECT
    cc.campaign_id,
    -- 'known' for a real member. The marts also carry one row each for the
    -- unknown member (facts with no key) and the invalid-reference member
    -- (facts whose key has no valid dimension row), so every mart ties out to
    -- the fact totals; filter member_type = 'known' for the real members only.
    CASE cc.campaign_id WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' ELSE 'known' END AS member_type,
    cc.campaign_name,
    cc.campaign_type,
    cc.channel,
    cc.traffic_source,
    cc.target_segment,
    cc.campaign_status,
    cc.ab_test_group,
    cc.campaign_start_date,
    cc.campaign_end_date,
    ROUND(COALESCE(cc.budget, 0), 2) AS campaign_budget,

    ROUND(COALESCE(s.total_spend, 0), 2) AS total_spend,
    ROUND(COALESCE(cc.budget, 0) - COALESCE(s.total_spend, 0), 2) AS budget_remaining,

    COALESCE(s.impressions, 0)::BIGINT AS impressions,
    COALESCE(s.clicks, 0)::BIGINT AS clicks,
    COALESCE(s.ad_platform_conversions, 0)::BIGINT AS ad_platform_conversions,

    COALESCE(w.web_event_count, 0)::BIGINT AS web_event_count,
    COALESCE(w.session_count, 0)::BIGINT AS session_count,
    COALESCE(w.engaged_customer_count, 0)::BIGINT AS engaged_customer_count,
    COALESCE(w.purchase_event_count, 0)::BIGINT AS purchase_event_count,

    COALESCE(o.order_count, 0)::BIGINT AS order_count,
    COALESCE(o.purchasing_customer_count, 0)::BIGINT AS purchasing_customer_count,
    COALESCE(c.warehouse_conversion_count, 0)::BIGINT AS warehouse_conversion_count,

    ROUND(COALESCE(o.warehouse_attributed_revenue, 0), 2) AS attributed_revenue,
    ROUND(COALESCE(o.net_sales, 0), 2) AS net_sales,
    ROUND(COALESCE(o.refund_amount, 0), 2) AS refund_amount,
    ROUND(COALESCE(s.platform_attributed_revenue, 0), 2) AS platform_attributed_revenue,
    ROUND(COALESCE(c.warehouse_conversion_value, 0), 2) AS warehouse_conversion_value,
    ROUND(COALESCE(o.average_order_value, 0), 2) AS average_order_value,

    COALESCE(o.cancelled_order_count, 0)::BIGINT AS cancelled_order_count,
    ROUND(COALESCE(o.cancelled_amount, 0), 2) AS cancelled_amount,
    COALESCE(o.returned_order_count, 0)::BIGINT AS returned_order_count,
    ROUND(COALESCE(o.returned_amount, 0), 2) AS returned_amount,

    -- The platform's own return on spend, beside roas (warehouse-measured) below.
    ROUND(
        CASE
            WHEN COALESCE(s.total_spend, 0) = 0 THEN 0
            ELSE COALESCE(s.platform_attributed_revenue, 0) / NULLIF(s.total_spend, 0)
        END,
        4
    ) AS platform_roas,

    ROUND(
        CASE
            WHEN COALESCE(s.total_spend, 0) = 0 THEN 0
            ELSE COALESCE(o.warehouse_attributed_revenue, 0) / NULLIF(s.total_spend, 0)
        END,
        4
    ) AS roas,

    ROUND(
        CASE
            WHEN COALESCE(s.impressions, 0) = 0 THEN 0
            ELSE COALESCE(s.clicks, 0)::NUMERIC / NULLIF(s.impressions, 0) * 100
        END,
        2
    ) AS click_through_rate_pct,

    ROUND(
        CASE
            WHEN COALESCE(s.clicks, 0) = 0 THEN 0
            ELSE COALESCE(s.total_spend, 0) / NULLIF(s.clicks, 0)
        END,
        4
    ) AS cost_per_click,

    ROUND(
        CASE
            WHEN COALESCE(o.order_count, 0) = 0 THEN 0
            ELSE COALESCE(s.total_spend, 0) / NULLIF(o.order_count, 0)
        END,
        4
    ) AS cost_per_order,

    ROUND(
        CASE
            WHEN COALESCE(w.session_count, 0) = 0 THEN 0
            ELSE COALESCE(o.order_count, 0)::NUMERIC / NULLIF(w.session_count, 0) * 100
        END,
        2
    ) AS session_to_order_conversion_rate_pct,

    ROUND(COALESCE(w.avg_engagement_score, 0), 4) AS avg_engagement_score,
    ROUND(COALESCE(w.avg_purchase_probability, 0), 4) AS avg_purchase_probability

FROM current_campaigns cc
LEFT JOIN spend_by_campaign s
    ON cc.campaign_id = s.campaign_id
LEFT JOIN orders_by_campaign o
    ON cc.campaign_id = o.campaign_id
LEFT JOIN web_by_campaign w
    ON cc.campaign_id = w.campaign_id
LEFT JOIN conversions_by_campaign c
    ON cc.campaign_id = c.campaign_id;

COMMENT ON VIEW marts.mart_campaign_performance IS
'BI-ready campaign mart with spend, impressions, clicks, conversions, attributed revenue, ROAS, CTR, CPC, CPA, and session-to-order conversion metrics.';

-- =============================================================================
-- Product Sales Mart
-- =============================================================================
-- Business use:
-- - Product/category revenue dashboard
-- - Top products by revenue and units sold
-- - Category contribution analysis
-- - Inventory and product performance reporting

CREATE OR REPLACE VIEW marts.mart_product_sales AS
WITH current_products AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        product_status,
        original_price,
        current_price,
        inventory_remaining
    FROM warehouse.dim_product
    WHERE is_current = TRUE
),
sales_by_product AS (
    SELECT
        dp.product_id,
        -- An item is sold when its order is completed.
        COUNT(DISTINCT foi.order_id) FILTER (WHERE fo.order_status <> 'cancelled') AS order_count,
        COUNT(DISTINCT foi.customer_id) FILTER (WHERE fo.order_status <> 'cancelled') AS purchasing_customer_count,
        COALESCE(SUM(foi.quantity) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS units_sold,
        COALESCE(SUM(foi.line_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) - COALESCE(SUM(foi.refund_amount), 0) AS product_revenue,
        COALESCE(SUM(foi.line_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS net_sales,
        COALESCE(SUM(foi.refund_amount), 0) AS refund_amount,
        COALESCE(AVG(foi.unit_price) FILTER (WHERE fo.order_status <> 'cancelled'), 0) AS average_selling_price,
        MIN(foi.order_timestamp) FILTER (WHERE fo.order_status <> 'cancelled') AS first_sold_timestamp,
        MAX(foi.order_timestamp) FILTER (WHERE fo.order_status <> 'cancelled') AS last_sold_timestamp,
        COALESCE(SUM(foi.quantity) FILTER (WHERE fo.order_status = 'cancelled'), 0) AS cancelled_units,
        COALESCE(SUM(foi.quantity) FILTER (WHERE fo.order_status = 'returned'), 0) AS returned_units,
        COALESCE(SUM(foi.line_revenue) FILTER (WHERE fo.order_status = 'returned'), 0) AS returned_amount
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo
        ON fo.order_sk = foi.order_sk
    JOIN warehouse.dim_product dp
        ON foi.product_sk = dp.product_sk
    GROUP BY dp.product_id
),
web_by_product AS (
    SELECT
        dp.product_id,
        COUNT(*) AS product_web_event_count,
        COUNT(DISTINCT fwe.session_id) FILTER (WHERE fwe.session_id IS NOT NULL) AS product_session_count,
        COUNT(*) FILTER (
            WHERE lower(fwe.event_type) IN ('product_view', 'view_product', 'view_item')
               OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%product%'
        ) AS product_view_event_count,
        COUNT(*) FILTER (
            WHERE lower(fwe.event_type) LIKE '%cart%'
               OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%cart%'
        ) AS cart_event_count,
        COUNT(*) FILTER (
            WHERE lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
               OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
        ) AS purchase_event_count,
        COUNT(DISTINCT fwe.session_id) FILTER (
            WHERE fwe.session_id IS NOT NULL
              AND (
                    lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
                 OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
              )
        ) AS purchase_session_count,
        COALESCE(AVG(fwe.engagement_score), 0) AS avg_product_engagement_score
    FROM warehouse.fact_web_events fwe
    JOIN warehouse.dim_product dp
        ON fwe.product_sk = dp.product_sk
    GROUP BY dp.product_id
),
product_base AS (
    SELECT
        cp.product_id,
        -- 'known' for a real member. The marts also carry one row each for the
        -- unknown member (facts with no key) and the invalid-reference member
        -- (facts whose key has no valid dimension row), so every mart ties out to
        -- the fact totals; filter member_type = 'known' for the real members only.
        CASE cp.product_id WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' ELSE 'known' END AS member_type,
        cp.product_name,
        cp.category,
        cp.brand,
        cp.product_status,
        ROUND(COALESCE(cp.original_price, 0), 2) AS original_price,
        ROUND(COALESCE(cp.current_price, 0), 2) AS current_price,
        cp.inventory_remaining,

        COALESCE(s.order_count, 0)::BIGINT AS order_count,
        COALESCE(s.purchasing_customer_count, 0)::BIGINT AS purchasing_customer_count,
        COALESCE(s.units_sold, 0)::BIGINT AS units_sold,
        ROUND(COALESCE(s.product_revenue, 0), 2) AS product_revenue,
        ROUND(COALESCE(s.net_sales, 0), 2) AS net_sales,
        ROUND(COALESCE(s.refund_amount, 0), 2) AS refund_amount,
        ROUND(COALESCE(s.average_selling_price, 0), 2) AS average_selling_price,
        s.first_sold_timestamp,
        s.last_sold_timestamp,
        COALESCE(s.cancelled_units, 0)::BIGINT AS cancelled_units,
        COALESCE(s.returned_units, 0)::BIGINT AS returned_units,
        ROUND(COALESCE(s.returned_amount, 0), 2) AS returned_amount,

        COALESCE(w.product_web_event_count, 0)::BIGINT AS product_web_event_count,
        COALESCE(w.product_session_count, 0)::BIGINT AS product_session_count,
        COALESCE(w.product_view_event_count, 0)::BIGINT AS product_view_event_count,
        COALESCE(w.cart_event_count, 0)::BIGINT AS cart_event_count,
        COALESCE(w.purchase_event_count, 0)::BIGINT AS purchase_event_count,

        ROUND(
            CASE
                WHEN COALESCE(w.product_session_count, 0) = 0 THEN 0
                ELSE LEAST(
                        COALESCE(w.purchase_session_count, 0),
                        COALESCE(w.product_session_count, 0)
                     )::NUMERIC
                     / NULLIF(w.product_session_count, 0) * 100
            END,
            2
        ) AS product_conversion_rate_pct,

        ROUND(COALESCE(w.avg_product_engagement_score, 0), 4) AS avg_product_engagement_score
    FROM current_products cp
    LEFT JOIN sales_by_product s
        ON cp.product_id = s.product_id
    LEFT JOIN web_by_product w
        ON cp.product_id = w.product_id
)
SELECT
    product_id,
    member_type,
    product_name,
    category,
    brand,
    product_status,
    original_price,
    current_price,
    inventory_remaining,
    order_count,
    purchasing_customer_count,
    units_sold,
    product_revenue,
    net_sales,
    refund_amount,
    average_selling_price,
    first_sold_timestamp,
    last_sold_timestamp,
    cancelled_units,
    returned_units,
    returned_amount,
    product_web_event_count,
    product_session_count,
    product_view_event_count,
    cart_event_count,
    purchase_event_count,
    product_conversion_rate_pct,
    avg_product_engagement_score,

    DENSE_RANK() OVER (
        PARTITION BY category
        ORDER BY product_revenue DESC, units_sold DESC, product_id
    ) AS category_revenue_rank,

    ROUND(
        CASE
            WHEN SUM(product_revenue) OVER (PARTITION BY category) = 0 THEN 0
            ELSE product_revenue / NULLIF(SUM(product_revenue) OVER (PARTITION BY category), 0) * 100
        END,
        2
    ) AS category_revenue_share_pct

FROM product_base;

COMMENT ON VIEW marts.mart_product_sales IS
'BI-ready product sales mart with product revenue, units sold, category rank, category revenue share, product engagement, cart behavior, and conversion metrics.';

-- =============================================================================
-- Daily Revenue Mart
-- =============================================================================
-- Business use:
-- - Executive revenue trend dashboard
-- - Daily order count and AOV reporting
-- - Repeat customer tracking
-- - Gross-to-net revenue monitoring

CREATE OR REPLACE VIEW marts.mart_revenue_daily AS
WITH orders_enriched AS (
    SELECT
        fo.order_sk,
        fo.order_id,
        fo.customer_sk,
        -- The customer's own ID, so orders on the invalid-reference member
        -- still count as their real customers.
        fo.customer_id,
        fo.order_date_sk,
        fo.order_timestamp,
        fo.order_status,
        -- A sale is every order that was not cancelled, recognised on its order
        -- date; cancelled orders and returns have columns of their own.
        (fo.order_status <> 'cancelled') AS sold,
        FALSE AS is_return,
        0::NUMERIC AS refund_amount,
        fo.gross_amount,
        fo.discount_amount,
        fo.net_revenue,
        fo.tax_amount,
        fo.shipping_amount,
        fo.total_amount,
        fo.item_count,
        -- The customer's n-th sale (cancelled orders are numbered apart).
        ROW_NUMBER() OVER (
            PARTITION BY fo.customer_id, (fo.order_status <> 'cancelled')
            ORDER BY fo.order_timestamp NULLS LAST, fo.order_sk
        ) AS customer_order_number
    FROM warehouse.fact_orders fo

    UNION ALL

    -- A return is recognised on the business date it happened: its own row,
    -- which counts in none of the sales measures.
    SELECT
        fo.order_sk,
        fo.order_id,
        fo.customer_sk,
        fo.customer_id,
        warehouse.date_sk(warehouse.business_date(fo.status_timestamp)) AS order_date_sk,
        fo.status_timestamp,
        fo.order_status,
        FALSE AS sold,
        TRUE AS is_return,
        fo.refund_amount,
        fo.gross_amount,
        fo.discount_amount,
        fo.net_revenue,
        fo.tax_amount,
        fo.shipping_amount,
        fo.total_amount,
        fo.item_count,
        NULL::BIGINT AS customer_order_number
    FROM warehouse.fact_orders fo
    WHERE fo.order_status = 'returned'
)
SELECT
    dd.full_date AS revenue_date,
    dd.day_name,
    dd.day_of_week,
    dd.week_of_year,
    dd.month_number,
    dd.month_name,
    dd.quarter_number,
    dd.year_number,
    dd.is_weekend,

    COUNT(DISTINCT oe.order_id) FILTER (WHERE oe.sold) AS order_count,
    COUNT(DISTINCT oe.customer_id) FILTER (WHERE oe.sold) AS purchasing_customer_count,
    COUNT(DISTINCT oe.customer_id) FILTER (WHERE oe.sold AND oe.customer_order_number > 1) AS repeat_customer_count,

    COALESCE(SUM(oe.item_count) FILTER (WHERE oe.sold), 0)::BIGINT AS item_count,
    ROUND(COALESCE(SUM(oe.gross_amount) FILTER (WHERE oe.sold), 0), 2) AS gross_revenue,
    ROUND(COALESCE(SUM(oe.discount_amount) FILTER (WHERE oe.sold), 0), 2) AS discount_amount,
    ROUND(COALESCE(SUM(oe.net_revenue) FILTER (WHERE oe.sold), 0), 2) AS net_revenue,
    -- What customers were charged for those orders: net + tax + shipping.
    ROUND(COALESCE(SUM(oe.tax_amount) FILTER (WHERE oe.sold), 0), 2) AS tax_amount,
    ROUND(COALESCE(SUM(oe.shipping_amount) FILTER (WHERE oe.sold), 0), 2) AS shipping_amount,
    ROUND(COALESCE(SUM(oe.total_amount) FILTER (WHERE oe.sold), 0), 2) AS total_amount,

    COUNT(DISTINCT oe.order_id) FILTER (WHERE oe.order_status = 'cancelled') AS cancelled_order_count,
    ROUND(COALESCE(SUM(oe.net_revenue) FILTER (WHERE oe.order_status = 'cancelled'), 0), 2) AS cancelled_amount,
    -- Returns and refunds on the date of the return (sales above are on the order date).
    COUNT(DISTINCT oe.order_id) FILTER (WHERE oe.is_return) AS returned_order_count,
    ROUND(COALESCE(SUM(oe.net_revenue) FILTER (WHERE oe.is_return), 0), 2) AS returned_amount,
    ROUND(COALESCE(SUM(oe.refund_amount) FILTER (WHERE oe.is_return), 0), 2) AS refund_amount,
    ROUND(COALESCE(SUM(oe.net_revenue) FILTER (WHERE oe.sold), 0)
          - COALESCE(SUM(oe.refund_amount) FILTER (WHERE oe.is_return), 0), 2) AS net_revenue_after_refunds,

    ROUND(
        CASE
            WHEN COUNT(DISTINCT oe.order_id) FILTER (WHERE oe.sold) = 0 THEN 0
            ELSE COALESCE(SUM(oe.net_revenue) FILTER (WHERE oe.sold), 0) / NULLIF(COUNT(DISTINCT oe.order_id) FILTER (WHERE oe.sold), 0)
        END,
        2
    ) AS average_order_value,

    ROUND(
        CASE
            WHEN COUNT(DISTINCT oe.customer_id) FILTER (WHERE oe.sold) = 0 THEN 0
            ELSE COUNT(DISTINCT oe.customer_id) FILTER (WHERE oe.sold AND oe.customer_order_number > 1)::NUMERIC
                 / NULLIF(COUNT(DISTINCT oe.customer_id) FILTER (WHERE oe.sold), 0) * 100
        END,
        2
    ) AS repeat_customer_rate_pct

FROM orders_enriched oe
JOIN warehouse.dim_date dd
    ON oe.order_date_sk = dd.date_sk
WHERE dd.date_sk <> 0
GROUP BY
    dd.full_date,
    dd.day_name,
    dd.day_of_week,
    dd.week_of_year,
    dd.month_number,
    dd.month_name,
    dd.quarter_number,
    dd.year_number,
    dd.is_weekend;

COMMENT ON VIEW marts.mart_revenue_daily IS
'BI-ready daily revenue mart with daily order count, revenue, AOV, repeat customer count, and repeat customer rate.';

-- =============================================================================
-- Marketing Funnel Mart
-- =============================================================================
-- Business use:
-- - Funnel dashboard from awareness to purchase
-- - Channel/campaign conversion tracking
-- - Engagement quality monitoring
-- - Session-to-purchase analysis

CREATE OR REPLACE VIEW marts.mart_marketing_funnel AS
WITH event_enriched AS (
    SELECT
        dd.full_date AS event_date,
        dd.month_number,
        dd.month_name,
        dd.quarter_number,
        dd.year_number,

        dch.channel_name,
        dch.channel_group,

        dc.campaign_id,
        dc.campaign_name,
        -- The campaign's objective: fixed for a campaign (SCD Type 0), so it does
        -- not change the grain.
        dc.campaign_type,
        dc.traffic_source,
        dc.target_segment,

        fwe.event_id,
        fwe.session_id,
        fwe.customer_id,
        fwe.event_type,
        fwe.user_journey_stage,
        fwe.engagement_score,
        fwe.purchase_probability,
        fwe.cart_abandonment_probability,

        lower(COALESCE(fwe.event_type, 'unknown')) AS event_type_norm,
        lower(COALESCE(fwe.user_journey_stage, 'unknown')) AS stage_norm
    FROM warehouse.fact_web_events fwe
    JOIN warehouse.dim_date dd
        ON fwe.event_date_sk = dd.date_sk
    LEFT JOIN warehouse.dim_channel dch
        ON fwe.channel_sk = dch.channel_sk
    LEFT JOIN warehouse.dim_campaign dc
        ON fwe.campaign_sk = dc.campaign_sk
    WHERE dd.date_sk <> 0
)
SELECT
    event_date,
    month_number,
    month_name,
    quarter_number,
    year_number,

    COALESCE(channel_name, 'Unknown') AS channel_name,
    COALESCE(channel_group, 'Unknown') AS channel_group,

    COALESCE(campaign_id, 'UNKNOWN') AS campaign_id,
    CASE COALESCE(campaign_id, 'UNKNOWN') WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference'
         ELSE 'known' END AS member_type,
    COALESCE(campaign_name, 'Unknown') AS campaign_name,
    COALESCE(campaign_type, 'Unknown') AS campaign_type,
    COALESCE(traffic_source, 'Unknown') AS traffic_source,
    COALESCE(target_segment, 'Unknown') AS target_segment,

    COUNT(*) AS total_event_count,
    COUNT(DISTINCT session_id) FILTER (WHERE session_id IS NOT NULL) AS session_count,
    -- By natural key, as in mart_campaign_performance.
    COUNT(DISTINCT customer_id) AS engaged_customer_count,

    COUNT(*) FILTER (
        WHERE stage_norm LIKE '%awareness%'
           OR event_type_norm IN ('impression', 'page_view', 'landing_page_view')
    ) AS awareness_event_count,

    COUNT(*) FILTER (
        WHERE stage_norm LIKE '%consideration%'
           OR event_type_norm IN ('product_view', 'view_product', 'view_item', 'search')
    ) AS consideration_event_count,

    COUNT(*) FILTER (
        WHERE event_type_norm IN ('product_view', 'view_product', 'view_item')
           OR stage_norm LIKE '%product%'
    ) AS product_view_event_count,

    COUNT(*) FILTER (
        WHERE event_type_norm LIKE '%cart%'
           OR stage_norm LIKE '%cart%'
    ) AS cart_event_count,

    COUNT(*) FILTER (
        WHERE event_type_norm LIKE '%checkout%'
           OR stage_norm LIKE '%checkout%'
    ) AS checkout_event_count,

    COUNT(*) FILTER (
        WHERE event_type_norm IN ('purchase', 'conversion', 'order_completed')
           OR stage_norm LIKE '%purchase%'
    ) AS purchase_event_count,

    COUNT(DISTINCT session_id) FILTER (
        WHERE session_id IS NOT NULL
          AND (
                event_type_norm IN ('purchase', 'conversion', 'order_completed')
             OR stage_norm LIKE '%purchase%'
          )
    ) AS purchase_session_count,

    ROUND(
        CASE
            WHEN COUNT(DISTINCT session_id) FILTER (WHERE session_id IS NOT NULL) = 0 THEN 0
            ELSE COUNT(DISTINCT session_id) FILTER (
                    WHERE session_id IS NOT NULL
                      AND (
                            event_type_norm IN ('purchase', 'conversion', 'order_completed')
                         OR stage_norm LIKE '%purchase%'
                      )
                 )::NUMERIC
                 / NULLIF(COUNT(DISTINCT session_id) FILTER (WHERE session_id IS NOT NULL), 0) * 100
        END,
        2
    ) AS funnel_conversion_rate_pct,

    ROUND(COALESCE(AVG(engagement_score), 0), 4) AS avg_engagement_score,
    ROUND(COALESCE(AVG(purchase_probability), 0), 4) AS avg_purchase_probability,
    ROUND(COALESCE(AVG(cart_abandonment_probability), 0), 4) AS avg_cart_abandonment_probability

FROM event_enriched
GROUP BY
    event_date,
    month_number,
    month_name,
    quarter_number,
    year_number,
    COALESCE(channel_name, 'Unknown'),
    COALESCE(channel_group, 'Unknown'),
    COALESCE(campaign_id, 'UNKNOWN'),
    COALESCE(campaign_name, 'Unknown'),
    COALESCE(campaign_type, 'Unknown'),
    COALESCE(traffic_source, 'Unknown'),
    COALESCE(target_segment, 'Unknown');

COMMENT ON VIEW marts.mart_marketing_funnel IS
'BI-ready marketing funnel mart with stage counts, sessions, engaged customers, purchase sessions, conversion rate, and engagement quality metrics by date/channel/campaign.';

-- =============================================================================
-- Column comments for interview/demo clarity
-- =============================================================================

COMMENT ON COLUMN marts.mart_customer_360.lifetime_value IS
'Total net revenue from all warehouse orders for the customer across historical SCD2 customer versions.';

COMMENT ON COLUMN marts.mart_customer_360.repeat_purchase_signal IS
'TRUE when the customer has two or more distinct completed orders.';

COMMENT ON COLUMN marts.mart_campaign_performance.roas IS
'Return on ad spend: warehouse attributed revenue divided by campaign spend.';

COMMENT ON COLUMN marts.mart_campaign_performance.session_to_order_conversion_rate_pct IS
'Orders divided by campaign sessions, expressed as a percentage.';

COMMENT ON COLUMN marts.mart_product_sales.category_revenue_rank IS
'Product rank within its category by product revenue.';

COMMENT ON COLUMN marts.mart_revenue_daily.average_order_value IS
'Daily net revenue divided by distinct order count.';

COMMENT ON COLUMN marts.mart_marketing_funnel.funnel_conversion_rate_pct IS
'Purchase sessions divided by total sessions for a date/channel/campaign group.';

-- =============================================================================
-- Daily snapshot marts: leak-free history for consumers
-- =============================================================================
-- One row per customer, per product and per campaign for every business date
-- from the entity's first activity to the last date in the data. A row holds
-- the columns of the corresponding lifetime mart (mart_customer_360,
-- mart_product_sales, mart_campaign_performance) as they would have been at
-- the end of that date: computed only from the orders placed up to it, each
-- order's status as of it (an order returned later is still a completed sale),
-- the refunds up to it, the spend and the web events up to it, and the
-- dimension versions in force on it. Nothing after snapshot_date reaches a
-- row, so a model can be trained on the snapshot of one date and tested on
-- what happened after it.
--
-- How: in one pass over the facts, not one query per date. A fact counts from
-- the first date it is known on (its own date, or the start of its dimension
-- version if that is later), with its order's status and refund as known on
-- each date (a cancellation or a return counts from the date it happened).
-- Per entity, the changes of each date become running totals, carried over
-- every date up to the next change. The columns use the lifetime marts' own
-- expressions (the same ROUND, ratios, NTILE and window), and the mart
-- validation checks that the snapshot of the last date equals the lifetime
-- mart column for column (4j-4l): a change to a lifetime mart that is not
-- made here as well fails the build. Business dates are Asia/Kolkata dates,
-- like the fact date keys.

CREATE TABLE marts.mart_customer_daily_snapshot AS
SELECT NULL::DATE AS snapshot_date, m.* FROM marts.mart_customer_360 m WITH NO DATA;
CREATE TABLE marts.mart_product_daily_snapshot AS
SELECT NULL::DATE AS snapshot_date, m.* FROM marts.mart_product_sales m WITH NO DATA;
CREATE TABLE marts.mart_campaign_daily_snapshot AS
SELECT NULL::DATE AS snapshot_date, m.* FROM marts.mart_campaign_performance m WITH NO DATA;

-- Calendar: every business date, and the first instant after it (Asia/Kolkata).

CREATE TEMP TABLE pg_temp.snap_cal ON COMMIT DROP AS
SELECT full_date AS d, date_sk AS ds,
       ((full_date + 1)::TEXT || ' 00:00:00 Asia/Kolkata')::TIMESTAMPTZ AS day_end
FROM warehouse.dim_date
WHERE date_sk <> 0;

-- f and l: the first and the last date. off: how far the day boundary lies before
-- the next UTC midnight (5:30 for Asia/Kolkata). A timestamp is before the end of
-- date d exactly when ((ts AT TIME ZONE 'UTC') + off)::DATE <= d: that date is its
-- business date (v, e and s below are such dates).
CREATE TEMP TABLE pg_temp.snap_b ON COMMIT DROP AS
SELECT min(d) AS f, max(d) AS l, min((d + 1)::TIMESTAMP AT TIME ZONE 'UTC' - day_end) AS off
FROM pg_temp.snap_cal;

-- What the snapshots below rely on. It always holds (every fact date key
-- references dim_date, whose date_sk is YYYYMMDD); if it ever does not, the
-- build stops here and says which part failed. With no dates there are no
-- snapshot rows, as before.
DO $$
BEGIN
    IF (SELECT count(DISTINCT (d + 1)::TIMESTAMP AT TIME ZONE 'UTC' - day_end) FROM pg_temp.snap_cal) > 1 THEN
        RAISE EXCEPTION 'Daily snapshots: the day boundary must be the same offset from UTC midnight on every date of warehouse.dim_date, and it is not.';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_temp.snap_cal WHERE ds < 1) THEN
        RAISE EXCEPTION 'Daily snapshots: warehouse.dim_date has a negative date_sk; every date key must be positive (0 is the unknown member).';
    END IF;
    IF EXISTS (SELECT 1 FROM (SELECT ds, lag(ds) OVER (ORDER BY d) AS prev FROM pg_temp.snap_cal) x WHERE x.ds <= x.prev) THEN
        RAISE EXCEPTION 'Daily snapshots: the date_sk of warehouse.dim_date must rise with full_date, and it does not.';
    END IF;
END $$;

-- Dimension versions: present on a date from v, the business date of effective_from; in force on [v, e).

CREATE TEMP TABLE pg_temp.snap_vcu ON COMMIT DROP AS
SELECT dv.customer_sk AS sk, dv.customer_id AS id,
       ((dv.effective_from AT TIME ZONE 'UTC') + b.off)::DATE AS v,
       ((dv.effective_to AT TIME ZONE 'UTC') + b.off)::DATE AS e
FROM warehouse.dim_customer dv CROSS JOIN pg_temp.snap_b b;

CREATE TEMP TABLE pg_temp.snap_vpr ON COMMIT DROP AS
SELECT dv.product_sk AS sk, dv.product_id AS id, dv.category,
       ((dv.effective_from AT TIME ZONE 'UTC') + b.off)::DATE AS v,
       ((dv.effective_to AT TIME ZONE 'UTC') + b.off)::DATE AS e
FROM warehouse.dim_product dv CROSS JOIN pg_temp.snap_b b;

CREATE TEMP TABLE pg_temp.snap_vca ON COMMIT DROP AS
SELECT dv.campaign_sk AS sk, dv.campaign_id AS id,
       ((dv.effective_from AT TIME ZONE 'UTC') + b.off)::DATE AS v,
       ((dv.effective_to AT TIME ZONE 'UTC') + b.off)::DATE AS e
FROM warehouse.dim_campaign dv CROSS JOIN pg_temp.snap_b b;

-- Facts with p, the date of their date key (and s, the business date of the final status, for orders and items).

CREATE TEMP TABLE pg_temp.snap_ord ON COMMIT DROP AS
SELECT o.order_sk, o.order_id, o.customer_sk, o.campaign_sk, o.customer_id, o.order_timestamp, o.order_status,
       o.gross_amount, o.discount_amount, o.net_revenue, o.refund_amount,
       c.d AS p, ((o.status_timestamp AT TIME ZONE 'UTC') + b.off)::DATE AS s
FROM warehouse.fact_orders o
JOIN pg_temp.snap_cal c ON c.ds = o.order_date_sk
CROSS JOIN pg_temp.snap_b b;

-- Items exist only with an order on the calendar: a NULL order_sk never does.
CREATE TEMP TABLE pg_temp.snap_itm ON COMMIT DROP AS
SELECT i.order_id, i.customer_sk, i.product_sk, i.customer_id, i.order_timestamp,
       i.quantity, i.unit_price, i.line_revenue, i.refund_amount,
       o.order_status, o.refund_amount <> 0 AS order_refunded, o.p, o.s
FROM warehouse.fact_order_items i
JOIN pg_temp.snap_ord o ON o.order_sk = i.order_sk;

CREATE TEMP TABLE pg_temp.snap_web ON COMMIT DROP AS
SELECT fwe.session_id, fwe.customer_sk, fwe.product_sk, fwe.campaign_sk, fwe.customer_id,
       fwe.event_type, fwe.user_journey_stage,
       fwe.engagement_score, fwe.purchase_probability, fwe.cart_abandonment_probability,
       c.d AS p
FROM warehouse.fact_web_events fwe
JOIN pg_temp.snap_cal c ON c.ds = fwe.event_date_sk;

CREATE TEMP TABLE pg_temp.snap_spd ON COMMIT DROP AS
SELECT fcs.campaign_sk, fcs.spend_amount, fcs.impressions, fcs.clicks, fcs.conversions, fcs.attributed_revenue, c.d AS p
FROM warehouse.fact_campaign_spend fcs
JOIN pg_temp.snap_cal c ON c.ds = fcs.spend_date_sk;

CREATE TEMP TABLE pg_temp.snap_cnv ON COMMIT DROP AS
SELECT fc.campaign_sk, fc.conversion_value, c.d AS p
FROM warehouse.fact_conversions fc
JOIN pg_temp.snap_cal c ON c.ds = fc.conversion_date_sk;

ANALYZE pg_temp.snap_cal, pg_temp.snap_vcu, pg_temp.snap_vpr, pg_temp.snap_vca, pg_temp.snap_ord, pg_temp.snap_itm,
        pg_temp.snap_web, pg_temp.snap_spd, pg_temp.snap_cnv;

-- Products (mart_product_sales). The windows run over every current product version on every date.

-- Items through their product version (an inner join in mart_product_sales): they count from a = GREATEST(p, v).
CREATE TEMP TABLE pg_temp.snap_p_itm ON COMMIT DROP AS
SELECT vp.id AS ent, GREATEST(x.p, vp.v) AS a, x.s,
       x.order_status = 'cancelled' AS is_c, x.order_status = 'returned' AS is_r, x.order_refunded,
       x.order_id, x.customer_id, x.order_timestamp, x.quantity, x.unit_price, x.line_revenue, x.refund_amount
FROM pg_temp.snap_itm x
JOIN pg_temp.snap_vpr vp ON vp.sk = x.product_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(x.p, vp.v) <= b.l;

-- Web events through their product version (an inner join in mart_product_sales).
CREATE TEMP TABLE pg_temp.snap_p_web ON COMMIT DROP AS
SELECT vp.id AS ent, GREATEST(fwe.p, vp.v) AS a, fwe.*
FROM pg_temp.snap_web fwe
JOIN pg_temp.snap_vpr vp ON vp.sk = fwe.product_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(fwe.p, vp.v) <= b.l;

CREATE TEMP TABLE pg_temp.snap_p_d (
    ent TEXT NOT NULL, day DATE NOT NULL,
    units BIGINT, net NUMERIC, refund NUMERIC, price_sum NUMERIC, price_n BIGINT,
    cancelled_units BIGINT, returned_units BIGINT, returned_amount NUMERIC,
    order_n BIGINT, customer_n BIGINT,
    web_n BIGINT, session_n BIGINT, view_n BIGINT, cart_n BIGINT, purchase_n BIGINT, purchase_session_n BIGINT,
    engagement_sum NUMERIC, engagement_n BIGINT,
    first_ts TIMESTAMPTZ, last_ts TIMESTAMPTZ
) ON COMMIT DROP;

-- Sums: two change rows per item (nc = +1/-1 for the measures of orders not cancelled, fin = 1 on the final-status row).
INSERT INTO pg_temp.snap_p_d (ent, day, units, net, refund, price_sum, price_n, cancelled_units, returned_units, returned_amount)
SELECT x.ent, ev.day,
       ev.nc * x.quantity,
       ev.nc * x.line_revenue,
       ev.fin * CASE WHEN x.order_refunded THEN 1 ELSE 0 END * x.refund_amount,
       ev.nc * x.unit_price,
       ev.nc,
       ev.fin * CASE WHEN x.is_c THEN 1 ELSE 0 END * x.quantity,
       ev.fin * CASE WHEN x.is_r THEN 1 ELSE 0 END * x.quantity,
       ev.fin * CASE WHEN x.is_r THEN 1 ELSE 0 END * x.line_revenue
FROM pg_temp.snap_p_itm x
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL (VALUES
    (x.a, CASE WHEN NOT x.is_c OR x.s > x.a THEN 1 ELSE 0 END, 0),
    (GREATEST(x.a, x.s), CASE WHEN x.is_c AND x.s > x.a THEN -1 ELSE 0 END, 1)
) AS ev(day, nc, fin)
WHERE ev.day <= b.l
  AND (ev.fin = 0 OR x.is_c OR x.is_r OR x.order_refunded);

-- Sums: web events never leave; the FILTERs are mart_product_sales', and each AVG is kept as its SUM and COUNT.
INSERT INTO pg_temp.snap_p_d (ent, day, web_n, view_n, cart_n, purchase_n, engagement_sum, engagement_n)
SELECT fwe.ent, fwe.a,
       COUNT(*),
       COUNT(*) FILTER (
           WHERE lower(fwe.event_type) IN ('product_view', 'view_product', 'view_item')
              OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%product%'),
       COUNT(*) FILTER (
           WHERE lower(fwe.event_type) LIKE '%cart%'
              OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%cart%'),
       COUNT(*) FILTER (
           WHERE lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
              OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'),
       SUM(fwe.engagement_score),
       COUNT(fwe.engagement_score)
FROM pg_temp.snap_p_web fwe
GROUP BY fwe.ent, fwe.a;

-- Distinct counts as key intervals: 1 order_id (a NULL never counts) and 2 customer_id while the order is not
-- cancelled; 3 session_id and 4 purchase sessions from first sight.
CREATE TEMP TABLE pg_temp.snap_p_k (m SMALLINT NOT NULL, ent TEXT NOT NULL, k TEXT NOT NULL, st DATE NOT NULL, en DATE)
    ON COMMIT DROP;
INSERT INTO pg_temp.snap_p_k
SELECT 1, ent, order_id, a, CASE WHEN is_c THEN s END FROM pg_temp.snap_p_itm
WHERE order_id IS NOT NULL AND (NOT is_c OR s > a)
UNION ALL
SELECT 2, ent, customer_id, a, CASE WHEN is_c THEN s END FROM pg_temp.snap_p_itm
WHERE customer_id IS NOT NULL AND (NOT is_c OR s > a)
UNION ALL
SELECT 3, fwe.ent, fwe.session_id, min(fwe.a), NULL FROM pg_temp.snap_p_web fwe
WHERE fwe.session_id IS NOT NULL
GROUP BY fwe.ent, fwe.session_id
UNION ALL
SELECT 4, fwe.ent, fwe.session_id, min(fwe.a), NULL FROM pg_temp.snap_p_web fwe
WHERE fwe.session_id IS NOT NULL
  AND (
        lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
     OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
  )
GROUP BY fwe.ent, fwe.session_id;

INSERT INTO pg_temp.snap_p_d (ent, day, order_n, customer_n, session_n, purchase_session_n)
SELECT ent, day,
       SUM(t) FILTER (WHERE m = 1), SUM(t) FILTER (WHERE m = 2), SUM(t) FILTER (WHERE m = 3), SUM(t) FILTER (WHERE m = 4)
FROM (
    SELECT m, ent, day,
           CASE WHEN cov - dlt = 0 AND cov > 0 THEN 1 WHEN cov - dlt > 0 AND cov = 0 THEN -1 ELSE 0 END AS t
    FROM (
        SELECT m, ent, k, day, dlt, SUM(dlt) OVER (PARTITION BY m, ent, k ORDER BY day) AS cov
        FROM (
            SELECT m, ent, k, day, SUM(sg) AS dlt
            FROM (
                SELECT m, ent, k, st AS day, 1 AS sg FROM pg_temp.snap_p_k
                UNION ALL
                SELECT m, ent, k, en, -1 FROM pg_temp.snap_p_k CROSS JOIN pg_temp.snap_b b WHERE en <= b.l
            ) edges
            GROUP BY m, ent, k, day
        ) per_day
    ) coverage
) transitions
WHERE t <> 0
GROUP BY ent, day;

-- First and last sale: items that never leave the not-cancelled set.
INSERT INTO pg_temp.snap_p_d (ent, day, first_ts, last_ts)
SELECT ent, a, MIN(order_timestamp), MAX(order_timestamp)
FROM pg_temp.snap_p_itm
WHERE NOT is_c
GROUP BY ent, a;

-- First and last sale: items of finally cancelled orders, on each date [a, s) on which they still counted.
CREATE TEMP TABLE pg_temp.snap_p_tmp ON COMMIT DROP AS
SELECT x.ent, x.a + g.k AS d, MIN(x.order_timestamp) AS first_ts, MAX(x.order_timestamp) AS last_ts
FROM pg_temp.snap_p_itm x
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL generate_series(0, LEAST(x.s - 1, b.l) - x.a) AS g(k)
WHERE x.is_c AND x.s > x.a
GROUP BY x.ent, x.a + g.k;

CREATE TEMP TABLE pg_temp.snap_p_run ON COMMIT DROP AS
SELECT ent, day, LEAD(day) OVER (PARTITION BY ent ORDER BY day) AS nxt,
       SUM(SUM(units)) OVER w AS units, SUM(SUM(net)) OVER w AS net, SUM(SUM(refund)) OVER w AS refund,
       SUM(SUM(price_sum)) OVER w AS price_sum, SUM(SUM(price_n)) OVER w AS price_n,
       SUM(SUM(cancelled_units)) OVER w AS cancelled_units, SUM(SUM(returned_units)) OVER w AS returned_units,
       SUM(SUM(returned_amount)) OVER w AS returned_amount,
       SUM(SUM(order_n)) OVER w AS order_n, SUM(SUM(customer_n)) OVER w AS customer_n,
       SUM(SUM(web_n)) OVER w AS web_n, SUM(SUM(session_n)) OVER w AS session_n, SUM(SUM(view_n)) OVER w AS view_n,
       SUM(SUM(cart_n)) OVER w AS cart_n, SUM(SUM(purchase_n)) OVER w AS purchase_n,
       SUM(SUM(purchase_session_n)) OVER w AS purchase_session_n,
       SUM(SUM(engagement_sum)) OVER w AS engagement_sum, SUM(SUM(engagement_n)) OVER w AS engagement_n,
       MIN(MIN(first_ts)) OVER w AS first_ts, MAX(MAX(last_ts)) OVER w AS last_ts
FROM pg_temp.snap_p_d
GROUP BY ent, day
WINDOW w AS (PARTITION BY ent ORDER BY day ROWS UNBOUNDED PRECEDING);

-- The values of sales_by_product and web_by_product, per product_id and date, named as in mart_product_sales.
CREATE TEMP TABLE pg_temp.snap_p_val ON COMMIT DROP AS
SELECT r.ent AS product_id, c.d,
       r.order_n::BIGINT AS order_count,
       r.customer_n::BIGINT AS purchasing_customer_count,
       COALESCE(r.units, 0) AS units_sold,
       COALESCE(r.net, 0) - COALESCE(r.refund, 0) AS product_revenue,
       COALESCE(r.net, 0) AS net_sales,
       COALESCE(r.refund, 0) AS refund_amount,
       COALESCE(r.price_sum / NULLIF(r.price_n, 0), 0) AS average_selling_price,
       LEAST(r.first_ts, t.first_ts) AS first_sold_timestamp,
       GREATEST(r.last_ts, t.last_ts) AS last_sold_timestamp,
       COALESCE(r.cancelled_units, 0) AS cancelled_units,
       COALESCE(r.returned_units, 0) AS returned_units,
       COALESCE(r.returned_amount, 0) AS returned_amount,
       r.web_n::BIGINT AS product_web_event_count,
       r.session_n::BIGINT AS product_session_count,
       r.view_n::BIGINT AS product_view_event_count,
       r.cart_n::BIGINT AS cart_event_count,
       r.purchase_n::BIGINT AS purchase_event_count,
       r.purchase_session_n::BIGINT AS purchase_session_count,
       COALESCE(r.engagement_sum / NULLIF(r.engagement_n, 0), 0) AS avg_product_engagement_score
FROM pg_temp.snap_p_run r
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL generate_series(0, COALESCE(r.nxt - 1, b.l) - r.day) AS g(k)
JOIN pg_temp.snap_cal c ON c.d = r.day + g.k
LEFT JOIN pg_temp.snap_p_tmp t ON t.ent = r.ent AND t.d = c.d;

-- product_base on every date: every product version on every date it is in force.
CREATE TEMP TABLE pg_temp.snap_p_base ON COMMIT DROP AS
SELECT
    c.d AS snapshot_date,
    cp.product_id,
    CASE cp.product_id WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' ELSE 'known' END AS member_type,
    cp.product_name,
    cp.category,
    cp.brand,
    cp.product_status,
    ROUND(COALESCE(cp.original_price, 0), 2) AS original_price,
    ROUND(COALESCE(cp.current_price, 0), 2) AS current_price,
    cp.inventory_remaining,

    COALESCE(s.order_count, 0)::BIGINT AS order_count,
    COALESCE(s.purchasing_customer_count, 0)::BIGINT AS purchasing_customer_count,
    COALESCE(s.units_sold, 0)::BIGINT AS units_sold,
    ROUND(COALESCE(s.product_revenue, 0), 2) AS product_revenue,
    ROUND(COALESCE(s.net_sales, 0), 2) AS net_sales,
    ROUND(COALESCE(s.refund_amount, 0), 2) AS refund_amount,
    ROUND(COALESCE(s.average_selling_price, 0), 2) AS average_selling_price,
    s.first_sold_timestamp,
    s.last_sold_timestamp,
    COALESCE(s.cancelled_units, 0)::BIGINT AS cancelled_units,
    COALESCE(s.returned_units, 0)::BIGINT AS returned_units,
    ROUND(COALESCE(s.returned_amount, 0), 2) AS returned_amount,

    COALESCE(s.product_web_event_count, 0)::BIGINT AS product_web_event_count,
    COALESCE(s.product_session_count, 0)::BIGINT AS product_session_count,
    COALESCE(s.product_view_event_count, 0)::BIGINT AS product_view_event_count,
    COALESCE(s.cart_event_count, 0)::BIGINT AS cart_event_count,
    COALESCE(s.purchase_event_count, 0)::BIGINT AS purchase_event_count,

    ROUND(
        CASE
            WHEN COALESCE(s.product_session_count, 0) = 0 THEN 0
            ELSE LEAST(
                    COALESCE(s.purchase_session_count, 0),
                    COALESCE(s.product_session_count, 0)
                 )::NUMERIC
                 / NULLIF(s.product_session_count, 0) * 100
        END,
        2
    ) AS product_conversion_rate_pct,

    ROUND(COALESCE(s.avg_product_engagement_score, 0), 4) AS avg_product_engagement_score
FROM pg_temp.snap_vpr vp
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL generate_series(0, LEAST(vp.e - 1, b.l) - GREATEST(vp.v, b.f)) AS g(k)
JOIN pg_temp.snap_cal c ON c.d = GREATEST(vp.v, b.f) + g.k
JOIN warehouse.dim_product cp ON cp.product_sk = vp.sk
LEFT JOIN pg_temp.snap_p_val s ON s.product_id = vp.id AND s.d = c.d;

INSERT INTO marts.mart_product_daily_snapshot (
    snapshot_date, product_id, member_type, product_name, category, brand, product_status, original_price,
    current_price, inventory_remaining, order_count, purchasing_customer_count, units_sold, product_revenue,
    net_sales, refund_amount, average_selling_price, first_sold_timestamp, last_sold_timestamp, cancelled_units,
    returned_units, returned_amount, product_web_event_count, product_session_count, product_view_event_count,
    cart_event_count, purchase_event_count, product_conversion_rate_pct, avg_product_engagement_score,
    category_revenue_rank, category_revenue_share_pct)
SELECT m.*
FROM (
    SELECT
        snapshot_date, product_id, member_type, product_name, category, brand, product_status, original_price,
        current_price, inventory_remaining, order_count, purchasing_customer_count, units_sold, product_revenue,
        net_sales, refund_amount, average_selling_price, first_sold_timestamp, last_sold_timestamp, cancelled_units,
        returned_units, returned_amount, product_web_event_count, product_session_count, product_view_event_count,
        cart_event_count, purchase_event_count, product_conversion_rate_pct, avg_product_engagement_score,

        DENSE_RANK() OVER (
            PARTITION BY snapshot_date, category
            ORDER BY product_revenue DESC, units_sold DESC, product_id
        ) AS category_revenue_rank,

        ROUND(
            CASE
                WHEN SUM(product_revenue) OVER (PARTITION BY snapshot_date, category) = 0 THEN 0
                ELSE product_revenue / NULLIF(SUM(product_revenue) OVER (PARTITION BY snapshot_date, category), 0) * 100
            END,
            2
        ) AS category_revenue_share_pct
    FROM pg_temp.snap_p_base
) m
WHERE m.order_count + m.cancelled_units + m.product_web_event_count > 0;

-- The product helpers are no longer needed. TRUNCATE gives their disk back
-- now (they were created in this transaction); DROP alone would keep it to the
-- end of the build transaction. The same for every helper below.
TRUNCATE pg_temp.snap_p_base, pg_temp.snap_p_val, pg_temp.snap_p_run, pg_temp.snap_p_tmp, pg_temp.snap_p_k,
         pg_temp.snap_p_d, pg_temp.snap_p_web, pg_temp.snap_p_itm;
DROP TABLE pg_temp.snap_p_base, pg_temp.snap_p_val, pg_temp.snap_p_run, pg_temp.snap_p_tmp, pg_temp.snap_p_k,
           pg_temp.snap_p_d, pg_temp.snap_p_web, pg_temp.snap_p_itm;

-- Campaigns (mart_campaign_performance; no windows).

CREATE TEMP TABLE pg_temp.snap_k_ord ON COMMIT DROP AS
SELECT vk.id AS ent, GREATEST(o.p, vk.v) AS a, o.s,
       o.order_status = 'cancelled' AS is_c, o.order_status = 'returned' AS is_r,
       o.customer_id, o.net_revenue, o.refund_amount
FROM pg_temp.snap_ord o
JOIN pg_temp.snap_vca vk ON vk.sk = o.campaign_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(o.p, vk.v) <= b.l;

CREATE TEMP TABLE pg_temp.snap_k_web ON COMMIT DROP AS
SELECT vk.id AS ent, GREATEST(fwe.p, vk.v) AS a, fwe.*
FROM pg_temp.snap_web fwe
JOIN pg_temp.snap_vca vk ON vk.sk = fwe.campaign_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(fwe.p, vk.v) <= b.l;

CREATE TEMP TABLE pg_temp.snap_k_d (
    ent TEXT NOT NULL, day DATE NOT NULL,
    spend NUMERIC, impressions NUMERIC, clicks NUMERIC, platform_conversions NUMERIC, platform_revenue NUMERIC,
    order_n BIGINT, net NUMERIC, refund NUMERIC, cancelled_n BIGINT, cancelled_amount NUMERIC,
    returned_n BIGINT, returned_amount NUMERIC, customer_n BIGINT,
    web_n BIGINT, session_n BIGINT, engaged_n BIGINT, purchase_n BIGINT,
    engagement_sum NUMERIC, engagement_n BIGINT, probability_sum NUMERIC, probability_n BIGINT,
    conversion_n BIGINT, conversion_value NUMERIC
) ON COMMIT DROP;

-- Sums: spend through the campaign version; it never leaves.
INSERT INTO pg_temp.snap_k_d (ent, day, spend, impressions, clicks, platform_conversions, platform_revenue)
SELECT vk.id, GREATEST(fcs.p, vk.v),
       SUM(fcs.spend_amount), SUM(fcs.impressions), SUM(fcs.clicks), SUM(fcs.conversions), SUM(fcs.attributed_revenue)
FROM pg_temp.snap_spd fcs
JOIN pg_temp.snap_vca vk ON vk.sk = fcs.campaign_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(fcs.p, vk.v) <= b.l
GROUP BY vk.id, GREATEST(fcs.p, vk.v);

-- Sums: orders through the campaign version, two change rows each.
INSERT INTO pg_temp.snap_k_d (ent, day, order_n, net, refund, cancelled_n, cancelled_amount, returned_n, returned_amount)
SELECT x.ent, ev.day,
       ev.nc,
       ev.nc * x.net_revenue,
       ev.fin * x.refund_amount,
       ev.fin * CASE WHEN x.is_c THEN 1 ELSE 0 END,
       ev.fin * CASE WHEN x.is_c THEN 1 ELSE 0 END * x.net_revenue,
       ev.fin * CASE WHEN x.is_r THEN 1 ELSE 0 END,
       ev.fin * CASE WHEN x.is_r THEN 1 ELSE 0 END * x.net_revenue
FROM pg_temp.snap_k_ord x
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL (VALUES
    (x.a, CASE WHEN NOT x.is_c OR x.s > x.a THEN 1 ELSE 0 END, 0),
    (GREATEST(x.a, x.s), CASE WHEN x.is_c AND x.s > x.a THEN -1 ELSE 0 END, 1)
) AS ev(day, nc, fin)
WHERE ev.day <= b.l
  AND (ev.fin = 0 OR x.is_c OR x.is_r OR x.refund_amount <> 0);

-- Sums: web events through the campaign version.
INSERT INTO pg_temp.snap_k_d (ent, day, web_n, purchase_n, engagement_sum, engagement_n, probability_sum, probability_n)
SELECT fwe.ent, fwe.a,
       COUNT(*),
       COUNT(*) FILTER (
           WHERE lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
              OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'),
       SUM(fwe.engagement_score), COUNT(fwe.engagement_score),
       SUM(fwe.purchase_probability), COUNT(fwe.purchase_probability)
FROM pg_temp.snap_k_web fwe
GROUP BY fwe.ent, fwe.a;

-- Sums: conversions through the campaign version.
INSERT INTO pg_temp.snap_k_d (ent, day, conversion_n, conversion_value)
SELECT vk.id, GREATEST(fc.p, vk.v), COUNT(*), SUM(fc.conversion_value)
FROM pg_temp.snap_cnv fc
JOIN pg_temp.snap_vca vk ON vk.sk = fc.campaign_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(fc.p, vk.v) <= b.l
GROUP BY vk.id, GREATEST(fc.p, vk.v);

-- Distinct counts: 1 purchasing customers (while the order is not cancelled), 2 sessions, 3 engaged customers.
CREATE TEMP TABLE pg_temp.snap_k_k (m SMALLINT NOT NULL, ent TEXT NOT NULL, k TEXT NOT NULL, st DATE NOT NULL, en DATE)
    ON COMMIT DROP;
INSERT INTO pg_temp.snap_k_k
SELECT 1, ent, customer_id, a, CASE WHEN is_c THEN s END FROM pg_temp.snap_k_ord
WHERE customer_id IS NOT NULL AND (NOT is_c OR s > a)
UNION ALL
SELECT 2, fwe.ent, fwe.session_id, min(fwe.a), NULL FROM pg_temp.snap_k_web fwe
WHERE fwe.session_id IS NOT NULL
GROUP BY fwe.ent, fwe.session_id
UNION ALL
SELECT 3, fwe.ent, fwe.customer_id, min(fwe.a), NULL FROM pg_temp.snap_k_web fwe
WHERE fwe.customer_id IS NOT NULL
GROUP BY fwe.ent, fwe.customer_id;

INSERT INTO pg_temp.snap_k_d (ent, day, customer_n, session_n, engaged_n)
SELECT ent, day, SUM(t) FILTER (WHERE m = 1), SUM(t) FILTER (WHERE m = 2), SUM(t) FILTER (WHERE m = 3)
FROM (
    SELECT m, ent, day,
           CASE WHEN cov - dlt = 0 AND cov > 0 THEN 1 WHEN cov - dlt > 0 AND cov = 0 THEN -1 ELSE 0 END AS t
    FROM (
        SELECT m, ent, k, day, dlt, SUM(dlt) OVER (PARTITION BY m, ent, k ORDER BY day) AS cov
        FROM (
            SELECT m, ent, k, day, SUM(sg) AS dlt
            FROM (
                SELECT m, ent, k, st AS day, 1 AS sg FROM pg_temp.snap_k_k
                UNION ALL
                SELECT m, ent, k, en, -1 FROM pg_temp.snap_k_k CROSS JOIN pg_temp.snap_b b WHERE en <= b.l
            ) edges
            GROUP BY m, ent, k, day
        ) per_day
    ) coverage
) transitions
WHERE t <> 0
GROUP BY ent, day;

CREATE TEMP TABLE pg_temp.snap_k_run ON COMMIT DROP AS
SELECT ent, day, LEAD(day) OVER (PARTITION BY ent ORDER BY day) AS nxt,
       SUM(SUM(spend)) OVER w AS spend, SUM(SUM(impressions)) OVER w AS impressions, SUM(SUM(clicks)) OVER w AS clicks,
       SUM(SUM(platform_conversions)) OVER w AS platform_conversions, SUM(SUM(platform_revenue)) OVER w AS platform_revenue,
       SUM(SUM(order_n)) OVER w AS order_n, SUM(SUM(net)) OVER w AS net, SUM(SUM(refund)) OVER w AS refund,
       SUM(SUM(cancelled_n)) OVER w AS cancelled_n, SUM(SUM(cancelled_amount)) OVER w AS cancelled_amount,
       SUM(SUM(returned_n)) OVER w AS returned_n, SUM(SUM(returned_amount)) OVER w AS returned_amount,
       SUM(SUM(customer_n)) OVER w AS customer_n,
       SUM(SUM(web_n)) OVER w AS web_n, SUM(SUM(session_n)) OVER w AS session_n, SUM(SUM(engaged_n)) OVER w AS engaged_n,
       SUM(SUM(purchase_n)) OVER w AS purchase_n,
       SUM(SUM(engagement_sum)) OVER w AS engagement_sum, SUM(SUM(engagement_n)) OVER w AS engagement_n,
       SUM(SUM(probability_sum)) OVER w AS probability_sum, SUM(SUM(probability_n)) OVER w AS probability_n,
       SUM(SUM(conversion_n)) OVER w AS conversion_n, SUM(SUM(conversion_value)) OVER w AS conversion_value
FROM pg_temp.snap_k_d
GROUP BY ent, day
WINDOW w AS (PARTITION BY ent ORDER BY day ROWS UNBOUNDED PRECEDING);

-- The CTE values of spend_by_campaign (s), orders_by_campaign (o), web_by_campaign (w) and
-- conversions_by_campaign (c), per campaign_id and calendar day, in one table (alias v below).
CREATE TEMP TABLE pg_temp.snap_k_val ON COMMIT DROP AS
SELECT r.ent AS campaign_id, c.d,
       COALESCE(r.spend, 0) AS total_spend,
       COALESCE(r.impressions, 0) AS impressions,
       COALESCE(r.clicks, 0) AS clicks,
       COALESCE(r.platform_conversions, 0) AS ad_platform_conversions,
       COALESCE(r.platform_revenue, 0) AS platform_attributed_revenue,
       r.order_n::BIGINT AS order_count,
       r.customer_n::BIGINT AS purchasing_customer_count,
       COALESCE(r.net, 0) - COALESCE(r.refund, 0) AS warehouse_attributed_revenue,
       COALESCE(r.net, 0) AS net_sales,
       COALESCE(r.refund, 0) AS refund_amount,
       COALESCE(r.net / NULLIF(r.order_n, 0), 0) AS average_order_value,
       r.cancelled_n::BIGINT AS cancelled_order_count,
       COALESCE(r.cancelled_amount, 0) AS cancelled_amount,
       r.returned_n::BIGINT AS returned_order_count,
       COALESCE(r.returned_amount, 0) AS returned_amount,
       r.web_n::BIGINT AS web_event_count,
       r.session_n::BIGINT AS session_count,
       r.engaged_n::BIGINT AS engaged_customer_count,
       r.purchase_n::BIGINT AS purchase_event_count,
       COALESCE(r.engagement_sum / NULLIF(r.engagement_n, 0), 0) AS avg_engagement_score,
       COALESCE(r.probability_sum / NULLIF(r.probability_n, 0), 0) AS avg_purchase_probability,
       r.conversion_n::BIGINT AS warehouse_conversion_count,
       COALESCE(r.conversion_value, 0) AS warehouse_conversion_value
FROM pg_temp.snap_k_run r
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL generate_series(0, COALESCE(r.nxt - 1, b.l) - r.day) AS g(k)
JOIN pg_temp.snap_cal c ON c.d = r.day + g.k;

INSERT INTO marts.mart_campaign_daily_snapshot (
    snapshot_date, campaign_id, member_type, campaign_name, campaign_type, channel, traffic_source, target_segment,
    campaign_status, ab_test_group, campaign_start_date, campaign_end_date, campaign_budget, total_spend,
    budget_remaining, impressions, clicks, ad_platform_conversions, web_event_count, session_count,
    engaged_customer_count, purchase_event_count, order_count, purchasing_customer_count, warehouse_conversion_count,
    attributed_revenue, net_sales, refund_amount, platform_attributed_revenue, warehouse_conversion_value,
    average_order_value, cancelled_order_count, cancelled_amount, returned_order_count, returned_amount,
    platform_roas, roas, click_through_rate_pct, cost_per_click, cost_per_order,
    session_to_order_conversion_rate_pct, avg_engagement_score, avg_purchase_probability)
SELECT m.*
FROM (
    SELECT
        v.d AS snapshot_date,
        cc.campaign_id,
        CASE cc.campaign_id WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' ELSE 'known' END AS member_type,
        cc.campaign_name,
        cc.campaign_type,
        cc.channel,
        cc.traffic_source,
        cc.target_segment,
        cc.campaign_status,
        cc.ab_test_group,
        cc.campaign_start_date,
        cc.campaign_end_date,
        ROUND(COALESCE(cc.budget, 0), 2) AS campaign_budget,

        ROUND(COALESCE(v.total_spend, 0), 2) AS total_spend,
        ROUND(COALESCE(cc.budget, 0) - COALESCE(v.total_spend, 0), 2) AS budget_remaining,

        COALESCE(v.impressions, 0)::BIGINT AS impressions,
        COALESCE(v.clicks, 0)::BIGINT AS clicks,
        COALESCE(v.ad_platform_conversions, 0)::BIGINT AS ad_platform_conversions,

        COALESCE(v.web_event_count, 0)::BIGINT AS web_event_count,
        COALESCE(v.session_count, 0)::BIGINT AS session_count,
        COALESCE(v.engaged_customer_count, 0)::BIGINT AS engaged_customer_count,
        COALESCE(v.purchase_event_count, 0)::BIGINT AS purchase_event_count,

        COALESCE(v.order_count, 0)::BIGINT AS order_count,
        COALESCE(v.purchasing_customer_count, 0)::BIGINT AS purchasing_customer_count,
        COALESCE(v.warehouse_conversion_count, 0)::BIGINT AS warehouse_conversion_count,

        ROUND(COALESCE(v.warehouse_attributed_revenue, 0), 2) AS attributed_revenue,
        ROUND(COALESCE(v.net_sales, 0), 2) AS net_sales,
        ROUND(COALESCE(v.refund_amount, 0), 2) AS refund_amount,
        ROUND(COALESCE(v.platform_attributed_revenue, 0), 2) AS platform_attributed_revenue,
        ROUND(COALESCE(v.warehouse_conversion_value, 0), 2) AS warehouse_conversion_value,
        ROUND(COALESCE(v.average_order_value, 0), 2) AS average_order_value,

        COALESCE(v.cancelled_order_count, 0)::BIGINT AS cancelled_order_count,
        ROUND(COALESCE(v.cancelled_amount, 0), 2) AS cancelled_amount,
        COALESCE(v.returned_order_count, 0)::BIGINT AS returned_order_count,
        ROUND(COALESCE(v.returned_amount, 0), 2) AS returned_amount,

        ROUND(
            CASE
                WHEN COALESCE(v.total_spend, 0) = 0 THEN 0
                ELSE COALESCE(v.platform_attributed_revenue, 0) / NULLIF(v.total_spend, 0)
            END,
            4
        ) AS platform_roas,

        ROUND(
            CASE
                WHEN COALESCE(v.total_spend, 0) = 0 THEN 0
                ELSE COALESCE(v.warehouse_attributed_revenue, 0) / NULLIF(v.total_spend, 0)
            END,
            4
        ) AS roas,

        ROUND(
            CASE
                WHEN COALESCE(v.impressions, 0) = 0 THEN 0
                ELSE COALESCE(v.clicks, 0)::NUMERIC / NULLIF(v.impressions, 0) * 100
            END,
            2
        ) AS click_through_rate_pct,

        ROUND(
            CASE
                WHEN COALESCE(v.clicks, 0) = 0 THEN 0
                ELSE COALESCE(v.total_spend, 0) / NULLIF(v.clicks, 0)
            END,
            4
        ) AS cost_per_click,

        ROUND(
            CASE
                WHEN COALESCE(v.order_count, 0) = 0 THEN 0
                ELSE COALESCE(v.total_spend, 0) / NULLIF(v.order_count, 0)
            END,
            4
        ) AS cost_per_order,

        ROUND(
            CASE
                WHEN COALESCE(v.session_count, 0) = 0 THEN 0
                ELSE COALESCE(v.order_count, 0)::NUMERIC / NULLIF(v.session_count, 0) * 100
            END,
            2
        ) AS session_to_order_conversion_rate_pct,

        ROUND(COALESCE(v.avg_engagement_score, 0), 4) AS avg_engagement_score,
        ROUND(COALESCE(v.avg_purchase_probability, 0), 4) AS avg_purchase_probability
    FROM pg_temp.snap_k_val v
    JOIN pg_temp.snap_vca vk ON vk.id = v.campaign_id AND vk.v <= v.d AND v.d < vk.e
    JOIN warehouse.dim_campaign cc ON cc.campaign_sk = vk.sk
) m
WHERE m.order_count + m.cancelled_order_count + m.web_event_count + m.warehouse_conversion_count
      + m.impressions > 0 OR m.total_spend > 0;

-- The campaign helpers are no longer needed.
TRUNCATE pg_temp.snap_k_val, pg_temp.snap_k_run, pg_temp.snap_k_k, pg_temp.snap_k_d, pg_temp.snap_k_web,
         pg_temp.snap_k_ord;
DROP TABLE pg_temp.snap_k_val, pg_temp.snap_k_run, pg_temp.snap_k_k, pg_temp.snap_k_d, pg_temp.snap_k_web,
           pg_temp.snap_k_ord;

-- Customers (mart_customer_360). NTILE over the customers with at least one order known on the date.

-- Orders through the customer version.
CREATE TEMP TABLE pg_temp.snap_c_ord ON COMMIT DROP AS
SELECT vc.id AS ent, GREATEST(o.p, vc.v) AS a, o.s,
       o.order_status = 'cancelled' AS is_c, o.order_status = 'returned' AS is_r,
       o.order_timestamp, o.gross_amount, o.discount_amount, o.net_revenue, o.refund_amount
FROM pg_temp.snap_ord o
JOIN pg_temp.snap_vcu vc ON vc.sk = o.customer_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(o.p, vc.v) <= b.l;

-- Items through the item's customer version; the product version only for the two distinct keys (a LEFT JOIN in
-- mart_customer_360: absent before its own v, so those keys start at GREATEST(a, v of the product version)).
CREATE TEMP TABLE pg_temp.snap_c_itm ON COMMIT DROP AS
SELECT vc.id AS ent, GREATEST(x.p, vc.v) AS a, x.s, x.order_status = 'cancelled' AS is_c,
       x.quantity, vp.v AS product_v, vp.category, vp.id AS product_id
FROM pg_temp.snap_itm x
JOIN pg_temp.snap_vcu vc ON vc.sk = x.customer_sk
LEFT JOIN pg_temp.snap_vpr vp ON vp.sk = x.product_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(x.p, vc.v) <= b.l;

-- Web events through the customer version.
CREATE TEMP TABLE pg_temp.snap_c_web ON COMMIT DROP AS
SELECT vc.id AS ent, GREATEST(fwe.p, vc.v) AS a, fwe.*
FROM pg_temp.snap_web fwe
JOIN pg_temp.snap_vcu vc ON vc.sk = fwe.customer_sk
CROSS JOIN pg_temp.snap_b b
WHERE GREATEST(fwe.p, vc.v) <= b.l;

-- The shared facts and the product and campaign versions are no longer needed.
TRUNCATE pg_temp.snap_ord, pg_temp.snap_itm, pg_temp.snap_web, pg_temp.snap_spd, pg_temp.snap_cnv,
         pg_temp.snap_vpr, pg_temp.snap_vca;
DROP TABLE pg_temp.snap_ord, pg_temp.snap_itm, pg_temp.snap_web, pg_temp.snap_spd, pg_temp.snap_cnv,
           pg_temp.snap_vpr, pg_temp.snap_vca;

CREATE TEMP TABLE pg_temp.snap_c_d (
    ent TEXT NOT NULL, day DATE NOT NULL,
    any_n BIGINT, order_n BIGINT, gross NUMERIC, discount NUMERIC, net NUMERIC, refund NUMERIC,
    cancelled_n BIGINT, cancelled_amount NUMERIC, returned_n BIGINT, returned_amount NUMERIC,
    units BIGINT, category_n BIGINT, product_n BIGINT,
    web_n BIGINT, session_n BIGINT, intent_n BIGINT, purchase_session_n BIGINT,
    engagement_sum NUMERIC, engagement_n BIGINT, probability_sum NUMERIC, probability_n BIGINT,
    abandonment_sum NUMERIC, abandonment_n BIGINT,
    first_ts TIMESTAMPTZ, last_ts TIMESTAMPTZ
) ON COMMIT DROP;

-- Sums: orders, two change rows each; any_n counts every order of any status (the NTILE population).
INSERT INTO pg_temp.snap_c_d (ent, day, any_n, order_n, gross, discount, net, refund, cancelled_n, cancelled_amount,
                     returned_n, returned_amount)
SELECT x.ent, ev.day,
       1 - ev.fin,
       ev.nc,
       ev.nc * x.gross_amount,
       ev.nc * x.discount_amount,
       ev.nc * x.net_revenue,
       ev.fin * x.refund_amount,
       ev.fin * CASE WHEN x.is_c THEN 1 ELSE 0 END,
       ev.fin * CASE WHEN x.is_c THEN 1 ELSE 0 END * x.net_revenue,
       ev.fin * CASE WHEN x.is_r THEN 1 ELSE 0 END,
       ev.fin * CASE WHEN x.is_r THEN 1 ELSE 0 END * x.net_revenue
FROM pg_temp.snap_c_ord x
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL (VALUES
    (x.a, CASE WHEN NOT x.is_c OR x.s > x.a THEN 1 ELSE 0 END, 0),
    (GREATEST(x.a, x.s), CASE WHEN x.is_c AND x.s > x.a THEN -1 ELSE 0 END, 1)
) AS ev(day, nc, fin)
WHERE ev.day <= b.l
  AND (ev.fin = 0 OR x.is_c OR x.is_r OR x.refund_amount <> 0);

-- Sums: units of items of orders not cancelled.
INSERT INTO pg_temp.snap_c_d (ent, day, units)
SELECT x.ent, ev.day, ev.nc * x.quantity
FROM pg_temp.snap_c_itm x
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL (VALUES
    (x.a, CASE WHEN NOT x.is_c OR x.s > x.a THEN 1 ELSE 0 END),
    (GREATEST(x.a, x.s), CASE WHEN x.is_c AND x.s > x.a THEN -1 ELSE 0 END)
) AS ev(day, nc)
WHERE ev.day <= b.l AND ev.nc <> 0;

-- Sums: web events.
INSERT INTO pg_temp.snap_c_d (ent, day, web_n, intent_n, engagement_sum, engagement_n, probability_sum, probability_n,
                     abandonment_sum, abandonment_n)
SELECT fwe.ent, fwe.a,
       COUNT(*),
       COUNT(*) FILTER (
           WHERE lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
              OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'),
       SUM(fwe.engagement_score), COUNT(fwe.engagement_score),
       SUM(fwe.purchase_probability), COUNT(fwe.purchase_probability),
       SUM(fwe.cart_abandonment_probability), COUNT(fwe.cart_abandonment_probability)
FROM pg_temp.snap_c_web fwe
GROUP BY fwe.ent, fwe.a;

-- Distinct counts: 1 categories and 2 products while the item counts and its product version is present;
-- 3 sessions and 4 purchase sessions from first sight.
CREATE TEMP TABLE pg_temp.snap_c_k (m SMALLINT NOT NULL, ent TEXT NOT NULL, k TEXT NOT NULL, st DATE NOT NULL, en DATE)
    ON COMMIT DROP;
INSERT INTO pg_temp.snap_c_k
SELECT 1, x.ent, x.category, GREATEST(x.a, x.product_v), CASE WHEN x.is_c THEN x.s END
FROM pg_temp.snap_c_itm x CROSS JOIN pg_temp.snap_b b
WHERE x.category IS NOT NULL
  AND GREATEST(x.a, x.product_v) <= b.l
  AND (NOT x.is_c OR x.s > GREATEST(x.a, x.product_v))
UNION ALL
SELECT 2, x.ent, x.product_id, GREATEST(x.a, x.product_v), CASE WHEN x.is_c THEN x.s END
FROM pg_temp.snap_c_itm x CROSS JOIN pg_temp.snap_b b
WHERE x.product_id NOT IN ('UNKNOWN', 'INVALID')
  AND GREATEST(x.a, x.product_v) <= b.l
  AND (NOT x.is_c OR x.s > GREATEST(x.a, x.product_v))
UNION ALL
SELECT 3, fwe.ent, fwe.session_id, min(fwe.a), NULL FROM pg_temp.snap_c_web fwe
WHERE fwe.session_id IS NOT NULL
GROUP BY fwe.ent, fwe.session_id
UNION ALL
SELECT 4, fwe.ent, fwe.session_id, min(fwe.a), NULL FROM pg_temp.snap_c_web fwe
WHERE fwe.session_id IS NOT NULL
  AND (
        lower(fwe.event_type) IN ('purchase', 'conversion', 'order_completed')
     OR lower(COALESCE(fwe.user_journey_stage, '')) LIKE '%purchase%'
  )
GROUP BY fwe.ent, fwe.session_id;

INSERT INTO pg_temp.snap_c_d (ent, day, category_n, product_n, session_n, purchase_session_n)
SELECT ent, day,
       SUM(t) FILTER (WHERE m = 1), SUM(t) FILTER (WHERE m = 2), SUM(t) FILTER (WHERE m = 3), SUM(t) FILTER (WHERE m = 4)
FROM (
    SELECT m, ent, day,
           CASE WHEN cov - dlt = 0 AND cov > 0 THEN 1 WHEN cov - dlt > 0 AND cov = 0 THEN -1 ELSE 0 END AS t
    FROM (
        SELECT m, ent, k, day, dlt, SUM(dlt) OVER (PARTITION BY m, ent, k ORDER BY day) AS cov
        FROM (
            SELECT m, ent, k, day, SUM(sg) AS dlt
            FROM (
                SELECT m, ent, k, st AS day, 1 AS sg FROM pg_temp.snap_c_k
                UNION ALL
                SELECT m, ent, k, en, -1 FROM pg_temp.snap_c_k CROSS JOIN pg_temp.snap_b b WHERE en <= b.l
            ) edges
            GROUP BY m, ent, k, day
        ) per_day
    ) coverage
) transitions
WHERE t <> 0
GROUP BY ent, day;

-- First and last order: orders that never leave the not-cancelled set.
INSERT INTO pg_temp.snap_c_d (ent, day, first_ts, last_ts)
SELECT ent, a, MIN(order_timestamp), MAX(order_timestamp)
FROM pg_temp.snap_c_ord
WHERE NOT is_c
GROUP BY ent, a;

CREATE TEMP TABLE pg_temp.snap_c_tmp ON COMMIT DROP AS
SELECT x.ent, x.a + g.k AS d, MIN(x.order_timestamp) AS first_ts, MAX(x.order_timestamp) AS last_ts
FROM pg_temp.snap_c_ord x
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL generate_series(0, LEAST(x.s - 1, b.l) - x.a) AS g(k)
WHERE x.is_c AND x.s > x.a
GROUP BY x.ent, x.a + g.k;

CREATE TEMP TABLE pg_temp.snap_c_run ON COMMIT DROP AS
SELECT ent, day, LEAD(day) OVER (PARTITION BY ent ORDER BY day) AS nxt,
       SUM(SUM(any_n)) OVER w AS any_n, SUM(SUM(order_n)) OVER w AS order_n,
       SUM(SUM(gross)) OVER w AS gross, SUM(SUM(discount)) OVER w AS discount,
       SUM(SUM(net)) OVER w AS net, SUM(SUM(refund)) OVER w AS refund,
       SUM(SUM(cancelled_n)) OVER w AS cancelled_n, SUM(SUM(cancelled_amount)) OVER w AS cancelled_amount,
       SUM(SUM(returned_n)) OVER w AS returned_n, SUM(SUM(returned_amount)) OVER w AS returned_amount,
       SUM(SUM(units)) OVER w AS units, SUM(SUM(category_n)) OVER w AS category_n, SUM(SUM(product_n)) OVER w AS product_n,
       SUM(SUM(web_n)) OVER w AS web_n, SUM(SUM(session_n)) OVER w AS session_n, SUM(SUM(intent_n)) OVER w AS intent_n,
       SUM(SUM(purchase_session_n)) OVER w AS purchase_session_n,
       SUM(SUM(engagement_sum)) OVER w AS engagement_sum, SUM(SUM(engagement_n)) OVER w AS engagement_n,
       SUM(SUM(probability_sum)) OVER w AS probability_sum, SUM(SUM(probability_n)) OVER w AS probability_n,
       SUM(SUM(abandonment_sum)) OVER w AS abandonment_sum, SUM(SUM(abandonment_n)) OVER w AS abandonment_n,
       MIN(MIN(first_ts)) OVER w AS first_ts, MAX(MAX(last_ts)) OVER w AS last_ts
FROM pg_temp.snap_c_d
GROUP BY ent, day
WINDOW w AS (PARTITION BY ent ORDER BY day ROWS UNBOUNDED PRECEDING);

-- Only the running values and the cancelled orders' first and last timestamps are needed from here on.
TRUNCATE pg_temp.snap_c_d, pg_temp.snap_c_k, pg_temp.snap_c_web, pg_temp.snap_c_itm, pg_temp.snap_c_ord;
DROP TABLE pg_temp.snap_c_d, pg_temp.snap_c_k, pg_temp.snap_c_web, pg_temp.snap_c_itm, pg_temp.snap_c_ord;

-- The CTE values of orders_by_customer (o), items_by_customer (i) and web_by_customer (w) per customer_id and day.
CREATE TEMP TABLE pg_temp.snap_c_val ON COMMIT DROP AS
SELECT r.ent AS customer_id, c.d,
       COALESCE(r.any_n, 0) AS any_order_count,
       r.order_n::BIGINT AS total_order_count,
       COALESCE(r.gross, 0) AS gross_revenue,
       COALESCE(r.discount, 0) AS total_discount_amount,
       COALESCE(r.net, 0) - COALESCE(r.refund, 0) AS lifetime_value,
       COALESCE(r.net, 0) AS net_sales,
       COALESCE(r.refund, 0) AS refund_amount,
       COALESCE(r.net / NULLIF(r.order_n, 0), 0) AS average_order_value,
       LEAST(r.first_ts, t.first_ts) AS first_order_timestamp,
       GREATEST(r.last_ts, t.last_ts) AS last_order_timestamp,
       r.cancelled_n::BIGINT AS cancelled_order_count,
       COALESCE(r.cancelled_amount, 0) AS cancelled_amount,
       r.returned_n::BIGINT AS returned_order_count,
       COALESCE(r.returned_amount, 0) AS returned_amount,
       COALESCE(r.units, 0) AS total_units_purchased,
       r.category_n::BIGINT AS product_categories_purchased,
       r.product_n::BIGINT AS distinct_products_purchased,
       r.web_n::BIGINT AS web_event_count,
       r.session_n::BIGINT AS session_count,
       r.intent_n::BIGINT AS purchase_intent_event_count,
       r.purchase_session_n::BIGINT AS purchase_session_count,
       COALESCE(r.engagement_sum / NULLIF(r.engagement_n, 0), 0) AS avg_engagement_score,
       COALESCE(r.probability_sum / NULLIF(r.probability_n, 0), 0) AS avg_purchase_probability,
       COALESCE(r.abandonment_sum / NULLIF(r.abandonment_n, 0), 0) AS avg_cart_abandonment_probability
FROM pg_temp.snap_c_run r
CROSS JOIN pg_temp.snap_b b
CROSS JOIN LATERAL generate_series(0, COALESCE(r.nxt - 1, b.l) - r.day) AS g(k)
JOIN pg_temp.snap_cal c ON c.d = r.day + g.k
LEFT JOIN pg_temp.snap_c_tmp t ON t.ent = r.ent AND t.d = c.d;

-- Only the values per customer and date are needed from here on.
TRUNCATE pg_temp.snap_c_run, pg_temp.snap_c_tmp;
DROP TABLE pg_temp.snap_c_run, pg_temp.snap_c_tmp;

-- value_third over the groups of orders_by_customer on the date: the customers with at least one order known
-- then, of any status, whether or not they have a version in force.
CREATE TEMP TABLE pg_temp.snap_c_nt ON COMMIT DROP AS
SELECT customer_id, d,
       NTILE(3) OVER (
           PARTITION BY d, (lifetime_value > 0 AND customer_id NOT IN ('UNKNOWN', 'INVALID'))
           ORDER BY lifetime_value DESC, customer_id
       ) AS value_third
FROM pg_temp.snap_c_val
WHERE any_order_count >= 1;

INSERT INTO marts.mart_customer_daily_snapshot (
    snapshot_date, customer_id, member_type, customer_name, email, gender, age, membership_tier, loyalty_points,
    preferred_language, customer_segment, is_prime_user, home_city, home_state, country, total_order_count,
    gross_revenue, total_discount_amount, lifetime_value, net_sales, refund_amount, average_order_value,
    cancelled_order_count, cancelled_amount, returned_order_count, returned_amount, total_units_purchased,
    product_categories_purchased, distinct_products_purchased, first_order_timestamp, last_order_timestamp,
    days_since_last_order, repeat_purchase_signal, customer_value_segment, web_event_count, session_count,
    purchase_intent_event_count, customer_conversion_rate_pct, avg_engagement_score, avg_purchase_probability,
    avg_cart_abandonment_probability)
SELECT m.*
FROM (
    SELECT
        v.d AS snapshot_date,
        cc.customer_id,
        CASE cc.customer_id WHEN 'UNKNOWN' THEN 'unknown_member' WHEN 'INVALID' THEN 'invalid_reference' ELSE 'known' END AS member_type,
        cc.customer_name,
        cc.email,
        cc.gender,
        cc.age,
        cc.membership_tier,
        cc.loyalty_points,
        cc.preferred_language,
        cc.customer_segment,
        cc.is_prime_user,
        cc.home_city,
        cc.home_state,
        cc.country,

        COALESCE(v.total_order_count, 0)::BIGINT AS total_order_count,
        ROUND(COALESCE(v.gross_revenue, 0), 2) AS gross_revenue,
        ROUND(COALESCE(v.total_discount_amount, 0), 2) AS total_discount_amount,
        ROUND(COALESCE(v.lifetime_value, 0), 2) AS lifetime_value,
        ROUND(COALESCE(v.net_sales, 0), 2) AS net_sales,
        ROUND(COALESCE(v.refund_amount, 0), 2) AS refund_amount,
        ROUND(COALESCE(v.average_order_value, 0), 2) AS average_order_value,

        COALESCE(v.cancelled_order_count, 0)::BIGINT AS cancelled_order_count,
        ROUND(COALESCE(v.cancelled_amount, 0), 2) AS cancelled_amount,
        COALESCE(v.returned_order_count, 0)::BIGINT AS returned_order_count,
        ROUND(COALESCE(v.returned_amount, 0), 2) AS returned_amount,

        COALESCE(v.total_units_purchased, 0)::BIGINT AS total_units_purchased,
        COALESCE(v.product_categories_purchased, 0)::BIGINT AS product_categories_purchased,
        COALESCE(v.distinct_products_purchased, 0)::BIGINT AS distinct_products_purchased,

        v.first_order_timestamp,
        v.last_order_timestamp,

        -- Recency is counted to the snapshot date itself (the lifetime mart counts it to the latest order date).
        (v.d - warehouse.business_date(v.last_order_timestamp)) AS days_since_last_order,

        (COALESCE(v.total_order_count, 0) >= 2) AS repeat_purchase_signal,

        CASE
            WHEN COALESCE(v.lifetime_value, 0) <= 0 THEN 'No Purchase Yet'
            WHEN nt.value_third = 1 THEN 'High Value'
            WHEN nt.value_third = 2 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_value_segment,

        COALESCE(v.web_event_count, 0)::BIGINT AS web_event_count,
        COALESCE(v.session_count, 0)::BIGINT AS session_count,
        COALESCE(v.purchase_intent_event_count, 0)::BIGINT AS purchase_intent_event_count,

        ROUND(
            CASE
                WHEN COALESCE(v.session_count, 0) = 0 THEN 0
                ELSE LEAST(
                        COALESCE(v.purchase_session_count, 0),
                        COALESCE(v.session_count, 0)
                     )::NUMERIC
                     / NULLIF(v.session_count, 0) * 100
            END,
            2
        ) AS customer_conversion_rate_pct,

        ROUND(COALESCE(v.avg_engagement_score, 0), 4) AS avg_engagement_score,
        ROUND(COALESCE(v.avg_purchase_probability, 0), 4) AS avg_purchase_probability,
        ROUND(COALESCE(v.avg_cart_abandonment_probability, 0), 4) AS avg_cart_abandonment_probability
    FROM pg_temp.snap_c_val v
    JOIN pg_temp.snap_vcu vc ON vc.id = v.customer_id AND vc.v <= v.d AND v.d < vc.e
    JOIN warehouse.dim_customer cc ON cc.customer_sk = vc.sk
    LEFT JOIN pg_temp.snap_c_nt nt ON nt.customer_id = v.customer_id AND nt.d = v.d
) m
WHERE m.total_order_count + m.cancelled_order_count + m.web_event_count > 0;

-- The last helpers.
TRUNCATE pg_temp.snap_c_nt, pg_temp.snap_c_val, pg_temp.snap_vcu, pg_temp.snap_b, pg_temp.snap_cal;
DROP TABLE pg_temp.snap_c_nt, pg_temp.snap_c_val, pg_temp.snap_vcu, pg_temp.snap_b, pg_temp.snap_cal;

ALTER TABLE marts.mart_customer_daily_snapshot
    ALTER COLUMN snapshot_date SET NOT NULL, ALTER COLUMN customer_id SET NOT NULL,
    ADD PRIMARY KEY (customer_id, snapshot_date);
ALTER TABLE marts.mart_product_daily_snapshot
    ALTER COLUMN snapshot_date SET NOT NULL, ALTER COLUMN product_id SET NOT NULL,
    ADD PRIMARY KEY (product_id, snapshot_date);
ALTER TABLE marts.mart_campaign_daily_snapshot
    ALTER COLUMN snapshot_date SET NOT NULL, ALTER COLUMN campaign_id SET NOT NULL,
    ADD PRIMARY KEY (campaign_id, snapshot_date);

ANALYZE marts.mart_customer_daily_snapshot, marts.mart_product_daily_snapshot, marts.mart_campaign_daily_snapshot;

COMMENT ON TABLE marts.mart_customer_daily_snapshot IS
'One row per customer and business date from the customer''s first activity: the columns of mart_customer_360 as of the end of snapshot_date, computed only from what was known then (orders, order statuses, refunds, web events and dimension versions up to that date).';
COMMENT ON TABLE marts.mart_product_daily_snapshot IS
'One row per product and business date from the product''s first activity: the columns of mart_product_sales as of the end of snapshot_date, computed only from what was known then.';
COMMENT ON TABLE marts.mart_campaign_daily_snapshot IS
'One row per campaign and business date from the campaign''s first activity: the columns of mart_campaign_performance as of the end of snapshot_date, computed only from what was known then (spend, orders, refunds, web events and the budget in force up to that date).';

-- =============================================================================
-- Mart Validation Audit
-- =============================================================================

DROP TABLE IF EXISTS audit.mart_validation_report CASCADE;

CREATE TABLE audit.mart_validation_report (
    validation_id BIGSERIAL PRIMARY KEY,
    validation_run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_category TEXT NOT NULL,
    source_object TEXT,
    target_object TEXT,
    metric_name TEXT NOT NULL,
    source_value NUMERIC(38, 6),
    target_value NUMERIC(38, 6),
    difference NUMERIC(38, 6),
    tolerance NUMERIC(38, 6) NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    details TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE audit.mart_validation_report IS
'Audit table for validating BI-ready marts against warehouse facts and expected metric rules.';

CREATE TEMP TABLE mart_validation_context AS
SELECT NULLIF(current_setting('p2.run_id', true), '') AS validation_run_id;

-- 1. All five mart views exist.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'expected_mart_view_count',
    'structure',
    'information_schema.views',
    'marts',
    'view_count',
    5,
    COUNT(*)::NUMERIC,
    ABS(5 - COUNT(*))::NUMERIC,
    0,
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END,
    'Validates that all five BI-ready mart views exist.'
FROM mart_validation_context mvc
CROSS JOIN information_schema.views v
WHERE v.table_schema = 'marts'
  AND v.table_name IN (
      'mart_customer_360',
      'mart_campaign_performance',
      'mart_product_sales',
      'mart_revenue_daily',
      'mart_marketing_funnel'
  )
GROUP BY mvc.validation_run_id;

-- 2. Daily revenue reconciles to warehouse fact_orders.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'daily_revenue_matches_fact_orders',
    'reconciliation',
    'warehouse.fact_orders',
    'marts.mart_revenue_daily',
    'net_revenue',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates total daily mart net revenue equals the net sales of the orders that were not cancelled in warehouse fact_orders (refunds are a column of their own).'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(fo.net_revenue), 0)::NUMERIC AS source_amount
    FROM warehouse.fact_orders fo
    WHERE fo.order_status <> 'cancelled'
) src
CROSS JOIN (
    SELECT COALESCE(SUM(net_revenue), 0)::NUMERIC AS target_amount
    FROM marts.mart_revenue_daily
) tgt;

-- 3. Product revenue reconciles to BI-visible product order items.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'product_revenue_matches_fact_order_items',
    'reconciliation',
    'warehouse.fact_order_items',
    'marts.mart_product_sales',
    'line_revenue',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates product mart revenue (every member row) equals the line revenue of the orders that were not cancelled in warehouse fact_order_items, less the item refunds.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (COALESCE(SUM(foi.line_revenue) FILTER (WHERE fo.order_status <> 'cancelled'), 0)
            - COALESCE(SUM(foi.refund_amount), 0))::NUMERIC AS source_amount
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo
        ON fo.order_sk = foi.order_sk
) src
CROSS JOIN (
    SELECT COALESCE(SUM(product_revenue), 0)::NUMERIC AS target_amount
    FROM marts.mart_product_sales
) tgt;

-- 4. Campaign spend reconciles to BI-visible campaign spend.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'campaign_spend_matches_fact_campaign_spend',
    'reconciliation',
    'warehouse.fact_campaign_spend',
    'marts.mart_campaign_performance',
    'spend_amount',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates campaign mart spend (every member row, summed over regions and days) equals warehouse fact_campaign_spend spend amount.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(fcs.spend_amount), 0)::NUMERIC AS source_amount
    FROM warehouse.fact_campaign_spend fcs
) src
CROSS JOIN (
    SELECT COALESCE(SUM(total_spend), 0)::NUMERIC AS target_amount
    FROM marts.mart_campaign_performance
) tgt;

-- 4a. Validates customer mart lifetime value (every member row) equals the net revenue of the completed orders.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'customer_lifetime_value_matches_fact_orders',
    'reconciliation',
    'warehouse.fact_orders',
    'marts.mart_customer_360',
    'net_revenue',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates customer mart lifetime value (every member row) equals the net sales of the orders that were not cancelled, less refunds.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (COALESCE(SUM(net_revenue) FILTER (WHERE order_status <> 'cancelled'), 0) - COALESCE(SUM(refund_amount), 0))::NUMERIC AS source_amount FROM warehouse.fact_orders
) src
CROSS JOIN (
    SELECT COALESCE(SUM(lifetime_value), 0)::NUMERIC AS target_amount FROM marts.mart_customer_360
) tgt;

-- 4b. Validates customer mart units (every member row) equal the units of the completed orders.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'customer_units_match_fact_order_items',
    'reconciliation',
    'warehouse.fact_order_items',
    'marts.mart_customer_360',
    'quantity',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates customer mart units (every member row) equal the units of the orders that were not cancelled.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(foi.quantity), 0)::NUMERIC AS source_amount FROM warehouse.fact_order_items foi JOIN warehouse.fact_orders fo ON fo.order_sk = foi.order_sk AND fo.order_status <> 'cancelled'
) src
CROSS JOIN (
    SELECT COALESCE(SUM(total_units_purchased), 0)::NUMERIC AS target_amount FROM marts.mart_customer_360
) tgt;

-- 4c. Validates campaign mart attributed revenue (every member row) equals the net revenue of the completed orders.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'campaign_revenue_matches_fact_orders',
    'reconciliation',
    'warehouse.fact_orders',
    'marts.mart_campaign_performance',
    'net_revenue',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates campaign mart attributed revenue (every member row) equals the net sales of the orders that were not cancelled, less refunds.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (COALESCE(SUM(net_revenue) FILTER (WHERE order_status <> 'cancelled'), 0) - COALESCE(SUM(refund_amount), 0))::NUMERIC AS source_amount FROM warehouse.fact_orders
) src
CROSS JOIN (
    SELECT COALESCE(SUM(attributed_revenue), 0)::NUMERIC AS target_amount FROM marts.mart_campaign_performance
) tgt;

-- 4d. Validates product mart units sold (every member row) equal the units of the completed orders.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'product_units_match_fact_order_items',
    'reconciliation',
    'warehouse.fact_order_items',
    'marts.mart_product_sales',
    'quantity',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates product mart units sold (every member row) equal the units of the orders that were not cancelled.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(foi.quantity), 0)::NUMERIC AS source_amount FROM warehouse.fact_order_items foi JOIN warehouse.fact_orders fo ON fo.order_sk = foi.order_sk AND fo.order_status <> 'cancelled'
) src
CROSS JOIN (
    SELECT COALESCE(SUM(units_sold), 0)::NUMERIC AS target_amount FROM marts.mart_product_sales
) tgt;

-- 4e. Validates campaign mart ad-platform conversions and attributed revenue (every member row) equal warehouse fact_campaign_spend.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'campaign_platform_results_match_fact_campaign_spend',
    'reconciliation',
    'warehouse.fact_campaign_spend',
    'marts.mart_campaign_performance',
    'attributed_revenue',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0.01,
    CASE WHEN ABS(src.source_amount - tgt.target_amount) <= 0.01 THEN 'PASS' ELSE 'FAIL' END,
    'Validates campaign mart ad-platform conversions and attributed revenue (every member row) equal warehouse fact_campaign_spend.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (COALESCE(SUM(attributed_revenue), 0) + COALESCE(SUM(conversions), 0))::NUMERIC AS source_amount FROM warehouse.fact_campaign_spend
) src
CROSS JOIN (
    SELECT (COALESCE(SUM(platform_attributed_revenue), 0) + COALESCE(SUM(ad_platform_conversions), 0))::NUMERIC AS target_amount FROM marts.mart_campaign_performance
) tgt;

-- 4f. Validates the refunds in the daily mart (on their return dates) equal the refunds in warehouse fact_orders, exactly.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'daily_refunds_match_fact_orders',
    'reconciliation',
    'warehouse.fact_orders',
    'marts.mart_revenue_daily',
    'refund_amount',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END,
    'Validates the refunds in the daily mart (on their return dates) equal the refunds in warehouse fact_orders, exactly.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS source_amount FROM warehouse.fact_orders
) src
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS target_amount FROM marts.mart_revenue_daily
) tgt;

-- 4g. Validates the refunds in the customer mart equal the refunds in warehouse fact_orders, exactly.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'customer_refunds_match_fact_orders',
    'reconciliation',
    'warehouse.fact_orders',
    'marts.mart_customer_360',
    'refund_amount',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END,
    'Validates the refunds in the customer mart equal the refunds in warehouse fact_orders, exactly.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS source_amount FROM warehouse.fact_orders
) src
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS target_amount FROM marts.mart_customer_360
) tgt;

-- 4h. Validates the refunds in the campaign mart equal the refunds in warehouse fact_orders, exactly.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'campaign_refunds_match_fact_orders',
    'reconciliation',
    'warehouse.fact_orders',
    'marts.mart_campaign_performance',
    'refund_amount',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END,
    'Validates the refunds in the campaign mart equal the refunds in warehouse fact_orders, exactly.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS source_amount FROM warehouse.fact_orders
) src
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS target_amount FROM marts.mart_campaign_performance
) tgt;

-- 4i. Validates the refunds in the product mart equal the item refunds in warehouse fact_order_items, exactly.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'product_refunds_match_fact_order_items',
    'reconciliation',
    'warehouse.fact_order_items',
    'marts.mart_product_sales',
    'refund_amount',
    src.source_amount,
    tgt.target_amount,
    ABS(src.source_amount - tgt.target_amount),
    0,
    CASE WHEN src.source_amount = tgt.target_amount THEN 'PASS' ELSE 'FAIL' END,
    'Validates the refunds in the product mart equal the item refunds in warehouse fact_order_items, exactly.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS source_amount FROM warehouse.fact_order_items
) src
CROSS JOIN (
    SELECT COALESCE(SUM(refund_amount), 0)::NUMERIC AS target_amount FROM marts.mart_product_sales
) tgt;

-- 4j-4l each read their lifetime mart twice: the whole row, then the activity
-- columns; check 7 reads them again. These checks read a copy instead, made
-- here in this transaction from the view (the same rows, column names, column
-- order and types). The copies are only ever named pg_temp.<copy>, so they
-- never stand in for marts.*, and they are dropped after check 7.
CREATE TEMP TABLE validation_customer_360 ON COMMIT DROP AS SELECT * FROM marts.mart_customer_360;
CREATE TEMP TABLE validation_product_sales ON COMMIT DROP AS SELECT * FROM marts.mart_product_sales;
CREATE TEMP TABLE validation_campaign_performance ON COMMIT DROP AS SELECT * FROM marts.mart_campaign_performance;
ANALYZE pg_temp.validation_customer_360, pg_temp.validation_product_sales, pg_temp.validation_campaign_performance;

-- 4j. The last date's snapshot is the lifetime mart.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'customer_snapshot_last_date_matches_lifetime_mart',
    'reconciliation',
    'marts.mart_customer_360',
    'marts.mart_customer_daily_snapshot',
    'differing_rows',
    0,
    diff.differing_rows,
    diff.differing_rows,
    0,
    CASE WHEN diff.differing_rows = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates that the snapshot of the last business date equals mart_customer_360 column for column, for every customer_id that has any activity, and that each (customer_id, snapshot_date) appears once.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (
        (SELECT COUNT(*) FROM marts.mart_customer_daily_snapshot s
         WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM marts.mart_customer_daily_snapshot)
           -- days_since_last_order is counted to snapshot_date in the snapshot
           -- and to the latest order date in the lifetime mart: compared apart.
           AND NOT EXISTS (SELECT 1 FROM pg_temp.validation_customer_360 m
                           WHERE to_jsonb(m) - 'days_since_last_order'
                                 = to_jsonb(s) - 'snapshot_date' - 'days_since_last_order'))
      + (SELECT COUNT(*) FROM marts.mart_customer_daily_snapshot s
         WHERE s.days_since_last_order IS DISTINCT FROM s.snapshot_date - warehouse.business_date(s.last_order_timestamp))
      + (SELECT COUNT(*) FROM pg_temp.validation_customer_360 m
         WHERE (m.total_order_count + m.cancelled_order_count + m.web_event_count > 0)
           AND NOT EXISTS (SELECT 1 FROM marts.mart_customer_daily_snapshot s
                           WHERE s.customer_id = m.customer_id
                             AND s.snapshot_date = (SELECT MAX(snapshot_date) FROM marts.mart_customer_daily_snapshot)))
      + (SELECT COUNT(*) FROM (SELECT customer_id, COUNT(*) AS days, MAX(snapshot_date) - MIN(snapshot_date) + 1 AS span
                               FROM marts.mart_customer_daily_snapshot GROUP BY customer_id) continuous
         WHERE continuous.days <> continuous.span)
    )::NUMERIC AS differing_rows
) diff;

-- 4k. The last date's snapshot is the lifetime mart.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'product_snapshot_last_date_matches_lifetime_mart',
    'reconciliation',
    'marts.mart_product_sales',
    'marts.mart_product_daily_snapshot',
    'differing_rows',
    0,
    diff.differing_rows,
    diff.differing_rows,
    0,
    CASE WHEN diff.differing_rows = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates that the snapshot of the last business date equals mart_product_sales column for column, for every product_id that has any activity, and that each (product_id, snapshot_date) appears once.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (
        (SELECT COUNT(*) FROM marts.mart_product_daily_snapshot s
         WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM marts.mart_product_daily_snapshot)
           AND NOT EXISTS (SELECT 1 FROM pg_temp.validation_product_sales m
                           WHERE to_jsonb(m) = to_jsonb(s) - 'snapshot_date'))
      + (SELECT COUNT(*) FROM pg_temp.validation_product_sales m
         WHERE (m.order_count + m.cancelled_units + m.product_web_event_count > 0)
           AND NOT EXISTS (SELECT 1 FROM marts.mart_product_daily_snapshot s
                           WHERE s.product_id = m.product_id
                             AND s.snapshot_date = (SELECT MAX(snapshot_date) FROM marts.mart_product_daily_snapshot)))
      + (SELECT COUNT(*) FROM (SELECT product_id, COUNT(*) AS days, MAX(snapshot_date) - MIN(snapshot_date) + 1 AS span
                               FROM marts.mart_product_daily_snapshot GROUP BY product_id) continuous
         WHERE continuous.days <> continuous.span)
    )::NUMERIC AS differing_rows
) diff;

-- 4l. The last date's snapshot is the lifetime mart.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'campaign_snapshot_last_date_matches_lifetime_mart',
    'reconciliation',
    'marts.mart_campaign_performance',
    'marts.mart_campaign_daily_snapshot',
    'differing_rows',
    0,
    diff.differing_rows,
    diff.differing_rows,
    0,
    CASE WHEN diff.differing_rows = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates that the snapshot of the last business date equals mart_campaign_performance column for column, for every campaign_id that has any activity, and that each (campaign_id, snapshot_date) appears once.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT (
        (SELECT COUNT(*) FROM marts.mart_campaign_daily_snapshot s
         WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM marts.mart_campaign_daily_snapshot)
           AND NOT EXISTS (SELECT 1 FROM pg_temp.validation_campaign_performance m
                           WHERE to_jsonb(m) = to_jsonb(s) - 'snapshot_date'))
      + (SELECT COUNT(*) FROM pg_temp.validation_campaign_performance m
         WHERE (m.order_count + m.cancelled_order_count + m.web_event_count + m.warehouse_conversion_count + m.impressions > 0 OR m.total_spend > 0)
           AND NOT EXISTS (SELECT 1 FROM marts.mart_campaign_daily_snapshot s
                           WHERE s.campaign_id = m.campaign_id
                             AND s.snapshot_date = (SELECT MAX(snapshot_date) FROM marts.mart_campaign_daily_snapshot)))
      + (SELECT COUNT(*) FROM (SELECT campaign_id, COUNT(*) AS days, MAX(snapshot_date) - MIN(snapshot_date) + 1 AS span
                               FROM marts.mart_campaign_daily_snapshot GROUP BY campaign_id) continuous
         WHERE continuous.days <> continuous.span)
    )::NUMERIC AS differing_rows
) diff;

-- 5. Customer 360 has no negative core metrics.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'customer_360_non_negative_metrics',
    'quality',
    'marts.mart_customer_360',
    'marts.mart_customer_360',
    'bad_row_count',
    0,
    bad_rows.bad_row_count,
    bad_rows.bad_row_count,
    0,
    CASE WHEN bad_rows.bad_row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates customer mart has no negative revenue, order count, or AOV.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COUNT(*)::NUMERIC AS bad_row_count
    FROM marts.mart_customer_360
    WHERE total_order_count < 0
       OR lifetime_value < 0
       OR average_order_value < 0
) bad_rows;

-- 6. Repeat purchase signal matches order count.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'repeat_purchase_signal_consistency',
    'quality',
    'marts.mart_customer_360',
    'marts.mart_customer_360',
    'bad_row_count',
    0,
    bad_rows.bad_row_count,
    bad_rows.bad_row_count,
    0,
    CASE WHEN bad_rows.bad_row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates repeat_purchase_signal matches total_order_count >= 2.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COUNT(*)::NUMERIC AS bad_row_count
    FROM marts.mart_customer_360
    WHERE (repeat_purchase_signal = TRUE AND total_order_count < 2)
       OR (repeat_purchase_signal = FALSE AND total_order_count >= 2)
) bad_rows;

-- 7. Conversion-rate metrics stay inside dashboard-safe percentage bounds.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'conversion_rates_within_valid_bounds',
    'quality',
    'marts',
    'marts',
    'bad_row_count',
    0,
    bad_rows.bad_row_count,
    bad_rows.bad_row_count,
    0,
    CASE WHEN bad_rows.bad_row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates conversion-rate percentages stay between 0 and 100.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT
        (
            SELECT COUNT(*) FROM pg_temp.validation_customer_360
            WHERE customer_conversion_rate_pct < 0 OR customer_conversion_rate_pct > 100
        )
        +
        (
            SELECT COUNT(*) FROM pg_temp.validation_campaign_performance
            WHERE session_to_order_conversion_rate_pct < 0 OR session_to_order_conversion_rate_pct > 100
        )
        +
        (
            SELECT COUNT(*) FROM pg_temp.validation_product_sales
            WHERE product_conversion_rate_pct < 0 OR product_conversion_rate_pct > 100
        )
        +
        (
            SELECT COUNT(*) FROM marts.mart_marketing_funnel
            WHERE funnel_conversion_rate_pct < 0 OR funnel_conversion_rate_pct > 100
        ) AS bad_row_count
) bad_rows;

DROP TABLE pg_temp.validation_customer_360, pg_temp.validation_product_sales, pg_temp.validation_campaign_performance;

-- 8. Mart views are queryable and non-empty.
INSERT INTO audit.mart_validation_report (
    validation_run_id,
    check_name,
    check_category,
    source_object,
    target_object,
    metric_name,
    source_value,
    target_value,
    difference,
    tolerance,
    status,
    details
)
SELECT
    mvc.validation_run_id,
    'mart_row_counts_are_queryable',
    'structure',
    'marts',
    'marts',
    'empty_mart_count',
    0,
    bad_rows.empty_mart_count,
    bad_rows.empty_mart_count,
    0,
    CASE WHEN bad_rows.empty_mart_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    'Validates mart views are queryable and not unexpectedly empty after warehouse load.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT
        (
            CASE WHEN (SELECT COUNT(*) FROM marts.mart_customer_360) = 0 THEN 1 ELSE 0 END
          + CASE WHEN (SELECT COUNT(*) FROM marts.mart_campaign_performance) = 0 THEN 1 ELSE 0 END
          + CASE WHEN (SELECT COUNT(*) FROM marts.mart_product_sales) = 0 THEN 1 ELSE 0 END
          + CASE WHEN (SELECT COUNT(*) FROM marts.mart_revenue_daily) = 0 THEN 1 ELSE 0 END
          + CASE WHEN (SELECT COUNT(*) FROM marts.mart_marketing_funnel) = 0 THEN 1 ELSE 0 END
        )::NUMERIC AS empty_mart_count
) bad_rows;

CREATE OR REPLACE VIEW audit.v_latest_mart_validation_report AS
SELECT *
FROM audit.mart_validation_report
WHERE validation_run_id = (
    SELECT validation_run_id
    FROM audit.mart_validation_report
    ORDER BY checked_at DESC
    LIMIT 1
)
ORDER BY validation_id;

COMMENT ON VIEW audit.v_latest_mart_validation_report IS
'Latest BI mart validation run showing PASS/FAIL checks for mart structure, reconciliation, and metric quality.';

COMMIT;
