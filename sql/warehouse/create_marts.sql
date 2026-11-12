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
        -- Only a completed order is a sale: counts, revenue, lifetime value and
        -- order value are those of completed orders. Cancelled and returned
        -- orders are shown beside them.
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'completed') AS total_order_count,
        COALESCE(SUM(fo.gross_amount) FILTER (WHERE fo.order_status = 'completed'), 0) AS gross_revenue,
        COALESCE(SUM(fo.discount_amount) FILTER (WHERE fo.order_status = 'completed'), 0) AS total_discount_amount,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) AS lifetime_value,
        COALESCE(AVG(fo.net_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) AS average_order_value,
        MIN(fo.order_timestamp) FILTER (WHERE fo.order_status = 'completed') AS first_order_timestamp,
        MAX(fo.order_timestamp) FILTER (WHERE fo.order_status = 'completed') AS last_order_timestamp,
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'cancelled') AS cancelled_order_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'cancelled'), 0) AS cancelled_amount,
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'returned') AS returned_order_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'returned'), 0) AS returned_amount,
        -- 1 = the third of the buying customers with the highest lifetime value.
        NTILE(3) OVER (
            PARTITION BY (COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) > 0
                          AND dc.customer_id NOT IN ('UNKNOWN', 'INVALID'))
            ORDER BY COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) DESC, dc.customer_id
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
       AND fo.order_status = 'completed'
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
        COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.order_status = 'completed') AS order_count,
        COUNT(DISTINCT fo.customer_id) FILTER (WHERE fo.order_status = 'completed') AS purchasing_customer_count,
        COALESCE(SUM(fo.net_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) AS warehouse_attributed_revenue,
        COALESCE(AVG(fo.net_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) AS average_order_value,
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
        COUNT(DISTINCT fwe.customer_sk) AS engaged_customer_count,
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
        COUNT(DISTINCT foi.order_id) FILTER (WHERE fo.order_status = 'completed') AS order_count,
        COUNT(DISTINCT foi.customer_id) FILTER (WHERE fo.order_status = 'completed') AS purchasing_customer_count,
        COALESCE(SUM(foi.quantity) FILTER (WHERE fo.order_status = 'completed'), 0) AS units_sold,
        COALESCE(SUM(foi.line_revenue) FILTER (WHERE fo.order_status = 'completed'), 0) AS product_revenue,
        COALESCE(AVG(foi.unit_price) FILTER (WHERE fo.order_status = 'completed'), 0) AS average_selling_price,
        MIN(foi.order_timestamp) FILTER (WHERE fo.order_status = 'completed') AS first_sold_timestamp,
        MAX(foi.order_timestamp) FILTER (WHERE fo.order_status = 'completed') AS last_sold_timestamp,
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
        -- Only a completed order is a sale; the revenue columns below count
        -- completed orders, and cancelled and returned orders have their own.
        (fo.order_status = 'completed') AS sold,
        fo.gross_amount,
        fo.discount_amount,
        fo.net_revenue,
        fo.tax_amount,
        fo.shipping_amount,
        fo.total_amount,
        fo.item_count,
        -- The customer's n-th completed order (numbered within the status).
        ROW_NUMBER() OVER (
            PARTITION BY fo.customer_id, fo.order_status
            ORDER BY fo.order_timestamp NULLS LAST, fo.order_sk
        ) AS customer_order_number
    FROM warehouse.fact_orders fo
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
    COUNT(DISTINCT oe.order_id) FILTER (WHERE oe.order_status = 'returned') AS returned_order_count,
    ROUND(COALESCE(SUM(oe.net_revenue) FILTER (WHERE oe.order_status = 'returned'), 0), 2) AS returned_amount,

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
        dc.traffic_source,
        dc.target_segment,

        fwe.event_id,
        fwe.session_id,
        fwe.customer_sk,
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
    COALESCE(traffic_source, 'Unknown') AS traffic_source,
    COALESCE(target_segment, 'Unknown') AS target_segment,

    COUNT(*) AS total_event_count,
    COUNT(DISTINCT session_id) FILTER (WHERE session_id IS NOT NULL) AS session_count,
    COUNT(DISTINCT customer_sk) AS engaged_customer_count,

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
    'Validates total daily mart net revenue equals the net revenue of the completed orders in warehouse fact_orders.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(fo.net_revenue), 0)::NUMERIC AS source_amount
    FROM warehouse.fact_orders fo
    WHERE fo.order_status = 'completed'
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
    'Validates product mart revenue (every member row) equals the line revenue of the completed orders in warehouse fact_order_items.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(foi.line_revenue), 0)::NUMERIC AS source_amount
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo
        ON fo.order_sk = foi.order_sk
       AND fo.order_status = 'completed'
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
    'Validates customer mart lifetime value (every member row) equals the net revenue of the completed orders.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(net_revenue), 0)::NUMERIC AS source_amount FROM warehouse.fact_orders WHERE order_status = 'completed'
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
    'Validates customer mart units (every member row) equal the units of the completed orders.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(foi.quantity), 0)::NUMERIC AS source_amount FROM warehouse.fact_order_items foi JOIN warehouse.fact_orders fo ON fo.order_sk = foi.order_sk AND fo.order_status = 'completed'
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
    'Validates campaign mart attributed revenue (every member row) equals the net revenue of the completed orders.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(net_revenue), 0)::NUMERIC AS source_amount FROM warehouse.fact_orders WHERE order_status = 'completed'
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
    'Validates product mart units sold (every member row) equal the units of the completed orders.'
FROM mart_validation_context mvc
CROSS JOIN (
    SELECT COALESCE(SUM(foi.quantity), 0)::NUMERIC AS source_amount FROM warehouse.fact_order_items foi JOIN warehouse.fact_orders fo ON fo.order_sk = foi.order_sk AND fo.order_status = 'completed'
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
            SELECT COUNT(*) FROM marts.mart_customer_360
            WHERE customer_conversion_rate_pct < 0 OR customer_conversion_rate_pct > 100
        )
        +
        (
            SELECT COUNT(*) FROM marts.mart_campaign_performance
            WHERE session_to_order_conversion_rate_pct < 0 OR session_to_order_conversion_rate_pct > 100
        )
        +
        (
            SELECT COUNT(*) FROM marts.mart_product_sales
            WHERE product_conversion_rate_pct < 0 OR product_conversion_rate_pct > 100
        )
        +
        (
            SELECT COUNT(*) FROM marts.mart_marketing_funnel
            WHERE funnel_conversion_rate_pct < 0 OR funnel_conversion_rate_pct > 100
        ) AS bad_row_count
) bad_rows;

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
