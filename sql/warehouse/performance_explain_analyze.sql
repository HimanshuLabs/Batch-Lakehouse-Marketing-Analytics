-- ============================================================================
-- PostgreSQL Warehouse EXPLAIN ANALYZE Performance Demo
-- ============================================================================
-- Purpose:
--   Demonstrates practical before/after query tuning for warehouse reporting
--   queries using PostgreSQL indexes and EXPLAIN ANALYZE.
--
-- Important:
--   This project uses a small local dataset, so PostgreSQL may still choose
--   sequential scans for some queries because scanning a tiny table can be
--   cheaper than using an index.
--
--   That is not a failure. The goal is to show:
--     1. which reporting queries are index-eligible,
--     2. which indexes support them,
--     3. how to inspect plans with EXPLAIN ANALYZE,
--     4. how the same design scales beyond the local sample.
--
-- Run:
--   env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/performance_explain_analyze.sql
-- ============================================================================

\echo 'Starting warehouse EXPLAIN ANALYZE performance demo...'


-- ---------------------------------------------------------------------------
-- Demo 1: Customer revenue by month and membership tier
-- ---------------------------------------------------------------------------
-- Reporting pattern:
--   Dashboard groups revenue by calendar month and customer membership tier.
--
-- Indexes involved:
--   - warehouse.fact_orders(customer_sk)
--   - warehouse.fact_orders(order_date_sk)
--   - warehouse.dim_customer(customer_sk) through primary key
--   - warehouse.dim_date(date_sk) through primary key
--
-- Note:
--   On the small local sample, PostgreSQL may still choose sequential scans.
--   Read the plan honestly: look at scan type, joins, buffers, and execution time.
-- ---------------------------------------------------------------------------

\echo 'Demo 1: Customer revenue by month and membership tier'

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT
    dd.year_number,
    dd.month_number,
    dd.month_name,
    dc.membership_tier,
    COUNT(DISTINCT fo.order_id) AS order_count,
    SUM(fo.net_revenue) AS net_revenue
FROM warehouse.fact_orders fo
JOIN warehouse.dim_date dd
    ON fo.order_date_sk = dd.date_sk
JOIN warehouse.dim_customer dc
    ON fo.customer_sk = dc.customer_sk
WHERE dd.full_date >= DATE '2026-01-01'
  AND dd.full_date < DATE '2027-01-01'
  AND dc.is_current = TRUE
GROUP BY
    dd.year_number,
    dd.month_number,
    dd.month_name,
    dc.membership_tier
ORDER BY
    dd.year_number,
    dd.month_number,
    dc.membership_tier;


-- ---------------------------------------------------------------------------
-- Demo 2: Product revenue lookup
-- ---------------------------------------------------------------------------
-- Reporting pattern:
--   Product performance drilldown for one selected product.
--
-- Indexes involved:
--   - warehouse.fact_order_items(product_sk)
--   - warehouse.dim_product(product_sk) through primary key
--   - warehouse.dim_product(product_id, is_current)
--
-- Why this is useful:
--   BI dashboards often start broad, then drill into one product. The product_sk
--   join index supports that drilldown path from dimension to fact rows.
-- ---------------------------------------------------------------------------

\echo 'Demo 2: Product revenue lookup'

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH target_product AS (
    SELECT
        dp.product_sk,
        dp.product_id,
        dp.product_name,
        dp.category
    FROM warehouse.dim_product dp
    JOIN (
        SELECT
            product_sk
        FROM warehouse.fact_order_items
        GROUP BY product_sk
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) sold_product
        ON dp.product_sk = sold_product.product_sk
    WHERE dp.is_current = TRUE
)
SELECT
    tp.product_id,
    tp.product_name,
    tp.category,
    COUNT(DISTINCT foi.order_id) AS order_count,
    SUM(foi.quantity) AS units_sold,
    SUM(foi.line_revenue) AS product_revenue
FROM target_product tp
JOIN warehouse.fact_order_items foi
    ON tp.product_sk = foi.product_sk
GROUP BY
    tp.product_id,
    tp.product_name,
    tp.category
ORDER BY
    product_revenue DESC;


-- ---------------------------------------------------------------------------
-- Demo 3: Campaign spend and ROAS lookup
-- ---------------------------------------------------------------------------
-- Reporting pattern:
--   Campaign dashboard filters by one campaign and reports spend,
--   attributed revenue, and ROAS.
--
-- Indexes involved:
--   - warehouse.fact_campaign_spend(campaign_sk, spend_date_sk)
--   - warehouse.dim_campaign(campaign_sk) through primary key
--   - warehouse.dim_campaign(campaign_id, is_current)
--
-- Why this is useful:
--   Campaign dashboards commonly filter to one campaign or compare campaigns.
--   The composite index starts with campaign_sk, so it supports campaign-level
--   lookup and can also help date-bounded campaign spend analysis.
-- ---------------------------------------------------------------------------

\echo 'Demo 3: Campaign spend and ROAS lookup'

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
WITH target_campaign AS (
    SELECT
        dc.campaign_sk,
        dc.campaign_id,
        dc.campaign_name,
        dc.campaign_type
    FROM warehouse.dim_campaign dc
    JOIN (
        SELECT
            campaign_sk
        FROM warehouse.fact_campaign_spend
        GROUP BY campaign_sk
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) active_campaign
        ON dc.campaign_sk = active_campaign.campaign_sk
    WHERE dc.is_current = TRUE
)
SELECT
    tc.campaign_id,
    tc.campaign_name,
    tc.campaign_type,
    COUNT(*) AS spend_days,
    SUM(fcs.spend_amount) AS total_spend,
    SUM(fcs.attributed_revenue) AS attributed_revenue,
    ROUND(
        SUM(fcs.attributed_revenue) / NULLIF(SUM(fcs.spend_amount), 0),
        4
    ) AS roas
FROM target_campaign tc
JOIN warehouse.fact_campaign_spend fcs
    ON tc.campaign_sk = fcs.campaign_sk
GROUP BY
    tc.campaign_id,
    tc.campaign_name,
    tc.campaign_type
ORDER BY
    roas DESC;


-- ---------------------------------------------------------------------------
-- Final verification: required warehouse performance index coverage
-- ---------------------------------------------------------------------------
-- This confirms the indexes supporting the performance demos exist in the
-- local PostgreSQL warehouse.
-- ---------------------------------------------------------------------------

\echo 'Final verification: warehouse performance index coverage'

SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'warehouse'
  AND (
      (tablename = 'fact_orders' AND indexname IN ('idx_fact_orders_date', 'idx_fact_orders_customer'))
      OR (tablename = 'fact_order_items' AND indexname = 'idx_fact_order_items_product')
      OR (tablename = 'fact_campaign_spend' AND indexname = 'idx_fact_campaign_spend_campaign_date')
      OR (tablename = 'dim_customer' AND indexname = 'idx_dim_customer_natural_current')
      OR (tablename = 'dim_product' AND indexname = 'idx_dim_product_natural_current')
      OR (tablename = 'dim_campaign' AND indexname = 'idx_dim_campaign_natural_current')
  )
ORDER BY
    tablename,
    indexname;

\echo 'Warehouse EXPLAIN ANALYZE performance demo complete.'
