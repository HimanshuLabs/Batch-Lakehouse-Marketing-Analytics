-- ============================================================================
-- PostgreSQL Warehouse Query Performance Indexes
-- ============================================================================
-- Purpose:
--   Practical, local, reproducible indexes for warehouse/dashboard queries.
--
-- Rules:
--   1. Safe to rerun.
--   2. Avoid duplicate index bloat.
--   3. Tune real warehouse joins and filters.
--   4. Pair with EXPLAIN ANALYZE before/after proof.
-- ============================================================================

\echo 'Starting warehouse performance index setup...'

-- ---------------------------------------------------------------------------
-- fact_orders(order_date_sk)
-- Supports date-range dashboard queries through the warehouse date surrogate key.
-- The project warehouse does not store order_date directly in fact_orders;
-- it stores order_date_sk and joins to warehouse.dim_date.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_orders_date
ON warehouse.fact_orders(order_date_sk);


-- ---------------------------------------------------------------------------
-- fact_orders(customer_sk)
-- Supports customer-level joins for customer 360, lifetime value,
-- repeat purchase, and segment reporting queries.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
ON warehouse.fact_orders(customer_sk);


-- ---------------------------------------------------------------------------
-- fact_order_items(product_sk)
-- Supports product performance joins, category reporting,
-- and product-level sales aggregation.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product
ON warehouse.fact_order_items(product_sk);


-- ---------------------------------------------------------------------------
-- fact_campaign_spend(campaign_sk)
-- Covered by existing composite index:
--   idx_fact_campaign_spend_campaign_date
--   ON warehouse.fact_campaign_spend(campaign_sk, spend_date_sk)
--
-- campaign_sk is the leading column, so this supports campaign joins and
-- campaign-level spend/ROAS reporting without adding duplicate index bloat.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- dim_customer(customer_id, is_current)
-- Covered by existing index:
--   idx_dim_customer_natural_current
--   ON warehouse.dim_customer(customer_id, is_current)
--
-- Supports current customer lookup by natural key in SCD2 reporting queries.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- dim_product(product_id, is_current)
-- Covered by existing index:
--   idx_dim_product_natural_current
--   ON warehouse.dim_product(product_id, is_current)
--
-- Supports current product lookup by natural key in SCD2 reporting queries.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- dim_campaign(campaign_id, is_current)
-- Covered by existing index:
--   idx_dim_campaign_natural_current
--   ON warehouse.dim_campaign(campaign_id, is_current)
--
-- Supports current campaign lookup by natural key in SCD2 reporting queries.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- Refresh PostgreSQL planner statistics after index validation/creation.
-- This helps the optimizer choose better plans for reporting queries.
-- ---------------------------------------------------------------------------
ANALYZE warehouse.fact_orders;
ANALYZE warehouse.fact_order_items;
ANALYZE warehouse.fact_campaign_spend;
ANALYZE warehouse.dim_customer;
ANALYZE warehouse.dim_product;
ANALYZE warehouse.dim_campaign;

\echo 'Warehouse performance index setup complete.'
