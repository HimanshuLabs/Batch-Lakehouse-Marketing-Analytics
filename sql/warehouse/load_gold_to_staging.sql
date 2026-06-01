-- Load contract for moving Project 2 Gold/SCD2 outputs into PostgreSQL staging.
-- This file defines staging tables only.
-- Actual data loading can later be implemented through Python, Spark JDBC, COPY, or an existing publish script.

BEGIN;

CREATE TABLE IF NOT EXISTS staging.gold_dim_customers_scd2 (
    customer_sk BIGINT,
    customer_id BIGINT,
    customer_name TEXT,
    email TEXT,
    gender TEXT,
    age INTEGER,
    membership_tier TEXT,
    customer_segment TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,
    record_hash TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.gold_dim_products_scd2 (
    product_sk BIGINT,
    product_id BIGINT,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    original_price NUMERIC(18,2),
    current_price NUMERIC(18,2),
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,
    record_hash TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.gold_dim_campaigns_scd2 (
    campaign_sk BIGINT,
    campaign_id TEXT,
    campaign_name TEXT,
    channel TEXT,
    traffic_source TEXT,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,
    record_hash TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.gold_fact_orders_scd2 (
    order_id TEXT,
    customer_sk BIGINT,
    campaign_sk BIGINT,
    order_timestamp TIMESTAMP,
    order_date DATE,
    order_status TEXT,
    gross_revenue NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    net_revenue NUMERIC(18,2),
    items_count INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.gold_fact_order_items_scd2 (
    order_item_id TEXT,
    order_id TEXT,
    product_sk BIGINT,
    quantity INTEGER,
    unit_price NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    line_revenue NUMERIC(18,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.gold_fact_campaign_spend_scd2 (
    spend_id TEXT,
    campaign_sk BIGINT,
    spend_date DATE,
    spend_amount NUMERIC(18,2),
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
