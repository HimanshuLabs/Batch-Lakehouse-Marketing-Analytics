-- Core warehouse dimensional model.
-- Facts join to SCD2 dimensions through surrogate keys.

BEGIN;

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_sk BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    customer_name TEXT,
    email TEXT,
    gender TEXT,
    age INTEGER,
    membership_tier TEXT,
    customer_segment TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    record_hash TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_sk BIGINT PRIMARY KEY,
    product_id BIGINT NOT NULL,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    original_price NUMERIC(18,2),
    current_price NUMERIC(18,2),
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    record_hash TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_campaign (
    campaign_sk BIGINT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    channel TEXT,
    traffic_source TEXT,
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    record_hash TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_sk INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    calendar_year INTEGER NOT NULL,
    calendar_quarter INTEGER NOT NULL,
    calendar_month INTEGER NOT NULL,
    calendar_day INTEGER NOT NULL,
    day_name TEXT,
    month_name TEXT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_region (
    region_sk BIGSERIAL PRIMARY KEY,
    country TEXT,
    state TEXT,
    city TEXT,
    UNIQUE (country, state, city)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_channel (
    channel_sk BIGSERIAL PRIMARY KEY,
    channel_name TEXT,
    traffic_source TEXT,
    UNIQUE (channel_name, traffic_source)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id TEXT PRIMARY KEY,
    order_date_sk INTEGER REFERENCES warehouse.dim_date(date_sk),
    order_timestamp TIMESTAMP NOT NULL,
    customer_sk BIGINT REFERENCES warehouse.dim_customer(customer_sk),
    campaign_sk BIGINT REFERENCES warehouse.dim_campaign(campaign_sk),
    region_sk BIGINT REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT REFERENCES warehouse.dim_channel(channel_sk),
    order_status TEXT,
    gross_revenue NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    net_revenue NUMERIC(18,2),
    items_count INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES warehouse.fact_orders(order_id),
    product_sk BIGINT REFERENCES warehouse.dim_product(product_sk),
    quantity INTEGER,
    unit_price NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    line_revenue NUMERIC(18,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_campaign_spend (
    spend_id TEXT PRIMARY KEY,
    campaign_sk BIGINT REFERENCES warehouse.dim_campaign(campaign_sk),
    spend_date_sk INTEGER REFERENCES warehouse.dim_date(date_sk),
    spend_amount NUMERIC(18,2),
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_web_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT,
    event_timestamp TIMESTAMP NOT NULL,
    event_date_sk INTEGER REFERENCES warehouse.dim_date(date_sk),
    customer_sk BIGINT REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT REFERENCES warehouse.dim_product(product_sk),
    campaign_sk BIGINT REFERENCES warehouse.dim_campaign(campaign_sk),
    event_type TEXT,
    user_journey_stage TEXT,
    engagement_score NUMERIC(10,4),
    purchase_probability NUMERIC(10,4),
    cart_abandonment_probability NUMERIC(10,4),
    page_load_time_ms NUMERIC(18,2),
    api_latency_ms NUMERIC(18,2),
    source TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_conversions (
    conversion_id TEXT PRIMARY KEY,
    event_id TEXT REFERENCES warehouse.fact_web_events(event_id),
    customer_sk BIGINT REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT REFERENCES warehouse.dim_product(product_sk),
    campaign_sk BIGINT REFERENCES warehouse.dim_campaign(campaign_sk),
    conversion_timestamp TIMESTAMP NOT NULL,
    conversion_date_sk INTEGER REFERENCES warehouse.dim_date(date_sk),
    conversion_type TEXT,
    conversion_value NUMERIC(18,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
