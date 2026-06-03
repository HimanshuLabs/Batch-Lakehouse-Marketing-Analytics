/*
===============================================================================
PostgreSQL Warehouse Star Schema Layer
===============================================================================

Purpose:
- Build the Project 3 warehouse core inside the Project 2 lakehouse repo.
- Promote trusted Gold/SCD2 staging data into warehouse dimensions and facts.
- Preserve point-in-time correctness by using SCD2 surrogate keys when present.
- Keep Bronze/Silver away from BI. BI reads warehouse/marts only.

Surrogate key rule:
- customer_id/product_id/campaign_id = natural keys from source/lakehouse.
- customer_sk/product_sk/campaign_sk = warehouse/SCD2 surrogate keys.
- Facts should store the *_sk value that was valid when the business event happened.
- If staging SCD2 facts already provide *_sk, this script preserves those keys.
- If *_sk is missing, the script falls back to point-in-time joins using natural key
  + event/order/spend timestamp.

This file is development-idempotent:
- Drops and recreates the warehouse star schema tables.
- Reloads warehouse tables from staging.
- Intended for local reproducible portfolio runs.
===============================================================================
*/

BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------------------
-- Helper functions
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION warehouse.safe_bigint(value TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    RETURN btrim(value)::BIGINT;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse.safe_int(value TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    RETURN btrim(value)::INTEGER;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse.safe_numeric(value TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    RETURN regexp_replace(btrim(value), ',', '', 'g')::NUMERIC;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse.safe_bool(value TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    normalized TEXT;
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    normalized := lower(btrim(value));

    IF normalized IN ('true', 't', '1', 'yes', 'y') THEN
        RETURN TRUE;
    END IF;

    IF normalized IN ('false', 'f', '0', 'no', 'n') THEN
        RETURN FALSE;
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse.safe_timestamp(value TEXT)
RETURNS TIMESTAMP
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    RETURN btrim(value)::TIMESTAMPTZ AT TIME ZONE 'UTC';
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse.safe_date(value TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    RETURN btrim(value)::DATE;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION warehouse.date_sk(input_date DATE)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN input_date IS NULL THEN 0
        ELSE to_char(input_date, 'YYYYMMDD')::INTEGER
    END;
$$;

CREATE OR REPLACE FUNCTION warehouse.deterministic_sk(prefix TEXT, value TEXT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT abs(hashtext(coalesce(prefix, 'unknown') || '|' || coalesce(value, 'unknown'))::BIGINT) + 1000000000;
$$;

CREATE OR REPLACE FUNCTION warehouse.region_nk(city_value TEXT, state_value TEXT, country_value TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT lower(
        coalesce(nullif(btrim(city_value), ''), 'unknown')
        || '|'
        || coalesce(nullif(btrim(state_value), ''), 'unknown')
        || '|'
        || coalesce(nullif(btrim(country_value), ''), 'unknown')
    );
$$;

CREATE OR REPLACE FUNCTION warehouse.channel_nk(channel_value TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT lower(coalesce(nullif(btrim(channel_value), ''), 'unknown'));
$$;

-- ---------------------------------------------------------------------------
-- Drop existing warehouse star schema tables
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS warehouse.fact_conversions CASCADE;
DROP TABLE IF EXISTS warehouse.fact_web_events CASCADE;
DROP TABLE IF EXISTS warehouse.fact_campaign_spend CASCADE;
DROP TABLE IF EXISTS warehouse.fact_order_items CASCADE;
DROP TABLE IF EXISTS warehouse.fact_orders CASCADE;

DROP TABLE IF EXISTS warehouse.dim_channel CASCADE;
DROP TABLE IF EXISTS warehouse.dim_region CASCADE;
DROP TABLE IF EXISTS warehouse.dim_date CASCADE;
DROP TABLE IF EXISTS warehouse.dim_campaign CASCADE;
DROP TABLE IF EXISTS warehouse.dim_product CASCADE;
DROP TABLE IF EXISTS warehouse.dim_customer CASCADE;

-- ---------------------------------------------------------------------------
-- Dimension DDL
-- ---------------------------------------------------------------------------

CREATE TABLE warehouse.dim_customer (
    customer_sk BIGINT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    email TEXT,
    gender TEXT,
    age INTEGER,
    membership_tier TEXT,
    loyalty_points INTEGER NOT NULL DEFAULT 0,
    preferred_language TEXT,
    customer_segment TEXT,
    is_prime_user BOOLEAN NOT NULL DEFAULT FALSE,
    home_city TEXT,
    home_state TEXT,
    country TEXT,
    effective_from TIMESTAMP NOT NULL DEFAULT TIMESTAMP '1900-01-01 00:00:00',
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMP,
    scd_hash TEXT,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_customer_natural_version UNIQUE (customer_id, effective_from),
    CONSTRAINT chk_dim_customer_effective_range CHECK (effective_to > effective_from),
    CONSTRAINT chk_dim_customer_age CHECK (age IS NULL OR age BETWEEN 0 AND 120)
);

COMMENT ON TABLE warehouse.dim_customer IS
'SCD2 customer dimension. customer_sk is the surrogate key used by facts for point-in-time reporting; customer_id is the source natural key.';

COMMENT ON COLUMN warehouse.dim_customer.customer_sk IS
'Surrogate key. Multiple customer_sk values can exist for one customer_id when SCD2 attributes change.';

CREATE TABLE warehouse.dim_product (
    product_sk BIGINT PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    original_price NUMERIC(18, 2),
    current_price NUMERIC(18, 2),
    inventory_remaining INTEGER,
    effective_from TIMESTAMP NOT NULL DEFAULT TIMESTAMP '1900-01-01 00:00:00',
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMP,
    scd_hash TEXT,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_product_natural_version UNIQUE (product_id, effective_from),
    CONSTRAINT chk_dim_product_effective_range CHECK (effective_to > effective_from),
    CONSTRAINT chk_dim_product_prices CHECK (
        (original_price IS NULL OR original_price >= 0)
        AND (current_price IS NULL OR current_price >= 0)
    )
);

COMMENT ON TABLE warehouse.dim_product IS
'SCD2 product dimension. product_sk preserves historical product/category/price context for facts.';

COMMENT ON COLUMN warehouse.dim_product.product_sk IS
'Surrogate key. Used by order items and web events instead of raw product_id.';

CREATE TABLE warehouse.dim_campaign (
    campaign_sk BIGINT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT,
    campaign_type TEXT,
    channel TEXT,
    traffic_source TEXT,
    target_segment TEXT,
    campaign_start_date DATE,
    campaign_end_date DATE,
    budget NUMERIC(18, 2),
    effective_from TIMESTAMP NOT NULL DEFAULT TIMESTAMP '1900-01-01 00:00:00',
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMP,
    scd_hash TEXT,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_campaign_natural_version UNIQUE (campaign_id, effective_from),
    CONSTRAINT chk_dim_campaign_effective_range CHECK (effective_to > effective_from),
    CONSTRAINT chk_dim_campaign_budget CHECK (budget IS NULL OR budget >= 0)
);

COMMENT ON TABLE warehouse.dim_campaign IS
'SCD2 campaign dimension. campaign_sk allows spend, events, conversions, and orders to join to the correct historical campaign version.';

COMMENT ON COLUMN warehouse.dim_campaign.campaign_sk IS
'Surrogate key. Multiple campaign_sk values can exist for one campaign_id across SCD2 versions.';

CREATE TABLE warehouse.dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    day_of_week INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter_number INTEGER NOT NULL,
    year_number INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    month_start_date DATE NOT NULL,
    quarter_start_date DATE NOT NULL,
    year_start_date DATE NOT NULL
);

COMMENT ON TABLE warehouse.dim_date IS
'Calendar dimension for joining facts by order, spend, event, and conversion dates. date_sk uses YYYYMMDD format; 0 is the unknown date member.';

CREATE TABLE warehouse.dim_region (
    region_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    region_nk TEXT NOT NULL UNIQUE,
    country TEXT NOT NULL DEFAULT 'Unknown',
    state TEXT NOT NULL DEFAULT 'Unknown',
    city TEXT NOT NULL DEFAULT 'Unknown',
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.dim_region IS
'Conformed geography dimension derived from customer, order, spend, and web-event locations.';

CREATE TABLE warehouse.dim_channel (
    channel_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    channel_nk TEXT NOT NULL UNIQUE,
    channel_name TEXT NOT NULL,
    channel_group TEXT,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.dim_channel IS
'Conformed marketing/acquisition channel dimension derived from campaign, ad spend, and web-event traffic source fields.';

-- ---------------------------------------------------------------------------
-- Fact DDL
-- ---------------------------------------------------------------------------

CREATE TABLE warehouse.fact_orders (
    order_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    order_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    order_timestamp TIMESTAMP,
    order_status TEXT NOT NULL DEFAULT 'completed',
    payment_method TEXT,
    item_count INTEGER NOT NULL DEFAULT 0,
    gross_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    discount_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    tax_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    shipping_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    net_revenue NUMERIC(18, 2) NOT NULL DEFAULT 0,
    source_record_hash TEXT,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_orders_amounts CHECK (
        gross_amount >= 0
        AND discount_amount >= 0
        AND tax_amount >= 0
        AND shipping_amount >= 0
        AND net_revenue >= 0
    )
);

COMMENT ON TABLE warehouse.fact_orders IS
'Order-grain fact table. Uses customer_sk/campaign_sk to preserve SCD2 point-in-time correctness.';

CREATE TABLE warehouse.fact_order_items (
    order_item_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    order_item_nk TEXT NOT NULL UNIQUE,
    order_sk BIGINT REFERENCES warehouse.fact_orders(order_sk),
    order_id TEXT,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    order_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    order_timestamp TIMESTAMP,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(18, 2) NOT NULL DEFAULT 0,
    discount_percent NUMERIC(9, 4) NOT NULL DEFAULT 0,
    line_revenue NUMERIC(18, 2) NOT NULL DEFAULT 0,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_order_items_metrics CHECK (
        quantity >= 0
        AND unit_price >= 0
        AND discount_percent >= 0
        AND line_revenue >= 0
    )
);

COMMENT ON TABLE warehouse.fact_order_items IS
'Order-item-grain fact table. product_sk preserves product SCD2 history at item purchase time.';

CREATE TABLE warehouse.fact_campaign_spend (
    campaign_spend_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    campaign_spend_nk TEXT NOT NULL UNIQUE,
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    spend_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    spend_date DATE,
    spend_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    impressions BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    conversions BIGINT NOT NULL DEFAULT 0,
    attributed_revenue NUMERIC(18, 2) NOT NULL DEFAULT 0,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_campaign_spend_metrics CHECK (
        spend_amount >= 0
        AND impressions >= 0
        AND clicks >= 0
        AND conversions >= 0
        AND attributed_revenue >= 0
    )
);

COMMENT ON TABLE warehouse.fact_campaign_spend IS
'Campaign-spend fact table. Joins campaign spend to SCD2 campaign version, channel, region, and spend date.';

CREATE TABLE warehouse.fact_web_events (
    event_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    event_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    event_timestamp TIMESTAMP,
    event_type TEXT NOT NULL,
    user_journey_stage TEXT,
    device_type TEXT,
    operating_system TEXT,
    browser TEXT,
    network_type TEXT,
    traffic_source TEXT,
    search_query TEXT,
    time_on_page_sec INTEGER,
    scroll_depth_percent NUMERIC(9, 4),
    engagement_score NUMERIC(18, 6),
    purchase_probability NUMERIC(18, 6),
    cart_abandonment_probability NUMERIC(18, 6),
    event_value NUMERIC(18, 2) NOT NULL DEFAULT 0,
    api_latency_ms INTEGER,
    page_load_time_ms INTEGER,
    fraud_score NUMERIC(18, 6),
    schema_version TEXT,
    source_system TEXT,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.fact_web_events IS
'Web-event-grain fact table. Stores behavior events and links them to point-in-time customer/product/campaign dimensions where possible.';

CREATE TABLE warehouse.fact_conversions (
    conversion_sk BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    conversion_nk TEXT NOT NULL UNIQUE,
    event_sk BIGINT REFERENCES warehouse.fact_web_events(event_sk),
    event_id TEXT,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    conversion_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    conversion_timestamp TIMESTAMP,
    conversion_type TEXT NOT NULL,
    conversion_value NUMERIC(18, 2) NOT NULL DEFAULT 0,
    warehouse_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_conversions_value CHECK (conversion_value >= 0)
);

COMMENT ON TABLE warehouse.fact_conversions IS
'Conversion fact derived from conversion/purchase web events. Supports funnel and campaign attribution reporting.';

-- ---------------------------------------------------------------------------
-- Unknown dimension members
-- ---------------------------------------------------------------------------

INSERT INTO warehouse.dim_customer (
    customer_sk, customer_id, customer_name, email, gender, age,
    membership_tier, loyalty_points, preferred_language, customer_segment,
    is_prime_user, home_city, home_state, country, effective_from, effective_to,
    is_current, source_updated_at, scd_hash
)
VALUES (
    0, 'UNKNOWN', 'Unknown Customer', NULL, NULL, NULL,
    'Unknown', 0, NULL, 'Unknown',
    FALSE, 'Unknown', 'Unknown', 'Unknown',
    TIMESTAMP '1900-01-01', TIMESTAMP '9999-12-31',
    TRUE, NULL, 'UNKNOWN'
)
ON CONFLICT (customer_sk) DO NOTHING;

INSERT INTO warehouse.dim_product (
    product_sk, product_id, product_name, category, brand,
    original_price, current_price, inventory_remaining,
    effective_from, effective_to, is_current, source_updated_at, scd_hash
)
VALUES (
    0, 'UNKNOWN', 'Unknown Product', 'Unknown', NULL,
    0, 0, NULL,
    TIMESTAMP '1900-01-01', TIMESTAMP '9999-12-31',
    TRUE, NULL, 'UNKNOWN'
)
ON CONFLICT (product_sk) DO NOTHING;

INSERT INTO warehouse.dim_campaign (
    campaign_sk, campaign_id, campaign_name, campaign_type, channel,
    traffic_source, target_segment, campaign_start_date, campaign_end_date,
    budget, effective_from, effective_to, is_current, source_updated_at, scd_hash
)
VALUES (
    0, 'UNKNOWN', 'Unknown Campaign', 'Unknown', 'Unknown',
    'Unknown', 'Unknown', NULL, NULL,
    0, TIMESTAMP '1900-01-01', TIMESTAMP '9999-12-31',
    TRUE, NULL, 'UNKNOWN'
)
ON CONFLICT (campaign_sk) DO NOTHING;

INSERT INTO warehouse.dim_date (
    date_sk, full_date, day_of_month, day_name, day_of_week,
    week_of_year, month_number, month_name, quarter_number, year_number,
    is_weekend, month_start_date, quarter_start_date, year_start_date
)
VALUES (
    0, DATE '0001-01-01', 0, 'Unknown', 0,
    0, 0, 'Unknown', 0, 0,
    FALSE, DATE '0001-01-01', DATE '0001-01-01', DATE '0001-01-01'
)
ON CONFLICT (date_sk) DO NOTHING;

INSERT INTO warehouse.dim_region (region_sk, region_nk, country, state, city)
VALUES (0, 'unknown|unknown|unknown', 'Unknown', 'Unknown', 'Unknown')
ON CONFLICT (region_sk) DO NOTHING;

INSERT INTO warehouse.dim_channel (channel_sk, channel_nk, channel_name, channel_group)
VALUES (0, 'unknown', 'Unknown', 'Unknown')
ON CONFLICT (channel_sk) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Load conformed dimensions: region and channel
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    source_table TEXT;
BEGIN
    FOREACH source_table IN ARRAY ARRAY[
        'staging.stg_gold_dim_customers_scd2',
        'staging.stg_gold_dim_customers',
        'staging.dim_customers_scd2',
        'staging.dim_customers',
        'staging.stg_gold_fact_orders_scd2',
        'staging.stg_gold_fact_orders',
        'staging.stg_gold_fact_web_events',
        'staging.fact_web_events'
    ]
    LOOP
        IF to_regclass(source_table) IS NOT NULL THEN
            EXECUTE format($sql$
                INSERT INTO warehouse.dim_region (region_nk, country, state, city)
                SELECT DISTINCT
                    warehouse.region_nk(city, state, country) AS region_nk,
                    country,
                    state,
                    city
                FROM (
                    SELECT
                        coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                        coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                        coalesce(nullif(r->>'country', ''), 'Unknown') AS country
                    FROM (SELECT to_jsonb(s) AS r FROM %s s) src
                ) mapped
                WHERE warehouse.region_nk(city, state, country) <> 'unknown|unknown|unknown'
                ON CONFLICT (region_nk) DO NOTHING;
            $sql$, source_table);
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    source_table TEXT;
BEGIN
    FOREACH source_table IN ARRAY ARRAY[
        'staging.stg_gold_dim_campaigns_scd2',
        'staging.stg_gold_dim_campaigns',
        'staging.dim_campaigns_scd2',
        'staging.dim_campaigns',
        'staging.stg_gold_fact_campaign_spend_scd2',
        'staging.stg_gold_fact_ad_spend',
        'staging.stg_gold_fact_web_events',
        'staging.fact_web_events'
    ]
    LOOP
        IF to_regclass(source_table) IS NOT NULL THEN
            EXECUTE format($sql$
                INSERT INTO warehouse.dim_channel (channel_nk, channel_name, channel_group)
                SELECT DISTINCT
                    warehouse.channel_nk(channel_value) AS channel_nk,
                    channel_value AS channel_name,
                    CASE
                        WHEN lower(channel_value) IN ('paid_search', 'google', 'bing', 'search') THEN 'Search'
                        WHEN lower(channel_value) IN ('facebook', 'instagram', 'social', 'paid_social') THEN 'Social'
                        WHEN lower(channel_value) IN ('email', 'newsletter') THEN 'Email'
                        WHEN lower(channel_value) IN ('direct') THEN 'Direct'
                        ELSE 'Other'
                    END AS channel_group
                FROM (
                    SELECT coalesce(
                        nullif(r->>'channel', ''),
                        nullif(r->>'traffic_source', ''),
                        nullif(r->>'source', ''),
                        nullif(r->>'marketing_channel', ''),
                        'Unknown'
                    ) AS channel_value
                    FROM (SELECT to_jsonb(s) AS r FROM %s s) src
                ) mapped
                WHERE warehouse.channel_nk(channel_value) <> 'unknown'
                ON CONFLICT (channel_nk) DO NOTHING;
            $sql$, source_table);
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Load SCD2 dimensions
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_dim_customers_scd2') IS NOT NULL THEN 'staging.stg_gold_dim_customers_scd2'
        WHEN to_regclass('staging.dim_customers_scd2') IS NOT NULL THEN 'staging.dim_customers_scd2'
        WHEN to_regclass('staging.stg_gold_dim_customers') IS NOT NULL THEN 'staging.stg_gold_dim_customers'
        WHEN to_regclass('staging.dim_customers') IS NOT NULL THEN 'staging.dim_customers'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No customer dimension staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            mapped AS (
                SELECT
                    coalesce(
                        warehouse.safe_bigint(r->>'customer_sk'),
                        warehouse.deterministic_sk(
                            'customer',
                            coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN')
                            || '|'
                            || coalesce(nullif(r->>'effective_from', ''), nullif(r->>'valid_from', ''), '1900-01-01')
                        )
                    ) AS customer_sk,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif(r->>'customer_name', ''), nullif(r->>'user_name', ''), nullif(r->>'name', '')) AS customer_name,
                    nullif(r->>'email', '') AS email,
                    nullif(r->>'gender', '') AS gender,
                    warehouse.safe_int(r->>'age') AS age,
                    coalesce(nullif(r->>'membership_tier', ''), nullif(r->>'tier', ''), 'Unknown') AS membership_tier,
                    coalesce(warehouse.safe_int(r->>'loyalty_points'), 0) AS loyalty_points,
                    nullif(r->>'preferred_language', '') AS preferred_language,
                    coalesce(nullif(r->>'customer_segment', ''), nullif(r->>'user_segment', ''), 'Unknown') AS customer_segment,
                    coalesce(warehouse.safe_bool(r->>'is_prime_user'), false) AS is_prime_user,
                    coalesce(nullif(r->>'home_city', ''), nullif(r->>'city', ''), 'Unknown') AS home_city,
                    coalesce(nullif(r->>'home_state', ''), nullif(r->>'state', ''), 'Unknown') AS home_state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_from', r->>'valid_from', r->>'start_date')),
                        TIMESTAMP '1900-01-01'
                    ) AS effective_from,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_to', r->>'valid_to', r->>'end_date')),
                        TIMESTAMP '9999-12-31'
                    ) AS effective_to,
                    coalesce(warehouse.safe_bool(r->>'is_current'), true) AS is_current,
                    warehouse.safe_timestamp(coalesce(r->>'updated_at', r->>'source_updated_at', r->>'gold_processed_at')) AS source_updated_at,
                    coalesce(nullif(r->>'scd_hash', ''), nullif(r->>'record_hash', '')) AS scd_hash
                FROM src
            )
            INSERT INTO warehouse.dim_customer (
                customer_sk, customer_id, customer_name, email, gender, age,
                membership_tier, loyalty_points, preferred_language, customer_segment,
                is_prime_user, home_city, home_state, country, effective_from, effective_to,
                is_current, source_updated_at, scd_hash
            )
            SELECT DISTINCT ON (customer_sk)
                customer_sk, customer_id, customer_name, email, gender, age,
                membership_tier, loyalty_points, preferred_language, customer_segment,
                is_prime_user, home_city, home_state, country,
                effective_from,
                CASE
                    WHEN effective_to <= effective_from THEN
                        CASE
                            WHEN is_current THEN TIMESTAMP '9999-12-31 00:00:00'
                            ELSE effective_from + INTERVAL '1 microsecond'
                        END
                    ELSE effective_to
                END AS effective_to,
                is_current, source_updated_at, scd_hash
            FROM mapped
            WHERE customer_sk IS NOT NULL
            ORDER BY customer_sk, source_updated_at DESC NULLS LAST
            ON CONFLICT (customer_sk) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                customer_name = EXCLUDED.customer_name,
                email = EXCLUDED.email,
                gender = EXCLUDED.gender,
                age = EXCLUDED.age,
                membership_tier = EXCLUDED.membership_tier,
                loyalty_points = EXCLUDED.loyalty_points,
                preferred_language = EXCLUDED.preferred_language,
                customer_segment = EXCLUDED.customer_segment,
                is_prime_user = EXCLUDED.is_prime_user,
                home_city = EXCLUDED.home_city,
                home_state = EXCLUDED.home_state,
                country = EXCLUDED.country,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                is_current = EXCLUDED.is_current,
                source_updated_at = EXCLUDED.source_updated_at,
                scd_hash = EXCLUDED.scd_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_dim_products_scd2') IS NOT NULL THEN 'staging.stg_gold_dim_products_scd2'
        WHEN to_regclass('staging.dim_products_scd2') IS NOT NULL THEN 'staging.dim_products_scd2'
        WHEN to_regclass('staging.stg_gold_dim_products') IS NOT NULL THEN 'staging.stg_gold_dim_products'
        WHEN to_regclass('staging.dim_products') IS NOT NULL THEN 'staging.dim_products'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No product dimension staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            mapped AS (
                SELECT
                    coalesce(
                        warehouse.safe_bigint(r->>'product_sk'),
                        warehouse.deterministic_sk(
                            'product',
                            coalesce(nullif(r->>'product_id', ''), 'UNKNOWN')
                            || '|'
                            || coalesce(nullif(r->>'effective_from', ''), nullif(r->>'valid_from', ''), '1900-01-01')
                        )
                    ) AS product_sk,
                    coalesce(nullif(r->>'product_id', ''), 'UNKNOWN') AS product_id,
                    coalesce(nullif(r->>'product_name', ''), nullif(r->>'name', '')) AS product_name,
                    coalesce(nullif(r->>'category', ''), nullif(r->>'product_category', ''), 'Unknown') AS category,
                    nullif(r->>'brand', '') AS brand,
                    coalesce(warehouse.safe_numeric(r->>'original_price'), warehouse.safe_numeric(r->>'list_price')) AS original_price,
                    coalesce(warehouse.safe_numeric(r->>'discounted_price'), warehouse.safe_numeric(r->>'current_price'), warehouse.safe_numeric(r->>'price')) AS current_price,
                    warehouse.safe_int(r->>'inventory_remaining') AS inventory_remaining,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_from', r->>'valid_from', r->>'start_date')),
                        TIMESTAMP '1900-01-01'
                    ) AS effective_from,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_to', r->>'valid_to', r->>'end_date')),
                        TIMESTAMP '9999-12-31'
                    ) AS effective_to,
                    coalesce(warehouse.safe_bool(r->>'is_current'), true) AS is_current,
                    warehouse.safe_timestamp(coalesce(r->>'updated_at', r->>'source_updated_at', r->>'gold_processed_at')) AS source_updated_at,
                    coalesce(nullif(r->>'scd_hash', ''), nullif(r->>'record_hash', '')) AS scd_hash
                FROM src
            )
            INSERT INTO warehouse.dim_product (
                product_sk, product_id, product_name, category, brand,
                original_price, current_price, inventory_remaining,
                effective_from, effective_to, is_current, source_updated_at, scd_hash
            )
            SELECT DISTINCT ON (product_sk)
                product_sk, product_id, product_name, category, brand,
                original_price, current_price, inventory_remaining,
                effective_from,
                CASE
                    WHEN effective_to <= effective_from THEN
                        CASE
                            WHEN is_current THEN TIMESTAMP '9999-12-31 00:00:00'
                            ELSE effective_from + INTERVAL '1 microsecond'
                        END
                    ELSE effective_to
                END AS effective_to,
                is_current, source_updated_at, scd_hash
            FROM mapped
            WHERE product_sk IS NOT NULL
            ORDER BY product_sk, source_updated_at DESC NULLS LAST
            ON CONFLICT (product_sk) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                brand = EXCLUDED.brand,
                original_price = EXCLUDED.original_price,
                current_price = EXCLUDED.current_price,
                inventory_remaining = EXCLUDED.inventory_remaining,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                is_current = EXCLUDED.is_current,
                source_updated_at = EXCLUDED.source_updated_at,
                scd_hash = EXCLUDED.scd_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_dim_campaigns_scd2') IS NOT NULL THEN 'staging.stg_gold_dim_campaigns_scd2'
        WHEN to_regclass('staging.dim_campaigns_scd2') IS NOT NULL THEN 'staging.dim_campaigns_scd2'
        WHEN to_regclass('staging.stg_gold_dim_campaigns') IS NOT NULL THEN 'staging.stg_gold_dim_campaigns'
        WHEN to_regclass('staging.dim_campaigns') IS NOT NULL THEN 'staging.dim_campaigns'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No campaign dimension staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            mapped AS (
                SELECT
                    coalesce(
                        warehouse.safe_bigint(r->>'campaign_sk'),
                        warehouse.deterministic_sk(
                            'campaign',
                            coalesce(nullif(r->>'campaign_id', ''), 'UNKNOWN')
                            || '|'
                            || coalesce(nullif(r->>'effective_from', ''), nullif(r->>'valid_from', ''), '1900-01-01')
                        )
                    ) AS campaign_sk,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(nullif(r->>'campaign_name', ''), nullif(r->>'name', '')) AS campaign_name,
                    coalesce(nullif(r->>'campaign_type', ''), nullif(r->>'type', ''), 'Unknown') AS campaign_type,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'marketing_channel', ''), nullif(r->>'traffic_source', ''), 'Unknown') AS channel,
                    coalesce(nullif(r->>'traffic_source', ''), nullif(r->>'source', ''), 'Unknown') AS traffic_source,
                    coalesce(nullif(r->>'target_segment', ''), nullif(r->>'customer_segment', ''), 'Unknown') AS target_segment,
                    coalesce(warehouse.safe_date(r->>'campaign_start_date'), warehouse.safe_date(r->>'start_date')) AS campaign_start_date,
                    coalesce(warehouse.safe_date(r->>'campaign_end_date'), warehouse.safe_date(r->>'end_date')) AS campaign_end_date,
                    coalesce(warehouse.safe_numeric(r->>'budget'), warehouse.safe_numeric(r->>'campaign_budget')) AS budget,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_from', r->>'valid_from', r->>'start_date')),
                        TIMESTAMP '1900-01-01'
                    ) AS effective_from,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_to', r->>'valid_to', r->>'end_date')),
                        TIMESTAMP '9999-12-31'
                    ) AS effective_to,
                    coalesce(warehouse.safe_bool(r->>'is_current'), true) AS is_current,
                    warehouse.safe_timestamp(coalesce(r->>'updated_at', r->>'source_updated_at', r->>'gold_processed_at')) AS source_updated_at,
                    coalesce(nullif(r->>'scd_hash', ''), nullif(r->>'record_hash', '')) AS scd_hash
                FROM src
            )
            INSERT INTO warehouse.dim_campaign (
                campaign_sk, campaign_id, campaign_name, campaign_type, channel,
                traffic_source, target_segment, campaign_start_date, campaign_end_date,
                budget, effective_from, effective_to, is_current, source_updated_at, scd_hash
            )
            SELECT DISTINCT ON (campaign_sk)
                campaign_sk, campaign_id, campaign_name, campaign_type, channel,
                traffic_source, target_segment, campaign_start_date, campaign_end_date,
                budget,
                effective_from,
                CASE
                    WHEN effective_to <= effective_from THEN
                        CASE
                            WHEN is_current THEN TIMESTAMP '9999-12-31 00:00:00'
                            ELSE effective_from + INTERVAL '1 microsecond'
                        END
                    ELSE effective_to
                END AS effective_to,
                is_current, source_updated_at, scd_hash
            FROM mapped
            WHERE campaign_sk IS NOT NULL
            ORDER BY campaign_sk, source_updated_at DESC NULLS LAST
            ON CONFLICT (campaign_sk) DO UPDATE SET
                campaign_id = EXCLUDED.campaign_id,
                campaign_name = EXCLUDED.campaign_name,
                campaign_type = EXCLUDED.campaign_type,
                channel = EXCLUDED.channel,
                traffic_source = EXCLUDED.traffic_source,
                target_segment = EXCLUDED.target_segment,
                campaign_start_date = EXCLUDED.campaign_start_date,
                campaign_end_date = EXCLUDED.campaign_end_date,
                budget = EXCLUDED.budget,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                is_current = EXCLUDED.is_current,
                source_updated_at = EXCLUDED.source_updated_at,
                scd_hash = EXCLUDED.scd_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Load dim_date from all available fact staging tables
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    source_table TEXT;
BEGIN
    FOREACH source_table IN ARRAY ARRAY[
        'staging.stg_gold_fact_orders_scd2',
        'staging.stg_gold_fact_orders',
        'staging.fact_orders_scd2',
        'staging.fact_orders',
        'staging.stg_gold_fact_order_items_scd2',
        'staging.stg_gold_fact_order_items',
        'staging.fact_order_items_scd2',
        'staging.fact_order_items',
        'staging.stg_gold_fact_campaign_spend_scd2',
        'staging.stg_gold_fact_ad_spend',
        'staging.fact_campaign_spend_scd2',
        'staging.fact_ad_spend',
        'staging.stg_gold_fact_web_events',
        'staging.fact_web_events'
    ]
    LOOP
        IF to_regclass(source_table) IS NOT NULL THEN
            EXECUTE format($sql$
                INSERT INTO warehouse.dim_date (
                    date_sk, full_date, day_of_month, day_name, day_of_week,
                    week_of_year, month_number, month_name, quarter_number, year_number,
                    is_weekend, month_start_date, quarter_start_date, year_start_date
                )
                SELECT DISTINCT
                    warehouse.date_sk(d) AS date_sk,
                    d AS full_date,
                    extract(day from d)::INTEGER AS day_of_month,
                    trim(to_char(d, 'Day')) AS day_name,
                    extract(isodow from d)::INTEGER AS day_of_week,
                    extract(week from d)::INTEGER AS week_of_year,
                    extract(month from d)::INTEGER AS month_number,
                    trim(to_char(d, 'Month')) AS month_name,
                    extract(quarter from d)::INTEGER AS quarter_number,
                    extract(year from d)::INTEGER AS year_number,
                    extract(isodow from d)::INTEGER IN (6, 7) AS is_weekend,
                    date_trunc('month', d)::DATE AS month_start_date,
                    date_trunc('quarter', d)::DATE AS quarter_start_date,
                    date_trunc('year', d)::DATE AS year_start_date
                FROM (
                    SELECT coalesce(
                        warehouse.safe_date(r->>'order_date'),
                        warehouse.safe_timestamp(r->>'order_timestamp')::DATE,
                        warehouse.safe_timestamp(r->>'event_timestamp')::DATE,
                        warehouse.safe_timestamp(r->>'event_time')::DATE,
                        warehouse.safe_date(r->>'spend_date'),
                        warehouse.safe_date(r->>'date'),
                        warehouse.safe_date(r->>'gold_processed_date')
                    ) AS d
                    FROM (SELECT to_jsonb(s) AS r FROM %s s) src
                ) dates
                WHERE d IS NOT NULL
                ON CONFLICT (date_sk) DO NOTHING;
            $sql$, source_table);
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Load facts
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_fact_orders_scd2') IS NOT NULL THEN 'staging.stg_gold_fact_orders_scd2'
        WHEN to_regclass('staging.fact_orders_scd2') IS NOT NULL THEN 'staging.fact_orders_scd2'
        WHEN to_regclass('staging.stg_gold_fact_orders') IS NOT NULL THEN 'staging.stg_gold_fact_orders'
        WHEN to_regclass('staging.fact_orders') IS NOT NULL THEN 'staging.fact_orders'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No orders fact staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(nullif(r->>'order_id', ''), nullif(r->>'transaction_id', ''), 'order|' || md5(r::TEXT)) AS order_id,
                    warehouse.safe_bigint(r->>'customer_sk') AS source_customer_sk,
                    warehouse.safe_bigint(r->>'campaign_sk') AS source_campaign_sk,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(
                        warehouse.safe_timestamp(r->>'order_timestamp'),
                        warehouse.safe_timestamp(r->>'event_timestamp'),
                        warehouse.safe_date(r->>'order_date')::TIMESTAMP
                    ) AS order_ts,
                    coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), nullif(r->>'source', ''), 'Unknown') AS channel_value,
                    coalesce(nullif(r->>'order_status', ''), 'completed') AS order_status,
                    nullif(r->>'payment_method', '') AS payment_method,
                    coalesce(warehouse.safe_int(r->>'item_count'), warehouse.safe_int(r->>'quantity'), 0) AS item_count,
                    coalesce(
                        warehouse.safe_numeric(r->>'gross_amount'),
                        warehouse.safe_numeric(r->>'total_amount'),
                        warehouse.safe_numeric(r->>'order_total'),
                        warehouse.safe_numeric(r->>'total_revenue'),
                        warehouse.safe_numeric(r->>'cart_value'),
                        0
                    ) AS gross_amount,
                    coalesce(warehouse.safe_numeric(r->>'discount_amount'), 0) AS discount_amount,
                    coalesce(warehouse.safe_numeric(r->>'tax_amount'), 0) AS tax_amount,
                    coalesce(warehouse.safe_numeric(r->>'shipping_amount'), 0) AS shipping_amount,
                    coalesce(
                        warehouse.safe_numeric(r->>'net_revenue'),
                        warehouse.safe_numeric(r->>'revenue'),
                        warehouse.safe_numeric(r->>'total_revenue'),
                        warehouse.safe_numeric(r->>'cart_value'),
                        warehouse.safe_numeric(r->>'gross_amount'),
                        0
                    ) AS net_revenue,
                    coalesce(nullif(r->>'record_hash', ''), nullif(r->>'source_record_hash', ''), md5(r::TEXT)) AS source_record_hash
                FROM src
            )
            INSERT INTO warehouse.fact_orders (
                order_id, customer_sk, campaign_sk, order_date_sk, region_sk, channel_sk,
                order_timestamp, order_status, payment_method, item_count,
                gross_amount, discount_amount, tax_amount, shipping_amount, net_revenue,
                source_record_hash
            )
            SELECT DISTINCT ON (b.order_id)
                b.order_id,
                coalesce(dc.customer_sk, nullif(b.source_customer_sk, 0), 0) AS customer_sk,
                coalesce(dcamp.campaign_sk, nullif(b.source_campaign_sk, 0), 0) AS campaign_sk,
                warehouse.date_sk(b.order_ts::DATE) AS order_date_sk,
                coalesce(dr.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, 0) AS channel_sk,
                b.order_ts,
                b.order_status,
                b.payment_method,
                b.item_count,
                b.gross_amount,
                b.discount_amount,
                b.tax_amount,
                b.shipping_amount,
                b.net_revenue,
                b.source_record_hash
            FROM base b
            LEFT JOIN LATERAL (
                SELECT customer_sk
                FROM warehouse.dim_customer dc
                WHERE dc.customer_id = b.customer_id
                  AND (
                    (b.order_ts IS NOT NULL AND b.order_ts >= dc.effective_from AND b.order_ts < dc.effective_to)
                    OR (b.order_ts IS NULL AND dc.is_current)
                  )
                ORDER BY dc.is_current DESC, dc.effective_from DESC
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN LATERAL (
                SELECT campaign_sk
                FROM warehouse.dim_campaign dcamp
                WHERE dcamp.campaign_id = b.campaign_id
                  AND (
                    (b.order_ts IS NOT NULL AND b.order_ts >= dcamp.effective_from AND b.order_ts < dcamp.effective_to)
                    OR (b.order_ts IS NULL AND dcamp.is_current)
                  )
                ORDER BY dcamp.is_current DESC, dcamp.effective_from DESC
                LIMIT 1
            ) dcamp ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value)
            ORDER BY b.order_id, b.order_ts DESC NULLS LAST
            ON CONFLICT (order_id) DO UPDATE SET
                customer_sk = EXCLUDED.customer_sk,
                campaign_sk = EXCLUDED.campaign_sk,
                order_date_sk = EXCLUDED.order_date_sk,
                region_sk = EXCLUDED.region_sk,
                channel_sk = EXCLUDED.channel_sk,
                order_timestamp = EXCLUDED.order_timestamp,
                order_status = EXCLUDED.order_status,
                payment_method = EXCLUDED.payment_method,
                item_count = EXCLUDED.item_count,
                gross_amount = EXCLUDED.gross_amount,
                discount_amount = EXCLUDED.discount_amount,
                tax_amount = EXCLUDED.tax_amount,
                shipping_amount = EXCLUDED.shipping_amount,
                net_revenue = EXCLUDED.net_revenue,
                source_record_hash = EXCLUDED.source_record_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_fact_order_items_scd2') IS NOT NULL THEN 'staging.stg_gold_fact_order_items_scd2'
        WHEN to_regclass('staging.fact_order_items_scd2') IS NOT NULL THEN 'staging.fact_order_items_scd2'
        WHEN to_regclass('staging.stg_gold_fact_order_items') IS NOT NULL THEN 'staging.stg_gold_fact_order_items'
        WHEN to_regclass('staging.fact_order_items') IS NOT NULL THEN 'staging.fact_order_items'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No order-items fact staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(nullif(r->>'order_item_id', ''), nullif(r->>'line_item_id', ''), 'item|' || md5(r::TEXT)) AS order_item_nk,
                    coalesce(nullif(r->>'order_id', ''), nullif(r->>'transaction_id', '')) AS order_id,
                    warehouse.safe_bigint(r->>'customer_sk') AS source_customer_sk,
                    warehouse.safe_bigint(r->>'product_sk') AS source_product_sk,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif(r->>'product_id', ''), 'UNKNOWN') AS product_id,
                    coalesce(
                        warehouse.safe_timestamp(r->>'order_timestamp'),
                        warehouse.safe_timestamp(r->>'event_timestamp'),
                        warehouse.safe_date(r->>'order_date')::TIMESTAMP
                    ) AS order_ts,
                    coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), nullif(r->>'source', ''), 'Unknown') AS channel_value,
                    coalesce(warehouse.safe_int(r->>'quantity'), 1) AS quantity,
                    coalesce(
                        warehouse.safe_numeric(r->>'unit_price'),
                        warehouse.safe_numeric(r->>'discounted_price'),
                        warehouse.safe_numeric(r->>'price'),
                        0
                    ) AS unit_price,
                    coalesce(warehouse.safe_numeric(r->>'discount_percent'), 0) AS discount_percent,
                    coalesce(
                        warehouse.safe_numeric(r->>'line_amount'),
                        warehouse.safe_numeric(r->>'line_revenue'),
                        warehouse.safe_numeric(r->>'line_total'),
                        0
                    ) AS source_line_revenue
                FROM src
            )
            INSERT INTO warehouse.fact_order_items (
                order_item_nk, order_sk, order_id, customer_sk, product_sk,
                order_date_sk, region_sk, channel_sk, order_timestamp,
                quantity, unit_price, discount_percent, line_revenue
            )
            SELECT DISTINCT ON (b.order_item_nk)
                b.order_item_nk,
                fo.order_sk,
                b.order_id,
                coalesce(nullif(fo.customer_sk, 0), dc.customer_sk, nullif(b.source_customer_sk, 0), 0) AS customer_sk,
                coalesce(dp.product_sk, nullif(b.source_product_sk, 0), 0) AS product_sk,
                coalesce(nullif(warehouse.date_sk(b.order_ts::DATE), 0), fo.order_date_sk, 0) AS order_date_sk,
                coalesce(dr.region_sk, fo.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, fo.channel_sk, 0) AS channel_sk,
                coalesce(fo.order_timestamp, b.order_ts) AS order_timestamp,
                b.quantity,
                b.unit_price,
                b.discount_percent,
                CASE
                    WHEN b.source_line_revenue > 0 THEN b.source_line_revenue
                    ELSE b.quantity * b.unit_price
                END AS line_revenue
            FROM base b
            LEFT JOIN warehouse.fact_orders fo
                ON fo.order_id = b.order_id
            LEFT JOIN LATERAL (
                SELECT customer_sk
                FROM warehouse.dim_customer dc
                WHERE dc.customer_id = b.customer_id
                  AND (
                    (coalesce(fo.order_timestamp, b.order_ts) IS NOT NULL AND coalesce(fo.order_timestamp, b.order_ts) >= dc.effective_from AND coalesce(fo.order_timestamp, b.order_ts) < dc.effective_to)
                    OR (coalesce(fo.order_timestamp, b.order_ts) IS NULL AND dc.is_current)
                  )
                ORDER BY dc.is_current DESC, dc.effective_from DESC
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN LATERAL (
                SELECT product_sk
                FROM warehouse.dim_product dp
                WHERE dp.product_id = b.product_id
                  AND (
                    (coalesce(fo.order_timestamp, b.order_ts) IS NOT NULL AND coalesce(fo.order_timestamp, b.order_ts) >= dp.effective_from AND coalesce(fo.order_timestamp, b.order_ts) < dp.effective_to)
                    OR (coalesce(fo.order_timestamp, b.order_ts) IS NULL AND dp.is_current)
                  )
                ORDER BY dp.is_current DESC, dp.effective_from DESC
                LIMIT 1
            ) dp ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value)
            ORDER BY b.order_item_nk, b.order_ts DESC NULLS LAST
            ON CONFLICT (order_item_nk) DO UPDATE SET
                order_sk = EXCLUDED.order_sk,
                order_id = EXCLUDED.order_id,
                customer_sk = EXCLUDED.customer_sk,
                product_sk = EXCLUDED.product_sk,
                order_date_sk = EXCLUDED.order_date_sk,
                region_sk = EXCLUDED.region_sk,
                channel_sk = EXCLUDED.channel_sk,
                order_timestamp = EXCLUDED.order_timestamp,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                discount_percent = EXCLUDED.discount_percent,
                line_revenue = EXCLUDED.line_revenue,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;


-- ---------------------------------------------------------------------------
-- Backfill order-level item_count from loaded order-item facts
-- ---------------------------------------------------------------------------
-- fact_orders is order-grain. Some Gold order outputs contain revenue but do
-- not contain item_count. After fact_order_items is loaded, derive item_count
-- from matching order item rows so order-level reporting is not misleading.

UPDATE warehouse.fact_orders fo
SET
    item_count = item_counts.item_count,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        order_id,
        COALESCE(
            NULLIF(SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END), 0),
            COUNT(*)
        )::INTEGER AS item_count
    FROM warehouse.fact_order_items
    WHERE order_id IS NOT NULL
    GROUP BY order_id
) item_counts
WHERE fo.order_id = item_counts.order_id;

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_fact_campaign_spend_scd2') IS NOT NULL THEN 'staging.stg_gold_fact_campaign_spend_scd2'
        WHEN to_regclass('staging.fact_campaign_spend_scd2') IS NOT NULL THEN 'staging.fact_campaign_spend_scd2'
        WHEN to_regclass('staging.stg_gold_fact_ad_spend') IS NOT NULL THEN 'staging.stg_gold_fact_ad_spend'
        WHEN to_regclass('staging.fact_ad_spend') IS NOT NULL THEN 'staging.fact_ad_spend'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No campaign-spend/ad-spend fact staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(
                        nullif(r->>'spend_id', ''),
                        nullif(r->>'campaign_spend_id', ''),
                        nullif(r->>'ad_spend_id', ''),
                        coalesce(nullif(r->>'campaign_id', ''), 'UNKNOWN')
                            || '|'
                            || coalesce(nullif(r->>'spend_date', ''), nullif(r->>'date', ''), 'unknown-date')
                            || '|'
                            || coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), 'unknown-channel'),
                        'spend|' || md5(r::TEXT)
                    ) AS campaign_spend_nk,
                    warehouse.safe_bigint(r->>'campaign_sk') AS source_campaign_sk,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(warehouse.safe_date(r->>'spend_date'), warehouse.safe_date(r->>'date')) AS spend_date,
                    coalesce(nullif(r->>'city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), nullif(r->>'source', ''), 'Unknown') AS channel_value,
                    coalesce(warehouse.safe_numeric(r->>'spend_amount'), warehouse.safe_numeric(r->>'ad_spend'), warehouse.safe_numeric(r->>'cost'), 0) AS spend_amount,
                    coalesce(warehouse.safe_bigint(r->>'impressions'), 0) AS impressions,
                    coalesce(warehouse.safe_bigint(r->>'clicks'), 0) AS clicks,
                    coalesce(warehouse.safe_bigint(r->>'conversions'), 0) AS conversions,
                    coalesce(warehouse.safe_numeric(r->>'attributed_revenue'), warehouse.safe_numeric(r->>'revenue'), 0) AS attributed_revenue
                FROM src
            )
            INSERT INTO warehouse.fact_campaign_spend (
                campaign_spend_nk, campaign_sk, spend_date_sk, region_sk, channel_sk,
                spend_date, spend_amount, impressions, clicks, conversions, attributed_revenue
            )
            SELECT DISTINCT ON (b.campaign_spend_nk)
                b.campaign_spend_nk,
                coalesce(dc.campaign_sk, nullif(b.source_campaign_sk, 0), 0) AS campaign_sk,
                warehouse.date_sk(b.spend_date) AS spend_date_sk,
                coalesce(dr.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, 0) AS channel_sk,
                b.spend_date,
                b.spend_amount,
                b.impressions,
                b.clicks,
                b.conversions,
                b.attributed_revenue
            FROM base b
            LEFT JOIN LATERAL (
                SELECT campaign_sk
                FROM warehouse.dim_campaign dc
                WHERE dc.campaign_id = b.campaign_id
                  AND (
                    (b.spend_date IS NOT NULL AND b.spend_date::TIMESTAMP >= dc.effective_from AND b.spend_date::TIMESTAMP < dc.effective_to)
                    OR (b.spend_date IS NULL AND dc.is_current)
                  )
                ORDER BY dc.is_current DESC, dc.effective_from DESC
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value)
            ORDER BY b.campaign_spend_nk, b.spend_date DESC NULLS LAST
            ON CONFLICT (campaign_spend_nk) DO UPDATE SET
                campaign_sk = EXCLUDED.campaign_sk,
                spend_date_sk = EXCLUDED.spend_date_sk,
                region_sk = EXCLUDED.region_sk,
                channel_sk = EXCLUDED.channel_sk,
                spend_date = EXCLUDED.spend_date,
                spend_amount = EXCLUDED.spend_amount,
                impressions = EXCLUDED.impressions,
                clicks = EXCLUDED.clicks,
                conversions = EXCLUDED.conversions,
                attributed_revenue = EXCLUDED.attributed_revenue,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Inferred late-arriving web-event dimension members
-- ---------------------------------------------------------------------------
-- Web events can contain natural keys that are not present in Gold/SCD2 dims.
-- These rows create current inferred dimension members so facts do not collapse
-- source-provided natural keys into the Unknown surrogate key.
INSERT INTO warehouse.dim_customer (
    customer_sk,
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
    country,
    effective_from,
    effective_to,
    is_current,
    source_updated_at,
    scd_hash,
    warehouse_loaded_at
)
SELECT
    warehouse.deterministic_sk('web_event_customer', web.customer_id) AS customer_sk,
    web.customer_id,
    'Inferred Customer ' || web.customer_id AS customer_name,
    NULL AS email,
    'Unknown' AS gender,
    NULL AS age,
    'Unknown' AS membership_tier,
    0 AS loyalty_points,
    NULL AS preferred_language,
    'Inferred Web Event Customer' AS customer_segment,
    FALSE AS is_prime_user,
    web.home_city,
    'Unknown' AS home_state,
    web.country,
    TIMESTAMP '1900-01-01 00:00:00' AS effective_from,
    TIMESTAMP '9999-12-31 00:00:00' AS effective_to,
    TRUE AS is_current,
    web.source_updated_at,
    md5('inferred_web_event_customer|' || web.customer_id) AS scd_hash,
    CURRENT_TIMESTAMP AS warehouse_loaded_at
FROM (
    SELECT DISTINCT ON (s.customer_id::TEXT)
        s.customer_id::TEXT AS customer_id,
        coalesce(nullif(s.city, ''), 'Unknown') AS home_city,
        coalesce(nullif(s.country, ''), 'Unknown') AS country,
        s.event_timestamp AS source_updated_at
    FROM staging.stg_gold_fact_web_events s
    WHERE s.customer_id IS NOT NULL
    ORDER BY s.customer_id::TEXT, s.event_timestamp DESC NULLS LAST
) web
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.dim_customer d
    WHERE d.customer_id = web.customer_id
)
ON CONFLICT DO NOTHING;

INSERT INTO warehouse.dim_product (
    product_sk,
    product_id,
    product_name,
    category,
    brand,
    original_price,
    current_price,
    inventory_remaining,
    effective_from,
    effective_to,
    is_current,
    source_updated_at,
    scd_hash,
    warehouse_loaded_at
)
SELECT
    warehouse.deterministic_sk('web_event_product', web.product_id) AS product_sk,
    web.product_id,
    coalesce(web.product_name, 'Inferred Product ' || web.product_id) AS product_name,
    coalesce(web.category, 'Unknown') AS category,
    'Unknown' AS brand,
    NULL AS original_price,
    NULL AS current_price,
    NULL AS inventory_remaining,
    TIMESTAMP '1900-01-01 00:00:00' AS effective_from,
    TIMESTAMP '9999-12-31 00:00:00' AS effective_to,
    TRUE AS is_current,
    web.source_updated_at,
    md5('inferred_web_event_product|' || web.product_id) AS scd_hash,
    CURRENT_TIMESTAMP AS warehouse_loaded_at
FROM (
    SELECT DISTINCT ON (s.product_id::TEXT)
        s.product_id::TEXT AS product_id,
        nullif(s.product_name, '') AS product_name,
        nullif(s.category, '') AS category,
        s.event_timestamp AS source_updated_at
    FROM staging.stg_gold_fact_web_events s
    WHERE s.product_id IS NOT NULL
    ORDER BY s.product_id::TEXT, s.event_timestamp DESC NULLS LAST
) web
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.dim_product d
    WHERE d.product_id = web.product_id
)
ON CONFLICT DO NOTHING;

INSERT INTO warehouse.dim_campaign (
    campaign_sk,
    campaign_id,
    campaign_name,
    campaign_type,
    channel,
    traffic_source,
    target_segment,
    campaign_start_date,
    campaign_end_date,
    budget,
    effective_from,
    effective_to,
    is_current,
    source_updated_at,
    scd_hash,
    warehouse_loaded_at
)
SELECT
    warehouse.deterministic_sk('web_event_campaign', web.campaign_id) AS campaign_sk,
    web.campaign_id,
    'Inferred Campaign ' || web.campaign_id AS campaign_name,
    'inferred_web_event' AS campaign_type,
    coalesce(web.traffic_source, 'Unknown') AS channel,
    coalesce(web.traffic_source, 'Unknown') AS traffic_source,
    'Unknown' AS target_segment,
    NULL AS campaign_start_date,
    NULL AS campaign_end_date,
    NULL AS budget,
    TIMESTAMP '1900-01-01 00:00:00' AS effective_from,
    TIMESTAMP '9999-12-31 00:00:00' AS effective_to,
    TRUE AS is_current,
    web.source_updated_at,
    md5('inferred_web_event_campaign|' || web.campaign_id) AS scd_hash,
    CURRENT_TIMESTAMP AS warehouse_loaded_at
FROM (
    SELECT DISTINCT ON ((s.campaign_id::BIGINT)::TEXT)
        (s.campaign_id::BIGINT)::TEXT AS campaign_id,
        nullif(s.traffic_source, '') AS traffic_source,
        s.event_timestamp AS source_updated_at
    FROM staging.stg_gold_fact_web_events s
    WHERE s.campaign_id IS NOT NULL
    ORDER BY (s.campaign_id::BIGINT)::TEXT, s.event_timestamp DESC NULLS LAST
) web
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.dim_campaign d
    WHERE d.campaign_id = web.campaign_id
)
ON CONFLICT DO NOTHING;

DO $$
DECLARE
    source_table TEXT;
BEGIN
    source_table := CASE
        WHEN to_regclass('staging.stg_gold_fact_web_events') IS NOT NULL THEN 'staging.stg_gold_fact_web_events'
        WHEN to_regclass('staging.fact_web_events') IS NOT NULL THEN 'staging.fact_web_events'
        ELSE NULL
    END;

    IF source_table IS NULL THEN
        RAISE NOTICE 'No web-events fact staging table found.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(nullif(r->>'event_id', ''), 'event|' || md5(r::TEXT)) AS event_id,
                    nullif(r->>'session_id', '') AS session_id,
                    warehouse.safe_bigint(r->>'customer_sk') AS source_customer_sk,
                    warehouse.safe_bigint(r->>'product_sk') AS source_product_sk,
                    warehouse.safe_bigint(r->>'campaign_sk') AS source_campaign_sk,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif(r->>'product_id', ''), 'UNKNOWN') AS product_id,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(warehouse.safe_timestamp(r->>'event_timestamp'), warehouse.safe_timestamp(r->>'event_time')) AS event_ts,
                    coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), nullif(r->>'source', ''), 'Unknown') AS channel_value,
                    coalesce(nullif(r->>'event_type', ''), 'unknown') AS event_type,
                    nullif(r->>'user_journey_stage', '') AS user_journey_stage,
                    nullif(r->>'device_type', '') AS device_type,
                    nullif(r->>'operating_system', '') AS operating_system,
                    nullif(r->>'browser', '') AS browser,
                    nullif(r->>'network_type', '') AS network_type,
                    nullif(r->>'traffic_source', '') AS traffic_source,
                    nullif(r->>'search_query', '') AS search_query,
                    warehouse.safe_int(r->>'time_on_page_sec') AS time_on_page_sec,
                    warehouse.safe_numeric(r->>'scroll_depth_percent') AS scroll_depth_percent,
                    warehouse.safe_numeric(r->>'engagement_score') AS engagement_score,
                    warehouse.safe_numeric(r->>'purchase_probability') AS purchase_probability,
                    warehouse.safe_numeric(r->>'cart_abandonment_probability') AS cart_abandonment_probability,
                    coalesce(
                        warehouse.safe_numeric(r->>'event_value'),
                        warehouse.safe_numeric(r->>'cart_value'),
                        warehouse.safe_numeric(r->>'discounted_price'),
                        warehouse.safe_numeric(r->>'revenue'),
                        0
                    ) AS event_value,
                    warehouse.safe_int(r->>'api_latency_ms') AS api_latency_ms,
                    warehouse.safe_int(r->>'page_load_time_ms') AS page_load_time_ms,
                    warehouse.safe_numeric(r->>'fraud_score') AS fraud_score,
                    nullif(r->>'schema_version', '') AS schema_version,
                    coalesce(nullif(r->>'source', ''), nullif(r->>'source_system', '')) AS source_system
                FROM src
            )
            INSERT INTO warehouse.fact_web_events (
                event_id, session_id, customer_sk, product_sk, campaign_sk,
                event_date_sk, region_sk, channel_sk, event_timestamp, event_type,
                user_journey_stage, device_type, operating_system, browser, network_type,
                traffic_source, search_query, time_on_page_sec, scroll_depth_percent,
                engagement_score, purchase_probability, cart_abandonment_probability,
                event_value, api_latency_ms, page_load_time_ms, fraud_score,
                schema_version, source_system
            )
            SELECT DISTINCT ON (b.event_id)
                b.event_id,
                b.session_id,
                coalesce(dc.customer_sk, nullif(b.source_customer_sk, 0), 0) AS customer_sk,
                coalesce(dp.product_sk, nullif(b.source_product_sk, 0), 0) AS product_sk,
                coalesce(dcamp.campaign_sk, nullif(b.source_campaign_sk, 0), 0) AS campaign_sk,
                warehouse.date_sk(b.event_ts::DATE) AS event_date_sk,
                coalesce(dr.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, 0) AS channel_sk,
                b.event_ts,
                b.event_type,
                b.user_journey_stage,
                b.device_type,
                b.operating_system,
                b.browser,
                b.network_type,
                b.traffic_source,
                b.search_query,
                b.time_on_page_sec,
                b.scroll_depth_percent,
                b.engagement_score,
                b.purchase_probability,
                b.cart_abandonment_probability,
                b.event_value,
                b.api_latency_ms,
                b.page_load_time_ms,
                b.fraud_score,
                b.schema_version,
                b.source_system
            FROM base b
            LEFT JOIN LATERAL (
                SELECT customer_sk
                FROM warehouse.dim_customer dc
                WHERE dc.customer_id = b.customer_id
                ORDER BY
                    CASE
                        WHEN b.event_ts IS NOT NULL
                         AND b.event_ts >= dc.effective_from
                         AND b.event_ts < dc.effective_to THEN 1
                        WHEN dc.is_current THEN 2
                        ELSE 3
                    END,
                    dc.effective_from DESC
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN LATERAL (
                SELECT product_sk
                FROM warehouse.dim_product dp
                WHERE dp.product_id = b.product_id
                ORDER BY
                    CASE
                        WHEN b.event_ts IS NOT NULL
                         AND b.event_ts >= dp.effective_from
                         AND b.event_ts < dp.effective_to THEN 1
                        WHEN dp.is_current THEN 2
                        ELSE 3
                    END,
                    dp.effective_from DESC
                LIMIT 1
            ) dp ON TRUE
            LEFT JOIN LATERAL (
                SELECT campaign_sk
                FROM warehouse.dim_campaign dcamp
                WHERE dcamp.campaign_id = b.campaign_id
                ORDER BY
                    CASE
                        WHEN b.event_ts IS NOT NULL
                         AND b.event_ts >= dcamp.effective_from
                         AND b.event_ts < dcamp.effective_to THEN 1
                        WHEN dcamp.is_current THEN 2
                        ELSE 3
                    END,
                    dcamp.effective_from DESC
                LIMIT 1
            ) dcamp ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value)
            ORDER BY b.event_id, b.event_ts DESC NULLS LAST
            ON CONFLICT (event_id) DO UPDATE SET
                session_id = EXCLUDED.session_id,
                customer_sk = EXCLUDED.customer_sk,
                product_sk = EXCLUDED.product_sk,
                campaign_sk = EXCLUDED.campaign_sk,
                event_date_sk = EXCLUDED.event_date_sk,
                region_sk = EXCLUDED.region_sk,
                channel_sk = EXCLUDED.channel_sk,
                event_timestamp = EXCLUDED.event_timestamp,
                event_type = EXCLUDED.event_type,
                user_journey_stage = EXCLUDED.user_journey_stage,
                device_type = EXCLUDED.device_type,
                operating_system = EXCLUDED.operating_system,
                browser = EXCLUDED.browser,
                network_type = EXCLUDED.network_type,
                traffic_source = EXCLUDED.traffic_source,
                search_query = EXCLUDED.search_query,
                time_on_page_sec = EXCLUDED.time_on_page_sec,
                scroll_depth_percent = EXCLUDED.scroll_depth_percent,
                engagement_score = EXCLUDED.engagement_score,
                purchase_probability = EXCLUDED.purchase_probability,
                cart_abandonment_probability = EXCLUDED.cart_abandonment_probability,
                event_value = EXCLUDED.event_value,
                api_latency_ms = EXCLUDED.api_latency_ms,
                page_load_time_ms = EXCLUDED.page_load_time_ms,
                fraud_score = EXCLUDED.fraud_score,
                schema_version = EXCLUDED.schema_version,
                source_system = EXCLUDED.source_system,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

INSERT INTO warehouse.fact_conversions (
    conversion_nk, event_sk, event_id, customer_sk, product_sk, campaign_sk,
    conversion_date_sk, region_sk, channel_sk, conversion_timestamp,
    conversion_type, conversion_value
)
SELECT
    'conversion|' || event_id AS conversion_nk,
    event_sk,
    event_id,
    customer_sk,
    product_sk,
    campaign_sk,
    event_date_sk,
    region_sk,
    channel_sk,
    event_timestamp,
    event_type AS conversion_type,
    event_value AS conversion_value
FROM warehouse.fact_web_events
WHERE lower(event_type) IN (
    'purchase',
    'conversion',
    'converted',
    'checkout_completed',
    'order_completed',
    'payment_success'
)
ON CONFLICT (conversion_nk) DO UPDATE SET
    event_sk = EXCLUDED.event_sk,
    event_id = EXCLUDED.event_id,
    customer_sk = EXCLUDED.customer_sk,
    product_sk = EXCLUDED.product_sk,
    campaign_sk = EXCLUDED.campaign_sk,
    conversion_date_sk = EXCLUDED.conversion_date_sk,
    region_sk = EXCLUDED.region_sk,
    channel_sk = EXCLUDED.channel_sk,
    conversion_timestamp = EXCLUDED.conversion_timestamp,
    conversion_type = EXCLUDED.conversion_type,
    conversion_value = EXCLUDED.conversion_value,
    warehouse_loaded_at = CURRENT_TIMESTAMP;


-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- Practical warehouse indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_dim_customer_natural_current
    ON warehouse.dim_customer(customer_id, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_customer_effective_range
    ON warehouse.dim_customer(customer_id, effective_from, effective_to);

CREATE INDEX IF NOT EXISTS idx_dim_product_natural_current
    ON warehouse.dim_product(product_id, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_product_effective_range
    ON warehouse.dim_product(product_id, effective_from, effective_to);

CREATE INDEX IF NOT EXISTS idx_dim_campaign_natural_current
    ON warehouse.dim_campaign(campaign_id, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_campaign_effective_range
    ON warehouse.dim_campaign(campaign_id, effective_from, effective_to);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date
    ON warehouse.fact_orders(order_date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
    ON warehouse.fact_orders(customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_campaign
    ON warehouse.fact_orders(campaign_sk);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_order
    ON warehouse.fact_order_items(order_sk);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_product
    ON warehouse.fact_order_items(product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_campaign_spend_campaign_date
    ON warehouse.fact_campaign_spend(campaign_sk, spend_date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_web_events_event_date
    ON warehouse.fact_web_events(event_date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_web_events_customer
    ON warehouse.fact_web_events(customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_conversions_campaign_date
    ON warehouse.fact_conversions(campaign_sk, conversion_date_sk);

COMMIT;

-- ---------------------------------------------------------------------------
-- Quick validation queries
-- Run manually after this file, or copy into psql.
-- ---------------------------------------------------------------------------

/*
SELECT 'warehouse.dim_customer' AS table_name, count(*) AS row_count FROM warehouse.dim_customer
UNION ALL SELECT 'warehouse.dim_product', count(*) FROM warehouse.dim_product
UNION ALL SELECT 'warehouse.dim_campaign', count(*) FROM warehouse.dim_campaign
UNION ALL SELECT 'warehouse.dim_date', count(*) FROM warehouse.dim_date
UNION ALL SELECT 'warehouse.dim_region', count(*) FROM warehouse.dim_region
UNION ALL SELECT 'warehouse.dim_channel', count(*) FROM warehouse.dim_channel
UNION ALL SELECT 'warehouse.fact_orders', count(*) FROM warehouse.fact_orders
UNION ALL SELECT 'warehouse.fact_order_items', count(*) FROM warehouse.fact_order_items
UNION ALL SELECT 'warehouse.fact_campaign_spend', count(*) FROM warehouse.fact_campaign_spend
UNION ALL SELECT 'warehouse.fact_web_events', count(*) FROM warehouse.fact_web_events
UNION ALL SELECT 'warehouse.fact_conversions', count(*) FROM warehouse.fact_conversions
ORDER BY table_name;

SELECT 'fact_orders null customer_sk' AS check_name, count(*) AS failed_rows
FROM warehouse.fact_orders WHERE customer_sk IS NULL
UNION ALL SELECT 'fact_orders null campaign_sk', count(*) FROM warehouse.fact_orders WHERE campaign_sk IS NULL
UNION ALL SELECT 'fact_orders null order_date_sk', count(*) FROM warehouse.fact_orders WHERE order_date_sk IS NULL
UNION ALL SELECT 'fact_order_items null product_sk', count(*) FROM warehouse.fact_order_items WHERE product_sk IS NULL
UNION ALL SELECT 'fact_campaign_spend null campaign_sk', count(*) FROM warehouse.fact_campaign_spend WHERE campaign_sk IS NULL
UNION ALL SELECT 'fact_web_events null customer_sk', count(*) FROM warehouse.fact_web_events WHERE customer_sk IS NULL
UNION ALL SELECT 'fact_conversions null campaign_sk', count(*) FROM warehouse.fact_conversions WHERE campaign_sk IS NULL;

SELECT customer_id, count(*) AS current_rows
FROM warehouse.dim_customer
WHERE is_current
GROUP BY customer_id
HAVING count(*) > 1;

SELECT product_id, count(*) AS current_rows
FROM warehouse.dim_product
WHERE is_current
GROUP BY product_id
HAVING count(*) > 1;

SELECT campaign_id, count(*) AS current_rows
FROM warehouse.dim_campaign
WHERE is_current
GROUP BY campaign_id
HAVING count(*) > 1;
*/
