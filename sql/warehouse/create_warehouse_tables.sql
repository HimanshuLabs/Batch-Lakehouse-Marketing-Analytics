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

-- This file is one step of the warehouse build. It refuses to run anywhere but
-- in the build transaction that sql/warehouse/begin_generation.sql started
-- (which sets p2.build_txid for that transaction only), so it can never drop or
-- rebuild the live tables on its own.
DO $$
BEGIN
    IF current_setting('p2.build_txid', true) IS DISTINCT FROM txid_current()::TEXT THEN
        RAISE EXCEPTION 'create_warehouse_tables.sql runs only inside the warehouse build transaction started by sql/warehouse/begin_generation.sql. Run the whole build: bash scripts/run_airflow_task.sh build_warehouse';
    END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------------------
-- Helper functions
-- ---------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION warehouse.normalized_natural_key(value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN value IS NULL THEN NULL
        WHEN btrim(value) = '' THEN NULL
        WHEN lower(btrim(value)) IN ('null', 'none', 'nan', 'unknown') THEN NULL
        ELSE btrim(value)
    END;
$$;


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
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
SET timezone = 'UTC'
AS $$
BEGIN
    IF value IS NULL OR btrim(value) = '' THEN
        RETURN NULL;
    END IF;

    RETURN btrim(value)::TIMESTAMPTZ;
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

-- Business dates are calendar days in Asia/Kolkata, the same as Gold (Spark
-- derives order_date and event_date in SOURCE_TIMEZONE). Stored timestamps
-- are instants (TIMESTAMPTZ, shown in UTC); only the date derived from them
-- uses IST.
CREATE OR REPLACE FUNCTION warehouse.business_date(ts TIMESTAMPTZ)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT (ts AT TIME ZONE 'Asia/Kolkata')::DATE;
$$;

-- Today in the business time zone (for "days since" measures).
CREATE OR REPLACE FUNCTION warehouse.business_today()
RETURNS DATE
LANGUAGE SQL
STABLE
AS $$
    SELECT (now() AT TIME ZONE 'Asia/Kolkata')::DATE;
$$;

-- Every surrogate key in the warehouse, dimension or fact, is the same 63-bit
-- hash, so collisions stay negligible as tables grow: the first 16 hex digits
-- of md5(prefix || '|' || natural_key), masked to 63 bits so the key is
-- non-negative, and never 0 (0 is the unknown member).
-- The dimension key of a fact. resolved_sk is the version covering the fact
-- time, when there is one. Without one: a natural ID that the dimension knows
-- gives the unknown member (0); a natural ID the dimension has never held
-- valid (its row was quarantined, or never delivered) gives the
-- invalid-reference member (-1). The fact keeps its natural ID either way, so
-- it resolves to the real member as soon as the dimension row is valid.
CREATE OR REPLACE FUNCTION warehouse.member_sk(resolved_sk BIGINT, natural_id_in_dimension BOOLEAN)
RETURNS BIGINT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT coalesce(resolved_sk, CASE WHEN natural_id_in_dimension THEN 0 ELSE -1 END);
$$;


CREATE OR REPLACE FUNCTION warehouse.deterministic_fact_sk(prefix TEXT, natural_key TEXT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE WHEN k = 0 THEN 1 ELSE k END
    FROM (
        SELECT ('x' || substr(md5(prefix || '|' || natural_key), 1, 16))::BIT(64)::BIGINT
               & 9223372036854775807 AS k
    ) AS hashed;
$$;

-- Dimension keys (customer, product, campaign, region, channel): the same hash.
CREATE OR REPLACE FUNCTION warehouse.deterministic_sk(prefix TEXT, value TEXT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown'));
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
    -- NULL when the source has no value (and on the unknown member).
    loyalty_points INTEGER,
    preferred_language TEXT,
    customer_segment TEXT,
    is_prime_user BOOLEAN,
    home_city TEXT,
    home_state TEXT,
    country TEXT,
    -- SCD Type 0: the day the account was opened, the same on every version.
    signup_date DATE,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '1900-01-01 00:00:00+00',
    effective_to TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '9999-12-31 00:00:00+00',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMPTZ,
    scd_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_customer_natural_version UNIQUE (customer_id, effective_from),
    CONSTRAINT chk_dim_customer_effective_range CHECK (effective_to > effective_from),
    CONSTRAINT chk_dim_customer_age CHECK (age IS NULL OR age BETWEEN 0 AND 120)
);

COMMENT ON TABLE warehouse.dim_customer IS
'SCD2 customer dimension. customer_sk is the surrogate key used by facts for point-in-time reporting; customer_id is the source natural key.';

COMMENT ON COLUMN warehouse.dim_customer.signup_date IS
'SCD Type 0 (fixed): the first signup date the source delivered. A later change in the source is ignored and counted in logs/scd2_quality_report.json.';
COMMENT ON COLUMN warehouse.dim_customer.email IS
'SCD Type 1 (overwrite): the customer''s latest email on every version; a change opens no version.';
COMMENT ON COLUMN warehouse.dim_customer.membership_tier IS
'SCD Type 2 (history): a change closes the current version and opens a new one (effective_from, effective_to, is_current).';

COMMENT ON COLUMN warehouse.dim_customer.customer_sk IS
'Surrogate key. Multiple customer_sk values can exist for one customer_id when SCD2 attributes change.';

CREATE TABLE warehouse.dim_product (
    product_sk BIGINT PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_name TEXT,
    category TEXT,
    brand TEXT,
    -- active or discontinued: SCD2-tracked, so a change opens a new version.
    product_status TEXT,
    original_price NUMERIC(18, 2),
    current_price NUMERIC(18, 2),
    inventory_remaining INTEGER,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '1900-01-01 00:00:00+00',
    effective_to TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '9999-12-31 00:00:00+00',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMPTZ,
    scd_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
    -- The campaign's objective: awareness, consideration, conversion or retention.
    campaign_type TEXT,
    channel TEXT,
    traffic_source TEXT,
    target_segment TEXT,
    campaign_status TEXT,
    ab_test_group TEXT,
    campaign_start_date DATE,
    campaign_end_date DATE,
    budget NUMERIC(18, 2),
    effective_from TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '1900-01-01 00:00:00+00',
    effective_to TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '9999-12-31 00:00:00+00',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    source_updated_at TIMESTAMPTZ,
    scd_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
    -- warehouse.deterministic_sk('region', region_nk); 0 is the unknown member.
    region_sk BIGINT PRIMARY KEY,
    region_nk TEXT NOT NULL UNIQUE,
    country TEXT NOT NULL DEFAULT 'Unknown',
    state TEXT NOT NULL DEFAULT 'Unknown',
    city TEXT NOT NULL DEFAULT 'Unknown',
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.dim_region IS
'Conformed geography dimension derived from customer, order, spend, and web-event locations.';

CREATE TABLE warehouse.dim_channel (
    -- warehouse.deterministic_sk('channel', channel_nk); 0 is the unknown member.
    channel_sk BIGINT PRIMARY KEY,
    channel_nk TEXT NOT NULL UNIQUE,
    channel_name TEXT NOT NULL,
    channel_group TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.dim_channel IS
'Conformed marketing/acquisition channel dimension derived from campaign, ad spend, and web-event traffic source fields.';

-- ---------------------------------------------------------------------------
-- Fact DDL
-- ---------------------------------------------------------------------------

CREATE TABLE warehouse.fact_orders (
    -- warehouse.deterministic_fact_sk('order', order_id), set by the load; a hash
    -- collision fails the load on this primary key.
    order_sk BIGINT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    -- The source natural IDs beside the keys ('UNKNOWN' when the source has none).
    customer_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    order_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    order_timestamp TIMESTAMPTZ,
    -- completed, cancelled or returned, from the source. Only a completed
    -- order is a sale: the marts count revenue from completed orders.
    order_status TEXT NOT NULL,
    -- When the status took effect: the order time for a completed order, the
    -- cancellation or the return time otherwise. The source delivers an order
    -- again when it changes; this is its latest version. A sale is recognised
    -- on the order date, a refund on the business date of status_timestamp.
    status_timestamp TIMESTAMPTZ NOT NULL,
    -- What was refunded for a returned order (all or part of net_revenue); 0 otherwise.
    refund_amount NUMERIC(18, 2) NOT NULL,
    payment_method TEXT,
    item_count INTEGER NOT NULL DEFAULT 0,
    gross_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    discount_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    net_revenue NUMERIC(18, 2) NOT NULL DEFAULT 0,
    -- What the customer was charged: total_amount = net_revenue + tax_amount
    -- + shipping_amount (checked by the reconciliation). net_revenue stays the
    -- sum of the lines.
    tax_amount NUMERIC(18, 2) NOT NULL,
    shipping_amount NUMERIC(18, 2) NOT NULL,
    total_amount NUMERIC(18, 2) NOT NULL,
    source_record_hash TEXT,
    -- The batch date (Asia/Kolkata day) of the Bronze batch that last delivered
    -- the row, and an md5 of the row's business columns. Both come from the
    -- data only, so a rebuild never changes them.
    source_updated_at TIMESTAMPTZ,
    record_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_orders_amounts CHECK (
        gross_amount >= 0
        AND discount_amount >= 0
        AND net_revenue >= 0
        AND tax_amount >= 0
        AND shipping_amount >= 0
        AND total_amount >= 0
    ),
    CONSTRAINT chk_fact_orders_status CHECK (order_status IN ('completed', 'cancelled', 'returned')),
    CONSTRAINT chk_fact_orders_lifecycle CHECK (
        status_timestamp >= order_timestamp
        AND refund_amount >= 0
        AND refund_amount <= net_revenue
        AND (order_status = 'returned') = (refund_amount > 0)
    )
);

COMMENT ON TABLE warehouse.fact_orders IS
'Order-grain fact table. Uses customer_sk/campaign_sk to preserve SCD2 point-in-time correctness.';

CREATE TABLE warehouse.fact_order_items (
    -- warehouse.deterministic_fact_sk('order_item', order_item_nk), set by the load; a hash
    -- collision fails the load on this primary key.
    order_item_sk BIGINT PRIMARY KEY,
    order_item_nk TEXT NOT NULL UNIQUE,
    order_sk BIGINT REFERENCES warehouse.fact_orders(order_sk),
    order_id TEXT,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    order_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    order_timestamp TIMESTAMPTZ,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(18, 2) NOT NULL DEFAULT 0,
    discount_percent NUMERIC(9, 4) NOT NULL DEFAULT 0,
    -- original_price x quantity, and (original_price - discounted_price) x quantity;
    -- NULL when a price is missing. gross_amount - discount_amount = line_revenue.
    gross_amount NUMERIC(18, 2),
    discount_amount NUMERIC(18, 2),
    line_revenue NUMERIC(18, 2) NOT NULL DEFAULT 0,
    -- The item's share of its order's refund (0 unless the order was returned).
    -- The point-in-time job splits the order's refund over its items in
    -- proportion to their lines, so the items of an order sum to the order's refund.
    refund_amount NUMERIC(18, 2) NOT NULL,
    -- The batch date (Asia/Kolkata day) of the Bronze batch that last delivered
    -- the row, and an md5 of the row's business columns. Both come from the
    -- data only, so a rebuild never changes them.
    source_updated_at TIMESTAMPTZ,
    record_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_order_items_metrics CHECK (
        quantity >= 0
        AND unit_price >= 0
        AND discount_percent >= 0
        AND (gross_amount IS NULL OR gross_amount >= 0)
        AND (discount_amount IS NULL OR discount_amount >= 0)
        AND line_revenue >= 0
        AND refund_amount >= 0
    )
);

COMMENT ON TABLE warehouse.fact_order_items IS
'Order-item-grain fact table. product_sk preserves product SCD2 history at item purchase time.';

CREATE TABLE warehouse.fact_campaign_spend (
    -- warehouse.deterministic_fact_sk('campaign_spend', campaign_spend_nk), set by the load; a hash
    -- collision fails the load on this primary key.
    campaign_spend_sk BIGINT PRIMARY KEY,
    campaign_spend_nk TEXT NOT NULL UNIQUE,
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    campaign_id TEXT NOT NULL,
    spend_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    -- The region the ad platform reports the row for (a state; city Unknown).
    -- The grain is campaign, day and region: a campaign's spend, and what is
    -- compared with its budget, is the sum over its regions.
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    spend_date DATE,
    spend_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
    impressions BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    -- What the ad platform itself reports for the campaign-day. The warehouse
    -- measures its own orders and conversions; the marts show both.
    conversions BIGINT NOT NULL,
    attributed_revenue NUMERIC(18, 2) NOT NULL,
    -- The batch date (Asia/Kolkata day) of the Bronze batch that last delivered
    -- the row, and an md5 of the row's business columns. Both come from the
    -- data only, so a rebuild never changes them.
    source_updated_at TIMESTAMPTZ,
    record_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_campaign_spend_metrics CHECK (
        spend_amount >= 0
        AND impressions >= 0
        AND clicks >= 0
        AND conversions >= 0
        AND attributed_revenue >= 0
    )
);

COMMENT ON TABLE warehouse.fact_campaign_spend IS
'Campaign-spend fact table. One row per campaign, day and region, as the ad platform reports it: spend, impressions, clicks, and the conversions and attributed revenue the platform itself measures. Joined to the SCD2 campaign version, channel, region and spend date.';

CREATE TABLE warehouse.fact_web_events (
    -- warehouse.deterministic_fact_sk('web_event', event_id), set by the load; a hash
    -- collision fails the load on this primary key.
    event_sk BIGINT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    session_id TEXT,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    event_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    event_timestamp TIMESTAMPTZ,
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
    -- The batch date (Asia/Kolkata day) of the Bronze batch that last delivered
    -- the row, and an md5 of the row's business columns. Both come from the
    -- data only, so a rebuild never changes them.
    source_updated_at TIMESTAMPTZ,
    record_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE warehouse.fact_web_events IS
'Web-event-grain fact table. Stores behavior events and links them to point-in-time customer/product/campaign dimensions where possible.';

CREATE TABLE warehouse.fact_conversions (
    -- warehouse.deterministic_fact_sk('conversion', conversion_nk), set by the load; a hash
    -- collision fails the load on this primary key.
    conversion_sk BIGINT PRIMARY KEY,
    conversion_nk TEXT NOT NULL UNIQUE,
    event_sk BIGINT REFERENCES warehouse.fact_web_events(event_sk),
    event_id TEXT,
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    campaign_sk BIGINT NOT NULL REFERENCES warehouse.dim_campaign(campaign_sk),
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    conversion_date_sk INTEGER NOT NULL REFERENCES warehouse.dim_date(date_sk),
    region_sk BIGINT NOT NULL REFERENCES warehouse.dim_region(region_sk),
    channel_sk BIGINT NOT NULL REFERENCES warehouse.dim_channel(channel_sk),
    conversion_timestamp TIMESTAMPTZ,
    conversion_type TEXT NOT NULL,
    conversion_value NUMERIC(18, 2) NOT NULL DEFAULT 0,
    -- The batch date (Asia/Kolkata day) of the Bronze batch that last delivered
    -- the row, and an md5 of the row's business columns. Both come from the
    -- data only, so a rebuild never changes them.
    source_updated_at TIMESTAMPTZ,
    record_hash TEXT,
    warehouse_loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_fact_conversions_value CHECK (conversion_value >= 0)
);

COMMENT ON TABLE warehouse.fact_conversions IS
'Conversion fact derived from conversion/purchase web events. Supports funnel and campaign attribution reporting.';

-- ---------------------------------------------------------------------------
-- Unknown dimension members
--
-- One convention: IDs and keys are 'UNKNOWN', display names and attribute
-- labels are 'Unknown'. The lowercase 'unknown' left inside hashed natural
-- keys (region_nk, channel_nk, deterministic_sk's NULL fallback, the spend
-- natural key) is normalized key text, not a label: changing it would change
-- existing region, channel and fact keys.
-- ---------------------------------------------------------------------------

INSERT INTO warehouse.dim_customer (
    customer_sk, customer_id, customer_name, email, gender, age,
    membership_tier, loyalty_points, preferred_language, customer_segment,
    is_prime_user, home_city, home_state, country, effective_from, effective_to,
    is_current, source_updated_at, scd_hash
)
VALUES (
    0, 'UNKNOWN', 'Unknown', NULL, NULL, NULL,
    'Unknown', NULL, NULL, 'Unknown',
    NULL, 'Unknown', 'Unknown', 'Unknown',
    TIMESTAMPTZ '1900-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE, NULL, 'UNKNOWN'
), (
    -- The invalid-reference member: facts whose key is present but has no
    -- valid dimension row (warehouse.member_sk). Distinct from the unknown
    -- member (0), which is for facts with no key at all.
    -1, 'INVALID', 'Invalid reference', NULL, NULL, NULL,
    'Invalid reference', NULL, NULL, 'Invalid reference',
    NULL, 'Invalid reference', 'Invalid reference', 'Invalid reference',
    TIMESTAMPTZ '1900-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE, NULL, 'INVALID'
)
ON CONFLICT (customer_sk) DO NOTHING;

INSERT INTO warehouse.dim_product (
    product_sk, product_id, product_name, category, brand, product_status,
    original_price, current_price, inventory_remaining,
    effective_from, effective_to, is_current, source_updated_at, scd_hash
)
VALUES (
    0, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
    0, 0, NULL,
    TIMESTAMPTZ '1900-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE, NULL, 'UNKNOWN'
), (
    -1, 'INVALID', 'Invalid reference', 'Invalid reference', 'Invalid reference', 'Invalid reference',
    0, 0, NULL,
    TIMESTAMPTZ '1900-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE, NULL, 'INVALID'
)
ON CONFLICT (product_sk) DO NOTHING;

INSERT INTO warehouse.dim_campaign (
    campaign_sk, campaign_id, campaign_name, campaign_type, channel,
    traffic_source, target_segment, campaign_status, ab_test_group, campaign_start_date, campaign_end_date,
    budget, effective_from, effective_to, is_current, source_updated_at, scd_hash
)
VALUES (
    0, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown',
    'Unknown', 'Unknown', 'Unknown', 'Unknown', NULL, NULL,
    0, TIMESTAMPTZ '1900-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE, NULL, 'UNKNOWN'
), (
    -1, 'INVALID', 'Invalid reference', 'Invalid reference', 'Invalid reference',
    'Invalid reference', 'Invalid reference', 'Invalid reference', 'Invalid reference', NULL, NULL,
    0, TIMESTAMPTZ '1900-01-01 00:00:00+00', TIMESTAMPTZ '9999-12-31 00:00:00+00',
    TRUE, NULL, 'INVALID'
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
        'staging.stg_gold_fact_campaign_spend_scd2',
        'staging.stg_gold_fact_web_events',
        'staging.fact_web_events'
    ]
    LOOP
        IF to_regclass(source_table) IS NOT NULL THEN
            -- The key is a hash of the natural key, so it never depends on insert
            -- order. Within a source the attributes are picked by a full ORDER BY;
            -- across sources the first source in the fixed list above wins.
            EXECUTE format($sql$
                INSERT INTO warehouse.dim_region (region_sk, region_nk, country, state, city)
                SELECT DISTINCT ON (region_nk)
                    warehouse.deterministic_sk('region', region_nk) AS region_sk,
                    region_nk,
                    country,
                    state,
                    city
                FROM (
                    SELECT
                        warehouse.region_nk(city, state, country) AS region_nk,
                        btrim(country) AS country,
                        btrim(state) AS state,
                        btrim(city) AS city
                    FROM (
                        SELECT
                            coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                            coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                            coalesce(nullif(r->>'country', ''), 'Unknown') AS country
                        FROM (SELECT to_jsonb(s) AS r FROM %s s) src
                    ) mapped
                ) keyed
                WHERE region_nk <> 'unknown|unknown|unknown'
                ORDER BY region_nk, city, state, country
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
            -- Same rules as dim_region: hashed key, full ORDER BY within a source,
            -- first source in the fixed list wins across sources.
            EXECUTE format($sql$
                INSERT INTO warehouse.dim_channel (channel_sk, channel_nk, channel_name, channel_group)
                SELECT DISTINCT ON (channel_nk)
                    warehouse.deterministic_sk('channel', channel_nk) AS channel_sk,
                    channel_nk,
                    channel_name,
                    channel_group
                FROM (
                    SELECT
                        warehouse.channel_nk(channel_value) AS channel_nk,
                        btrim(channel_value) AS channel_name,
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
                            nullif(r->>'marketing_channel', ''),
                            'Unknown'
                        ) AS channel_value
                        FROM (SELECT to_jsonb(s) AS r FROM %s s) src
                    ) mapped
                ) keyed
                WHERE channel_nk <> 'unknown'
                ORDER BY channel_nk, channel_name, channel_group
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
        RAISE EXCEPTION 'Missing staging table: no customer dimension staging table found; load Gold into staging first.';
    ELSE
        EXECUTE format($sql$
            CREATE TEMP TABLE dimension_input ON COMMIT DROP AS
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            mapped AS (
                SELECT
                    warehouse.deterministic_sk(
                        'customer',
                        coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN')
                        || '|'
                        || coalesce(nullif(r->>'source_updated_at', ''), nullif(r->>'effective_from', ''), nullif(r->>'valid_from', ''), '1900-01-01')
                    ) AS customer_sk,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif(r->>'customer_name', ''), nullif(r->>'user_name', ''), nullif(r->>'name', '')) AS customer_name,
                    nullif(r->>'email', '') AS email,
                    nullif(r->>'gender', '') AS gender,
                    warehouse.safe_int(r->>'age') AS age,
                    coalesce(nullif(r->>'membership_tier', ''), nullif(r->>'loyalty_tier', ''), nullif(r->>'tier', ''), 'Unknown') AS membership_tier,
                    warehouse.safe_int(r->>'loyalty_points') AS loyalty_points,
                    nullif(r->>'preferred_language', '') AS preferred_language,
                    coalesce(nullif(r->>'customer_segment', ''), nullif(r->>'user_segment', ''), 'Unknown') AS customer_segment,
                    warehouse.safe_bool(r->>'is_prime_user') AS is_prime_user,
                    coalesce(nullif(r->>'home_city', ''), nullif(r->>'city', ''), 'Unknown') AS home_city,
                    coalesce(nullif(r->>'home_state', ''), nullif(r->>'state', ''), 'Unknown') AS home_state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    warehouse.safe_date(r->>'signup_date') AS signup_date,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_from', r->>'valid_from', r->>'start_date')),
                        TIMESTAMPTZ '1900-01-01 00:00:00+00'
                    ) AS effective_from,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_to', r->>'valid_to', r->>'end_date')),
                        TIMESTAMPTZ '9999-12-31 00:00:00+00'
                    ) AS effective_to,
                    coalesce(warehouse.safe_bool(r->>'is_current'), true) AS is_current,
                    warehouse.safe_timestamp(coalesce(r->>'source_updated_at', r->>'effective_from', r->>'updated_at')) AS source_updated_at,
                    coalesce(nullif(r->>'scd_hash', ''), nullif(r->>'record_hash', '')) AS scd_hash,
                    jsonb_build_array(
                        coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN'),
                        coalesce(warehouse.safe_timestamp(coalesce(
                            nullif(r->>'source_updated_at', ''), nullif(r->>'effective_from', ''),
                            nullif(r->>'valid_from', '')
                        )), TIMESTAMPTZ '1900-01-01 00:00:00+00')
                    ) AS dimension_identity,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            )
            SELECT * FROM mapped;

            DO $guard$
            BEGIN
                IF EXISTS (
                    SELECT customer_sk FROM dimension_input
                    GROUP BY customer_sk
                    HAVING COUNT(DISTINCT dimension_identity) > 1
                ) THEN
                    RAISE EXCEPTION 'Surrogate-key collision in dim_customer: different version identities share a key';
                END IF;
            END $guard$;

            INSERT INTO warehouse.dim_customer (
                customer_sk, customer_id, customer_name, email, gender, age,
                membership_tier, loyalty_points, preferred_language, customer_segment,
                is_prime_user, home_city, home_state, country, signup_date, effective_from, effective_to,
                is_current, source_updated_at, scd_hash
            )
            SELECT DISTINCT ON (customer_sk)
                customer_sk, customer_id, customer_name, email, gender, age,
                membership_tier, loyalty_points, preferred_language, customer_segment,
                is_prime_user, home_city, home_state, country, signup_date,
                effective_from,
                CASE
                    WHEN effective_to <= effective_from THEN
                        CASE
                            WHEN is_current THEN TIMESTAMPTZ '9999-12-31 00:00:00+00'
                            ELSE effective_from + INTERVAL '1 microsecond'
                        END
                    ELSE effective_to
                END AS effective_to,
                is_current, source_updated_at, scd_hash
            FROM dimension_input
            WHERE customer_sk IS NOT NULL
            ORDER BY customer_sk, source_updated_at DESC NULLS LAST, row_fingerprint
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
                signup_date = EXCLUDED.signup_date,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                is_current = EXCLUDED.is_current,
                source_updated_at = EXCLUDED.source_updated_at,
                scd_hash = EXCLUDED.scd_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
            DROP TABLE dimension_input;
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
        RAISE EXCEPTION 'Missing staging table: no product dimension staging table found; load Gold into staging first.';
    ELSE
        EXECUTE format($sql$
            CREATE TEMP TABLE dimension_input ON COMMIT DROP AS
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            mapped AS (
                SELECT
                    warehouse.deterministic_sk(
                        'product',
                        coalesce(nullif(r->>'product_id', ''), 'UNKNOWN')
                        || '|'
                        || coalesce(nullif(r->>'source_updated_at', ''), nullif(r->>'effective_from', ''), nullif(r->>'valid_from', ''), '1900-01-01')
                    ) AS product_sk,
                    coalesce(nullif(r->>'product_id', ''), 'UNKNOWN') AS product_id,
                    coalesce(nullif(r->>'product_name', ''), nullif(r->>'name', '')) AS product_name,
                    coalesce(nullif(r->>'category', ''), nullif(r->>'product_category', ''), 'Unknown') AS category,
                    nullif(r->>'brand', '') AS brand,
                    coalesce(nullif(r->>'status', ''), nullif(r->>'product_status', '')) AS product_status,
                    coalesce(warehouse.safe_numeric(r->>'original_price'), warehouse.safe_numeric(r->>'list_price')) AS original_price,
                    coalesce(warehouse.safe_numeric(r->>'discounted_price'), warehouse.safe_numeric(r->>'current_price'), warehouse.safe_numeric(r->>'price')) AS current_price,
                    warehouse.safe_int(r->>'inventory_remaining') AS inventory_remaining,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_from', r->>'valid_from', r->>'start_date')),
                        TIMESTAMPTZ '1900-01-01 00:00:00+00'
                    ) AS effective_from,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_to', r->>'valid_to', r->>'end_date')),
                        TIMESTAMPTZ '9999-12-31 00:00:00+00'
                    ) AS effective_to,
                    coalesce(warehouse.safe_bool(r->>'is_current'), true) AS is_current,
                    warehouse.safe_timestamp(coalesce(r->>'source_updated_at', r->>'effective_from', r->>'updated_at')) AS source_updated_at,
                    coalesce(nullif(r->>'scd_hash', ''), nullif(r->>'record_hash', '')) AS scd_hash,
                    jsonb_build_array(
                        coalesce(nullif(r->>'product_id', ''), 'UNKNOWN'),
                        coalesce(warehouse.safe_timestamp(coalesce(
                            nullif(r->>'source_updated_at', ''), nullif(r->>'effective_from', ''),
                            nullif(r->>'valid_from', '')
                        )), TIMESTAMPTZ '1900-01-01 00:00:00+00')
                    ) AS dimension_identity,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            )
            SELECT * FROM mapped;

            DO $guard$
            BEGIN
                IF EXISTS (
                    SELECT product_sk FROM dimension_input
                    GROUP BY product_sk
                    HAVING COUNT(DISTINCT dimension_identity) > 1
                ) THEN
                    RAISE EXCEPTION 'Surrogate-key collision in dim_product: different version identities share a key';
                END IF;
            END $guard$;

            INSERT INTO warehouse.dim_product (
                product_sk, product_id, product_name, category, brand, product_status,
                original_price, current_price, inventory_remaining,
                effective_from, effective_to, is_current, source_updated_at, scd_hash
            )
            SELECT DISTINCT ON (product_sk)
                product_sk, product_id, product_name, category, brand, product_status,
                original_price, current_price, inventory_remaining,
                effective_from,
                CASE
                    WHEN effective_to <= effective_from THEN
                        CASE
                            WHEN is_current THEN TIMESTAMPTZ '9999-12-31 00:00:00+00'
                            ELSE effective_from + INTERVAL '1 microsecond'
                        END
                    ELSE effective_to
                END AS effective_to,
                is_current, source_updated_at, scd_hash
            FROM dimension_input
            WHERE product_sk IS NOT NULL
            ORDER BY product_sk, source_updated_at DESC NULLS LAST, row_fingerprint
            ON CONFLICT (product_sk) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                brand = EXCLUDED.brand,
                product_status = EXCLUDED.product_status,
                original_price = EXCLUDED.original_price,
                current_price = EXCLUDED.current_price,
                inventory_remaining = EXCLUDED.inventory_remaining,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                is_current = EXCLUDED.is_current,
                source_updated_at = EXCLUDED.source_updated_at,
                scd_hash = EXCLUDED.scd_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
            DROP TABLE dimension_input;
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
        RAISE EXCEPTION 'Missing staging table: no campaign dimension staging table found; load Gold into staging first.';
    ELSE
        EXECUTE format($sql$
            CREATE TEMP TABLE dimension_input ON COMMIT DROP AS
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            mapped AS (
                SELECT
                    warehouse.deterministic_sk(
                        'campaign',
                        coalesce(nullif(r->>'campaign_id', ''), 'UNKNOWN')
                        || '|'
                        || coalesce(nullif(r->>'source_updated_at', ''), nullif(r->>'effective_from', ''), nullif(r->>'valid_from', ''), '1900-01-01')
                    ) AS campaign_sk,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(nullif(r->>'campaign_name', ''), nullif(r->>'name', '')) AS campaign_name,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'marketing_channel', ''), nullif(r->>'traffic_source', ''), 'Unknown') AS channel,
                    coalesce(nullif(r->>'traffic_source', ''), nullif(r->>'channel', ''), 'Unknown') AS traffic_source,
                    coalesce(nullif(r->>'target_segment', ''), nullif(r->>'customer_segment', ''), 'Unknown') AS target_segment,
                    nullif(r->>'campaign_status', '') AS campaign_status,
                    nullif(r->>'campaign_type', '') AS campaign_type,
                    nullif(r->>'ab_test_group', '') AS ab_test_group,
                    coalesce(warehouse.safe_date(r->>'campaign_start_date'), warehouse.safe_date(r->>'start_date')) AS campaign_start_date,
                    coalesce(warehouse.safe_date(r->>'campaign_end_date'), warehouse.safe_date(r->>'end_date')) AS campaign_end_date,
                    coalesce(warehouse.safe_numeric(r->>'budget'), warehouse.safe_numeric(r->>'campaign_budget')) AS budget,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_from', r->>'valid_from', r->>'start_date')),
                        TIMESTAMPTZ '1900-01-01 00:00:00+00'
                    ) AS effective_from,
                    coalesce(
                        warehouse.safe_timestamp(coalesce(r->>'effective_to', r->>'valid_to', r->>'end_date')),
                        TIMESTAMPTZ '9999-12-31 00:00:00+00'
                    ) AS effective_to,
                    coalesce(warehouse.safe_bool(r->>'is_current'), true) AS is_current,
                    warehouse.safe_timestamp(coalesce(r->>'source_updated_at', r->>'effective_from', r->>'updated_at')) AS source_updated_at,
                    coalesce(nullif(r->>'scd_hash', ''), nullif(r->>'record_hash', '')) AS scd_hash,
                    jsonb_build_array(
                        coalesce(nullif(r->>'campaign_id', ''), 'UNKNOWN'),
                        coalesce(warehouse.safe_timestamp(coalesce(
                            nullif(r->>'source_updated_at', ''), nullif(r->>'effective_from', ''),
                            nullif(r->>'valid_from', '')
                        )), TIMESTAMPTZ '1900-01-01 00:00:00+00')
                    ) AS dimension_identity,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            )
            SELECT * FROM mapped;

            DO $guard$
            BEGIN
                IF EXISTS (
                    SELECT campaign_sk FROM dimension_input
                    GROUP BY campaign_sk
                    HAVING COUNT(DISTINCT dimension_identity) > 1
                ) THEN
                    RAISE EXCEPTION 'Surrogate-key collision in dim_campaign: different version identities share a key';
                END IF;
            END $guard$;

            INSERT INTO warehouse.dim_campaign (
                campaign_sk, campaign_id, campaign_name, channel,
                traffic_source, target_segment, campaign_status, campaign_type, ab_test_group, campaign_start_date, campaign_end_date,
                budget, effective_from, effective_to, is_current, source_updated_at, scd_hash
            )
            SELECT DISTINCT ON (campaign_sk)
                campaign_sk, campaign_id, campaign_name, channel,
                traffic_source, target_segment, campaign_status, campaign_type, ab_test_group, campaign_start_date, campaign_end_date,
                budget,
                effective_from,
                CASE
                    WHEN effective_to <= effective_from THEN
                        CASE
                            WHEN is_current THEN TIMESTAMPTZ '9999-12-31 00:00:00+00'
                            ELSE effective_from + INTERVAL '1 microsecond'
                        END
                    ELSE effective_to
                END AS effective_to,
                is_current, source_updated_at, scd_hash
            FROM dimension_input
            WHERE campaign_sk IS NOT NULL
            ORDER BY campaign_sk, source_updated_at DESC NULLS LAST, row_fingerprint
            ON CONFLICT (campaign_sk) DO UPDATE SET
                campaign_id = EXCLUDED.campaign_id,
                campaign_name = EXCLUDED.campaign_name,
                channel = EXCLUDED.channel,
                traffic_source = EXCLUDED.traffic_source,
                target_segment = EXCLUDED.target_segment,
                campaign_status = EXCLUDED.campaign_status,
                campaign_type = EXCLUDED.campaign_type,
                ab_test_group = EXCLUDED.ab_test_group,
                campaign_start_date = EXCLUDED.campaign_start_date,
                campaign_end_date = EXCLUDED.campaign_end_date,
                budget = EXCLUDED.budget,
                effective_from = EXCLUDED.effective_from,
                effective_to = EXCLUDED.effective_to,
                is_current = EXCLUDED.is_current,
                source_updated_at = EXCLUDED.source_updated_at,
                scd_hash = EXCLUDED.scd_hash,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
            DROP TABLE dimension_input;
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
                    -- The same business-date derivation as the facts, so every
                    -- fact date key has its dim_date row.
                    SELECT coalesce(
                        warehouse.business_date(warehouse.safe_timestamp(r->>'order_timestamp')),
                        warehouse.business_date(warehouse.safe_timestamp(r->>'event_timestamp')),
                        warehouse.business_date(warehouse.safe_timestamp(r->>'event_time')),
                        warehouse.business_date(warehouse.safe_timestamp(r->>'order_date')),
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
        RAISE EXCEPTION 'Missing staging table: no orders fact staging table found; load Gold into staging first.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(nullif(r->>'order_id', ''), nullif(r->>'transaction_id', ''), 'order|' || md5(r::TEXT)) AS order_id,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(
                        warehouse.safe_timestamp(r->>'order_timestamp'),
                        warehouse.safe_timestamp(r->>'event_timestamp'),
                        warehouse.safe_timestamp(r->>'order_date')
                    ) AS order_ts,
                    coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    lower(nullif(r->>'order_status', '')) AS order_status,
                    warehouse.safe_timestamp(r->>'status_timestamp') AS status_timestamp,
                    warehouse.safe_numeric(r->>'refund_amount') AS refund_amount,
                    warehouse.safe_numeric(r->>'tax_amount') AS tax_amount,
                    warehouse.safe_numeric(r->>'shipping_amount') AS shipping_amount,
                    warehouse.safe_numeric(r->>'total_amount') AS total_amount,
                    nullif(r->>'payment_method', '') AS payment_method,
                    coalesce(warehouse.safe_int(r->>'item_count'), warehouse.safe_int(r->>'quantity'), 0) AS item_count,
                    -- gross_amount and discount_amount are set from the order's
                    -- items after the item load (below); these are placeholders.
                    0::NUMERIC AS gross_amount,
                    0::NUMERIC AS discount_amount,
                    -- The order's net value is the source's cart_value (the sum
                    -- of its lines). No other column stands in for it.
                    warehouse.safe_numeric(r->>'cart_value') AS net_revenue,
                    coalesce(nullif(r->>'record_hash', ''), nullif(r->>'source_record_hash', ''), md5(r::TEXT)) AS source_record_hash,
                    (warehouse.safe_date(r->>'ingestion_date')::TIMESTAMP AT TIME ZONE 'UTC') AS source_updated_at,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            )
            INSERT INTO warehouse.fact_orders (
                order_sk, order_id, customer_sk, campaign_sk, order_date_sk, region_sk, channel_sk,
                order_timestamp, order_status, status_timestamp, refund_amount, payment_method, item_count,
                gross_amount, discount_amount, net_revenue, tax_amount, shipping_amount, total_amount,
                source_record_hash, customer_id, campaign_id, source_updated_at
            )
            SELECT DISTINCT ON (b.order_id)
                warehouse.deterministic_fact_sk('order', b.order_id) AS order_sk,
                b.order_id,
                warehouse.member_sk(dc.customer_sk, b.customer_id IN (SELECT known.customer_id FROM warehouse.dim_customer known)) AS customer_sk,
                warehouse.member_sk(dcamp.campaign_sk, b.campaign_id IN (SELECT known.campaign_id FROM warehouse.dim_campaign known)) AS campaign_sk,
                warehouse.date_sk(warehouse.business_date(b.order_ts)) AS order_date_sk,
                coalesce(dr.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, 0) AS channel_sk,
                b.order_ts,
                b.order_status,
                b.status_timestamp,
                b.refund_amount,
                b.payment_method,
                b.item_count,
                b.gross_amount,
                b.discount_amount,
                b.net_revenue,
                b.tax_amount,
                b.shipping_amount,
                b.total_amount,
                b.source_record_hash,
                b.customer_id,
                b.campaign_id,
                b.source_updated_at
            FROM base b
            LEFT JOIN LATERAL (
                SELECT customer_sk
                FROM warehouse.dim_customer dc
                WHERE dc.customer_id = b.customer_id
                  AND b.order_ts >= dc.effective_from
                  AND b.order_ts < dc.effective_to
                ORDER BY dc.effective_from DESC, dc.customer_sk
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN LATERAL (
                SELECT campaign_sk, channel
                FROM warehouse.dim_campaign dcamp
                WHERE dcamp.campaign_id = b.campaign_id
                  AND b.order_ts >= dcamp.effective_from
                  AND b.order_ts < dcamp.effective_to
                ORDER BY dcamp.effective_from DESC, dcamp.campaign_sk
                LIMIT 1
            ) dcamp ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            -- An order carries no channel of its own: it takes the channel of
            -- its point-in-time campaign version. An organic order (unknown
            -- campaign, channel 'Unknown') is on the unknown channel (0).
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(dcamp.channel)
            ORDER BY b.order_id, b.order_ts DESC NULLS LAST, b.source_record_hash, b.row_fingerprint
            ON CONFLICT (order_id) DO UPDATE SET
                customer_sk = EXCLUDED.customer_sk,
                campaign_sk = EXCLUDED.campaign_sk,
                order_date_sk = EXCLUDED.order_date_sk,
                region_sk = EXCLUDED.region_sk,
                channel_sk = EXCLUDED.channel_sk,
                order_timestamp = EXCLUDED.order_timestamp,
                order_status = EXCLUDED.order_status,
                status_timestamp = EXCLUDED.status_timestamp,
                refund_amount = EXCLUDED.refund_amount,
                tax_amount = EXCLUDED.tax_amount,
                shipping_amount = EXCLUDED.shipping_amount,
                total_amount = EXCLUDED.total_amount,
                payment_method = EXCLUDED.payment_method,
                item_count = EXCLUDED.item_count,
                gross_amount = EXCLUDED.gross_amount,
                discount_amount = EXCLUDED.discount_amount,
                net_revenue = EXCLUDED.net_revenue,
                source_record_hash = EXCLUDED.source_record_hash,
                customer_id = EXCLUDED.customer_id,
                campaign_id = EXCLUDED.campaign_id,
                source_updated_at = EXCLUDED.source_updated_at,
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
        RAISE EXCEPTION 'Missing staging table: no order-items fact staging table found; load Gold into staging first.';
    ELSE
        EXECUTE format($sql$
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(nullif(r->>'order_item_id', ''), nullif(r->>'line_item_id', ''), 'item|' || md5(r::TEXT)) AS order_item_nk,
                    coalesce(nullif(r->>'order_id', ''), nullif(r->>'transaction_id', '')) AS order_id,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif(r->>'product_id', ''), 'UNKNOWN') AS product_id,
                    coalesce(
                        warehouse.safe_timestamp(r->>'order_timestamp'),
                        warehouse.safe_timestamp(r->>'event_timestamp'),
                        warehouse.safe_timestamp(r->>'order_date')
                    ) AS order_ts,
                    coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), 'Unknown') AS channel_value,
                    coalesce(warehouse.safe_int(r->>'quantity'), 1) AS quantity,
                    coalesce(
                        warehouse.safe_numeric(r->>'unit_price'),
                        warehouse.safe_numeric(r->>'discounted_price'),
                        warehouse.safe_numeric(r->>'price'),
                        0
                    ) AS unit_price,
                    coalesce(warehouse.safe_numeric(r->>'discount_percent'), 0) AS discount_percent,
                    warehouse.safe_numeric(r->>'original_price')
                        * coalesce(warehouse.safe_int(r->>'quantity'), 1) AS gross_amount,
                    (warehouse.safe_numeric(r->>'original_price') - warehouse.safe_numeric(r->>'discounted_price'))
                        * coalesce(warehouse.safe_int(r->>'quantity'), 1) AS discount_amount,
                    coalesce(
                        warehouse.safe_numeric(r->>'line_amount'),
                        warehouse.safe_numeric(r->>'line_revenue'),
                        warehouse.safe_numeric(r->>'line_total'),
                        0
                    ) AS source_line_revenue,
                    warehouse.safe_numeric(r->>'refund_amount') AS refund_amount,
                    (warehouse.safe_date(r->>'ingestion_date')::TIMESTAMP AT TIME ZONE 'UTC') AS source_updated_at,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            )
            INSERT INTO warehouse.fact_order_items (
                order_item_sk, order_item_nk, order_sk, order_id, customer_sk, product_sk,
                order_date_sk, region_sk, channel_sk, order_timestamp,
                quantity, unit_price, discount_percent, gross_amount, discount_amount, line_revenue, refund_amount,
                customer_id, product_id, source_updated_at
            )
            SELECT DISTINCT ON (b.order_item_nk)
                warehouse.deterministic_fact_sk('order_item', b.order_item_nk) AS order_item_sk,
                b.order_item_nk,
                fo.order_sk,
                b.order_id,
                coalesce(nullif(fo.customer_sk, 0), dc.customer_sk, 0) AS customer_sk,
                warehouse.member_sk(dp.product_sk, b.product_id IN (SELECT known.product_id FROM warehouse.dim_product known)) AS product_sk,
                -- An item is dated by its order, like its order_timestamp below.
                coalesce(nullif(fo.order_date_sk, 0), warehouse.date_sk(warehouse.business_date(b.order_ts)), 0) AS order_date_sk,
                -- An item without its own geography or channel resolves to the
                -- unknown member (0), so take the order's instead.
                coalesce(nullif(dr.region_sk, 0), fo.region_sk, 0) AS region_sk,
                coalesce(nullif(dch.channel_sk, 0), fo.channel_sk, 0) AS channel_sk,
                coalesce(fo.order_timestamp, b.order_ts) AS order_timestamp,
                b.quantity,
                b.unit_price,
                b.discount_percent,
                b.gross_amount,
                b.discount_amount,
                CASE
                    WHEN b.source_line_revenue > 0 THEN b.source_line_revenue
                    ELSE b.quantity * b.unit_price
                END AS line_revenue,
                b.refund_amount,
                -- An item has no customer of its own: its order's customer.
                coalesce(nullif(b.customer_id, 'UNKNOWN'), fo.customer_id, 'UNKNOWN') AS customer_id,
                b.product_id,
                b.source_updated_at
            FROM base b
            LEFT JOIN warehouse.fact_orders fo
                ON fo.order_id = b.order_id
            LEFT JOIN LATERAL (
                SELECT customer_sk
                FROM warehouse.dim_customer dc
                WHERE dc.customer_id = b.customer_id
                  AND coalesce(fo.order_timestamp, b.order_ts) >= dc.effective_from
                  AND coalesce(fo.order_timestamp, b.order_ts) < dc.effective_to
                ORDER BY dc.effective_from DESC, dc.customer_sk
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN LATERAL (
                SELECT product_sk
                FROM warehouse.dim_product dp
                WHERE dp.product_id = b.product_id
                  AND coalesce(fo.order_timestamp, b.order_ts) >= dp.effective_from
                  AND coalesce(fo.order_timestamp, b.order_ts) < dp.effective_to
                ORDER BY dp.effective_from DESC, dp.product_sk
                LIMIT 1
            ) dp ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value)
            ORDER BY b.order_item_nk, b.order_ts DESC NULLS LAST, b.row_fingerprint
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
                gross_amount = EXCLUDED.gross_amount,
                discount_amount = EXCLUDED.discount_amount,
                line_revenue = EXCLUDED.line_revenue,
                refund_amount = EXCLUDED.refund_amount,
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                source_updated_at = EXCLUDED.source_updated_at,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;


-- ---------------------------------------------------------------------------
-- Every calendar day from the first fact date to the last one, including the
-- days on which nothing happened and the day of the latest cancellation or
-- return: refunds are recognised on their own date, and the daily snapshot
-- marts have one row per entity for every day.
-- ---------------------------------------------------------------------------

INSERT INTO warehouse.dim_date (
    date_sk, full_date, day_of_month, day_name, day_of_week,
    week_of_year, month_number, month_name, quarter_number, year_number,
    is_weekend, month_start_date, quarter_start_date, year_start_date
)
SELECT
    warehouse.date_sk(d), d, extract(day from d)::INTEGER, trim(to_char(d, 'Day')), extract(isodow from d)::INTEGER,
    extract(week from d)::INTEGER, extract(month from d)::INTEGER, trim(to_char(d, 'Month')),
    extract(quarter from d)::INTEGER, extract(year from d)::INTEGER,
    extract(isodow from d)::INTEGER IN (6, 7), date_trunc('month', d)::DATE, date_trunc('quarter', d)::DATE,
    date_trunc('year', d)::DATE
FROM (
    SELECT generate_series(span.first_day, span.last_day, INTERVAL '1 day')::DATE AS d
    FROM (
        SELECT min(full_date) AS first_day,
               greatest(max(full_date),
                        (SELECT max(warehouse.business_date(status_timestamp)) FROM warehouse.fact_orders)) AS last_day
        FROM warehouse.dim_date
        WHERE date_sk <> 0
    ) span
    WHERE span.first_day IS NOT NULL
) days
ON CONFLICT (date_sk) DO NOTHING;

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

-- gross_amount and discount_amount of every order are the sums over its loaded
-- items (original_price x quantity, and (original_price - discounted_price) x
-- quantity); an order with no loaded item has 0. net_revenue (the discounted
-- order total from Gold) is unchanged, so gross_amount - discount_amount =
-- net_revenue for every complete order. An order that lost items to quarantine
-- or rejection keeps its full net_revenue but only its loaded items' gross and
-- discount (audit: reconciliation_report.sql).

UPDATE warehouse.fact_orders fo
SET
    gross_amount = order_amounts.gross_amount,
    discount_amount = order_amounts.discount_amount,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        o.order_sk,
        COALESCE(SUM(i.gross_amount), 0) AS gross_amount,
        COALESCE(SUM(i.discount_amount), 0) AS discount_amount
    FROM warehouse.fact_orders o
    LEFT JOIN warehouse.fact_order_items i
        ON i.order_sk = o.order_sk
    GROUP BY o.order_sk
) order_amounts
WHERE fo.order_sk = order_amounts.order_sk;

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
        RAISE EXCEPTION 'Missing staging table: no campaign-spend/ad-spend fact staging table found; load Gold into staging first.';
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
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(warehouse.safe_date(r->>'spend_date'), warehouse.safe_date(r->>'date')) AS spend_date,
                    nullif(r->>'state', '') AS state,
                    nullif(r->>'country', '') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), 'Unknown') AS channel_value,
                    coalesce(warehouse.safe_numeric(r->>'spend_amount'), warehouse.safe_numeric(r->>'ad_spend'), warehouse.safe_numeric(r->>'cost'), 0) AS spend_amount,
                    coalesce(warehouse.safe_bigint(r->>'impressions'), 0) AS impressions,
                    coalesce(warehouse.safe_bigint(r->>'clicks'), 0) AS clicks,
                    warehouse.safe_bigint(r->>'conversions') AS conversions,
                    warehouse.safe_numeric(r->>'attributed_revenue') AS attributed_revenue,
                    (warehouse.safe_date(r->>'ingestion_date')::TIMESTAMP AT TIME ZONE 'UTC') AS source_updated_at,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            )
            INSERT INTO warehouse.fact_campaign_spend (
                campaign_spend_sk, campaign_spend_nk, campaign_sk, spend_date_sk, region_sk, channel_sk,
                spend_date, spend_amount, impressions, clicks, conversions, attributed_revenue,
                campaign_id, source_updated_at
            )
            SELECT DISTINCT ON (b.campaign_spend_nk)
                warehouse.deterministic_fact_sk('campaign_spend', b.campaign_spend_nk) AS campaign_spend_sk,
                b.campaign_spend_nk,
                warehouse.member_sk(dc.campaign_sk, b.campaign_id IN (SELECT known.campaign_id FROM warehouse.dim_campaign known)) AS campaign_sk,
                warehouse.date_sk(b.spend_date) AS spend_date_sk,
                coalesce(dr.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, 0) AS channel_sk,
                b.spend_date,
                b.spend_amount,
                b.impressions,
                b.clicks,
                b.conversions,
                b.attributed_revenue,
                b.campaign_id,
                b.source_updated_at
            FROM base b
            LEFT JOIN LATERAL (
                SELECT campaign_sk
                FROM warehouse.dim_campaign dc
                WHERE dc.campaign_id = b.campaign_id
                  AND (b.spend_date::TIMESTAMP AT TIME ZONE 'UTC') >= dc.effective_from
                  AND (b.spend_date::TIMESTAMP AT TIME ZONE 'UTC') < dc.effective_to
                ORDER BY dc.effective_from DESC, dc.campaign_sk
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(NULL, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value)
            ORDER BY b.campaign_spend_nk, b.spend_date DESC NULLS LAST, b.row_fingerprint
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
                campaign_id = EXCLUDED.campaign_id,
                source_updated_at = EXCLUDED.source_updated_at,
                warehouse_loaded_at = CURRENT_TIMESTAMP;
        $sql$, source_table);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Web events
-- ---------------------------------------------------------------------------
-- One rule for every fact key, as for orders, items and spend
-- (spark-batch/point_in_time_fact_joins.py):
--   - a NULL customer, product or campaign key resolves to the unknown member (0);
--   - a key that is present but has no valid dimension row (for example one
--     quarantined in Silver) resolves to the invalid-reference member (-1).
--     The event is kept, with its natural ID, and resolves to the real member
--     once the dimension row is valid (warehouse.member_sk).
-- No event is dropped for a bad reference, and no member is invented per key.

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
        RAISE EXCEPTION 'Missing staging table: no web-events fact staging table found; load Gold into staging first.';
    ELSE
        DROP TABLE IF EXISTS pg_temp.web_events_resolved;

        EXECUTE format($sql$
            CREATE TEMP TABLE web_events_resolved ON COMMIT DROP AS
            WITH src AS (
                SELECT to_jsonb(s) AS r FROM %s s
            ),
            base AS (
                SELECT
                    coalesce(nullif(r->>'event_id', ''), 'event|' || md5(r::TEXT)) AS event_id,
                    nullif(r->>'session_id', '') AS session_id,
                    coalesce(nullif(r->>'customer_id', ''), nullif(r->>'user_id', ''), 'UNKNOWN') AS customer_id,
                    coalesce(nullif(r->>'product_id', ''), 'UNKNOWN') AS product_id,
                    coalesce(nullif((warehouse.safe_numeric(r->>'campaign_id')::BIGINT)::TEXT, ''), nullif(r->>'campaign_id', ''), 'UNKNOWN') AS campaign_id,
                    coalesce(warehouse.safe_timestamp(r->>'event_timestamp'), warehouse.safe_timestamp(r->>'event_time')) AS event_ts,
                    coalesce(nullif(r->>'city', ''), nullif(r->>'home_city', ''), 'Unknown') AS city,
                    coalesce(nullif(r->>'state', ''), nullif(r->>'home_state', ''), 'Unknown') AS state,
                    coalesce(nullif(r->>'country', ''), 'Unknown') AS country,
                    coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), 'Unknown') AS channel_value,
                    coalesce(nullif(r->>'event_type', ''), 'Unknown') AS event_type,
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
                    coalesce(nullif(r->>'source', ''), nullif(r->>'source_system', '')) AS source_system,
                    (warehouse.safe_date(r->>'ingestion_date')::TIMESTAMP AT TIME ZONE 'UTC') AS source_updated_at,
                    md5(r::TEXT) AS row_fingerprint
                FROM src
            ),
            deduplicated AS (
                SELECT DISTINCT ON (event_id) *
                FROM base
                ORDER BY event_id, event_ts DESC NULLS LAST, row_fingerprint
            )
            SELECT
                b.*,
                warehouse.member_sk(dc.customer_sk, b.customer_id IN (SELECT known.customer_id FROM warehouse.dim_customer known)) AS customer_sk,
                warehouse.member_sk(dp.product_sk, b.product_id IN (SELECT known.product_id FROM warehouse.dim_product known)) AS product_sk,
                warehouse.member_sk(dcamp.campaign_sk, b.campaign_id IN (SELECT known.campaign_id FROM warehouse.dim_campaign known)) AS campaign_sk,
                coalesce(dr.region_sk, 0) AS region_sk,
                coalesce(dch.channel_sk, 0) AS channel_sk
            FROM deduplicated b
            -- A NULL key is 'UNKNOWN' here, which the unknown member (0, open
            -- from 1900 to 9999) matches; any other key must match a real version.
            LEFT JOIN LATERAL (
                SELECT customer_sk
                FROM warehouse.dim_customer dc
                WHERE dc.customer_id = b.customer_id
                  AND b.event_ts >= dc.effective_from
                  AND b.event_ts < dc.effective_to
                ORDER BY dc.effective_from DESC, dc.customer_sk
                LIMIT 1
            ) dc ON TRUE
            LEFT JOIN LATERAL (
                SELECT product_sk
                FROM warehouse.dim_product dp
                WHERE dp.product_id = b.product_id
                  AND b.event_ts >= dp.effective_from
                  AND b.event_ts < dp.effective_to
                ORDER BY dp.effective_from DESC, dp.product_sk
                LIMIT 1
            ) dp ON TRUE
            LEFT JOIN LATERAL (
                SELECT campaign_sk
                FROM warehouse.dim_campaign dcamp
                WHERE dcamp.campaign_id = b.campaign_id
                  AND b.event_ts >= dcamp.effective_from
                  AND b.event_ts < dcamp.effective_to
                ORDER BY dcamp.effective_from DESC, dcamp.campaign_sk
                LIMIT 1
            ) dcamp ON TRUE
            LEFT JOIN warehouse.dim_region dr
                ON dr.region_nk = warehouse.region_nk(b.city, b.state, b.country)
            LEFT JOIN warehouse.dim_channel dch
                ON dch.channel_nk = warehouse.channel_nk(b.channel_value);
        $sql$, source_table);

        INSERT INTO warehouse.fact_web_events (
            event_sk, event_id, session_id, customer_sk, product_sk, campaign_sk,
            event_date_sk, region_sk, channel_sk, event_timestamp, event_type,
            user_journey_stage, device_type, operating_system, browser, network_type,
            traffic_source, search_query, time_on_page_sec, scroll_depth_percent,
            engagement_score, purchase_probability, cart_abandonment_probability,
            event_value, api_latency_ms, page_load_time_ms, fraud_score,
            schema_version, source_system,
            customer_id, product_id, campaign_id, source_updated_at
        )
        SELECT
            warehouse.deterministic_fact_sk('web_event', b.event_id) AS event_sk,
            b.event_id,
            b.session_id,
            b.customer_sk,
            b.product_sk,
            b.campaign_sk,
            warehouse.date_sk(warehouse.business_date(b.event_ts)) AS event_date_sk,
            b.region_sk,
            b.channel_sk,
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
            b.source_system,
            b.customer_id,
            b.product_id,
            b.campaign_id,
            b.source_updated_at
        FROM web_events_resolved b
        ORDER BY b.event_id
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
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            campaign_id = EXCLUDED.campaign_id,
            source_updated_at = EXCLUDED.source_updated_at,
            warehouse_loaded_at = CURRENT_TIMESTAMP;
    END IF;
END $$;

INSERT INTO warehouse.fact_conversions (
    conversion_sk, conversion_nk, event_sk, event_id, customer_sk, product_sk, campaign_sk,
    conversion_date_sk, region_sk, channel_sk, conversion_timestamp,
    conversion_type, conversion_value,
    customer_id, product_id, campaign_id, source_updated_at
)
SELECT
    warehouse.deterministic_fact_sk('conversion', 'conversion|' || event_id) AS conversion_sk,
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
    event_value AS conversion_value,
    customer_id,
    product_id,
    campaign_id,
    source_updated_at
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
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    campaign_id = EXCLUDED.campaign_id,
    source_updated_at = EXCLUDED.source_updated_at,
    warehouse_loaded_at = CURRENT_TIMESTAMP;


-- ---------------------------------------------------------------------------
-- record_hash: an md5 of each fact row's business columns (natural IDs, times
-- and measures; no surrogate key, no load time). Computed after every load and
-- backfill above, so it describes the row as consumers read it, and a rebuild
-- from the same data gives the same hash. NULL is hashed as the text <NULL>.
-- ---------------------------------------------------------------------------

UPDATE warehouse.fact_orders SET record_hash = md5(array_to_string(ARRAY[
    order_id, customer_id, campaign_id, (order_timestamp AT TIME ZONE 'UTC')::TEXT, order_status, (status_timestamp AT TIME ZONE 'UTC')::TEXT, refund_amount::TEXT,
    payment_method, item_count::TEXT,
    gross_amount::TEXT, discount_amount::TEXT, net_revenue::TEXT, tax_amount::TEXT, shipping_amount::TEXT, total_amount::TEXT
], '|', '<NULL>'));

UPDATE warehouse.fact_order_items SET record_hash = md5(array_to_string(ARRAY[
    order_item_nk, order_id, customer_id, product_id, (order_timestamp AT TIME ZONE 'UTC')::TEXT, quantity::TEXT, unit_price::TEXT,
    discount_percent::TEXT, gross_amount::TEXT, discount_amount::TEXT, line_revenue::TEXT, refund_amount::TEXT
], '|', '<NULL>'));

UPDATE warehouse.fact_campaign_spend SET record_hash = md5(array_to_string(ARRAY[
    campaign_spend_nk, campaign_id, spend_date::TEXT, spend_amount::TEXT, impressions::TEXT, clicks::TEXT,
    conversions::TEXT, attributed_revenue::TEXT
], '|', '<NULL>'));

UPDATE warehouse.fact_web_events SET record_hash = md5(array_to_string(ARRAY[
    event_id, session_id, customer_id, product_id, campaign_id, (event_timestamp AT TIME ZONE 'UTC')::TEXT, event_type,
    user_journey_stage, device_type, operating_system, browser, network_type, traffic_source, search_query,
    time_on_page_sec::TEXT, scroll_depth_percent::TEXT, engagement_score::TEXT, purchase_probability::TEXT,
    cart_abandonment_probability::TEXT, event_value::TEXT, api_latency_ms::TEXT, page_load_time_ms::TEXT,
    fraud_score::TEXT, schema_version, source_system
], '|', '<NULL>'));

UPDATE warehouse.fact_conversions SET record_hash = md5(array_to_string(ARRAY[
    conversion_nk, event_id, customer_id, product_id, campaign_id, (conversion_timestamp AT TIME ZONE 'UTC')::TEXT, conversion_type,
    conversion_value::TEXT
], '|', '<NULL>'));

ALTER TABLE warehouse.fact_orders ALTER COLUMN record_hash SET NOT NULL;
ALTER TABLE warehouse.fact_order_items ALTER COLUMN record_hash SET NOT NULL;
ALTER TABLE warehouse.fact_campaign_spend ALTER COLUMN record_hash SET NOT NULL;
ALTER TABLE warehouse.fact_web_events ALTER COLUMN record_hash SET NOT NULL;
ALTER TABLE warehouse.fact_conversions ALTER COLUMN record_hash SET NOT NULL;

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
