/*
Repair web-event surrogate keys after warehouse build.

This file:
1. Creates inferred dimension rows for source-provided web-event natural keys
   missing from Gold/SCD2 dimensions.
2. Directly updates warehouse.fact_web_events so source-provided customer,
   product, and campaign keys do not remain mapped to Unknown key 0.
*/

BEGIN;

CREATE OR REPLACE FUNCTION warehouse.normalized_natural_key(value TEXT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    WITH cleaned AS (
        SELECT lower(regexp_replace(btrim(value), '\.0$', '')) AS v
    ),
    normalized AS (
        SELECT CASE
            WHEN value IS NULL THEN NULL
            WHEN v IN (
                '',
                'unknown',
                'null',
                'none',
                'nan',
                'n/a',
                'na',
                'undefined',
                'not_available',
                'not available',
                '0',
                '-1'
            ) THEN NULL
            ELSE v
        END AS v
        FROM cleaned
    ),
    tokenized AS (
        SELECT CASE
            WHEN v IS NULL THEN NULL
            WHEN v ~ '^[a-z]+[_ -]*[0-9]+$'
                THEN COALESCE(NULLIF(ltrim(regexp_replace(v, '^.*?([0-9]+)$', '\1'), '0'), ''), '0')
            WHEN v ~ '^[0-9]+$'
                THEN COALESCE(NULLIF(ltrim(v, '0'), ''), '0')
            ELSE v
        END AS v
        FROM normalized
    )
    SELECT v FROM tokenized;
$$;

DROP TABLE IF EXISTS tmp_web_event_source_keys;

CREATE TEMP TABLE tmp_web_event_source_keys ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT
    NULLIF(to_jsonb(s)->>'event_id', '') AS event_id,
    warehouse.normalized_natural_key(
        COALESCE(to_jsonb(s)->>'customer_id', to_jsonb(s)->>'user_id')
    ) AS customer_id_norm,
    warehouse.normalized_natural_key(to_jsonb(s)->>'product_id') AS product_id_norm,
    warehouse.normalized_natural_key(to_jsonb(s)->>'campaign_id') AS campaign_id_norm
FROM staging.stg_gold_fact_web_events s
WHERE NULLIF(to_jsonb(s)->>'event_id', '') IS NOT NULL;

CREATE INDEX tmp_web_event_source_keys_event_id_idx
ON tmp_web_event_source_keys (event_id);

CREATE INDEX tmp_web_event_source_keys_customer_idx
ON tmp_web_event_source_keys (customer_id_norm);

CREATE INDEX tmp_web_event_source_keys_product_idx
ON tmp_web_event_source_keys (product_id_norm);

CREATE INDEX tmp_web_event_source_keys_campaign_idx
ON tmp_web_event_source_keys (campaign_id_norm);

-- ---------------------------------------------------------------------------
-- Create inferred customer members
-- ---------------------------------------------------------------------------

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
    scd_hash
)
SELECT
    warehouse.deterministic_sk('inferred_customer', customer_id_norm),
    customer_id_norm,
    'Inferred Customer ' || customer_id_norm,
    NULL,
    NULL,
    NULL,
    'Inferred',
    0,
    NULL,
    'Inferred',
    FALSE,
    'Unknown',
    'Unknown',
    'Unknown',
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '9999-12-31 00:00:00',
    TRUE,
    CURRENT_TIMESTAMP,
    'INFERRED_WEB_EVENT_CUSTOMER|' || customer_id_norm
FROM (
    SELECT DISTINCT customer_id_norm
    FROM tmp_web_event_source_keys src
    WHERE customer_id_norm IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM warehouse.dim_customer dc
          WHERE dc.customer_sk <> 0
            AND warehouse.normalized_natural_key(dc.customer_id) = src.customer_id_norm
      )
) missing
ON CONFLICT (customer_sk) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Create inferred product members
-- ---------------------------------------------------------------------------

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
    scd_hash
)
SELECT
    warehouse.deterministic_sk('inferred_product', product_id_norm),
    product_id_norm,
    'Inferred Product ' || product_id_norm,
    'Inferred',
    'Inferred',
    0,
    0,
    NULL,
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '9999-12-31 00:00:00',
    TRUE,
    CURRENT_TIMESTAMP,
    'INFERRED_WEB_EVENT_PRODUCT|' || product_id_norm
FROM (
    SELECT DISTINCT product_id_norm
    FROM tmp_web_event_source_keys src
    WHERE product_id_norm IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM warehouse.dim_product dp
          WHERE dp.product_sk <> 0
            AND warehouse.normalized_natural_key(dp.product_id) = src.product_id_norm
      )
) missing
ON CONFLICT (product_sk) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Create inferred campaign members
-- ---------------------------------------------------------------------------

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
    scd_hash
)
SELECT
    warehouse.deterministic_sk('inferred_campaign', campaign_id_norm),
    campaign_id_norm,
    'Inferred Campaign ' || campaign_id_norm,
    'Inferred',
    'Inferred',
    'Inferred',
    'Inferred',
    NULL,
    NULL,
    0,
    TIMESTAMP '1900-01-01 00:00:00',
    TIMESTAMP '9999-12-31 00:00:00',
    TRUE,
    CURRENT_TIMESTAMP,
    'INFERRED_WEB_EVENT_CAMPAIGN|' || campaign_id_norm
FROM (
    SELECT DISTINCT campaign_id_norm
    FROM tmp_web_event_source_keys src
    WHERE campaign_id_norm IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM warehouse.dim_campaign dc
          WHERE dc.campaign_sk <> 0
            AND warehouse.normalized_natural_key(dc.campaign_id) = src.campaign_id_norm
      )
) missing
ON CONFLICT (campaign_sk) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Directly repair fact_web_events customer_sk
-- ---------------------------------------------------------------------------

UPDATE warehouse.fact_web_events fwe
SET
    customer_sk = resolved.customer_sk,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        src.event_id,
        (
            SELECT dc.customer_sk
            FROM warehouse.dim_customer dc
            WHERE dc.customer_sk <> 0
              AND warehouse.normalized_natural_key(dc.customer_id) = src.customer_id_norm
            ORDER BY
                CASE WHEN dc.scd_hash LIKE 'INFERRED_WEB_EVENT_CUSTOMER|%' THEN 0 ELSE 1 END,
                dc.is_current DESC,
                dc.effective_from DESC,
                dc.customer_sk
            LIMIT 1
        ) AS customer_sk
    FROM tmp_web_event_source_keys src
    WHERE src.customer_id_norm IS NOT NULL
) resolved
WHERE fwe.event_id = resolved.event_id
  AND fwe.customer_sk = 0
  AND resolved.customer_sk IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Directly repair fact_web_events product_sk
-- ---------------------------------------------------------------------------

UPDATE warehouse.fact_web_events fwe
SET
    product_sk = resolved.product_sk,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        src.event_id,
        (
            SELECT dp.product_sk
            FROM warehouse.dim_product dp
            WHERE dp.product_sk <> 0
              AND warehouse.normalized_natural_key(dp.product_id) = src.product_id_norm
            ORDER BY
                CASE WHEN dp.scd_hash LIKE 'INFERRED_WEB_EVENT_PRODUCT|%' THEN 0 ELSE 1 END,
                dp.is_current DESC,
                dp.effective_from DESC,
                dp.product_sk
            LIMIT 1
        ) AS product_sk
    FROM tmp_web_event_source_keys src
    WHERE src.product_id_norm IS NOT NULL
) resolved
WHERE fwe.event_id = resolved.event_id
  AND fwe.product_sk = 0
  AND resolved.product_sk IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Directly repair fact_web_events campaign_sk
-- ---------------------------------------------------------------------------

UPDATE warehouse.fact_web_events fwe
SET
    campaign_sk = resolved.campaign_sk,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM (
    SELECT
        src.event_id,
        (
            SELECT dc.campaign_sk
            FROM warehouse.dim_campaign dc
            WHERE dc.campaign_sk <> 0
              AND warehouse.normalized_natural_key(dc.campaign_id) = src.campaign_id_norm
            ORDER BY
                CASE WHEN dc.scd_hash LIKE 'INFERRED_WEB_EVENT_CAMPAIGN|%' THEN 0 ELSE 1 END,
                dc.is_current DESC,
                dc.effective_from DESC,
                dc.campaign_sk
            LIMIT 1
        ) AS campaign_sk
    FROM tmp_web_event_source_keys src
    WHERE src.campaign_id_norm IS NOT NULL
) resolved
WHERE fwe.event_id = resolved.event_id
  AND fwe.campaign_sk = 0
  AND resolved.campaign_sk IS NOT NULL;

COMMIT;

SELECT
    COUNT(*) FILTER (
        WHERE src.customer_id_norm IS NOT NULL
          AND fwe.customer_sk = 0
    ) AS unresolved_customer_rows,
    COUNT(*) FILTER (
        WHERE src.product_id_norm IS NOT NULL
          AND fwe.product_sk = 0
    ) AS unresolved_product_rows,
    COUNT(*) FILTER (
        WHERE src.campaign_id_norm IS NOT NULL
          AND fwe.campaign_sk = 0
    ) AS unresolved_campaign_rows
FROM tmp_web_event_source_keys src
JOIN warehouse.fact_web_events fwe
    ON fwe.event_id = src.event_id;
