/* =============================================================================
Repair fact_web_events surrogate keys after inferred dimensions are loaded.

Purpose:
- Re-resolves customer_sk, product_sk, and campaign_sk from the source natural
  keys in staging.stg_gold_fact_web_events, point-in-time, with the same rule as
  the fact_web_events load in create_warehouse_tables.sql: only the version
  whose effective range covers the event time; with no covering version the key
  is the unknown member (0), counted by audit.scd2_fact_key_without_covering_version.
- On a warehouse built by create_warehouse_tables.sql this changes nothing; it
  only fixes rows if dimension members were added after the facts were loaded.
- Idempotent: only rows whose keys differ are updated, and fact_conversions is
  kept in step with fact_web_events.
============================================================================= */

BEGIN;

WITH src AS (
    SELECT
        fwe.event_id,
        fwe.event_timestamp,
        fwe.customer_sk,
        fwe.product_sk,
        fwe.campaign_sk,
        s.customer_id::TEXT AS customer_id,
        s.product_id::TEXT AS product_id,
        CASE
            WHEN s.campaign_id IS NULL THEN NULL
            WHEN s.campaign_id = FLOOR(s.campaign_id)
                THEN s.campaign_id::BIGINT::TEXT
            ELSE s.campaign_id::TEXT
        END AS campaign_id
    FROM warehouse.fact_web_events fwe
    JOIN staging.stg_gold_fact_web_events s
        ON s.event_id::TEXT = fwe.event_id::TEXT
),
resolved AS (
    SELECT
        src.event_id,
        COALESCE(dc.customer_sk, 0) AS resolved_customer_sk,
        COALESCE(dp.product_sk, 0) AS resolved_product_sk,
        COALESCE(dcamp.campaign_sk, 0) AS resolved_campaign_sk
    FROM src
    LEFT JOIN LATERAL (
        SELECT d.customer_sk
        FROM warehouse.dim_customer d
        WHERE d.customer_id = src.customer_id
          AND src.event_timestamp >= d.effective_from
          AND src.event_timestamp < d.effective_to
        ORDER BY d.effective_from DESC, d.customer_sk
        LIMIT 1
    ) dc ON TRUE
    LEFT JOIN LATERAL (
        SELECT d.product_sk
        FROM warehouse.dim_product d
        WHERE d.product_id = src.product_id
          AND src.event_timestamp >= d.effective_from
          AND src.event_timestamp < d.effective_to
        ORDER BY d.effective_from DESC, d.product_sk
        LIMIT 1
    ) dp ON TRUE
    LEFT JOIN LATERAL (
        SELECT d.campaign_sk
        FROM warehouse.dim_campaign d
        WHERE d.campaign_id = src.campaign_id
          AND src.event_timestamp >= d.effective_from
          AND src.event_timestamp < d.effective_to
        ORDER BY d.effective_from DESC, d.campaign_sk
        LIMIT 1
    ) dcamp ON TRUE
)
UPDATE warehouse.fact_web_events fwe
SET
    customer_sk = resolved.resolved_customer_sk,
    product_sk = resolved.resolved_product_sk,
    campaign_sk = resolved.resolved_campaign_sk,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM resolved
WHERE fwe.event_id = resolved.event_id
  AND (
        fwe.customer_sk IS DISTINCT FROM resolved.resolved_customer_sk
     OR fwe.product_sk IS DISTINCT FROM resolved.resolved_product_sk
     OR fwe.campaign_sk IS DISTINCT FROM resolved.resolved_campaign_sk
  );

-- Keep conversions (copied from fact_web_events at build time) in step.
UPDATE warehouse.fact_conversions fc
SET
    customer_sk = fwe.customer_sk,
    product_sk = fwe.product_sk,
    campaign_sk = fwe.campaign_sk,
    warehouse_loaded_at = CURRENT_TIMESTAMP
FROM warehouse.fact_web_events fwe
WHERE fc.event_id = fwe.event_id
  AND (
        fc.customer_sk IS DISTINCT FROM fwe.customer_sk
     OR fc.product_sk IS DISTINCT FROM fwe.product_sk
     OR fc.campaign_sk IS DISTINCT FROM fwe.campaign_sk
  );

COMMIT;
