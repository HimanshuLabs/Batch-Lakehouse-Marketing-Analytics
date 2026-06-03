/* =============================================================================
Repair fact_web_events surrogate keys after inferred dimensions are loaded.

Purpose:
- create_warehouse_tables.sql loads fact_web_events before the separate inferred
  customer/product/campaign dimension rows are inserted.
- This file re-resolves customer_sk, product_sk, and campaign_sk from the source
  natural keys in staging.stg_gold_fact_web_events.
- It fixes web-event fact rows that fell back to UNKNOWN surrogate keys even
  though source natural keys are available.
============================================================================= */

BEGIN;

WITH resolved AS (
    SELECT
        fwe.event_id,
        COALESCE(dc.customer_sk, fwe.customer_sk) AS resolved_customer_sk,
        COALESCE(dp.product_sk, fwe.product_sk) AS resolved_product_sk,
        COALESCE(dcamp.campaign_sk, fwe.campaign_sk) AS resolved_campaign_sk
    FROM warehouse.fact_web_events fwe
    JOIN staging.stg_gold_fact_web_events src
        ON src.event_id::TEXT = fwe.event_id::TEXT
    LEFT JOIN warehouse.dim_customer dc
        ON src.customer_id::TEXT = dc.customer_id
       AND dc.is_current = TRUE
    LEFT JOIN warehouse.dim_product dp
        ON src.product_id::TEXT = dp.product_id
       AND dp.is_current = TRUE
    LEFT JOIN warehouse.dim_campaign dcamp
        ON (
            CASE
                WHEN src.campaign_id IS NULL THEN NULL
                WHEN src.campaign_id = FLOOR(src.campaign_id)
                    THEN src.campaign_id::BIGINT::TEXT
                ELSE src.campaign_id::TEXT
            END
        ) = dcamp.campaign_id
       AND dcamp.is_current = TRUE
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

COMMIT;
