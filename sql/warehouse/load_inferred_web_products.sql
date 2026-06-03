/* =============================================================================
Load inferred product dimension rows from web-event-only natural keys.

Purpose:
- Some web events contain source product_id values that do not exist in the
  Gold SCD2 product dimension.
- This inserts inferred product dimension members so those source keys can
  resolve to real product_sk values.
============================================================================= */

BEGIN;

WITH web_product_keys AS (
    SELECT DISTINCT product_id::TEXT AS product_id
    FROM staging.stg_gold_fact_web_events
    WHERE product_id IS NOT NULL
),
missing_products AS (
    SELECT
        w.product_id,
        ROW_NUMBER() OVER (ORDER BY w.product_id) AS rn
    FROM web_product_keys w
    WHERE NOT EXISTS (
        SELECT 1
        FROM warehouse.dim_product d
        WHERE d.product_id = w.product_id
    )
),
product_template AS (
    SELECT to_jsonb(t) AS template_json
    FROM warehouse.dim_product t
    ORDER BY
        CASE WHEN t.product_id = 'UNKNOWN' OR t.product_sk = 0 THEN 0 ELSE 1 END,
        t.product_sk
    LIMIT 1
),
product_max AS (
    SELECT COALESCE(MAX(product_sk), 0) AS max_sk
    FROM warehouse.dim_product
)
INSERT INTO warehouse.dim_product
SELECT (
    jsonb_populate_record(
        NULL::warehouse.dim_product,
        jsonb_set(
            jsonb_set(
                jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                pt.template_json,
                                '{product_sk}'::TEXT[],
                                to_jsonb((pm.max_sk + mp.rn)::BIGINT),
                                true
                            ),
                            '{product_id}'::TEXT[],
                            to_jsonb(mp.product_id::TEXT),
                            true
                        ),
                        '{product_name}'::TEXT[],
                        to_jsonb(('Inferred Product ' || mp.product_id)::TEXT),
                        true
                    ),
                    '{is_current}'::TEXT[],
                    to_jsonb(TRUE),
                    true
                ),
                '{warehouse_loaded_at}'::TEXT[],
                to_jsonb(CURRENT_TIMESTAMP),
                true
            ),
            '{source_system}'::TEXT[],
            to_jsonb('web_events_inferred'::TEXT),
            true
        )
    )
).*
FROM missing_products mp
CROSS JOIN product_template pt
CROSS JOIN product_max pm;

COMMIT;
