/* =============================================================================
Load inferred customer dimension rows from web-event-only natural keys.

Purpose:
- Some web events contain source customer_id values that do not exist in the
  Gold SCD2 customer dimension.
- This inserts inferred customer dimension members so those source keys can
  resolve to real customer_sk values.
- This file is intentionally separate from create_warehouse_tables.sql to avoid
  injecting SQL into the dynamic fact_web_events loader.
============================================================================= */

BEGIN;

WITH web_customer_keys AS (
    SELECT DISTINCT customer_id::TEXT AS customer_id
    FROM staging.stg_gold_fact_web_events
    WHERE customer_id IS NOT NULL
),
missing_customers AS (
    SELECT
        w.customer_id,
        ROW_NUMBER() OVER (ORDER BY w.customer_id) AS rn
    FROM web_customer_keys w
    WHERE NOT EXISTS (
        SELECT 1
        FROM warehouse.dim_customer d
        WHERE d.customer_id = w.customer_id
    )
),
customer_template AS (
    SELECT to_jsonb(t) AS template_json
    FROM warehouse.dim_customer t
    ORDER BY
        CASE WHEN t.customer_id = 'UNKNOWN' OR t.customer_sk = 0 THEN 0 ELSE 1 END,
        t.customer_sk
    LIMIT 1
),
customer_max AS (
    SELECT COALESCE(MAX(customer_sk), 0) AS max_sk
    FROM warehouse.dim_customer
)
INSERT INTO warehouse.dim_customer
SELECT (
    jsonb_populate_record(
        NULL::warehouse.dim_customer,
        jsonb_set(
            jsonb_set(
                jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                ct.template_json,
                                '{customer_sk}',
                                to_jsonb((cm.max_sk + mc.rn)::BIGINT),
                                true
                            ),
                            '{customer_id}',
                            to_jsonb(mc.customer_id),
                            true
                        ),
                        '{customer_name}',
                        to_jsonb('Inferred Customer ' || mc.customer_id),
                        true
                    ),
                    '{email}',
                    to_jsonb('inferred_customer_' || mc.customer_id || '@unknown.local'),
                    true
                ),
                '{is_current}',
                to_jsonb(TRUE),
                true
            ),
            '{warehouse_loaded_at}',
            to_jsonb(CURRENT_TIMESTAMP),
            true
        )
    )
).*
FROM missing_customers mc
CROSS JOIN customer_template ct
CROSS JOIN customer_max cm;

COMMIT;
