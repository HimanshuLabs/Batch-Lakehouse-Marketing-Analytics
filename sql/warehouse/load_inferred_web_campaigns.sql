/* =============================================================================
Load inferred campaign dimension rows from web-event-only natural keys.

Purpose:
- Some web events contain source campaign_id values that do not exist in the
  Gold SCD2 campaign dimension.
- This inserts inferred campaign dimension members so those source keys can
  resolve to real campaign_sk values.
============================================================================= */

BEGIN;

WITH web_campaign_keys AS (
    SELECT DISTINCT
        CASE
            WHEN campaign_id = FLOOR(campaign_id)
                THEN campaign_id::BIGINT::TEXT
            ELSE campaign_id::TEXT
        END AS campaign_id
    FROM staging.stg_gold_fact_web_events
    WHERE campaign_id IS NOT NULL
),
missing_campaigns AS (
    SELECT
        w.campaign_id,
        ROW_NUMBER() OVER (ORDER BY w.campaign_id) AS rn
    FROM web_campaign_keys w
    WHERE NOT EXISTS (
        SELECT 1
        FROM warehouse.dim_campaign d
        WHERE d.campaign_id = w.campaign_id
    )
),
campaign_template AS (
    SELECT to_jsonb(t) AS template_json
    FROM warehouse.dim_campaign t
    ORDER BY
        CASE WHEN t.campaign_id = 'UNKNOWN' OR t.campaign_sk = 0 THEN 0 ELSE 1 END,
        t.campaign_sk
    LIMIT 1
),
campaign_max AS (
    SELECT COALESCE(MAX(campaign_sk), 0) AS max_sk
    FROM warehouse.dim_campaign
)
INSERT INTO warehouse.dim_campaign
SELECT (
    jsonb_populate_record(
        NULL::warehouse.dim_campaign,
        jsonb_set(
            jsonb_set(
                jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                ct.template_json,
                                '{campaign_sk}'::TEXT[],
                                to_jsonb((cm.max_sk + mc.rn)::BIGINT),
                                true
                            ),
                            '{campaign_id}'::TEXT[],
                            to_jsonb(mc.campaign_id::TEXT),
                            true
                        ),
                        '{campaign_name}'::TEXT[],
                        to_jsonb(('Inferred Campaign ' || mc.campaign_id)::TEXT),
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
FROM missing_campaigns mc
CROSS JOIN campaign_template ct
CROSS JOIN campaign_max cm;

COMMIT;
