-- Warehouse staging loader support objects
-- Actual Parquet loading is implemented in scripts/load_gold_to_postgres_staging.py

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.gold_to_staging_load_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    load_id UUID NOT NULL,
    source_name TEXT NOT NULL,
    source_path TEXT NOT NULL,
    target_schema TEXT NOT NULL DEFAULT 'staging',
    target_table TEXT NOT NULL,
    source_required BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL CHECK (status IN ('LOADED', 'MISSING', 'SKIPPED', 'FAILED', 'DRY_RUN')),
    row_count BIGINT,
    column_count INTEGER,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_gold_to_staging_load_audit_finished_at
ON audit.gold_to_staging_load_audit (finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_gold_to_staging_load_audit_target
ON audit.gold_to_staging_load_audit (target_schema, target_table, finished_at DESC);

CREATE OR REPLACE VIEW audit.v_gold_to_staging_latest_counts AS
WITH ranked_loads AS (
    SELECT
        load_id,
        source_name,
        source_path,
        target_schema,
        target_table,
        source_required,
        status,
        row_count,
        column_count,
        started_at,
        finished_at,
        error_message,
        ROW_NUMBER() OVER (
            PARTITION BY target_schema, target_table
            ORDER BY finished_at DESC, audit_id DESC
        ) AS row_rank
    FROM audit.gold_to_staging_load_audit
)
SELECT
    source_name,
    source_path,
    target_schema,
    target_table,
    source_required,
    status,
    row_count,
    column_count,
    started_at,
    finished_at,
    error_message
FROM ranked_loads
WHERE row_rank = 1
ORDER BY target_schema, target_table;
