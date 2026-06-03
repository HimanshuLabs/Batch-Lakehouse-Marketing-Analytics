-- Airflow rerun safety:
-- dbt creates views/models that depend on staging.stg_gold_* tables.
-- The staging loader replaces those tables, so old dbt schemas must be removed first.
DROP SCHEMA IF EXISTS dbt_audit CASCADE;
DROP SCHEMA IF EXISTS dbt_marts CASCADE;
DROP SCHEMA IF EXISTS dbt_warehouse CASCADE;
DROP SCHEMA IF EXISTS dbt_staging CASCADE;

/*
PostgreSQL Warehouse Schema Layer
==================================

Boundary:
- Project 2 lakehouse owns Raw/Bronze/Silver/Gold.
- PostgreSQL starts from trusted Gold/SCD2 outputs.
- No duplicate lakehouse raw/bronze/silver logic belongs here.

Schema ownership:
- staging   : relational landing area for trusted Gold/SCD2 lakehouse outputs
- warehouse : dimensional warehouse model; SCD2 dimensions, facts, surrogate-key joins
- marts     : BI-ready reporting tables/views for dashboards and analytics
- audit     : reconciliation, validation, freshness checks, and publish health results
*/

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS
'Relational landing schema for trusted Project 2 Gold and SCD2 lakehouse outputs. Does not own raw, bronze, or silver processing.';

CREATE SCHEMA IF NOT EXISTS warehouse;
COMMENT ON SCHEMA warehouse IS
'Core dimensional warehouse schema containing SCD2 dimensions, fact tables, surrogate keys, and point-in-time reporting structures.';

CREATE SCHEMA IF NOT EXISTS marts;
COMMENT ON SCHEMA marts IS
'Business-facing BI/reporting schema containing curated marts for revenue, customer, campaign, product, and funnel analytics.';

CREATE SCHEMA IF NOT EXISTS audit;
COMMENT ON SCHEMA audit IS
'Trust and control schema for reconciliation checks, data quality results, freshness checks, and warehouse publish validation.';

COMMIT;

SELECT
    schema_name
FROM information_schema.schemata
WHERE schema_name IN ('staging', 'warehouse', 'marts', 'audit')
ORDER BY schema_name;
