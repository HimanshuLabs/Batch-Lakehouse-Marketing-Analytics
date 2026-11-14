-- This file changes nothing a reader can see: it only makes sure the schemas
-- and the read-only role exist. A rebuild replaces the warehouse as a whole new
-- generation of these schemas (sql/warehouse/begin_generation.sql), and the dbt
-- views are replaced one by one by dbt run, so nothing is dropped here.

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

-- Every new session reads and writes timestamps in UTC, whatever the client zone.
ALTER DATABASE marketing_analytics SET timezone TO 'UTC';

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

-- ---------------------------------------------------------------------------
-- p2_reader: the read-only role for consumers (BI tools, Project 4, Project 5).
-- It can SELECT from warehouse, marts and audit, and from the dbt schemas (dbt
-- grants those itself: dbt/dbt_project.yml). It cannot read staging and cannot
-- write anything. Every build creates new schemas and tables, so the grants
-- are default privileges of the owner, repeated for each new schema by
-- sql/warehouse/begin_generation.sql, not one-off grants.
-- The role can log in only when a password is given (P2_READER_PASSWORD in
-- .env, passed by scripts/run_warehouse_sql.sh); without one it is NOLOGIN.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'p2_reader') THEN
        CREATE ROLE p2_reader NOLOGIN;
    END IF;
END $$;

ALTER ROLE p2_reader SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE marketing_analytics TO p2_reader;
GRANT USAGE ON SCHEMA warehouse, marts, audit TO p2_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse, marts, audit TO p2_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse, marts, audit GRANT SELECT ON TABLES TO p2_reader;
REVOKE ALL ON SCHEMA staging FROM p2_reader;
REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM p2_reader;

COMMIT;

\if :{?p2_reader_password}
ALTER ROLE p2_reader LOGIN PASSWORD :'p2_reader_password';
\endif

SELECT
    schema_name
FROM information_schema.schemata
WHERE schema_name IN ('staging', 'warehouse', 'marts', 'audit')
ORDER BY schema_name;
