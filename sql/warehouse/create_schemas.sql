-- Warehouse reporting integration schemas.
-- Project 2 owns the lakehouse layers: Raw, Bronze, Silver, Gold, and Gold/SCD2.
-- This PostgreSQL warehouse layer starts only after trusted Gold/SCD2 outputs exist.

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA staging IS 'Relational landing area for trusted Project 2 Gold and SCD2 outputs.';
COMMENT ON SCHEMA warehouse IS 'Dimensional warehouse layer with SCD2 dimensions and fact tables.';
COMMENT ON SCHEMA marts IS 'BI-ready reporting marts for customer, revenue, campaign, product, and funnel analytics.';
COMMENT ON SCHEMA audit IS 'Reconciliation, SCD2 validation, freshness, and warehouse quality results.';

COMMIT;
