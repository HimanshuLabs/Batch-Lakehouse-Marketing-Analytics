#!/usr/bin/env bash
set -euo pipefail
export P2_RUN_ID="${P2_RUN_ID:-warehouse-$(date +%s%N)-$$}"

echo "==> Building warehouse facts and dimensions"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_warehouse_tables.sql

echo "==> Building SCD2 validation views and results"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/scd2_validation_queries.sql

echo "==> Building BI-ready marts"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_marts.sql

echo "==> Running warehouse reconciliation"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/reconciliation_report.sql

echo "==> Final mart validation status"
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT
    status,
    COUNT(*) AS check_count
FROM audit.v_latest_mart_validation_report
GROUP BY status
ORDER BY status;
"

echo "==> Final warehouse reconciliation status"
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT
    status,
    COUNT(*) AS check_count
FROM audit.reconciliation_report
GROUP BY status
ORDER BY status;
"
