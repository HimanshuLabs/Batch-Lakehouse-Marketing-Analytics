#!/usr/bin/env bash
set -euo pipefail
export P2_RUN_ID="${P2_RUN_ID:-warehouse-$(date +%s%N)-$$}"

# The warehouse is rebuilt as one transaction (sql/warehouse/begin_generation.sql):
# readers see the old warehouse until it commits, then the new one.
echo "==> Building the warehouse: facts and dimensions, SCD2 validation, marts, reconciliation"
env -u PGPORT bash scripts/run_warehouse_sql.sh \
  sql/warehouse/begin_generation.sql \
  sql/warehouse/create_warehouse_tables.sql \
  sql/warehouse/scd2_validation_queries.sql \
  sql/warehouse/create_marts.sql \
  sql/warehouse/reconciliation_report.sql

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
