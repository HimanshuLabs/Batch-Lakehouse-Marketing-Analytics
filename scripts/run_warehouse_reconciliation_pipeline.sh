#!/usr/bin/env bash
set -euo pipefail
export P2_RUN_ID="${P2_RUN_ID:-warehouse-$(date +%s%N)-$$}"

# The warehouse is rebuilt as one transaction (sql/warehouse/begin_generation.sql):
# readers see the old warehouse until it commits, then the new one.
# run_warehouse_sql.sh takes the pipeline run lock for the build, so this
# cannot overlap a pipeline run (exit code 75: another run owns the lock).
echo "Running warehouse reconciliation pipeline..."

echo
echo "==> Building the warehouse: facts and dimensions, SCD2 validation, marts, reconciliation"
env -u PGPORT bash scripts/run_warehouse_sql.sh \
  sql/warehouse/begin_generation.sql \
  sql/warehouse/create_warehouse_tables.sql \
  sql/warehouse/scd2_validation_queries.sql \
  sql/warehouse/create_marts.sql \
  sql/warehouse/reconciliation_report.sql

echo
echo "==> Assert all reconciliation checks pass"
docker exec project2_postgres psql -U project2 -d marketing_analytics -v ON_ERROR_STOP=1 -c "
DO \$\$
DECLARE
    fail_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO fail_count
    FROM audit.v_latest_reconciliation_report
    WHERE status <> 'PASS';

    IF fail_count > 0 THEN
        RAISE EXCEPTION 'Warehouse reconciliation failed: % failing checks', fail_count;
    END IF;
END \$\$;
"

echo
echo "Warehouse reconciliation pipeline completed successfully."
