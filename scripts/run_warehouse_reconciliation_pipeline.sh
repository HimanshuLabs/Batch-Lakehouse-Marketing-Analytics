#!/usr/bin/env bash
set -euo pipefail

echo "Running warehouse reconciliation pipeline..."

echo
echo "1. Build warehouse tables"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_warehouse_tables.sql

echo
echo "2. Repair web-event surrogate keys"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/repair_web_event_surrogate_keys.sql

echo
echo "3. Build marts"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_marts.sql

echo
echo "4. Run reconciliation report"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/reconciliation_report.sql

echo
echo "5. Assert all reconciliation checks pass"
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
