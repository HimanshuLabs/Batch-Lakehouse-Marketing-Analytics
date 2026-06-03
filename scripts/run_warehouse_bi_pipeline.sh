#!/usr/bin/env bash
set -euo pipefail

echo "==> Building warehouse facts and dimensions"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_warehouse_tables.sql

echo "==> Loading inferred web-event customer dimension rows"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/load_inferred_web_customers.sql

echo "==> Loading inferred web-event product dimension rows"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/load_inferred_web_products.sql

echo "==> Loading inferred web-event campaign dimension rows"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/load_inferred_web_campaigns.sql

echo "==> Repairing web-event surrogate keys after inferred dimensions"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/repair_web_event_surrogate_keys.sql

echo "==> Building BI-ready marts"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_marts.sql

echo "==> Running warehouse reconciliation"
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/reconciliation_report.sql

echo "==> Final reconciliation status"
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT
    status,
    COUNT(*) AS check_count
FROM audit.reconciliation_report
GROUP BY status
ORDER BY status;
"
