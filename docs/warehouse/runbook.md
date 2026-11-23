# Warehouse Runbook

## Purpose

This runbook validates the merged warehouse/reporting integration structure.

The warehouse layer begins after Project 2 Gold/SCD2 outputs are available.

## Connection

No script reads a `WAREHOUSE_DATABASE_URL`. The warehouse scripts connect to the Docker container `project2_postgres`, which publishes PostgreSQL on `127.0.0.1:5434`, as user `project2` to database `marketing_analytics` (`scripts/run_warehouse_sql.sh`). To rebuild the warehouse and require every reconciliation check to pass:

```bash
bash scripts/run_warehouse_reconciliation_pipeline.sh
```
