# Airflow Working State — Warehouse Orchestration

This document records the local Airflow/dbt setup that successfully ran the full lakehouse → warehouse → dbt → reporting orchestration.

This is not the full project README. It is a focused working-state note so the setup can be understood and reproduced later.

---

## Final working result

The Airflow DAG completed successfully after fixing three separate issues:

```text
1. Airflow metadata DB moved from SQLite to PostgreSQL
2. Airflow UI/API connection pool saturation fixed
3. dbt runtime fixed by rebuilding the dbt virtual environment
```

Working DAG:

```text
batch_lakehouse_marketing_analytics
```

The DAG was started using three Airflow processes:

```text
airflow dag-processor
airflow scheduler
airflow api-server
```

Then it was triggered from the Airflow UI and passed.

---

## Final architecture

```text
Lakehouse processing
        ↓
SCD2 dimensions and point-in-time fact joins
        ↓
PostgreSQL staging / warehouse / marts / audit
        ↓
SCD2 validation
        ↓
Warehouse reconciliation
        ↓
dbt run
        ↓
dbt test
        ↓
dbt docs
        ↓
Power BI export
```

---

## Important local paths

Project root:

```text
/home/manshu/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics
```

Airflow venv:

```text
venv-airflow/
```

dbt venv:

```text
venv-dbt/
```

Airflow local metadata folder:

```text
.airflow/
```

Local Airflow env file:

```text
scripts/airflow_local_env.sh
```

The local env file is intentionally not committed.

---

## Issue 1 — SQLite metadata DB failed under UI/API load

Original Airflow metadata DB:

```text
sqlite:////home/manshu/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics/.airflow/airflow.db
```

Observed error:

```text
sqlalchemy.exc.TimeoutError:
QueuePool limit of size 5 overflow 10 reached, connection timed out
```

This happened when the Airflow UI/API made multiple parallel metadata DB requests while the DAG was running.

### Fix

Airflow metadata DB was moved to PostgreSQL.

Final metadata DB shape:

```text
postgresql+psycopg2://airflow:<password>@127.0.0.1:5434/airflow_metadata
```

PostgreSQL container:

```text
project2_postgres
```

Host port:

```text
5434
```

---

## Issue 2 — Airflow 3 CLI differences

This environment uses Airflow 3-style commands:

```text
airflow dag-processor
airflow scheduler
airflow api-server
```

The old Airflow 2 command below is not available:

```text
airflow users create
```

For local development, the setup uses:

```bash
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
```

---

## Issue 3 — dbt runtime broke

The DAG reached `dbt_run`, then failed because the dbt executable was missing:

```text
dbt binary not found or not executable:
venv-dbt/bin/dbt
```

After rebuilding `venv-dbt` incorrectly with Python 3.14, dbt failed with:

```text
mashumaro.exceptions.UnserializableField:
Field "schema" of type Optional[str] in JSONObjectSchema is not serializable
```

The working correction was to rebuild `venv-dbt` separately and ensure the DAG runner can find:

```text
/home/manshu/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics/venv-dbt/bin/dbt
```

Final rule:

```text
venv-airflow = Airflow runtime
venv-dbt     = dbt runtime
```

---

## Local Airflow environment values

The local environment file used for the working run was:

```text
scripts/airflow_local_env.sh
```

Important values:

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:<password>@127.0.0.1:5434/airflow_metadata"

export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=30
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=60
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_TIMEOUT=120
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING=True
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE=300

export AIRFLOW__CORE__PARALLELISM=2
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
```

---

## Start Airflow

Use three terminals.

### Terminal 1 — DAG processor

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

source venv-airflow/bin/activate
source scripts/airflow_local_env.sh

airflow dag-processor
```

### Terminal 2 — Scheduler

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

source venv-airflow/bin/activate
source scripts/airflow_local_env.sh

airflow scheduler
```

### Terminal 3 — API/UI server

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

source venv-airflow/bin/activate
source scripts/airflow_local_env.sh

airflow api-server --host 0.0.0.0 --port 8080
```

Open:

```text
http://localhost:8080
```

---

## Verify Airflow setup

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

source venv-airflow/bin/activate
source scripts/airflow_local_env.sh

airflow db check
airflow config get-value database sql_alchemy_conn

airflow dags list-import-errors
airflow dags list | grep batch_lakehouse_marketing_analytics
airflow tasks list batch_lakehouse_marketing_analytics
```

Expected:

```text
Connection successful.
No data found
batch_lakehouse_marketing_analytics
```

---

## Working DAG task chain

```text
generate_data
bronze_ingestion
bronze_quality_checks
silver_transformations
silver_quality_checks
gold_transformations
gold_quality_checks
scd2_dimensions
point_in_time_fact_joins
scd2_gold_marts
scd2_duckdb_analytics
create_postgresql_schemas
load_gold_scd2_to_staging
build_warehouse_tables
run_scd2_validation
build_reporting_marts
run_warehouse_reconciliation
dbt_run
dbt_test
dbt_docs
power_bi_export
```

---

## Manual dbt verification

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics/dbt

rm -f profiles.yml
cp profiles.yml.example profiles.yml

../venv-dbt/bin/dbt parse --profiles-dir . --no-partial-parse
../venv-dbt/bin/dbt run --profiles-dir .
../venv-dbt/bin/dbt test --profiles-dir .
../venv-dbt/bin/dbt docs generate --profiles-dir .
```

---

## Airflow dbt task verification

```bash
cd ~/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics

source venv-airflow/bin/activate
source scripts/airflow_local_env.sh

airflow tasks test batch_lakehouse_marketing_analytics dbt_run 2026-06-04
airflow tasks test batch_lakehouse_marketing_analytics dbt_test 2026-06-04
airflow tasks test batch_lakehouse_marketing_analytics dbt_docs 2026-06-04
airflow tasks test batch_lakehouse_marketing_analytics power_bi_export 2026-06-04
```

---

## Warehouse reconciliation proof

After the DAG completes:

```bash
docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT status, COUNT(*) AS check_count
FROM audit.reconciliation_report
GROUP BY status
ORDER BY status;
"
```

Expected:

```text
PASS | 14
```

---

## Files not to commit

Do not commit:

```text
.airflow/
venv-airflow/
venv-dbt/
dbt/target/
dbt/profiles.yml
scripts/airflow_local_env.sh
```

Runtime reports under `logs/` can change when the DAG runs. Do not commit them unless intentionally updating expected proof artifacts.

---

## Final note

The project now has a working Airflow-controlled execution path from lakehouse processing into PostgreSQL warehouse tables, SCD2 validation, reporting marts, reconciliation, dbt run/test/docs, and reporting export.
