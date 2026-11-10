# Airflow Working State — Warehouse Orchestration

This document records the local Airflow setup that ran the whole DAG, `batch_lakehouse_marketing_analytics`, end to end. Every command here runs from the repository root.

Verified on 2026-11-10 with run `manual__rerun2_2026-06-18` (BATCH_DATE 2026-06-18):

- all 23 tasks succeeded on the first try, in about 4 min 48 s;
- `dbt run` passed all 21 models, and `dbt test` passed all 65 tests;
- warehouse reconciliation was 14 of 14 PASS;
- `power_bi_export` succeeded, with every `data_quality_summary.csv` row PASS.

---

## Versions and environments

| Environment | Python | Purpose |
|---|---|---|
| `venv/` | 3.14 | pipeline tasks (PySpark, DuckDB, pandas, psycopg) |
| `venv-dbt/` | 3.13 (uv-managed) | dbt-core 1.10.15 and dbt-postgres 1.10.0; dbt 1.10 does not run on 3.14 |
| `venv-airflow/` | 3.14 | Apache Airflow 3.2.0 only; it orchestrates and never imports pipeline code |

The DAG's `DEFAULT_ENV` points the tasks at `venv/bin/python` and `venv-dbt/bin/dbt`, so Airflow's own environment stays separate.

---

## 1. Install Airflow

```bash
python3.14 -m venv venv-airflow
venv-airflow/bin/python -m pip install --upgrade pip
venv-airflow/bin/python -m pip install "apache-airflow[postgres]==3.2.0" apache-airflow-providers-standard \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.14.txt
venv-airflow/bin/airflow version   # 3.2.0
```

The DAG uses only `BashOperator`, which comes from `apache-airflow-providers-standard`. The `postgres` extra supplies the metadata database driver.

---

## 2. Prerequisites before any DAG run

- `venv/` and `venv-dbt/` exist (see the README, Environments section).
- `project2_postgres` is running on port 5434 (see the README, PostgreSQL section).
- The git-ignored dbt profile exists:

  ```bash
  cp dbt/profiles.yml.example dbt/profiles.yml
  ```

  The dbt tasks run `dbt ... --profiles-dir .` inside `dbt/`. Without this file, `dbt_run` fails with `Could not find profile named 'marketing_lakehouse_warehouse'`.

---

## 3. Metadata database: a separate container

Airflow's metadata lives in its own container, `airflow_postgres`, and never in `project2_postgres`.

```bash
source scripts/airflow_local_env.sh
docker volume create airflow_pgdata
docker run -d --name airflow_postgres --restart unless-stopped \
  -p 127.0.0.1:5435:5432 -v airflow_pgdata:/var/lib/postgresql/data \
  -e POSTGRES_DB=airflow_metadata -e POSTGRES_USER=airflow \
  -e POSTGRES_PASSWORD="$AIRFLOW_PG_PASSWORD" postgres:16.14
docker exec airflow_postgres pg_isready -U airflow -d airflow_metadata -h 127.0.0.1
```

---

## 4. Local environment file

The file is `scripts/airflow_local_env.sh`. It is git-ignored because it holds local secrets.

Generate each `<SECRET>` with `openssl rand -hex 16` for the password and `openssl rand -hex 32` for the two keys:

```bash
# Local Airflow 3.2.0 dev environment for Project 2 (git-ignored; holds local secrets).
# Usage: source scripts/airflow_local_env.sh
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AIRFLOW_PG_PASSWORD='<SECRET>'
export AIRFLOW_HOME="${REPO_ROOT}/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="${REPO_ROOT}/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:${AIRFLOW_PG_PASSWORD}@127.0.0.1:5435/airflow_metadata"
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING=True
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/
export AIRFLOW__API__BASE_URL=http://127.0.0.1:8080
export AIRFLOW__API_AUTH__JWT_SECRET='<SECRET>'
export AIRFLOW__API__SECRET_KEY='<SECRET>'
export PATH="${REPO_ROOT}/venv-airflow/bin:${PATH}"
```

Why these settings:

- **LocalExecutor with parallelism 1.** At most one task runs at a time, so two Spark jobs never compete for memory. Airflow 3 has no SequentialExecutor.
- **One shared JWT secret.** Tasks call back to the api-server's Execution API with JWTs, so all three processes need the same `JWT_SECRET`.
- **Simple auth manager with all admins.** This suits local development only, and the api-server listens on 127.0.0.1 only.
- **Paused at creation.** Nothing runs until you unpause the DAG, so a run's `BATCH_DATE` can be checked first (step 8).

---

## 5. One-time initialisation and checks

```bash
source scripts/airflow_local_env.sh
airflow db migrate
airflow db check
airflow dags list-import-errors
airflow tasks list batch_lakehouse_marketing_analytics   # 23 tasks
```

---

## 6. Start the three processes

Airflow 3 runs as three processes: the dag-processor, the scheduler and the api-server. The api-server serves the UI, the REST API and the Execution API.

```bash
source scripts/airflow_local_env.sh
mkdir -p "$AIRFLOW_HOME/run"
nohup airflow dag-processor > "$AIRFLOW_HOME/run/dag-processor.log" 2>&1 < /dev/null &
nohup airflow scheduler     > "$AIRFLOW_HOME/run/scheduler.log"     2>&1 < /dev/null &
nohup airflow api-server --host 127.0.0.1 --port 8080 > "$AIRFLOW_HOME/run/api-server.log" 2>&1 < /dev/null &
until curl -sf http://127.0.0.1:8080/api/v2/monitor/health >/dev/null; do sleep 2; done
curl -s http://127.0.0.1:8080/api/v2/monitor/health
```

Record the real process IDs from `ps` after start-up, not from `$!`. Two reasons:

- the api-server renames its process title to `airflow api_server -- host:127.0.0.1 port:8080`;
- under `setsid`, `$!` is a wrapper's PID, not Airflow's.

```bash
ps -eo pid,args | awk '$2 ~ /venv-airflow\/bin\/python$/ && $3 ~ /venv-airflow\/bin\/airflow$/ && ($4=="scheduler" || $4=="dag-processor") {print $1 > ENVIRON["AIRFLOW_HOME"]"/run/"$4".pid"}'
ss -ltnp | grep ':8080 ' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2 > "$AIRFLOW_HOME/run/api-server.pid"
for n in dag-processor scheduler api-server; do echo "$n $(cat "$AIRFLOW_HOME/run/$n.pid")"; done
```

The UI is at `http://127.0.0.1:8080`.

---

## 7. Trigger a run for a chosen BATCH_DATE

The DAG sets `BATCH_DATE` from `{{ (dag_run.logical_date or dag_run.run_after) | ds }}`: the run's logical date when it has one, otherwise its `run_after` time. Both are in UTC, so `BATCH_DATE` is the UTC day.

**From the UI.** Trigger the DAG and set **Logical Date** to the day you want to process or rerun. Use 12:00 on that day: noon keeps the UTC day the same whichever timezone the UI shows. To process today, leave the field at its default, now. "Today" means the current UTC day, so between 00:00 and 05:30 IST it is still the previous day.

**From the CLI**, the same applies: `airflow dags trigger batch_lakehouse_marketing_analytics -l 2026-06-04T12:00:00+00:00` for a given day, or the same command without `-l` for today.

Each logical date can have only one run. To rerun a day that already has a run, either clear that run in the UI, or trigger again with another time on the same day, for example 12:01.

**Scripted (REST).** The REST API sets both dates explicitly, which suits scripts. Pair it with the render gate in step 8.

First run of a date, when no run holds that logical date yet:

```bash
TOKEN=$(curl -s http://127.0.0.1:8080/auth/token | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
curl -s -X POST http://127.0.0.1:8080/api/v2/dags/batch_lakehouse_marketing_analytics/dagRuns \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"dag_run_id":"manual__2026-06-18","logical_date":"2026-06-18T12:00:00Z","run_after":"2026-06-18T12:00:00Z","conf":{}}'
```

Rerun of a date that already has a run: send `"logical_date": null` with `run_after` set and a new run id. Without a logical date, `BATCH_DATE` comes from `run_after`, so it is the same day.

```bash
curl -s -X POST http://127.0.0.1:8080/api/v2/dags/batch_lakehouse_marketing_analytics/dagRuns \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"dag_run_id":"manual__rerun2_2026-06-18","logical_date":null,"run_after":"2026-06-18T12:00:00Z","conf":{}}'
```

---

## 8. Render gate, then unpause

With the DAG still paused, render `BATCH_DATE` from the stored run, however it was triggered. Both lines must print exactly `'BATCH_DATE': '2026-06-18'`:

```bash
RUN_ID=manual__rerun2_2026-06-18
for t in generate_data power_bi_export; do
  airflow tasks render batch_lakehouse_marketing_analytics $t "$RUN_ID" 2>/dev/null | grep -o "'BATCH_DATE': '[^']*'"
done
airflow dags unpause batch_lakehouse_marketing_analytics   # only if both matched
```

If either line prints anything else, keep the DAG paused and delete the run:

```bash
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/api/v2/dags/batch_lakehouse_marketing_analytics/dagRuns/$RUN_ID"
```

For a batch date whose raw files already exist, `generate_data` logs `Raw data for batch_date=... already complete. Skipping generation.`, and the run is an idempotent rerun.

---

## 9. Check the result

```bash
airflow tasks states-for-dag-run batch_lakehouse_marketing_analytics "$RUN_ID"

docker exec project2_postgres psql -U project2 -d marketing_analytics -c "
SELECT status, COUNT(*) FROM audit.v_latest_reconciliation_report GROUP BY status;"   # PASS | 14

awk -F, 'NR>1 && $4!="PASS"' data/bi_exports/data_quality_summary.csv              # prints nothing
```

---

## 10. Stop

```bash
airflow dags pause batch_lakehouse_marketing_analytics
for n in api-server scheduler dag-processor; do kill -TERM "$(cat "$AIRFLOW_HOME/run/$n.pid")"; done
sleep 10
ps -eo pid,args | grep -E 'airflow (api_server|scheduler|dag-processor|dag_processor)' | grep -v grep || echo "no airflow processes"
ss -ltn | grep ':8080 ' || echo "8080 free"
```

Both containers keep running and restart with Docker (`--restart unless-stopped`).

---

## DAG task chain

```text
generate_data
bronze_ingestion
bronze_quality_checks
silver_transformations
silver_quality_checks
gold_transformations
gold_quality_checks
duckdb_gold_analytics
publish_gold_to_postgres
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

## Problems met and how they were solved

| Symptom | Cause | Fix |
|---|---|---|
| `QueuePool limit of size 5 overflow 10 reached` (historical) | SQLite metadata database under UI/API load | PostgreSQL metadata database, now in its own `airflow_postgres` container |
| `mashumaro.exceptions.UnserializableField` from dbt (historical) | dbt 1.10 installed on Python 3.14 | `venv-dbt` on Python 3.13 via uv |
| `Could not find profile named 'marketing_lakehouse_warehouse'` | `dbt/profiles.yml` missing | `cp dbt/profiles.yml.example dbt/profiles.yml` |
| A run triggered for an earlier day rendered today's `BATCH_DATE` (historical) | `BATCH_DATE` came only from `run_after`, which the UI and CLI set to now | `BATCH_DATE` now uses the logical date when the run has one |
| A second trigger for the same logical date is rejected | one run per logical date | clear the existing run, use another time on the same day, or REST with `"logical_date": null` |
| `kill $!` leaves Airflow running | wrapper PID; api-server renames its title | record PIDs from `ps`/`ss`, then send SIGTERM |
| `airflow users create` not found | Airflow 3 removed it | simple auth manager with `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` |

Earlier setups kept the metadata database inside `project2_postgres` and started the api-server on `0.0.0.0`. Both are superseded by the setup above.

---

## Files not to commit

```text
.airflow/
venv/
venv-airflow/
venv-dbt/
dbt/target/
dbt/logs/
dbt/profiles.yml
scripts/airflow_local_env.sh
logs/ (everything except .gitkeep placeholders)
```

All of these are git-ignored.
