#!/usr/bin/env bash
set -euo pipefail

TASK_NAME="${1:-}"

PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PROJECT_PYTHON="${PROJECT_PYTHON:-${PROJECT_ROOT}/venv/bin/python}"
DBT_BIN="${DBT_BIN:-${PROJECT_ROOT}/venv-dbt/bin/dbt}"

cd "${PROJECT_ROOT}"

# Every task runs under the pipeline run lock (scripts/pipeline_lock.py). The
# DAG and tests/integration/run_dag.py take it before they call this script
# (P2_LOCK_HELD is set). Run by hand, the task takes it here, as a one-task run
# that gives the lock back when the task ends; while another run owns the lock
# it exits with 75 and does nothing.
if [[ -z "${P2_LOCK_HELD:-}" ]]; then
  export PROJECT_ROOT P2_RUN_ID="${P2_RUN_ID:-manual-$(date +%s%N)-$$}"
  LOCK_PYTHON="${PROJECT_PYTHON}"
  [[ -x "${LOCK_PYTHON}" ]] || LOCK_PYTHON="python3"
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  exec "${LOCK_PYTHON}" "${SCRIPT_DIR}/pipeline_lock.py" --last "${TASK_NAME:-no_task}" -- \
    bash "${SCRIPT_DIR}/run_airflow_task.sh" "$@"
fi

# An empty first-ever batch: the batch is declared empty (a _EMPTY_BATCH file in
# its raw folder), the raw files of BATCH_DATE have no rows and the
# lake holds no Bronze data from any earlier batch. Bronze has ingested it (it
# writes nothing for an empty file) and the Bronze checks have confirmed that
# nothing is missing, so there is nothing to build: each later task succeeds
# without running and says so. No lake table and no warehouse is created; the
# first batch that has rows builds everything as usual. When an earlier batch
# exists, an empty batch is processed normally (Silver and Gold are rebuilt
# from all of Bronze).
TASKS_AFTER_BRONZE=" silver_transformations silver_quality_checks gold_transformations gold_quality_checks \
duckdb_gold_analytics publish_gold_to_postgres scd2_dimensions point_in_time_fact_joins scd2_gold_marts \
scd2_duckdb_analytics create_postgresql_schemas load_gold_scd2_to_staging build_warehouse dbt_run dbt_test \
dbt_docs power_bi_export "

nothing_to_process() {
  local raw_dir="data/raw/batch_date=${BATCH_DATE:-$(date +%F)}" f
  [[ -z "$(find data/bronze -name '*.parquet' -print -quit 2>/dev/null)" ]] || return 1
  # The batch must be declared empty (the Bronze checks fail an empty feed otherwise).
  [[ -f "${raw_dir}/_EMPTY_BATCH" ]] || return 1
  for f in customers.csv products.csv campaigns.csv orders.csv order_items.csv ad_spend.csv; do
    # A CSV with rows has more than its header line.
    [[ -f "${raw_dir}/${f}" && "$(wc -l < "${raw_dir}/${f}")" -le 1 ]] || return 1
  done
  [[ -f "${raw_dir}/web_events.json" ]] || return 1
  ! grep -q '[^[:space:]]' "${raw_dir}/web_events.json"
}

if [[ "${TASKS_AFTER_BRONZE}" == *" ${TASK_NAME:-none} "* ]] && nothing_to_process; then
  echo "NOTHING TO PROCESS: batch ${BATCH_DATE:-$(date +%F)} is empty and no earlier batch has been loaded."
  echo "Task ${TASK_NAME} did not run; nothing was created or changed."
  echo "Task completed: ${TASK_NAME}"
  exit 0
fi

run_python_file() {
  local file_path="$1"

  if [[ ! -f "${file_path}" ]]; then
    echo "Missing required file: ${file_path}"
    exit 1
  fi

  echo "Running: ${PROJECT_PYTHON} ${file_path}"
  "${PROJECT_PYTHON}" "${file_path}"
}

run_optional_python_file() {
  local file_path="$1"

  if [[ ! -f "${file_path}" ]]; then
    echo "SKIPPED: ${file_path} not found"
    return 0
  fi

  echo "Running optional file: ${PROJECT_PYTHON} ${file_path}"
  "${PROJECT_PYTHON}" "${file_path}"
}

case "${TASK_NAME}" in

  generate_data)
    batch_date="${BATCH_DATE:-$(date +%F)}"
    [[ "${batch_date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "Invalid BATCH_DATE: ${batch_date} (expected YYYY-MM-DD)"; exit 1; }
    export BATCH_DATE="${batch_date}"
    raw_dir="data/raw/batch_date=${batch_date}"
    missing=0
    # A CSV always has its header; an empty batch has an empty JSON-lines file.
    for f in customers.csv products.csv campaigns.csv orders.csv order_items.csv ad_spend.csv; do
      [[ -s "${raw_dir}/${f}" ]] || missing=1
    done
    [[ -f "${raw_dir}/web_events.json" ]] || missing=1
    if [[ "${missing}" -eq 0 ]]; then
      echo "Raw data for batch_date=${batch_date} already complete. Skipping generation."
    else
      run_python_file "data-generator/generate_data.py"
    fi
    ;;

  bronze_ingestion)
    run_python_file "spark-batch/bronze_ingestion.py"
    ;;

  bronze_quality_checks)
    run_python_file "spark-batch/bronze_quality_checks.py"
    ;;

  silver_transformations)
    run_python_file "spark-batch/silver_transformations.py"
    ;;

  silver_quality_checks)
    run_python_file "spark-batch/silver_quality_checks.py"
    ;;

  gold_transformations)
    run_python_file "spark-batch/gold_transformations.py"
    ;;

  gold_quality_checks)
    run_python_file "spark-batch/gold_quality_checks.py"
    ;;

  duckdb_gold_analytics)
    run_optional_python_file "sql/duckdb_gold_analytics.py"
    ;;

  publish_gold_to_postgres)
    run_optional_python_file "sql/publish_gold_to_postgres.py"
    ;;

  scd2_dimensions)
    run_python_file "spark-batch/scd2_dimensions.py"
    ;;

  point_in_time_fact_joins)
    run_python_file "spark-batch/point_in_time_fact_joins.py"
    ;;

  scd2_gold_marts)
    run_python_file "spark-batch/scd2_gold_marts.py"
    ;;

  scd2_duckdb_analytics)
    run_python_file "sql/run_scd2_duckdb_analytics.py"
    ;;

  create_postgresql_schemas)
    # Makes sure the schemas and the read-only role exist; drops nothing.
    env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_schemas.sql
    ;;

  load_gold_scd2_to_staging)
    # Loads staging_next; build_warehouse publishes it as staging.
    if [[ -f scripts/load_gold_to_postgres_staging.py ]]; then
      run_python_file "scripts/load_gold_to_postgres_staging.py"
    else
      env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/load_gold_to_staging.sql
    fi
    ;;

  build_warehouse)
    # One transaction (several files to run_warehouse_sql.sh): a new generation
    # of the warehouse, marts and audit schemas, visible to readers only when
    # all of it is built and every check has passed. A failing check rolls
    # the build back, fails the task and is written to
    # logs/warehouse/failed_build_<timestamp>.json.
    env -u PGPORT bash scripts/run_warehouse_sql.sh \
      sql/warehouse/begin_generation.sql \
      sql/warehouse/create_warehouse_tables.sql \
      sql/warehouse/scd2_validation_queries.sql \
      sql/warehouse/create_marts.sql \
      sql/warehouse/reconciliation_report.sql
    ;;

  dbt_run)
    if [[ ! -x "${DBT_BIN}" ]]; then
      echo "dbt binary not found or not executable: ${DBT_BIN}"
      exit 1
    fi

    cd "${PROJECT_ROOT}/dbt"
    "${DBT_BIN}" run --profiles-dir .
    ;;

  dbt_test)
    if [[ ! -x "${DBT_BIN}" ]]; then
      echo "dbt binary not found or not executable: ${DBT_BIN}"
      exit 1
    fi

    cd "${PROJECT_ROOT}/dbt"
    "${DBT_BIN}" test --profiles-dir .
    ;;

  dbt_docs)
    if [[ ! -x "${DBT_BIN}" ]]; then
      echo "dbt binary not found or not executable: ${DBT_BIN}"
      exit 1
    fi

    cd "${PROJECT_ROOT}/dbt"
    "${DBT_BIN}" docs generate --profiles-dir .
    ;;

  power_bi_export)
    run_python_file "sql/export_power_bi_dataset.py"
    ;;

  *)
    echo "Unknown task: ${TASK_NAME}"
    echo "Usage: scripts/run_airflow_task.sh <task_name>"
    exit 1
    ;;

esac

echo "Task completed: ${TASK_NAME}"
