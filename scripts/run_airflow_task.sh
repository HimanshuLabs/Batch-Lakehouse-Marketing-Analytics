#!/usr/bin/env bash
set -euo pipefail

TASK_NAME="${1:-}"

PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PROJECT_PYTHON="${PROJECT_PYTHON:-${PROJECT_ROOT}/venv/bin/python}"
DBT_BIN="${DBT_BIN:-${PROJECT_ROOT}/venv-dbt/bin/dbt}"

cd "${PROJECT_ROOT}"

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
    echo "Optional file not found, skipping: ${file_path}"
    return 0
  fi

  echo "Running optional file: ${PROJECT_PYTHON} ${file_path}"
  "${PROJECT_PYTHON}" "${file_path}"
}

case "${TASK_NAME}" in

  generate_data)
    echo "Checking synthetic data generation step"

    if [[ -f "spark-batch/generate_synthetic_data.py" ]]; then
      run_python_file "spark-batch/generate_synthetic_data.py"
    elif [[ -f "data-generator/generate_synthetic_data.py" ]]; then
      run_python_file "data-generator/generate_synthetic_data.py"
    elif [[ -f "scripts/generate_synthetic_data.py" ]]; then
      run_python_file "scripts/generate_synthetic_data.py"
    elif find data/raw -type f 2>/dev/null | grep -q .; then
      echo "Raw data already exists. Skipping generation."
    else
      echo "No generator script found and data/raw is empty."
      exit 1
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
