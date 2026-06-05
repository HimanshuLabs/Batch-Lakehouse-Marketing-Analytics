#!/usr/bin/env bash
set -euo pipefail

echo "Validating required platform directories..."

required_dirs=(
  "data-generator"
  "spark-batch"
  "sql"
  "sql/warehouse"
  "scripts"
  "dbt"
  "dbt/models"
  "dbt/models/staging"
  "dbt/models/warehouse"
  "dbt/models/marts"
  "dbt/models/audit"
  "airflow/dags"
  "docs"
  "docs/warehouse"
  "exports/power_bi"
  "screenshots/power_bi"
)

for dir_path in "${required_dirs[@]}"; do
  test -d "$dir_path" || {
    echo "Missing required directory: $dir_path"
    exit 1
  }
done

echo "Validating required platform files..."

required_files=(
  "README.md"
  "airflow/dags/batch_lakehouse_pipeline.py"
  "scripts/run_airflow_task.sh"
  "scripts/run_warehouse_sql.sh"
  "scripts/run_warehouse_bi_pipeline.sh"
  "scripts/run_warehouse_reconciliation_pipeline.sh"
  "scripts/load_gold_to_postgres_staging.py"
  "scripts/export_power_bi_marts.py"
  "dbt/dbt_project.yml"
  "dbt/profiles.yml.example"
  "dbt/models/schema.yml"
  "dbt/models/exposures.yml"
  "docs/airflow_orchestration.md"
  "docs/github_actions_ci_cd.md"
  "docs/power_bi_dashboard_spec.md"
  "docs/terraform_aws_bi_exports.md"
  "docs/warehouse/airflow_working_state.md"
  "docs/warehouse/merged_architecture.md"
  "docs/warehouse/bi_reporting_layer.md"
  "docs/warehouse/runbook.md"
  "docs/warehouse/power_bi_dashboard.md"
  "docs/warehouse/power_bi_data_dictionary.md"
  "docs/warehouse/power_bi_refresh_flow.md"
  "docs/warehouse/reconciliation.md"
  "docs/warehouse/scd2_design.md"
  "docs/warehouse/warehouse_data_model.md"
)

for file_path in "${required_files[@]}"; do
  test -s "$file_path" || {
    echo "Missing or empty required file: $file_path"
    exit 1
  }
done

echo "Checking for tracked runtime, backup, database, and sensitive files..."

blocked_pattern='(^|/)(__pycache__|\.airflow|dbt/target|dbt/dbt_packages)(/|$)|(^|/)dbt/profiles\.yml$|(^|/).*\.py[co]$|(^|/).*\.bak($|-)|(^|/).*\.sqlite$|(^|/).*\.db$|(^|/)terraform\.tfvars$|(^|/).*\.tfstate(\.backup)?$|(^|/)tfplan$|(^|/)\.aws/(credentials|config)$'

if git ls-files | grep -E "$blocked_pattern"; then
  echo "Tracked runtime, backup, database, Terraform state/plan, or credential file found."
  echo "Remove it from Git before merging."
  exit 1
fi

echo "Checking documentation placeholders..."

if grep -RInE 'REPLACE_ME|FIXME|<your_|YOUR_[A-Z0-9_]+|CHANGE_ME|PASTE_' README.md docs; then
  echo "Unresolved documentation placeholder found."
  exit 1
fi

echo "Required file and repository guardrail validation passed."
