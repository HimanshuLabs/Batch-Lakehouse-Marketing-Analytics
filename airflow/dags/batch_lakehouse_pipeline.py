from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    str(Path(__file__).resolve().parents[2]),
)

RUNNER = f"{PROJECT_ROOT}/scripts/run_airflow_task.sh"
RUN_LOCK = f"{PROJECT_ROOT}/scripts/pipeline_lock.py"


def locked(task_id: str, command: str, last: bool = False) -> str:
    """The task's command, run under the pipeline run lock (scripts/pipeline_lock.py).

    Only one run may work on the project at a time; a task of a second run fails
    at once instead of overwriting the first run's outputs. The run that owns the
    lock (P2_RUN_ID) keeps it until its last task succeeds or a task fails.
    """
    flag = "--last " if last else ""
    return f'"$PROJECT_PYTHON" "{RUN_LOCK}" {flag}{task_id} -- bash -c {shlex.quote(command)}'

DEFAULT_ENV = {
    "PROJECT_ROOT": PROJECT_ROOT,
    "PROJECT_PYTHON": os.environ.get(
        "PROJECT_PYTHON",
        f"{PROJECT_ROOT}/venv/bin/python",
    ),
    "DBT_BIN": os.environ.get(
        "DBT_BIN",
        f"{PROJECT_ROOT}/venv-dbt/bin/dbt",
    ),
    "P2_RUN_ID": "{{ run_id }}",
    # The batch day is the business day (Asia/Kolkata) of the run's logical date,
    # or of run_after when the run has none: a run at 00:30 IST processes the day
    # that has just started in India, not the UTC day that is still yesterday.
    "BATCH_DATE": "{{ (dag_run.logical_date or dag_run.run_after).astimezone(macros.dateutil.tz.gettz('Asia/Kolkata')) | ds }}",
}


default_args = {
    "owner": "project-2-batch-lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="batch_lakehouse_marketing_analytics",
    description="Batch lakehouse orchestration for marketing analytics medallion pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "batch",
        "lakehouse",
        "marketing-analytics",
        "medallion",
        "scd2",
        "dbt",
        "power-bi",
    ],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=locked("generate_data", f"bash {RUNNER} generate_data"),
        env=DEFAULT_ENV,
    )

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=locked("bronze_ingestion", f"bash {RUNNER} bronze_ingestion"),
        env=DEFAULT_ENV,
    )

    bronze_quality_checks = BashOperator(
        task_id="bronze_quality_checks",
        bash_command=locked("bronze_quality_checks", f"bash {RUNNER} bronze_quality_checks"),
        env=DEFAULT_ENV,
    )

    silver_transformations = BashOperator(
        task_id="silver_transformations",
        bash_command=locked("silver_transformations", f"bash {RUNNER} silver_transformations"),
        env=DEFAULT_ENV,
    )

    silver_quality_checks = BashOperator(
        task_id="silver_quality_checks",
        bash_command=locked("silver_quality_checks", f"bash {RUNNER} silver_quality_checks"),
        env=DEFAULT_ENV,
    )

    gold_transformations = BashOperator(
        task_id="gold_transformations",
        bash_command=locked("gold_transformations", f"bash {RUNNER} gold_transformations"),
        env=DEFAULT_ENV,
    )

    gold_quality_checks = BashOperator(
        task_id="gold_quality_checks",
        bash_command=locked("gold_quality_checks", f"bash {RUNNER} gold_quality_checks"),
        env=DEFAULT_ENV,
    )

    duckdb_gold_analytics = BashOperator(
        task_id="duckdb_gold_analytics",
        bash_command=locked("duckdb_gold_analytics", f"bash {RUNNER} duckdb_gold_analytics"),
        env=DEFAULT_ENV,
    )

    publish_gold_to_postgres = BashOperator(
        task_id="publish_gold_to_postgres",
        bash_command=locked("publish_gold_to_postgres", f"bash {RUNNER} publish_gold_to_postgres"),
        env=DEFAULT_ENV,
    )

    scd2_dimensions = BashOperator(
        task_id="scd2_dimensions",
        bash_command=locked("scd2_dimensions", f"bash {RUNNER} scd2_dimensions"),
        env=DEFAULT_ENV,
    )

    point_in_time_fact_joins = BashOperator(
        task_id="point_in_time_fact_joins",
        bash_command=locked("point_in_time_fact_joins", f"bash {RUNNER} point_in_time_fact_joins"),
        env=DEFAULT_ENV,
    )

    scd2_gold_marts = BashOperator(
        task_id="scd2_gold_marts",
        bash_command=locked("scd2_gold_marts", f"bash {RUNNER} scd2_gold_marts"),
        env=DEFAULT_ENV,
    )

    scd2_duckdb_analytics = BashOperator(
        task_id="scd2_duckdb_analytics",
        bash_command=locked("scd2_duckdb_analytics", f"bash {RUNNER} scd2_duckdb_analytics"),
        env=DEFAULT_ENV,
    )

    create_postgresql_schemas = BashOperator(
        task_id="create_postgresql_schemas",
        bash_command=locked(
            "create_postgresql_schemas",
            (
                "set -euo pipefail\n"
                f"cd {PROJECT_ROOT}\n"
                "env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_schemas.sql"
            ),
        ),
        env=DEFAULT_ENV,
    )

    load_gold_scd2_to_staging = BashOperator(
        task_id="load_gold_scd2_to_staging",
        bash_command=locked(
            "load_gold_scd2_to_staging",
            (
                "set -euo pipefail\n"
                f"cd {PROJECT_ROOT}\n"
                "if [ -f scripts/load_gold_to_postgres_staging.py ]; then\n"
                "  source venv/bin/activate\n"
                "  python scripts/load_gold_to_postgres_staging.py\n"
                "else\n"
                "  env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/load_gold_to_staging.sql\n"
                "fi"
            ),
        ),
        env=DEFAULT_ENV,
    )

    build_warehouse_tables = BashOperator(
        task_id="build_warehouse_tables",
        bash_command=locked(
            "build_warehouse_tables",
            (
                "set -euo pipefail\n"
                f"cd {PROJECT_ROOT}\n"
                "env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_warehouse_tables.sql"
            ),
        ),
        env=DEFAULT_ENV,
    )

    run_scd2_validation = BashOperator(
        task_id="run_scd2_validation",
        bash_command=locked(
            "run_scd2_validation",
            (
                "set -euo pipefail\n"
                f"cd {PROJECT_ROOT}\n"
                "env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/scd2_validation_queries.sql"
            ),
        ),
        env=DEFAULT_ENV,
    )

    run_warehouse_reconciliation = BashOperator(
        task_id="run_warehouse_reconciliation",
        bash_command=locked(
            "run_warehouse_reconciliation",
            (
                "set -euo pipefail\n"
                f"cd {PROJECT_ROOT}\n"
                "env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/reconciliation_report.sql"
            ),
        ),
        env=DEFAULT_ENV,
    )

    build_reporting_marts = BashOperator(
        task_id="build_reporting_marts",
        bash_command=locked(
            "build_reporting_marts",
            (
                "set -euo pipefail\n"
                f"cd {PROJECT_ROOT}\n"
                "env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_marts.sql"
            ),
        ),
        env=DEFAULT_ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=locked("dbt_run", f"bash {RUNNER} dbt_run"),
        env=DEFAULT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=locked("dbt_test", f"bash {RUNNER} dbt_test"),
        env=DEFAULT_ENV,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=locked("dbt_docs", f"bash {RUNNER} dbt_docs"),
        env=DEFAULT_ENV,
    )

    power_bi_export = BashOperator(
        task_id="power_bi_export",
        bash_command=locked("power_bi_export", f"bash {RUNNER} power_bi_export", last=True),
        env=DEFAULT_ENV,
    )

    (
        generate_data
        >> bronze_ingestion
        >> bronze_quality_checks
        >> silver_transformations
        >> silver_quality_checks
        >> gold_transformations
        >> gold_quality_checks
        >> [duckdb_gold_analytics, publish_gold_to_postgres]
        >> scd2_dimensions
        >> point_in_time_fact_joins
        >> scd2_gold_marts
        >> scd2_duckdb_analytics
        >> create_postgresql_schemas
        >> load_gold_scd2_to_staging
        >> build_warehouse_tables
        >> run_scd2_validation
        >> build_reporting_marts
        >> run_warehouse_reconciliation
        >> dbt_run
        >> dbt_test
        >> dbt_docs
        >> power_bi_export
    )
