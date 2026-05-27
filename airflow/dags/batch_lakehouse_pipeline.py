from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    "/home/manshu/Desktop/Project-2-Batch-Lakehouse-Marketing-Analytics",
)

RUNNER = f"{PROJECT_ROOT}/scripts/run_airflow_task.sh"

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
        bash_command=f"bash {RUNNER} generate_data",
        env=DEFAULT_ENV,
    )

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"bash {RUNNER} bronze_ingestion",
        env=DEFAULT_ENV,
    )

    bronze_quality_checks = BashOperator(
        task_id="bronze_quality_checks",
        bash_command=f"bash {RUNNER} bronze_quality_checks",
        env=DEFAULT_ENV,
    )

    silver_transformations = BashOperator(
        task_id="silver_transformations",
        bash_command=f"bash {RUNNER} silver_transformations",
        env=DEFAULT_ENV,
    )

    silver_quality_checks = BashOperator(
        task_id="silver_quality_checks",
        bash_command=f"bash {RUNNER} silver_quality_checks",
        env=DEFAULT_ENV,
    )

    gold_transformations = BashOperator(
        task_id="gold_transformations",
        bash_command=f"bash {RUNNER} gold_transformations",
        env=DEFAULT_ENV,
    )

    gold_quality_checks = BashOperator(
        task_id="gold_quality_checks",
        bash_command=f"bash {RUNNER} gold_quality_checks",
        env=DEFAULT_ENV,
    )

    duckdb_gold_analytics = BashOperator(
        task_id="duckdb_gold_analytics",
        bash_command=f"bash {RUNNER} duckdb_gold_analytics",
        env=DEFAULT_ENV,
    )

    publish_gold_to_postgres = BashOperator(
        task_id="publish_gold_to_postgres",
        bash_command=f"bash {RUNNER} publish_gold_to_postgres",
        env=DEFAULT_ENV,
    )

    scd2_dimensions = BashOperator(
        task_id="scd2_dimensions",
        bash_command=f"bash {RUNNER} scd2_dimensions",
        env=DEFAULT_ENV,
    )

    point_in_time_fact_joins = BashOperator(
        task_id="point_in_time_fact_joins",
        bash_command=f"bash {RUNNER} point_in_time_fact_joins",
        env=DEFAULT_ENV,
    )

    scd2_gold_marts = BashOperator(
        task_id="scd2_gold_marts",
        bash_command=f"bash {RUNNER} scd2_gold_marts",
        env=DEFAULT_ENV,
    )

    scd2_duckdb_analytics = BashOperator(
        task_id="scd2_duckdb_analytics",
        bash_command=f"bash {RUNNER} scd2_duckdb_analytics",
        env=DEFAULT_ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"bash {RUNNER} dbt_run",
        env=DEFAULT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"bash {RUNNER} dbt_test",
        env=DEFAULT_ENV,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"bash {RUNNER} dbt_docs",
        env=DEFAULT_ENV,
    )

    power_bi_export = BashOperator(
        task_id="power_bi_export",
        bash_command=f"bash {RUNNER} power_bi_export",
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
        >> dbt_run
        >> dbt_test
        >> dbt_docs
        >> power_bi_export
    )
