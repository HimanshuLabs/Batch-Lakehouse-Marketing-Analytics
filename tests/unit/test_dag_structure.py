"""The Airflow DAG's tasks, order and environment, checked without Airflow.

Airflow is replaced by small stand-ins, so the real DAG file runs and every
BashOperator and >> dependency is recorded.
Fixed in 044b433: PROJECT_ROOT came from a hardcoded local path.
Fixed in f2cdde7: every task gets one BATCH_DATE per run through DEFAULT_ENV.
"""
from __future__ import annotations

import sys
import types

import pytest

DAG_FILE = "airflow/dags/batch_lakehouse_pipeline.py"
DAG_ID = "batch_lakehouse_marketing_analytics"
BATCH_DATE_TEMPLATE = "{{ (dag_run.logical_date or dag_run.run_after) | ds }}"

CHAIN = [
    "generate_data", "bronze_ingestion", "bronze_quality_checks", "silver_transformations",
    "silver_quality_checks", "gold_transformations", "gold_quality_checks",
    ["duckdb_gold_analytics", "publish_gold_to_postgres"],
    "scd2_dimensions", "point_in_time_fact_joins", "scd2_gold_marts", "scd2_duckdb_analytics",
    "create_postgresql_schemas", "load_gold_scd2_to_staging", "build_warehouse_tables",
    "run_scd2_validation", "build_reporting_marts", "run_warehouse_reconciliation",
    "dbt_run", "dbt_test", "dbt_docs", "power_bi_export",
]


def expected_edges() -> set[tuple[str, str]]:
    edges = set()
    for upstream, downstream in zip(CHAIN, CHAIN[1:]):
        for a in upstream if isinstance(upstream, list) else [upstream]:
            for b in downstream if isinstance(downstream, list) else [downstream]:
                edges.add((a, b))
    return edges


class Recorder:
    def __init__(self):
        self.tasks: dict[str, dict] = {}
        self.edges: set[tuple[str, str]] = set()
        self.dag_ids: list[str] = []


def stub_airflow(monkeypatch) -> Recorder:
    recorder = Recorder()

    class Task:
        def __init__(self, task_id, **kwargs):
            self.task_id = task_id
            recorder.tasks[task_id] = kwargs

        def _link(self, other):
            for task in other if isinstance(other, list) else [other]:
                recorder.edges.add((self.task_id, task.task_id))
            return other

        def __rshift__(self, other):
            return self._link(other)

        def __rrshift__(self, other):
            for task in other if isinstance(other, list) else [other]:
                task._link(self)
            return self

    class DAG:
        def __init__(self, dag_id=None, **kwargs):
            recorder.dag_ids.append(dag_id)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    airflow = types.ModuleType("airflow")
    airflow.DAG = DAG
    operators = types.ModuleType("airflow.operators")
    bash = types.ModuleType("airflow.operators.bash")
    bash.BashOperator = Task
    for name, module in (("airflow", airflow), ("airflow.operators", operators), ("airflow.operators.bash", bash)):
        monkeypatch.setitem(sys.modules, name, module)
    return recorder


@pytest.fixture
def dag(load_job, monkeypatch):
    monkeypatch.delenv("PROJECT_ROOT", raising=False)
    monkeypatch.delenv("PROJECT_PYTHON", raising=False)
    monkeypatch.delenv("DBT_BIN", raising=False)
    recorder = stub_airflow(monkeypatch)
    module = load_job(DAG_FILE)
    return module, recorder


def test_the_dag_has_every_task_in_order(dag):
    module, recorder = dag
    assert recorder.dag_ids == [DAG_ID]
    assert recorder.edges == expected_edges()


def test_every_task_gets_one_environment_with_the_run_batch_date(dag):
    module, recorder = dag
    assert set(module.DEFAULT_ENV) == {"PROJECT_ROOT", "PROJECT_PYTHON", "DBT_BIN", "BATCH_DATE", "P2_RUN_ID"}
    assert module.DEFAULT_ENV["P2_RUN_ID"] == "{{ run_id }}"
    assert module.DEFAULT_ENV["BATCH_DATE"] == BATCH_DATE_TEMPLATE
    without_env = [task for task, kwargs in recorder.tasks.items() if kwargs.get("env") is not module.DEFAULT_ENV]
    assert without_env == [], f"tasks without DEFAULT_ENV: {without_env}"


def test_project_root_comes_from_the_checkout(dag, project_root):
    module, recorder = dag
    project_root = project_root.resolve()
    assert module.DEFAULT_ENV["PROJECT_ROOT"] == str(project_root)
    assert module.DEFAULT_ENV["PROJECT_PYTHON"] == f"{project_root}/venv/bin/python"
    assert module.DEFAULT_ENV["DBT_BIN"] == f"{project_root}/venv-dbt/bin/dbt"
    commands = " ".join(str(kwargs.get("bash_command", "")) for kwargs in recorder.tasks.values())
    assert "/home/" not in commands and "/Users/" not in commands
