"""Run the Airflow DAG's own task commands, in DAG order, without Airflow.

Each task gets what Airflow's BashOperator would give it: only DEFAULT_ENV,
with BATCH_DATE rendered for the run. Airflow is replaced by small stand-ins,
so the real DAG file is loaded, every BashOperator and >> dependency is
recorded, and the tasks run in topological order (ties in file order).

Usage (from anywhere):
    python tests/integration/run_dag.py --batch-date 2026-05-18 --log-dir /tmp/p2-runs/0518
    python tests/integration/run_dag.py --batch-date 2026-06-18 --log-dir DIR \
        --skip dbt_run dbt_test dbt_docs --env PGCONTAINER=p2it_postgres
    python tests/integration/run_dag.py --batch-date 2026-06-18 --log-dir DIR --dry-run

--env adds variables to every task's environment on top of DEFAULT_ENV (for
example to point the SQL tasks at a throwaway Postgres container).
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import time
import types
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DAG_FILE = REPO_ROOT / "airflow" / "dags" / "batch_lakehouse_pipeline.py"
BATCH_DATE_TEMPLATE = "{{ (dag_run.logical_date or dag_run.run_after).astimezone(macros.dateutil.tz.gettz('Asia/Kolkata')) | ds }}"


def load_dag() -> tuple[object, dict[str, dict], list[tuple[str, str]]]:
    """Load the DAG file with stand-in Airflow modules; return (module, tasks, edges)."""
    tasks: dict[str, dict] = {}
    edges: list[tuple[str, str]] = []

    class Task:
        def __init__(self, task_id, **kwargs):
            self.task_id = task_id
            tasks[task_id] = kwargs

        def _link(self, other):
            for task in other if isinstance(other, list) else [other]:
                edges.append((self.task_id, task.task_id))
            return other

        def __rshift__(self, other):
            return self._link(other)

        def __rrshift__(self, other):
            for task in other if isinstance(other, list) else [other]:
                task._link(self)
            return self

    class DAG:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    airflow = types.ModuleType("airflow")
    airflow.DAG = DAG
    bash = types.ModuleType("airflow.operators.bash")
    bash.BashOperator = Task
    sys.modules["airflow"] = airflow
    sys.modules["airflow.operators"] = types.ModuleType("airflow.operators")
    sys.modules["airflow.operators.bash"] = bash

    # The DAG honours PROJECT_ROOT / PROJECT_PYTHON / DBT_BIN from the
    # environment; clear them so the runner always runs the checkout it lives
    # in (a copy under /tmp must never point back at the real repository).
    for name in ("PROJECT_ROOT", "PROJECT_PYTHON", "DBT_BIN"):
        os.environ.pop(name, None)

    spec = importlib.util.spec_from_file_location("p2_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks, edges


def task_order(tasks: dict[str, dict], edges: list[tuple[str, str]]) -> list[str]:
    """Topological order; among ready tasks, the one declared first in the DAG file runs first."""
    declared = list(tasks)
    upstream = {task: {a for a, b in edges if b == task} for task in declared}
    order: list[str] = []
    while len(order) < len(declared):
        ready = [t for t in declared if t not in order and upstream[t] <= set(order)]
        if not ready:
            raise SystemExit(f"DAG has a cycle among: {sorted(set(declared) - set(order))}")
        order.append(ready[0])
    return order


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--log-dir", type=Path, default=REPO_ROOT / "logs" / ("dag-" + uuid.uuid4().hex))
    parser.add_argument("--skip", nargs="*", default=[])
    parser.add_argument("--from", dest="start")
    parser.add_argument("--to", dest="stop")
    parser.add_argument("--env", nargs="*", default=[], metavar="KEY=VALUE")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if dt.date.fromisoformat(args.batch_date).isoformat() != args.batch_date:
        raise SystemExit("--batch-date must be YYYY-MM-DD")

    module, tasks, edges = load_dag()
    order = task_order(tasks, edges)
    for name in args.skip + [args.start, args.stop]:
        if name is not None and name not in order:
            raise SystemExit(f"unknown task: {name}")

    default_env = getattr(module, "DEFAULT_ENV")
    if default_env.get("BATCH_DATE") != BATCH_DATE_TEMPLATE:
        raise SystemExit("DEFAULT_ENV no longer renders BATCH_DATE from the DAG run")
    env = {key: (args.batch_date if value == BATCH_DATE_TEMPLATE else value) for key, value in default_env.items()}
    env["P2_RUN_ID"] = "local-" + uuid.uuid4().hex
    for item in args.env:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise SystemExit(f"--env expects KEY=VALUE, got {item!r}")
        env[key] = value

    first = order.index(args.start) if args.start else 0
    last = order.index(args.stop) if args.stop else len(order) - 1
    selected = order[first:last + 1]

    print(f"BATCH_DATE={args.batch_date} PROJECT_ROOT={env['PROJECT_ROOT']} tasks={len(selected)}", flush=True)
    if not args.dry_run:
        args.log_dir.mkdir(parents=True, exist_ok=True)
    bash = shutil.which("bash") or "/bin/bash"

    try:
        return run_tasks(args, order, selected, tasks, env, bash)
    finally:
        # This invocation is the whole run (it may stop before the DAG's last
        # task), so give the pipeline run lock back whatever happened.
        if not args.dry_run:
            subprocess.run([env["PROJECT_PYTHON"], str(Path(env["PROJECT_ROOT"]) / "scripts" / "pipeline_lock.py"),
                            "--release"], env=env, stdout=subprocess.DEVNULL, check=False)


def run_tasks(args, order, selected, tasks, env, bash) -> int:
    for number, task in enumerate(order, 1):
        if task not in selected:
            continue
        label = f"{number:02d} {task:30s}"
        if task in args.skip:
            print(f"{label} SKIPPED", flush=True)
            continue
        command = tasks[task]["bash_command"]
        if args.dry_run:
            print(f"{label} {command!r}")
            continue
        log = args.log_dir / f"{number:02d}_{task}.log"
        started = time.time()
        # Each task runs in its own empty working directory, removed afterwards,
        # as an Airflow worker would (Spark writes scratch files to its cwd).
        with log.open("w", encoding="utf-8") as handle, tempfile.TemporaryDirectory(prefix="p2task-") as task_dir:
            code = subprocess.run(
                [bash, "-c", command],
                env={**env, "AIRFLOW_CTX_TASK_ID": task},
                cwd=task_dir,
                stdout=handle,
                stderr=subprocess.STDOUT,
            ).returncode
        print(f"{label} rc={code} {time.time() - started:6.1f}s", flush=True)
        if code != 0:
            print("---- last 25 log lines ----")
            print("".join(log.read_text(encoding="utf-8").splitlines(True)[-25:]))
            print(f"STOPPED at {task}. Log: {log}")
            return 1
    print("RUN COMPLETE" if not args.dry_run else "DRY RUN ONLY")
    return 0


if __name__ == "__main__":
    sys.exit(main())
