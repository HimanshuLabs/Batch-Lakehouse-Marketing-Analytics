#!/usr/bin/env python3
"""Run one pipeline task under the pipeline run lock.

    python scripts/pipeline_lock.py [--last] <task_id> -- <command> [args...]
    python scripts/pipeline_lock.py --release [--force]

Only one pipeline run may work on a project at a time: every task rebuilds
shared outputs (the lake folders, the warehouse), so two runs at once would
overwrite each other. The lock is logs/pipeline_run.lock under PROJECT_ROOT and
is owned by a run, named by P2_RUN_ID (the Airflow run_id, or run_dag.py's id):

  - While a task runs it holds an exclusive flock on the file, so a second task
    started at the same moment fails at once.
  - Between tasks the file still names the owning run. A task of another run
    fails until the owner is released: when its last task (--last) succeeds,
    when one of its tasks fails, or with --release. A retry of the owner's own
    task always continues.
  - An owner that has not started a task for P2_RUN_LOCK_STALE_SECONDS (default
    3600) is taken to be dead and may be replaced.

Exit code 75 means the lock is held by another run; nothing was executed.
"""
from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

LOCKED_EXIT_CODE = 75
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])
LOCK_FILE = PROJECT_ROOT / "logs" / "pipeline_run.lock"
STALE_SECONDS = float(os.environ.get("P2_RUN_LOCK_STALE_SECONDS", "3600"))


def read_state(handle) -> dict:
    handle.seek(0)
    text = handle.read().strip()
    try:
        return json.loads(text) if text else {}
    except json.JSONDecodeError:
        return {}


def write_state(handle, state: dict) -> None:
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps(state) + "\n")
    handle.flush()
    os.fsync(handle.fileno())


def describe(state: dict) -> str:
    return f"run {state.get('run_id')!r}, task {state.get('task')!r}, since {state.get('updated_at')}"


def open_locked():
    """The lock file, held with an exclusive flock; None when a task holds it."""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    handle = LOCK_FILE.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        state = read_state(handle)
        handle.close()
        print(f"PIPELINE LOCKED: another pipeline task is running now ({describe(state)}). "
              f"Nothing was run. Lock file: {LOCK_FILE}", file=sys.stderr)
        return None
    return handle


def release(force: bool) -> int:
    run_id = os.environ.get("P2_RUN_ID", "")
    handle = open_locked()
    if handle is None:
        return LOCKED_EXIT_CODE
    with handle:
        state = read_state(handle)
        if state.get("state") == "active" and (force or state.get("run_id") == run_id):
            write_state(handle, {**state, "state": "released", "released_at": now_text()})
            print(f"Released the pipeline lock of {describe(state)}.")
        elif state.get("state") == "active":
            print(f"The pipeline lock belongs to {describe(state)}; not released (use --force).", file=sys.stderr)
            return LOCKED_EXIT_CODE
    return 0


def now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_task(task_id: str, command: list[str], last: bool) -> int:
    run_id = os.environ.get("P2_RUN_ID")
    if not run_id:
        print("P2_RUN_ID is not set: the pipeline lock needs the id of the run.", file=sys.stderr)
        return 2

    handle = open_locked()
    if handle is None:
        return LOCKED_EXIT_CODE
    with handle:
        state = read_state(handle)
        owned_by_another_run = state.get("state") == "active" and state.get("run_id") != run_id
        if owned_by_another_run and time.time() - float(state.get("updated_epoch", 0)) < STALE_SECONDS:
            print(f"PIPELINE LOCKED: another pipeline run is in progress ({describe(state)}). "
                  f"Nothing was run. Wait for it to finish; if it is dead, run "
                  f"'python scripts/pipeline_lock.py --release --force'. Lock file: {LOCK_FILE}", file=sys.stderr)
            return LOCKED_EXIT_CODE
        if owned_by_another_run:
            print(f"Replacing the stale pipeline lock of {describe(state)}.")

        mine = {"run_id": run_id, "task": task_id, "state": "active", "pid": os.getpid(),
                "updated_at": now_text(), "updated_epoch": time.time()}
        write_state(handle, mine)

        child = subprocess.Popen(command)

        def stop(signum, _frame):
            child.send_signal(signum)

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        code = child.wait()

        # The run keeps the lock between its tasks. It gives it up when its last
        # task succeeds, or when a task fails (a retry of the same run takes it
        # again unless another run has started meanwhile).
        finished = (last and code == 0) or code != 0
        write_state(handle, {**mine, "state": "released" if finished else "active",
                             "exit_code": code, "finished_at": now_text()})
    return code if code >= 0 else 128 - code


def main(argv: list[str]) -> int:
    if argv and argv[0] == "--release":
        return release(force="--force" in argv[1:])
    last = bool(argv) and argv[0] == "--last"
    if last:
        argv = argv[1:]
    if len(argv) < 3 or argv[1] != "--":
        print(__doc__, file=sys.stderr)
        return 2
    return run_task(argv[0], argv[2:], last)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
