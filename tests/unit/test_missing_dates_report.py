"""The missing-dates report: business dates without orders or web events, reported and never a failure.

scripts/report_missing_dates.py runs at the end of the build_warehouse task. It
reads the database that build wrote, through the same route and settings as
scripts/run_warehouse_sql.sh, writes logs/missing_dates_report.json (read by
nothing) and always exits 0. No database is needed here: docker and psql are
stand-ins that record where they were sent.
"""
from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

from conftest import CODE_ROOT

SCRIPT = "scripts/report_missing_dates.py"
RUNNER = CODE_ROOT / "scripts" / "run_airflow_task.sh"
WAREHOUSE_SQL = CODE_ROOT / "scripts" / "run_warehouse_sql.sh"


def every_day(first: str, last: str, except_from: str | None = None, except_to: str | None = None) -> set[date]:
    day, end = date.fromisoformat(first), date.fromisoformat(last)
    gap = (date.fromisoformat(except_from), date.fromisoformat(except_to)) if except_from else None
    days = set()
    while day <= end:
        if not gap or not gap[0] <= day <= gap[1]:
            days.add(day)
        day += timedelta(days=1)
    return days


# --- The ranges --------------------------------------------------------------------


def test_the_three_missing_days_of_the_default_run(load_job):
    module = load_job(SCRIPT)
    present = every_day("2026-01-18", "2026-06-18", "2026-05-19", "2026-05-21")
    report = module.build_report({"orders": present, "web_events": set(present)})
    assert (report["first_date"], report["last_date"], report["days"]) == ("2026-01-18", "2026-06-18", 152)
    for feed in ("orders", "web_events"):
        assert report[feed] == {"missing_ranges": [{"from": "2026-05-19", "to": "2026-05-21", "days": 3}], "missing_days": 3}


def test_the_gap_of_the_two_batch_ci_run(load_job):
    module = load_job(SCRIPT)
    first, last = date(2026, 1, 18), date(2026, 6, 18)
    assert module.missing_ranges(every_day("2026-01-18", "2026-06-18", "2026-05-19", "2026-06-04"), first, last) \
        == [{"from": "2026-05-19", "to": "2026-06-04", "days": 17}]
    assert module.missing_ranges(every_day("2026-01-18", "2026-06-18"), first, last) == []
    single = every_day("2026-01-18", "2026-06-18") - {date(2026, 2, 1), date(2026, 6, 18)}
    assert module.missing_ranges(single, first, last) == [{"from": "2026-02-01", "to": "2026-02-01", "days": 1},
                                                          {"from": "2026-06-18", "to": "2026-06-18", "days": 1}]
    # One span for both feeds: a day with orders but without web events is listed for web events.
    everything = every_day("2026-01-18", "2026-06-18")
    report = module.build_report({"orders": everything, "web_events": everything - {last}})
    assert report["orders"] == {"missing_ranges": [], "missing_days": 0}
    assert report["web_events"] == {"missing_ranges": [{"from": "2026-06-18", "to": "2026-06-18", "days": 1}],
                                    "missing_days": 1}


# --- It reads the database the build wrote (stand-ins for docker and psql) ----------------


FAKE_DOCKER = r"""#!/usr/bin/env bash
if [ "$1" = ps ]; then echo "$FAKE_RUNNING"; exit 0; fi
if [ "$1" = exec ]; then
  shift
  while [ $# -gt 0 ]; do case "$1" in -i) shift ;; -e) shift 2 ;; *) break ;; esac; done
  container=$1; shift
  user=; db=
  while [ $# -gt 0 ]; do case "$1" in -U) user=$2; shift ;; -d) db=$2; shift ;; esac; shift; done
  echo "docker $container $user $db" >> "$FAKE_LOG"
  cat > /dev/null
  [ -n "${FAKE_FAIL:-}" ] && { echo "Error response from daemon: No such container: $container" >&2; exit 1; }
  printf 'orders|20260518\nweb_events|20260518\n'
  exit 0
fi
exit 1
"""
FAKE_PSQL = r"""#!/usr/bin/env bash
host=; port=; db=; user=
while [ $# -gt 0 ]; do
  case "$1" in --host|-h) host=$2; shift ;; --port|-p) port=$2; shift ;; --dbname|-d) db=$2; shift ;;
    --username|-U) user=$2; shift ;; esac
  shift
done
echo "psql $host $port $user $db" >> "$FAKE_LOG"
cat > /dev/null
printf 'orders|20260518\nweb_events|20260518\n'
"""


@pytest.fixture
def stand_ins(tmp_path):
    folder = tmp_path / "bin"
    folder.mkdir()
    for name, text in (("docker", FAKE_DOCKER), ("psql", FAKE_PSQL)):
        path = folder / name
        path.write_text(text, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return folder


def run_both(tmp_path: Path, stand_ins: Path, settings: dict[str, str], running: str) -> tuple[list[str], list[str], dict]:
    """What the build's runner and the report each reached, with the same environment."""
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    (project / "build.sql").write_text("SELECT 1;\n", encoding="utf-8")
    base = {"PATH": f"{stand_ins}:{os.environ['PATH']}", "HOME": str(tmp_path), "P2_LOCK_HELD": "1",
            "FAKE_RUNNING": running, "PROJECT_ROOT": str(project), **settings}
    build_log, report_log = tmp_path / "build.log", tmp_path / "report.log"
    built = subprocess.run(["bash", str(WAREHOUSE_SQL), "build.sql"], cwd=project, stdin=subprocess.DEVNULL,
                           capture_output=True, text=True, env={**base, "FAKE_LOG": str(build_log)})
    assert built.returncode == 0, built.stdout + built.stderr
    reported = subprocess.run([sys.executable, str(CODE_ROOT / SCRIPT)], cwd=project, stdin=subprocess.DEVNULL,
                              capture_output=True, text=True, env={**base, "FAKE_LOG": str(report_log)})
    assert reported.returncode == 0, reported.stdout + reported.stderr
    report = json.loads((project / "logs" / "missing_dates_report.json").read_text(encoding="utf-8"))
    return build_log.read_text().splitlines(), report_log.read_text().splitlines(), report


@pytest.mark.parametrize("settings, running, target", [
    # The DAG, CI and a scale run: nothing set.
    ({}, "project2_postgres", "docker project2_postgres project2 marketing_analytics"),
    # run_local.sh: its sandbox container, with P2_POSTGRES_PORT beside it (ignored by both).
    ({"PGCONTAINER": "p2it_postgres_4242", "P2_POSTGRES_PORT": "5440", "P2_POSTGRES_HOST": "127.0.0.1"},
     "p2it_postgres_4242", "docker p2it_postgres_4242 project2 marketing_analytics"),
    # run_dag.py --env PGCONTAINER=... alone (a rehearsal on a copy): never the live database.
    ({"PGCONTAINER": "p2_scale_copy"}, "p2_scale_copy", "docker p2_scale_copy project2 marketing_analytics"),
    # RUN_MODE=host with every connection setting.
    ({"RUN_MODE": "host", "PGHOST": "db.example.internal", "PGPORT": "6543", "PGDATABASE": "rehearsal", "PGUSER": "builder"},
     "", "psql db.example.internal 6543 builder rehearsal"),
    ({"RUN_MODE": "host"}, "", "psql localhost 5434 project2 marketing_analytics"),
])
def test_the_report_reads_the_database_the_build_wrote(tmp_path, stand_ins, settings, running, target):
    build, report, written = run_both(tmp_path, stand_ins, settings, running)
    assert build == [target], "where the build went"
    assert report == build, "the report reads that same database"
    assert written["status"] == "ok" and written["first_date"] == written["last_date"] == "2026-05-18"


def test_without_the_builds_route_it_reports_not_checked_and_exits_0(tmp_path, stand_ins):
    project = tmp_path / "project"
    project.mkdir(exist_ok=True)
    log = tmp_path / "report.log"
    base = {"PATH": f"{stand_ins}:{os.environ['PATH']}", "HOME": str(tmp_path), "PROJECT_ROOT": str(project),
            "FAKE_LOG": str(log)}
    for settings in ({"RUN_MODE": "elsewhere"}, {"FAKE_FAIL": "1"}):
        result = subprocess.run([sys.executable, str(CODE_ROOT / SCRIPT)], cwd=project, stdin=subprocess.DEVNULL,
                                capture_output=True, text=True, env={**base, **settings})
        assert result.returncode == 0, result.stdout + result.stderr
        assert "MISSING DATES (a report; it never fails a task): not checked" in result.stdout
        written = json.loads((project / "logs" / "missing_dates_report.json").read_text(encoding="utf-8"))
        assert written["status"] == "not_available" and written["informational"] is True
    assert log.read_text().splitlines() == ["docker project2_postgres project2 marketing_analytics"], \
        "another RUN_MODE reaches no database at all"


def test_the_build_task_reports_after_the_build_with_its_settings_and_ignores_the_result():
    runner = re.sub(r"\\\n\s*", "", RUNNER.read_text(encoding="utf-8"))
    task = re.search(r"\n  build_warehouse\)\n(.*?)\n    ;;", runner, re.S).group(1)
    lines = [line.strip() for line in task.splitlines() if line.strip() and not line.strip().startswith("#")]
    removed = lines.index("rm -f logs/missing_dates_report.json")
    built = next(n for n, line in enumerate(lines) if line.startswith("env -u PGPORT bash scripts/run_warehouse_sql.sh "))
    reported = next(n for n, line in enumerate(lines) if "scripts/report_missing_dates.py" in line)
    assert removed < built < reported == len(lines) - 1
    assert lines[reported].startswith('env -u PGPORT "${PROJECT_PYTHON}" scripts/report_missing_dates.py || echo ')


def test_nothing_reads_the_report():
    skip = {"target", "dbt_packages", "logs", "__pycache__", ".terraform"}
    naming = set()
    for folder in ("sql", "spark-batch", "scripts", "dbt", "airflow", "data-generator", "tests/integration", ".github"):
        for path in (CODE_ROOT / folder).rglob("*"):
            if path.is_file() and not skip & set(path.relative_to(CODE_ROOT).parts) \
                    and path.suffix in {".py", ".sh", ".sql", ".yml", ".yaml", ".json", ".md"} \
                    and "missing_dates_report" in path.read_text(encoding="utf-8", errors="replace"):
                naming.add(path.relative_to(CODE_ROOT).as_posix())
    assert naming == {"scripts/report_missing_dates.py", "scripts/run_airflow_task.sh"}
