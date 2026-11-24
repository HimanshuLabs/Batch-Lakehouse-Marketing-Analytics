"""Business dates without orders or web events, between the first and the last: a report, never a failure.

Run at the end of the build_warehouse task (scripts/run_airflow_task.sh), after
the build has committed. It reads warehouse.fact_orders and
warehouse.fact_web_events in a read-only session and writes
logs/missing_dates_report.json. Nothing reads that file. A gap is not an
error (two batches more than FACT_WINDOW_DAYS apart leave one), so the script
always exits 0, whatever happens, and never changes the task's result.

It reads the database the build wrote, through the same route and the same
settings as scripts/run_warehouse_sql.sh: RUN_MODE (docker or host),
PGCONTAINER, PGHOST, PGPORT, PGDATABASE and PGUSER, with that script's
defaults, and psql started by bash as there. The task runs both with PGPORT
unset. Without that route (no docker, no container, no psql, another
RUN_MODE) the report says "not checked".
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT") or Path(__file__).resolve().parents[1])
REPORT_PATH = PROJECT_ROOT / "logs" / "missing_dates_report.json"
FEEDS = ("orders", "web_events")
# Business dates (Asia/Kolkata) as the warehouse keys them; 0 is the unknown date.
QUERY = """
SELECT 'orders', order_date_sk FROM warehouse.fact_orders WHERE order_date_sk <> 0 GROUP BY order_date_sk
UNION ALL
SELECT 'web_events', event_date_sk FROM warehouse.fact_web_events WHERE event_date_sk <> 0 GROUP BY event_date_sk;
"""
ROW = re.compile(r"^(orders|web_events)\|(\d{8})$")
# A read-only session that cannot wait long for anything.
SESSION_OPTIONS = "-c default_transaction_read_only=on -c statement_timeout=600000 -c lock_timeout=10000"
TIMEOUT_SECONDS = 660


def warehouse_route(environ: dict[str, str]) -> dict[str, str]:
    """Where scripts/run_warehouse_sql.sh sends its SQL, with the same settings and defaults."""
    route = {
        "run_mode": environ.get("RUN_MODE") or "docker",
        "database": environ.get("PGDATABASE") or "marketing_analytics",
        "user": environ.get("PGUSER") or "project2",
    }
    if route["run_mode"] == "docker":
        route["container"] = environ.get("PGCONTAINER") or "project2_postgres"
    elif route["run_mode"] == "host":
        route["host"] = environ.get("PGHOST") or "localhost"
        route["port"] = environ.get("PGPORT") or "5434"
    else:
        raise ValueError("RUN_MODE must be either 'docker' or 'host'")
    return route


def psql_command(route: dict[str, str]) -> list[str]:
    """psql on that route, started by bash as the build's is (so docker and psql are found the same way)."""
    psql = "psql -X -q -A -t -v ON_ERROR_STOP=1"
    if route["run_mode"] == "docker":
        return ["bash", "-c", f'exec docker exec -i -e "PGOPTIONS=$1" "$2" {psql} -U "$3" -d "$4"', "report",
                SESSION_OPTIONS, route["container"], route["user"], route["database"]]
    return ["bash", "-c", f'exec {psql} --host "$1" --port "$2" --dbname "$3" --username "$4"', "report",
            route["host"], route["port"], route["database"], route["user"]]


def describe_route(route: dict[str, str]) -> str:
    if route["run_mode"] == "docker":
        return f"container {route['container']}, database {route['database']}"
    return f"{route['host']}:{route['port']}, database {route['database']}"


def day_of(date_sk: int) -> date:
    return date(date_sk // 10000, date_sk // 100 % 100, date_sk % 100)


def missing_ranges(present: set[date], first: date, last: date) -> list[dict]:
    """Each run of consecutive dates from first to last that is not in present."""
    ranges, start, day = [], None, first
    while day <= last + timedelta(days=1):
        if day <= last and day not in present:
            start = start or day
        elif start:
            end = day - timedelta(days=1)
            ranges.append({"from": start.isoformat(), "to": end.isoformat(), "days": (end - start).days + 1})
            start = None
        day += timedelta(days=1)
    return ranges


def build_report(dates: dict[str, set[date]]) -> dict:
    every = set().union(*dates.values())
    if not every:
        return {"status": "no_dates"}
    first, last = min(every), max(every)
    report = {"status": "ok", "first_date": first.isoformat(), "last_date": last.isoformat(),
              "days": (last - first).days + 1}
    for feed in FEEDS:
        ranges = missing_ranges(dates[feed], first, last)
        report[feed] = {"missing_ranges": ranges, "missing_days": sum(r["days"] for r in ranges)}
    return report


def read_dates(route: dict[str, str]) -> dict[str, set[date]]:
    env = {**os.environ, "PGOPTIONS": SESSION_OPTIONS, "PGCONNECT_TIMEOUT": "10"}
    result = subprocess.run(psql_command(route), input=QUERY, capture_output=True, text=True,
                            timeout=TIMEOUT_SECONDS, env=env)
    if result.returncode != 0:
        first_line = (result.stderr.strip().splitlines() or [""])[0][:200]
        raise RuntimeError(f"psql exited {result.returncode}: {first_line}")
    dates: dict[str, set[date]] = {feed: set() for feed in FEEDS}
    for line in result.stdout.splitlines():
        match = ROW.match(line.strip())
        if match:
            dates[match.group(1)].add(day_of(int(match.group(2))))
    return dates


def describe(report: dict) -> list[str]:
    title = "MISSING DATES (a report; it never fails a task):"
    if report["status"] != "ok":
        return [f"{title} not checked ({report.get('error', report['status'])})"]
    lines = [f"{title} business dates {report['first_date']} to {report['last_date']} ({report['days']} days)"]
    for feed, label in (("orders", "without orders:    "), ("web_events", "without web events:")):
        ranges = report[feed]["missing_ranges"]
        lines.append(f"  {label} " + ("none" if not ranges else ", ".join(
            f"{r['from']} to {r['to']} ({r['days']} days)" if r["days"] > 1 else f"{r['from']} (1 day)"
            for r in ranges)))
    return lines


def main() -> int:
    source = "not resolved"
    try:
        route = warehouse_route(dict(os.environ))
        source = describe_route(route)
        report = build_report(read_dates(route))
    except Exception as error:   # a report, never a failure
        message = str(error).splitlines()[0] if str(error) else ""
        report = {"status": "not_available", "error": f"{type(error).__name__}: {message}".rstrip(": ")}
    report = {"report": "missing_business_dates", "informational": True,
              "batch_date": os.environ.get("BATCH_DATE"), "run_id": os.environ.get("P2_RUN_ID"),
              "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "read_from": source,
              "source": "warehouse.fact_orders.order_date_sk, warehouse.fact_web_events.event_date_sk", **report}
    for line in describe(report):
        print(line)
    print(f"  read from {source}")
    try:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"  written to {REPORT_PATH.relative_to(PROJECT_ROOT)}")
    except (OSError, ValueError) as error:
        print(f"  not written ({type(error).__name__})")
    return 0


if __name__ == "__main__":
    try:
        main()
    finally:
        sys.exit(0)
