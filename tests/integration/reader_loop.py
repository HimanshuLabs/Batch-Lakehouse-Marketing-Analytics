#!/usr/bin/env python3
"""A consumer that keeps reading while the warehouse is rebuilt.

    python tests/integration/reader_loop.py --container C --stop-file F --out result.json

Connects as the read-only role p2_reader (one session, kept open, as a BI tool
would) and counts the rows of every mart and every dbt view, again and again,
until the stop file appears. A rebuild must be invisible to it:

  - no query may fail (a dropped view, a missing table, a lost grant),
  - no relation may ever be empty,
  - a relation may only show the row counts of whole generations: at most
    --max-states different counts (old data, new data), never a half-built one.

Writes what it saw to --out and exits 1 when any of these is broken, or when it
saw fewer than --min-generations warehouse generations (so a run that rebuilt
nothing cannot pass).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

READ_SCHEMAS = ("marts", "dbt_staging", "dbt_warehouse", "dbt_marts", "dbt_audit")
END = "__reader_loop_end__"


class Session:
    """One psql session of the role, inside the Postgres container."""

    def __init__(self, container: str, role: str):
        self.command = ["docker", "exec", "-i", container, "psql", "-X", "-q", "-At", "-F", "\t",
                        "-U", role, "-d", "marketing_analytics"]
        self.process = None
        self.connect()

    def connect(self) -> None:
        self.process = subprocess.Popen(self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, text=True, bufsize=1)

    def ask(self, query: str) -> tuple[list[str], str | None]:
        """The rows of one query, and the error text when it failed."""
        try:
            # psql reports each query's outcome in its own variables, on stdout,
            # so an error can never be mistaken for another query's rows.
            self.process.stdin.write(f"{query};\n\\echo {END} :SQLSTATE :LAST_ERROR_MESSAGE\n")
            self.process.stdin.flush()
            lines = []
            while True:
                line = self.process.stdout.readline()
                if line == "":
                    raise BrokenPipeError("the session ended")
                line = line.rstrip("\n")
                if line.startswith(END):
                    state, _, message = line[len(END):].strip().partition(" ")
                    break
                lines.append(line)
        except (BrokenPipeError, OSError) as exc:
            self.connect()
            return [], f"connection lost: {exc}"
        if state != "00000":
            return [], f"SQLSTATE {state}: {message}"
        if not lines:
            return [], "the query returned no row"
        return lines, None

    def close(self) -> None:
        try:
            self.process.stdin.close()
            self.process.wait(timeout=10)
        except Exception:  # noqa: BLE001
            self.process.kill()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--container", required=True)
    parser.add_argument("--role", default="p2_reader")
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-states", type=int, default=1,
                        help="different row counts a relation may show (1: every rebuild loads the same data)")
    parser.add_argument("--min-generations", type=int, default=3,
                        help="warehouse generations the loop must have seen (3: two rebuilds)")
    parser.add_argument("--pause", type=float, default=0.02, help="seconds between rounds")
    args = parser.parse_args()

    session = Session(args.container, args.role)
    schemas = ", ".join(f"'{name}'" for name in READ_SCHEMAS)
    listed, error = session.ask(
        "select table_schema || '.' || table_name from information_schema.tables "
        f"where table_schema in ({schemas}) order by 1")
    if error or not listed:
        print(f"reader loop: cannot list the marts and dbt views as {args.role}: {error or 'none found'}")
        return 2
    relations = listed
    print(f"reader loop: {args.role} reads {len(relations)} relations until {args.stop_file} appears", flush=True)

    counts: dict[str, list[int]] = {relation: [] for relation in relations}
    errors: list[dict] = []
    empties: list[dict] = []
    generations: list[str] = []
    rounds = queries = 0
    started = time.time()
    while not args.stop_file.exists():
        rows, error = session.ask("select to_regnamespace('warehouse')::oid")
        if not error and rows and rows[0] not in generations:
            generations.append(rows[0])
        for relation in relations:
            rows, error = session.ask(f"select count(*) from {relation}")
            queries += 1
            at = round(time.time() - started, 2)
            if error:
                errors.append({"relation": relation, "seconds": at, "error": error[:500]})
                continue
            count = int(rows[0])
            if count == 0:
                empties.append({"relation": relation, "seconds": at})
            if count not in counts[relation]:
                counts[relation].append(count)
        rounds += 1
        time.sleep(args.pause)
    session.close()

    mixed = {relation: seen for relation, seen in counts.items() if len(seen) > args.max_states}
    result = {
        "role": args.role, "relations": relations, "rounds": rounds, "queries": queries,
        "seconds": round(time.time() - started, 1), "warehouse_generations_seen": len(generations),
        "error_count": len(errors), "empty_count": len(empties),
        "relations_with_more_than_max_states": mixed, "row_counts_seen": counts,
        "errors": errors[:50], "empties": empties[:50],
    }
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")

    failures = []
    if errors:
        failures.append(f"{len(errors)} failed queries (first: {errors[0]['relation']}: {errors[0]['error'][:200]})")
    if empties:
        failures.append(f"{len(empties)} empty results (first: {empties[0]['relation']})")
    if mixed:
        failures.append(f"more than {args.max_states} row count(s) for {sorted(mixed)}: {mixed}")
    if len(generations) < args.min_generations:
        failures.append(f"saw {len(generations)} warehouse generation(s), expected at least {args.min_generations}")
    summary = (f"{rounds} rounds, {queries} queries over {len(relations)} relations in {result['seconds']} s, "
               f"{len(generations)} warehouse generations: {len(errors)} errors, {len(empties)} empty results")
    if failures:
        print(f"FAIL  reader loop: {summary}")
        for failure in failures:
            print(f"      {failure}")
        return 1
    print(f"PASS  reader loop: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
