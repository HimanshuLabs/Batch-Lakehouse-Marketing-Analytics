"""Fingerprint the lake: one line per table with its row count and an md5 of its rows.

Only the run-time columns listed below are left out, so two runs over the same
raw data must print identical lines. Read-only.

    python tests/integration/fingerprint.py <project root>
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import duckdb

# Columns that record when or as which run a job wrote the row, not what the
# data says; they differ on every run. Every other column is fingerprinted.
RUN_TIME_COLUMNS = {
    "ingestion_timestamp",       # bronze_ingestion: current_timestamp() at ingestion
    "batch_id",                  # bronze_ingestion: batch_<date>_<uuid4> per run
    "processed_timestamp",       # silver_transformations: current_timestamp()
    "gold_processed_timestamp",  # gold_transformations: current_timestamp()
    "built_at",                  # scd2_gold_marts: current_timestamp()
}
# scd2_dimensions stamps created_at / updated_at with current_timestamp(); in
# every other table updated_at is the source record's change time and is kept.
RUN_TIME_COLUMNS_BY_TABLE = {
    "gold/dim_customers_scd2": {"created_at", "updated_at"},
    "gold/dim_products_scd2": {"created_at", "updated_at"},
    "gold/dim_campaigns_scd2": {"created_at", "updated_at"},
}


def is_run_time(table: str, column: str) -> bool:
    return column in RUN_TIME_COLUMNS or column in RUN_TIME_COLUMNS_BY_TABLE.get(table, set())


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    data = Path(sys.argv[1]).resolve() / "data"
    connection = duckdb.connect()
    for layer in ("bronze", "silver", "quarantine", "gold"):
        for table in sorted(p for p in (data / layer).iterdir() if p.is_dir()):
            if not any(table.rglob("*.parquet")):
                print(f"{layer}/{table.name}\t0\t-")
                continue
            source = f"read_parquet('{table}/**/*.parquet', hive_partitioning=false, union_by_name=true)"
            name = f"{layer}/{table.name}"
            columns = [row[0] for row in connection.sql(f"describe select * from {source}").fetchall()
                       if not is_run_time(name, row[0])]
            # Every column is rendered, NULL as an explicit marker, joined with
            # the ASCII unit separator: a value that moves into a neighbouring
            # NULL column changes the row string.
            rendered = ", ".join(f"coalesce(cast(\"{c}\" as varchar), '<NULL>')" for c in sorted(columns))
            rows = connection.sql(f"select concat_ws(chr(31), {rendered}) r from {source} order by r").fetchall()
            digest = hashlib.md5("\n".join(r[0] or "" for r in rows).encode()).hexdigest()
            print(f"{layer}/{table.name}\t{len(rows)}\t{digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
