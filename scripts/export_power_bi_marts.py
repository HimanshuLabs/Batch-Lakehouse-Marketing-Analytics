#!/usr/bin/env python3
"""
Export PostgreSQL BI marts to CSV files for Power BI Service / Power BI Online.

This script is intentionally thin:
- It reads only from PostgreSQL marts.*
- It writes CSV files under exports/power_bi/
- It writes exports/power_bi/export_manifest.json
- It does not touch Raw/Bronze/Silver/Gold lakehouse logic
- It does not rebuild warehouse/dbt/Airflow assets

Connection priority:
1. WAREHOUSE_DB_URL, if set
2. PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD

Default local values match the Project 2 warehouse container pattern:
- PGHOST=localhost
- PGPORT=5434
- PGDATABASE=marketing_analytics
- PGUSER=project2
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import psycopg2
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: psycopg2. Activate the project venv and install requirements first.\n"
        "Try:\n"
        "  source venv/bin/activate\n"
        "  python -m pip install psycopg2-binary\n"
    ) from exc


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "exports" / "power_bi"
MANIFEST_FILE = DEFAULT_EXPORT_DIR / "export_manifest.json"


@dataclass(frozen=True)
class MartExport:
    table_name: str
    csv_file_name: str
    order_by: str


MART_EXPORTS: list[MartExport] = [
    MartExport(
        table_name="marts.mart_revenue_daily",
        csv_file_name="revenue_daily.csv",
        order_by="1",
    ),
    MartExport(
        table_name="marts.mart_campaign_performance",
        csv_file_name="campaign_performance.csv",
        order_by="1",
    ),
    MartExport(
        table_name="marts.mart_product_sales",
        csv_file_name="product_sales.csv",
        order_by="1",
    ),
    MartExport(
        table_name="marts.mart_customer_360",
        csv_file_name="customer_360.csv",
        order_by="1",
    ),
    MartExport(
        table_name="marts.mart_marketing_funnel",
        csv_file_name="marketing_funnel.csv",
        order_by="1",
    ),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_connection():
    db_url = os.getenv("WAREHOUSE_DB_URL")
    if db_url:
        return psycopg2.connect(db_url)

    connection_args = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5434"),
        "dbname": os.getenv("PGDATABASE", "marketing_analytics"),
        "user": os.getenv("PGUSER", "project2"),
        "password": os.getenv("PGPASSWORD", ""),
    }

    return psycopg2.connect(**connection_args)


def validate_table_name(table_name: str) -> None:
    allowed = {export.table_name for export in MART_EXPORTS}
    if table_name not in allowed:
        raise ValueError(f"Unsafe or unsupported table name: {table_name}")


def export_mart(cursor, mart: MartExport, export_dir: Path, allow_empty: bool) -> dict[str, Any]:
    validate_table_name(mart.table_name)

    exported_at = utc_now_iso()
    csv_path = export_dir / mart.csv_file_name

    manifest_record: dict[str, Any] = {
        "table_name": mart.table_name,
        "csv_file_path": str(csv_path.relative_to(PROJECT_ROOT)),
        "row_count": 0,
        "exported_at": exported_at,
        "status": "STARTED",
    }

    query = f"SELECT * FROM {mart.table_name} ORDER BY {mart.order_by};"

    try:
        cursor.execute(query)
        column_names = [description[0] for description in cursor.description]

        row_count = 0
        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(column_names)

            for row in cursor:
                writer.writerow(row)
                row_count += 1

        manifest_record["row_count"] = row_count

        if row_count == 0 and not allow_empty:
            manifest_record["status"] = "FAILED_EMPTY"
            manifest_record["error"] = "Export produced zero data rows."
        else:
            manifest_record["status"] = "SUCCESS"

    except Exception as exc:  # noqa: BLE001
        manifest_record["status"] = "FAILED"
        manifest_record["error"] = str(exc)

    return manifest_record


def write_manifest(records: list[dict[str, Any]], export_dir: Path) -> Path:
    manifest_path = export_dir / "export_manifest.json"

    payload = {
        "generated_at": utc_now_iso(),
        "export_type": "power_bi_service_csv_demo",
        "source_system": "postgresql_warehouse_marts",
        "records": records,
    }

    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        json.dump(payload, manifest_file, indent=2)

    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL marts to CSV files for Power BI Service."
    )
    parser.add_argument(
        "--export-dir",
        default=str(DEFAULT_EXPORT_DIR),
        help="Directory where Power BI CSV exports will be written.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow zero-row CSV exports without failing the script.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    export_dir = Path(args.export_dir).resolve()
    export_dir.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, Any]] = []

    print("Power BI mart export starting")
    print(f"Export directory: {export_dir}")

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                for mart in MART_EXPORTS:
                    print(f"Exporting {mart.table_name} -> {mart.csv_file_name}")
                    record = export_mart(
                        cursor=cursor,
                        mart=mart,
                        export_dir=export_dir,
                        allow_empty=args.allow_empty,
                    )
                    records.append(record)
                    print(
                        f"  status={record['status']} "
                        f"rows={record['row_count']} "
                        f"path={record['csv_file_path']}"
                    )

    except Exception as exc:  # noqa: BLE001
        failure_record = {
            "table_name": "__connection__",
            "csv_file_path": "",
            "row_count": 0,
            "exported_at": utc_now_iso(),
            "status": "FAILED",
            "error": str(exc),
        }
        records.append(failure_record)
        print(f"Connection/export failure: {exc}", file=sys.stderr)

    manifest_path = write_manifest(records, export_dir)
    print(f"Manifest written: {manifest_path}")

    failed_records = [record for record in records if record["status"] != "SUCCESS"]
    if failed_records:
        print("One or more exports failed:", file=sys.stderr)
        for record in failed_records:
            print(
                f"  {record['table_name']}: {record['status']} "
                f"{record.get('error', '')}",
                file=sys.stderr,
            )
        return 1

    print("Power BI mart export completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
