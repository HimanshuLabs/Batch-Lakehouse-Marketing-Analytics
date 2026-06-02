#!/usr/bin/env python3
"""
Load lakehouse Gold/SCD2 Parquet outputs into PostgreSQL staging tables.

Contract:
- Source: data/gold/*
- Target: PostgreSQL staging.stg_gold_*
- Bronze/Silver are intentionally excluded.
- Staging tables are replaced by default for idempotent local runs.
- Audit results are written to audit.gold_to_staging_load_audit.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


@dataclass(frozen=True)
class SourceSpec:
    source_name: str
    target_table: str
    required: bool = True


GOLD_SOURCES: list[SourceSpec] = [
    SourceSpec("dim_customers", "stg_gold_dim_customers", True),
    SourceSpec("dim_products", "stg_gold_dim_products", True),
    SourceSpec("dim_campaigns", "stg_gold_dim_campaigns", True),
    SourceSpec("fact_orders", "stg_gold_fact_orders", True),
    SourceSpec("fact_order_items", "stg_gold_fact_order_items", True),
    SourceSpec("fact_ad_spend", "stg_gold_fact_ad_spend", True),
    SourceSpec("fact_web_events", "stg_gold_fact_web_events", True),
    SourceSpec("mart_campaign_performance", "stg_gold_mart_campaign_performance", True),
    SourceSpec("mart_product_performance", "stg_gold_mart_product_performance", True),
    SourceSpec("mart_customer_value", "stg_gold_mart_customer_value", True),
    SourceSpec("mart_marketing_funnel", "stg_gold_mart_marketing_funnel", True),
]

SCD2_SOURCES: list[SourceSpec] = [
    SourceSpec("dim_customers_scd2", "stg_gold_dim_customers_scd2", False),
    SourceSpec("dim_products_scd2", "stg_gold_dim_products_scd2", False),
    SourceSpec("dim_campaigns_scd2", "stg_gold_dim_campaigns_scd2", False),
    SourceSpec("fact_orders_scd2", "stg_gold_fact_orders_scd2", False),
    SourceSpec("fact_order_items_scd2", "stg_gold_fact_order_items_scd2", False),
    SourceSpec("fact_campaign_spend_scd2", "stg_gold_fact_campaign_spend_scd2", False),
]

ALL_SOURCES = GOLD_SOURCES + SCD2_SOURCES


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def validate_sql_identifier(identifier: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return identifier


def normalize_identifier(value: str) -> str:
    identifier = re.sub(r"[^A-Za-z0-9_]+", "_", value.strip().lower())
    identifier = re.sub(r"_+", "_", identifier).strip("_")

    if not identifier:
        identifier = "column"

    if identifier[0].isdigit():
        identifier = f"_{identifier}"

    return identifier


def dedupe_columns(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    output: list[str] = []

    for column in columns:
        base = normalize_identifier(str(column))
        seen[base] = seen.get(base, 0) + 1

        if seen[base] == 1:
            output.append(base)
        else:
            output.append(f"{base}_{seen[base]}")

    return output


def parquet_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob("*.parquet"))


def coerce_nested_objects(df: pd.DataFrame) -> pd.DataFrame:
    def convert(value: Any) -> Any:
        if isinstance(value, (dict, list, tuple, set)):
            return json.dumps(value, default=str, sort_keys=True)
        return value

    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].map(convert)

    return df


def add_loader_column(df: pd.DataFrame, column_name: str, value: Any) -> None:
    safe_name = column_name
    if safe_name in df.columns:
        safe_name = f"loader_{column_name}"
    df[safe_name] = value


def read_parquet_dataset(source_path: Path, load_id: str) -> pd.DataFrame:
    df = pd.read_parquet(source_path, engine="pyarrow")
    df.columns = dedupe_columns(list(df.columns))
    df = coerce_nested_objects(df)

    loaded_at = utc_now()
    add_loader_column(df, "warehouse_load_id", load_id)
    add_loader_column(df, "warehouse_loaded_at_utc", loaded_at)
    add_loader_column(df, "warehouse_source_path", str(source_path))

    return df


def build_engine() -> Engine:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url, future=True, pool_pre_ping=True)

    required_env = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER"]
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        missing_list = ", ".join(missing)
        raise RuntimeError(
            f"Missing PostgreSQL environment variables: {missing_list}. "
            "Create .env from .env.example or export the variables."
        )

    url = URL.create(
        "postgresql+psycopg",
        username=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD", ""),
        host=os.getenv("PGHOST"),
        port=int(os.getenv("PGPORT", "5432")),
        database=os.getenv("PGDATABASE"),
    )

    return create_engine(url, future=True, pool_pre_ping=True)


def create_support_objects(engine: Engine, target_schema: str) -> None:
    target_schema = validate_sql_identifier(target_schema)

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"'))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS audit.gold_to_staging_load_audit (
                    audit_id BIGSERIAL PRIMARY KEY,
                    load_id UUID NOT NULL,
                    source_name TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    target_schema TEXT NOT NULL DEFAULT 'staging',
                    target_table TEXT NOT NULL,
                    source_required BOOLEAN NOT NULL DEFAULT TRUE,
                    status TEXT NOT NULL CHECK (
                        status IN ('LOADED', 'MISSING', 'SKIPPED', 'FAILED', 'DRY_RUN')
                    ),
                    row_count BIGINT,
                    column_count INTEGER,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    error_message TEXT
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_gold_to_staging_load_audit_finished_at
                ON audit.gold_to_staging_load_audit (finished_at DESC)
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_gold_to_staging_load_audit_target
                ON audit.gold_to_staging_load_audit (
                    target_schema,
                    target_table,
                    finished_at DESC
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE OR REPLACE VIEW audit.v_gold_to_staging_latest_counts AS
                WITH ranked_loads AS (
                    SELECT
                        load_id,
                        source_name,
                        source_path,
                        target_schema,
                        target_table,
                        source_required,
                        status,
                        row_count,
                        column_count,
                        started_at,
                        finished_at,
                        error_message,
                        ROW_NUMBER() OVER (
                            PARTITION BY target_schema, target_table
                            ORDER BY finished_at DESC, audit_id DESC
                        ) AS row_rank
                    FROM audit.gold_to_staging_load_audit
                )
                SELECT
                    source_name,
                    source_path,
                    target_schema,
                    target_table,
                    source_required,
                    status,
                    row_count,
                    column_count,
                    started_at,
                    finished_at,
                    error_message
                FROM ranked_loads
                WHERE row_rank = 1
                ORDER BY target_schema, target_table
                """
            )
        )


def insert_audit_record(
    engine: Engine,
    *,
    load_id: str,
    source_name: str,
    source_path: Path,
    target_schema: str,
    target_table: str,
    source_required: bool,
    status: str,
    row_count: int | None,
    column_count: int | None,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO audit.gold_to_staging_load_audit (
                    load_id,
                    source_name,
                    source_path,
                    target_schema,
                    target_table,
                    source_required,
                    status,
                    row_count,
                    column_count,
                    started_at,
                    finished_at,
                    error_message
                )
                VALUES (
                    :load_id,
                    :source_name,
                    :source_path,
                    :target_schema,
                    :target_table,
                    :source_required,
                    :status,
                    :row_count,
                    :column_count,
                    :started_at,
                    :finished_at,
                    :error_message
                )
                """
            ),
            {
                "load_id": load_id,
                "source_name": source_name,
                "source_path": str(source_path),
                "target_schema": target_schema,
                "target_table": target_table,
                "source_required": source_required,
                "status": status,
                "row_count": row_count,
                "column_count": column_count,
                "started_at": started_at,
                "finished_at": finished_at,
                "error_message": error_message,
            },
        )


def print_summary(results: list[dict[str, Any]]) -> None:
    headers = [
        "status",
        "required",
        "source",
        "target",
        "rows",
        "columns",
        "parquet_files",
    ]

    rows = []
    for result in results:
        rows.append(
            [
                str(result.get("status", "")),
                str(result.get("required", "")),
                str(result.get("source", "")),
                str(result.get("target", "")),
                str(result.get("rows", "")),
                str(result.get("columns", "")),
                str(result.get("parquet_files", "")),
            ]
        )

    widths = [
        max(len(header), *(len(row[index]) for row in rows)) if rows else len(header)
        for index, header in enumerate(headers)
    ]

    print("\nGold/SCD2 → PostgreSQL staging load summary")
    print("-" * (sum(widths) + (3 * (len(widths) - 1))))

    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))

    for row in rows:
        print(" | ".join(row[index].ljust(widths[index]) for index in range(len(headers))))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load lakehouse Gold/SCD2 Parquet outputs into PostgreSQL staging tables."
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="Repository root. Default: current directory.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Environment file path. Default: .env",
    )
    parser.add_argument(
        "--gold-base-path",
        default=None,
        help="Gold base path. Default: GOLD_BASE_PATH env var or data/gold.",
    )
    parser.add_argument(
        "--target-schema",
        default=None,
        help="PostgreSQL target schema. Default: PGSCHEMA env var or staging.",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append"],
        default="replace",
        help="How to write staging tables. Default: replace.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail when required Gold outputs are missing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only inspect source folders. Do not connect to PostgreSQL or load tables.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    project_root = Path(args.project_root).resolve()
    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = project_root / env_path

    load_env_file(env_path)

    gold_base_path = Path(args.gold_base_path or os.getenv("GOLD_BASE_PATH", "data/gold"))
    if not gold_base_path.is_absolute():
        gold_base_path = project_root / gold_base_path

    target_schema = validate_sql_identifier(args.target_schema or os.getenv("PGSCHEMA", "staging"))
    load_id = str(uuid.uuid4())

    engine: Engine | None = None
    if not args.dry_run:
        engine = build_engine()
        create_support_objects(engine, target_schema)

    results: list[dict[str, Any]] = []

    for spec in ALL_SOURCES:
        started_at = utc_now()
        source_path = gold_base_path / spec.source_name
        parquet_files = parquet_file_count(source_path)

        if not source_path.exists() or parquet_files == 0:
            status = "MISSING" if spec.required else "SKIPPED"
            finished_at = utc_now()

            result = {
                "status": status,
                "required": spec.required,
                "source": spec.source_name,
                "target": f"{target_schema}.{spec.target_table}",
                "rows": "",
                "columns": "",
                "parquet_files": parquet_files,
                "error": "No Parquet files found.",
            }
            results.append(result)

            if engine is not None:
                insert_audit_record(
                    engine,
                    load_id=load_id,
                    source_name=spec.source_name,
                    source_path=source_path,
                    target_schema=target_schema,
                    target_table=spec.target_table,
                    source_required=spec.required,
                    status=status,
                    row_count=None,
                    column_count=None,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_message="No Parquet files found.",
                )

            continue

        if args.dry_run:
            results.append(
                {
                    "status": "DRY_RUN",
                    "required": spec.required,
                    "source": spec.source_name,
                    "target": f"{target_schema}.{spec.target_table}",
                    "rows": "",
                    "columns": "",
                    "parquet_files": parquet_files,
                }
            )
            continue

        try:
            df = read_parquet_dataset(source_path, load_id=load_id)
            row_count = int(len(df))
            column_count = int(len(df.columns))

            df.to_sql(
                name=spec.target_table,
                con=engine,
                schema=target_schema,
                if_exists=args.if_exists,
                index=False,
                chunksize=1_000,
                method=None,
            )

            finished_at = utc_now()

            insert_audit_record(
                engine,
                load_id=load_id,
                source_name=spec.source_name,
                source_path=source_path,
                target_schema=target_schema,
                target_table=spec.target_table,
                source_required=spec.required,
                status="LOADED",
                row_count=row_count,
                column_count=column_count,
                started_at=started_at,
                finished_at=finished_at,
                error_message=None,
            )

            results.append(
                {
                    "status": "LOADED",
                    "required": spec.required,
                    "source": spec.source_name,
                    "target": f"{target_schema}.{spec.target_table}",
                    "rows": row_count,
                    "columns": column_count,
                    "parquet_files": parquet_files,
                }
            )

        except Exception as exc:
            finished_at = utc_now()
            error_message = str(exc)[:4000]

            if engine is not None:
                insert_audit_record(
                    engine,
                    load_id=load_id,
                    source_name=spec.source_name,
                    source_path=source_path,
                    target_schema=target_schema,
                    target_table=spec.target_table,
                    source_required=spec.required,
                    status="FAILED",
                    row_count=None,
                    column_count=None,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_message=error_message,
                )

            results.append(
                {
                    "status": "FAILED",
                    "required": spec.required,
                    "source": spec.source_name,
                    "target": f"{target_schema}.{spec.target_table}",
                    "rows": "",
                    "columns": "",
                    "parquet_files": parquet_files,
                    "error": error_message,
                }
            )

    print_summary(results)

    failed = [result for result in results if result["status"] == "FAILED"]
    missing_required = [
        result for result in results
        if result["status"] == "MISSING" and result["required"] == "True"
    ]

    if failed:
        print("\nFAILED sources:")
        for result in failed:
            print(f"- {result['source']}: {result.get('error', '')}")
        return 1

    if args.strict and missing_required:
        print("\nSTRICT MODE FAILED: required Gold outputs are missing:")
        for result in missing_required:
            print(f"- {result['source']}")
        return 1

    if missing_required:
        print(
            "\nSome required Gold outputs are missing. Non-strict mode allowed the run. "
            "Use --strict once all required Gold outputs exist."
        )

    print(f"\nload_id={load_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
