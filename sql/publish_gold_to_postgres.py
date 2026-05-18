from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

POSTGRES_USER = "project2"
POSTGRES_PASSWORD = "project2"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5434"
POSTGRES_DB = "marketing_analytics"
POSTGRES_SCHEMA = "gold"

DATABASE_URL = (
    f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

GOLD_TABLES = {
    "dim_customers": GOLD_DIR / "dim_customers" / "**" / "*.parquet",
    "dim_products": GOLD_DIR / "dim_products" / "**" / "*.parquet",
    "dim_campaigns": GOLD_DIR / "dim_campaigns" / "**" / "*.parquet",
    "fact_orders": GOLD_DIR / "fact_orders" / "**" / "*.parquet",
    "fact_order_items": GOLD_DIR / "fact_order_items" / "**" / "*.parquet",
    "fact_ad_spend": GOLD_DIR / "fact_ad_spend" / "**" / "*.parquet",
    "fact_web_events": GOLD_DIR / "fact_web_events" / "**" / "*.parquet",
    "mart_campaign_performance": GOLD_DIR / "mart_campaign_performance" / "**" / "*.parquet",
    "mart_product_performance": GOLD_DIR / "mart_product_performance" / "**" / "*.parquet",
    "mart_customer_value": GOLD_DIR / "mart_customer_value" / "**" / "*.parquet",
    "mart_marketing_funnel": GOLD_DIR / "mart_marketing_funnel" / "**" / "*.parquet",
}


def ensure_gold_files_exist() -> None:
    missing_tables = []

    for table_name, parquet_glob in GOLD_TABLES.items():
        table_dir = GOLD_DIR / table_name
        files = list(table_dir.glob("**/*.parquet"))

        if not files:
            missing_tables.append(f"{table_name}: {parquet_glob}")

    if missing_tables:
        missing_text = "\n".join(missing_tables)
        raise FileNotFoundError(f"Missing Gold parquet files:\n{missing_text}")


def read_gold_table(parquet_glob: Path) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_glob.as_posix()}');
    """

    return duckdb.sql(query).df()


def prepare_dataframe_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()

    for column_name in cleaned.columns:
        if cleaned[column_name].dtype == "object":
            cleaned[column_name] = cleaned[column_name].where(
                cleaned[column_name].notna(),
                None,
            )

    return cleaned


def create_schema(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA};"))


def publish_table(engine, table_name: str, df: pd.DataFrame) -> int:
    df = prepare_dataframe_for_postgres(df)

    df.to_sql(
        name=table_name,
        con=engine,
        schema=POSTGRES_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    return len(df)


def write_publish_audit(engine, audit_rows: list[dict]) -> None:
    audit_df = pd.DataFrame(audit_rows)

    audit_df.to_sql(
        name="gold_publish_audit",
        con=engine,
        schema=POSTGRES_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )


def main() -> None:
    ensure_gold_files_exist()

    engine = create_engine(DATABASE_URL)

    print("Starting Gold publish to PostgreSQL")
    print(f"Database: {POSTGRES_DB}")
    print(f"Schema: {POSTGRES_SCHEMA}")
    print(f"Gold directory: {GOLD_DIR}")

    create_schema(engine)

    audit_rows = []

    for table_name, parquet_glob in GOLD_TABLES.items():
        df = read_gold_table(parquet_glob)
        row_count = publish_table(engine, table_name, df)

        audit_rows.append(
            {
                "table_name": table_name,
                "row_count": row_count,
                "published_at": datetime.now().isoformat(timespec="seconds"),
                "postgres_schema": POSTGRES_SCHEMA,
                "source_path": parquet_glob.as_posix(),
            }
        )

        print(f"{table_name}: published rows={row_count}")

    write_publish_audit(engine, audit_rows)

    print("gold_publish_audit: published")
    print("Gold publish to PostgreSQL completed successfully.")


if __name__ == "__main__":
    main()


