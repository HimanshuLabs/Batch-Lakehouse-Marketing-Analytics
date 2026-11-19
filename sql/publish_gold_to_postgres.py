from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

POSTGRES_USER = os.getenv("P2_POSTGRES_USER", "project2")
POSTGRES_PASSWORD = os.getenv("P2_POSTGRES_PASSWORD", "project2")
POSTGRES_HOST = os.getenv("P2_POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("P2_POSTGRES_PORT", "5434")
POSTGRES_DB = os.getenv("P2_POSTGRES_DB", "marketing_analytics")
POSTGRES_SCHEMA = os.getenv("P2_POSTGRES_SCHEMA", "gold")

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


# A Gold table is read and published in chunks of about this many rows (whole
# DuckDB vectors of 2,048 rows), all in one transaction, so a large table never
# has to fit in memory at once. A table up to this size is one chunk.
def chunk_rows_setting(value: str | None) -> int:
    """The rows per chunk P2_LOAD_SLICE_ROWS asks for (unset or empty: 200,000)."""
    if not value:
        return 200_000
    try:
        rows = int(value)
    except ValueError:
        raise SystemExit(f"P2_LOAD_SLICE_ROWS must be a whole number, got {value!r}") from None
    if rows < 1:
        raise SystemExit(f"P2_LOAD_SLICE_ROWS must be at least 1, got {rows}")
    return rows


PUBLISH_CHUNK_ROWS = chunk_rows_setting(os.getenv("P2_LOAD_SLICE_ROWS"))
DUCKDB_VECTOR_ROWS = 2048

# DuckDB types that reach pandas with a dtype of their own; pandas derives the
# SQL type from that dtype, the same for every chunk. Any other column (text,
# time of day, ...) reaches pandas as objects, whose SQL type pandas infers
# from the values, so a chunk without values would get another type.
TYPED_DUCKDB_TYPES = ("BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
                      "UINTEGER", "UBIGINT", "UHUGEINT", "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIMESTAMP",
                      "INTERVAL")
TIME_OF_DAY_TYPES = ("TIME", "TIME WITH TIME ZONE", "TIME_NS")


def gold_table_query(parquet_glob: Path) -> str:
    """The one query that reads a Gold table, whole or in chunks."""
    return f"SELECT * FROM read_parquet('{parquet_glob.as_posix()}')"


def read_gold_table(parquet_glob: Path) -> pd.DataFrame:
    return duckdb.sql(gold_table_query(parquet_glob)).df()


def gold_table_sql_types(parquet_glob: Path) -> dict:
    """The SQL type of every column that reaches pandas as objects, decided once from the whole table.

    It is what pandas infers for the whole column (pandas.io.sql): a time of
    day with at least one value is TIME, everything else (text, a column
    without any value) is TEXT.
    """
    from sqlalchemy.types import Text, Time

    connection = duckdb.connect()
    try:
        relation = connection.sql(gold_table_query(parquet_glob))
        untyped = [(name, str(kind)) for name, kind in zip(relation.columns, relation.types)
                   if not str(kind).startswith(TYPED_DUCKDB_TYPES) or str(kind) in TIME_OF_DAY_TYPES]
        times = [name for name, kind in untyped if kind in TIME_OF_DAY_TYPES]
        filled = {}
        if times:
            counts = connection.sql(
                "SELECT " + ", ".join(f'count("{name}")' for name in times)
                + f" FROM ({gold_table_query(parquet_glob)})"
            ).fetchone()
            filled = dict(zip(times, counts))
        return {name: Time if filled.get(name, 0) > 0 else Text for name, _ in untyped}
    finally:
        connection.close()


def read_gold_table_chunks(parquet_glob: Path, chunk_rows: int | None = None):
    """The table as DataFrames of whole DuckDB vectors, converted as read_gold_table converts it; at least one."""
    chunk_rows = chunk_rows or PUBLISH_CHUNK_ROWS
    if chunk_rows < 1:
        raise ValueError(f"P2_LOAD_SLICE_ROWS must be at least 1, got {chunk_rows}")
    vectors = -(-chunk_rows // DUCKDB_VECTOR_ROWS)
    connection = duckdb.connect()
    try:
        connection.execute(gold_table_query(parquet_glob))
        first = True
        while True:
            chunk = connection.fetch_df_chunk(vectors)
            if len(chunk) == 0 and not first:
                return
            yield chunk   # an empty first chunk still creates the (empty) table
            if len(chunk) == 0:
                return
            first = False
    finally:
        connection.close()


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


def publish_table(engine, table_name: str, chunks, sql_types: dict | None = None) -> int:
    """The chunks as one table, replaced in one transaction: the first chunk creates it, the others append.

    sql_types (gold_table_sql_types) fixes the types pandas would otherwise infer per chunk.
    """
    rows = 0
    with engine.begin() as connection:
        for position, df in enumerate(chunks):
            df = prepare_dataframe_for_postgres(df)

            df.to_sql(
                name=table_name,
                con=connection,
                schema=POSTGRES_SCHEMA,
                if_exists="replace" if position == 0 else "append",
                index=False,
                method="multi",
                chunksize=1000,
                dtype=sql_types or None,
            )
            rows += len(df)

    return rows


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
        row_count = publish_table(engine, table_name, read_gold_table_chunks(parquet_glob),
                                  gold_table_sql_types(parquet_glob))

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


