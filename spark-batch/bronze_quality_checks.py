from __future__ import annotations

import csv
import os
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()

BRONZE_METADATA_COLUMNS = {
    "ingestion_timestamp",
    "ingestion_date",
    "batch_id",
    "source_system",
    "source_file_name",
    "record_hash",
}

TABLES = {
    "customers": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "customers.csv",
        "bronze_path": BRONZE_BASE_DIR / "bronze_customers",
        "raw_has_header": True,
    },
    "products": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "products.csv",
        "bronze_path": BRONZE_BASE_DIR / "bronze_products",
        "raw_has_header": True,
    },
    "campaigns": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "campaigns.csv",
        "bronze_path": BRONZE_BASE_DIR / "bronze_campaigns",
        "raw_has_header": True,
    },
    "orders": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "orders.csv",
        "bronze_path": BRONZE_BASE_DIR / "bronze_orders",
        "raw_has_header": True,
    },
    "order_items": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "order_items.csv",
        "bronze_path": BRONZE_BASE_DIR / "bronze_order_items",
        "raw_has_header": True,
    },
    "ad_spend": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "ad_spend.csv",
        "bronze_path": BRONZE_BASE_DIR / "bronze_ad_spend",
        "raw_has_header": True,
    },
    "web_events": {
        "raw_file": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "web_events.json",
        "bronze_path": BRONZE_BASE_DIR / "bronze_web_events",
        "raw_has_header": False,
        "expected_bronze_columns": 65,
    },
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2_Bronze_Quality_Checks")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def count_raw_file_lines(path: Path, has_header: bool) -> int:
    """Rows in a raw file: CSV records for a CSV with a header (a quoted field
    can span lines, like Bronze reads it), lines for JSON lines."""
    if not path.exists():
        raise FileNotFoundError(f"Missing raw file: {path}")

    if has_header:
        with path.open("r", encoding="utf-8", newline="") as file:
            record_count = sum(1 for _ in csv.reader(file))
        return record_count - 1

    with path.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def validate_table(
    spark: SparkSession,
    table_name: str,
    raw_file: Path,
    bronze_path: Path,
    raw_has_header: bool,
    expected_bronze_columns: int | None = None,
) -> dict:
    raw_row_count = count_raw_file_lines(raw_file, raw_has_header)

    if raw_row_count == 0:
        # An empty batch: valid when Bronze also has nothing for this batch date
        # (bronze_ingestion writes no partition for it).
        bronze_row_count = 0
        if bronze_path.exists():
            bronze_row_count = (
                spark.read.parquet(str(bronze_path))
                .filter(col("ingestion_date") == lit(BATCH_DATE).cast("date"))
                .count()
            )
        return {
            "table_name": table_name,
            "status": "PASSED" if bronze_row_count == 0 else "FAILED",
            "raw_row_count": 0,
            "bronze_row_count": bronze_row_count,
            "bronze_column_count": 0,
            "missing_metadata_columns": "",
            "failure_reason": "" if bronze_row_count == 0 else (
                f"the raw file is empty but Bronze has {bronze_row_count} row(s) for ingestion_date={BATCH_DATE}"
            ),
        }

    if not bronze_path.exists():
        return {
            "table_name": table_name,
            "status": "FAILED",
            "raw_row_count": raw_row_count,
            "bronze_row_count": 0,
            "bronze_column_count": 0,
            "missing_metadata_columns": "ALL",
            "failure_reason": f"Missing Bronze path: {bronze_path}",
        }

    # Read the table root (keeps the ingestion_date partition column) and
    # check only this run's partition; earlier batches stay in Bronze.
    bronze_df = (
        spark.read.parquet(str(bronze_path))
        .filter(col("ingestion_date") == lit(BATCH_DATE).cast("date"))
    )
    bronze_row_count = bronze_df.count()
    bronze_columns = set(bronze_df.columns)
    missing_metadata = sorted(BRONZE_METADATA_COLUMNS - bronze_columns)

    failure_reasons = []

    if "batch_id" in bronze_columns:
        batch_id_count = bronze_df.select("batch_id").distinct().count()
        if batch_id_count != 1:
            failure_reasons.append(
                f"expected exactly one batch_id for ingestion_date={BATCH_DATE}, found {batch_id_count}"
            )

    if bronze_row_count <= 0:
        failure_reasons.append("bronze row count is zero")

    if raw_row_count != bronze_row_count:
        failure_reasons.append(
            f"row count mismatch raw={raw_row_count}, bronze={bronze_row_count}"
        )

    if missing_metadata:
        failure_reasons.append(f"missing metadata columns: {missing_metadata}")

    if expected_bronze_columns is not None and len(bronze_df.columns) != expected_bronze_columns:
        failure_reasons.append(
            f"column count mismatch expected={expected_bronze_columns}, actual={len(bronze_df.columns)}"
        )

    status = "PASSED" if not failure_reasons else "FAILED"

    return {
        "table_name": table_name,
        "status": status,
        "raw_row_count": raw_row_count,
        "bronze_row_count": bronze_row_count,
        "bronze_column_count": len(bronze_df.columns),
        "missing_metadata_columns": ",".join(missing_metadata) if missing_metadata else "",
        "failure_reason": "; ".join(failure_reasons),
    }


def main() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    print(f"spark.sql.session.timeZone={spark.conf.get('spark.sql.session.timeZone')}")
    results = []

    for table_name, config in TABLES.items():
        result = validate_table(
            spark=spark,
            table_name=table_name,
            raw_file=config["raw_file"],
            bronze_path=config["bronze_path"],
            raw_has_header=config["raw_has_header"],
            expected_bronze_columns=config.get("expected_bronze_columns"),
        )
        results.append(result)

        print(
            f"{result['table_name']}: {result['status']} "
            f"raw={result['raw_row_count']} "
            f"bronze={result['bronze_row_count']} "
            f"columns={result['bronze_column_count']}"
        )

        if result["failure_reason"]:
            print(f"  reason: {result['failure_reason']}")

    report_df = spark.createDataFrame(results)
    report_path = LOGS_DIR / f"bronze_quality_report_{BATCH_DATE}"

    (
        report_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(report_path))
    )

    failed = [result for result in results if result["status"] == "FAILED"]

    print(f"Bronze quality report written to: {report_path}")

    spark.stop()

    if failed:
        raise RuntimeError(f"Bronze quality checks failed for {len(failed)} table(s).")

    print("All Bronze quality checks passed.")


if __name__ == "__main__":
    main()