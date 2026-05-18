from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.today().isoformat()

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
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def count_raw_file_lines(path: Path, has_header: bool) -> int:
    if not path.exists():
        raise FileNotFoundError(f"Missing raw file: {path}")

    with path.open("r", encoding="utf-8") as file:
        line_count = sum(1 for _ in file)

    return line_count - 1 if has_header else line_count


def validate_table(
    spark: SparkSession,
    table_name: str,
    raw_file: Path,
    bronze_path: Path,
    raw_has_header: bool,
    expected_bronze_columns: int | None = None,
) -> dict:
    if not bronze_path.exists():
        return {
            "table_name": table_name,
            "status": "FAILED",
            "raw_row_count": None,
            "bronze_row_count": None,
            "bronze_column_count": None,
            "missing_metadata_columns": "ALL",
            "failure_reason": f"Missing Bronze path: {bronze_path}",
        }

    raw_row_count = count_raw_file_lines(raw_file, raw_has_header)

    bronze_df = spark.read.parquet(str(bronze_path))
    bronze_row_count = bronze_df.count()
    bronze_columns = set(bronze_df.columns)
    missing_metadata = sorted(BRONZE_METADATA_COLUMNS - bronze_columns)

    failure_reasons = []

    if raw_row_count <= 0:
        failure_reasons.append("raw row count is zero")

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