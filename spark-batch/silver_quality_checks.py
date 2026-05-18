from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_BASE_DIR = PROJECT_ROOT / "data" / "silver"
QUARANTINE_BASE_DIR = PROJECT_ROOT / "data" / "quarantine"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.today().isoformat()


TABLES = {
    "customers": {
        "bronze": BRONZE_BASE_DIR / "bronze_customers",
        "silver": SILVER_BASE_DIR / "silver_customers",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_customers",
        "required_columns": ["user_id", "email", "age", "processed_date", "silver_record_hash"],
        "not_null_columns": ["user_id", "email", "age"],
    },
    "products": {
        "bronze": BRONZE_BASE_DIR / "bronze_products",
        "silver": SILVER_BASE_DIR / "silver_products",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_products",
        "required_columns": ["product_id", "product_name", "category", "processed_date", "silver_record_hash"],
        "not_null_columns": ["product_id", "product_name", "category"],
    },
    "campaigns": {
        "bronze": BRONZE_BASE_DIR / "bronze_campaigns",
        "silver": SILVER_BASE_DIR / "silver_campaigns",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_campaigns",
        "required_columns": ["campaign_id", "campaign_name", "traffic_source", "processed_date", "silver_record_hash"],
        "not_null_columns": ["campaign_id", "campaign_name", "traffic_source"],
    },
    "orders": {
        "bronze": BRONZE_BASE_DIR / "bronze_orders",
        "silver": SILVER_BASE_DIR / "silver_orders",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_orders",
        "required_columns": ["order_id", "user_id", "order_timestamp", "cart_value", "processed_date", "silver_record_hash"],
        "not_null_columns": ["order_id", "user_id", "order_timestamp", "cart_value"],
    },
    "order_items": {
        "bronze": BRONZE_BASE_DIR / "bronze_order_items",
        "silver": SILVER_BASE_DIR / "silver_order_items",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_order_items",
        "required_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_amount", "processed_date", "silver_record_hash"],
        "not_null_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_amount"],
    },
    "ad_spend": {
        "bronze": BRONZE_BASE_DIR / "bronze_ad_spend",
        "silver": SILVER_BASE_DIR / "silver_ad_spend",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_ad_spend",
        "required_columns": ["spend_id", "campaign_id", "spend_date", "spend_amount", "processed_date", "silver_record_hash"],
        "not_null_columns": ["spend_id", "campaign_id", "spend_date", "spend_amount"],
    },
    "web_events": {
        "bronze": BRONZE_BASE_DIR / "bronze_web_events",
        "silver": SILVER_BASE_DIR / "silver_web_events",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_web_events",
        "required_columns": ["event_id", "session_id", "user_id", "event_type", "event_timestamp", "processed_date", "silver_record_hash"],
        "not_null_columns": ["event_id", "session_id", "user_id", "event_type", "event_timestamp"],
        "minimum_silver_rows": 7000,
    },
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2_Silver_Quality_Checks")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def read_parquet_if_exists(spark: SparkSession, path: Path) -> DataFrame | None:
    if not path.exists():
        return None

    return spark.read.parquet(str(path))


def count_df(df: DataFrame | None) -> int:
    if df is None:
        return 0

    return df.count()


def missing_columns(df: DataFrame | None, required_columns: list[str]) -> list[str]:
    if df is None:
        return required_columns

    existing_columns = set(df.columns)
    return sorted([column for column in required_columns if column not in existing_columns])


def count_nulls(df: DataFrame, columns: list[str]) -> dict[str, int]:
    result = {}

    for column_name in columns:
        if column_name in df.columns:
            result[column_name] = df.filter(col(column_name).isNull()).count()
        else:
            result[column_name] = -1

    return result


def validate_table(spark: SparkSession, table_name: str, config: dict) -> dict:
    bronze_df = read_parquet_if_exists(spark, config["bronze"])
    silver_df = read_parquet_if_exists(spark, config["silver"])
    quarantine_df = read_parquet_if_exists(spark, config["quarantine"])

    bronze_count = count_df(bronze_df)
    silver_count = count_df(silver_df)
    quarantine_count = count_df(quarantine_df)

    required_missing = missing_columns(silver_df, config["required_columns"])

    failure_reasons = []

    if bronze_df is None:
        failure_reasons.append(f"missing bronze path: {config['bronze']}")

    if silver_df is None:
        failure_reasons.append(f"missing silver path: {config['silver']}")

    if bronze_count <= 0:
        failure_reasons.append("bronze row count is zero")

    if silver_count <= 0:
        failure_reasons.append("silver row count is zero")

    duplicate_dropped_count = bronze_count - silver_count - quarantine_count

    if duplicate_dropped_count < 0:
        failure_reasons.append(
            f"reconciliation failed: bronze={bronze_count}, "
            f"silver={silver_count}, quarantine={quarantine_count}, "
            f"silver+quarantine={silver_count + quarantine_count}, "
            f"duplicate_dropped_count={duplicate_dropped_count}"
        )

    if required_missing:
        failure_reasons.append(f"missing required silver columns: {required_missing}")

    null_violations = {}

    if silver_df is not None:
        null_violations = count_nulls(silver_df, config["not_null_columns"])

        bad_nulls = {
            column_name: null_count
            for column_name, null_count in null_violations.items()
            if null_count != 0
        }

        if bad_nulls:
            failure_reasons.append(f"not-null violations: {bad_nulls}")

    minimum_silver_rows = config.get("minimum_silver_rows")

    if minimum_silver_rows is not None and silver_count < minimum_silver_rows:
        failure_reasons.append(
            f"silver row count below minimum threshold: actual={silver_count}, minimum={minimum_silver_rows}"
        )

    status = "PASSED" if not failure_reasons else "FAILED"

    return {
        "table_name": table_name,
        "status": status,
        "bronze_row_count": bronze_count,
        "silver_row_count": silver_count,
        "quarantine_row_count": quarantine_count,
        "reconciled_row_count": silver_count + quarantine_count + duplicate_dropped_count,
        "duplicate_dropped_count": duplicate_dropped_count,
        "missing_required_columns": ",".join(required_missing),
        "null_violations": str(null_violations),
        "failure_reason": "; ".join(failure_reasons),
        "batch_date": BATCH_DATE,
    }


def main() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    results = []

    print("Starting Silver quality checks")
    print(f"Batch date: {BATCH_DATE}")

    for table_name, config in TABLES.items():
        result = validate_table(spark, table_name, config)
        results.append(result)

        print(
            f"{table_name}: {result['status']} "
            f"bronze={result['bronze_row_count']} "
            f"silver={result['silver_row_count']} "
            f"quarantine={result['quarantine_row_count']} "
            f"duplicates_dropped={result['duplicate_dropped_count']} "
            f"reconciled={result['reconciled_row_count']}"
        )

        if result["failure_reason"]:
            print(f"  reason: {result['failure_reason']}")

    report_df = spark.createDataFrame(results)
    report_path = LOGS_DIR / f"silver_quality_report_{BATCH_DATE}"

    (
        report_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(report_path))
    )

    print(f"Silver quality report written to: {report_path}")

    spark.stop()

    failed = [result for result in results if result["status"] == "FAILED"]

    if failed:
        raise RuntimeError(f"Silver quality checks failed for {len(failed)} table(s).")

    print("All Silver quality checks passed.")


if __name__ == "__main__":
    main()