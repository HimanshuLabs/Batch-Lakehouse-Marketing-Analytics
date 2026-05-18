from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    input_file_name,
    lit,
    sha2,
    to_date,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.today().isoformat()
BATCH_ID = f"batch_{BATCH_DATE}_{uuid.uuid4().hex[:8]}"


SOURCE_TABLES = {
    "customers": {
        "format": "csv",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "customers.csv",
        "output": BRONZE_BASE_DIR / "bronze_customers",
    },
    "products": {
        "format": "csv",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "products.csv",
        "output": BRONZE_BASE_DIR / "bronze_products",
    },
    "campaigns": {
        "format": "csv",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "campaigns.csv",
        "output": BRONZE_BASE_DIR / "bronze_campaigns",
    },
    "orders": {
        "format": "csv",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "orders.csv",
        "output": BRONZE_BASE_DIR / "bronze_orders",
    },
    "order_items": {
        "format": "csv",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "order_items.csv",
        "output": BRONZE_BASE_DIR / "bronze_order_items",
    },
    "ad_spend": {
        "format": "csv",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "ad_spend.csv",
        "output": BRONZE_BASE_DIR / "bronze_ad_spend",
    },
    "web_events": {
        "format": "json",
        "path": RAW_BASE_DIR / f"batch_date={BATCH_DATE}" / "web_events.json",
        "output": BRONZE_BASE_DIR / "bronze_web_events",
    },
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2_Bronze_Ingestion")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )


def read_source(spark: SparkSession, source_format: str, source_path: Path) -> DataFrame:
    if source_format == "csv":
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(str(source_path))
        )

    if source_format == "json":
        return (
            spark.read
            .option("mode", "PERMISSIVE")
            .json(str(source_path))
        )

    raise ValueError(f"Unsupported source format: {source_format}")


def add_bronze_metadata(df: DataFrame, source_name: str) -> DataFrame:
    business_columns = df.columns

    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", to_date(lit(BATCH_DATE)))
        .withColumn("batch_id", lit(BATCH_ID))
        .withColumn("source_system", lit(source_name))
        .withColumn("source_file_name", input_file_name())
        .withColumn(
            "record_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in business_columns]), 256),
        )
    )


def write_bronze_table(df: DataFrame, output_path: Path) -> None:
    (
        df
        .write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(str(output_path))
    )


def main() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_BASE_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    row_count_report = []

    print("Starting Bronze ingestion")
    print(f"Batch date: {BATCH_DATE}")
    print(f"Batch ID: {BATCH_ID}")

    for source_name, config in SOURCE_TABLES.items():
        source_path = config["path"]
        output_path = config["output"]
        source_format = config["format"]

        if not source_path.exists():
            raise FileNotFoundError(f"Missing source file: {source_path}")

        raw_df = read_source(spark, source_format, source_path)
        raw_count = raw_df.count()

        bronze_df = add_bronze_metadata(raw_df, source_name)
        bronze_count = bronze_df.count()

        write_bronze_table(bronze_df, output_path)

        row_count_report.append(
            {
                "source_name": source_name,
                "source_format": source_format,
                "source_path": str(source_path),
                "bronze_path": str(output_path),
                "raw_row_count": raw_count,
                "bronze_row_count": bronze_count,
                "batch_id": BATCH_ID,
                "batch_date": BATCH_DATE,
            }
        )

        print(f"{source_name}: raw={raw_count}, bronze={bronze_count}")

    report_df = spark.createDataFrame(row_count_report)
    report_path = LOGS_DIR / f"bronze_row_count_report_{BATCH_DATE}"

    (
        report_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(report_path))
    )

    print(f"Bronze row count report written to: {report_path}")
    print("Bronze ingestion completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()