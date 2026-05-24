from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)


FAR_FUTURE_DATE = "9999-12-31"


DIMENSION_CONFIGS: Dict[str, Dict[str, object]] = {
    "customers": {
        "source_candidates": [
            SILVER_DIR / "silver_customers",
            SILVER_DIR / "customers",
        ],
        "output_path": GOLD_DIR / "dim_customers_scd2",
        "natural_key": "customer_id",
        "surrogate_key": "customer_sk",
        "tracked_columns": [
            "customer_name",
            "email",
            "city",
            "state",
            "country",
            "customer_segment",
            "loyalty_tier",
        ],
        "date_candidates": [
            "updated_at",
            "event_timestamp",
            "event_time",
            "signup_date",
            "ingestion_timestamp",
        ],
    },
    "products": {
        "source_candidates": [
            SILVER_DIR / "silver_products",
            SILVER_DIR / "products",
        ],
        "output_path": GOLD_DIR / "dim_products_scd2",
        "natural_key": "product_id",
        "surrogate_key": "product_sk",
        "tracked_columns": [
            "product_name",
            "category",
            "brand",
            "price",
            "status",
        ],
        "date_candidates": [
            "updated_at",
            "event_timestamp",
            "event_time",
            "ingestion_timestamp",
        ],
    },
    "campaigns": {
        "source_candidates": [
            SILVER_DIR / "silver_campaigns",
            SILVER_DIR / "campaigns",
        ],
        "output_path": GOLD_DIR / "dim_campaigns_scd2",
        "natural_key": "campaign_id",
        "surrogate_key": "campaign_sk",
        "tracked_columns": [
            "campaign_name",
            "channel",
            "target_segment",
            "budget",
            "campaign_status",
            "start_date",
            "end_date",
        ],
        "date_candidates": [
            "updated_at",
            "start_date",
            "event_timestamp",
            "event_time",
            "ingestion_timestamp",
        ],
    },
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2-SCD2-Dimensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def resolve_existing_path(candidates: List[Path]) -> Path:
    for path in candidates:
        if path.exists():
            return path

    checked = "\n".join(str(path) for path in candidates)
    raise FileNotFoundError(f"No valid source path found. Checked:\n{checked}")


def read_table(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.parquet(str(path))


def first_existing_column(df: DataFrame, candidates: List[str]) -> str | None:
    available = set(df.columns)
    for column in candidates:
        if column in available:
            return column
    return None


def add_missing_tracked_columns(df: DataFrame, tracked_columns: List[str]) -> DataFrame:
    result = df
    for column in tracked_columns:
        if column not in result.columns:
            result = result.withColumn(column, F.lit(None).cast(StringType()))
    return result


def build_record_hash(df: DataFrame, tracked_columns: List[str]) -> DataFrame:
    normalized_columns = [
        F.coalesce(F.col(column).cast("string"), F.lit("__NULL__"))
        for column in tracked_columns
    ]

    return df.withColumn(
        "record_hash",
        F.sha2(F.concat_ws("||", *normalized_columns), 256),
    )


def normalize_dimension_columns(df: DataFrame, natural_key: str) -> DataFrame:
    """
    Normalize source-specific column names into the dimension contract.

    Project 1-style user behavior data uses user_id/home_city/home_state/event_time.
    Project 2-style lakehouse dimensions may use customer_id/city/state/updated_at.
    This function bridges that contract cleanly before SCD2 logic runs.
    """
    result = df

    rename_map = {
        "customer_id": ["user_id"],
        "customer_name": ["user_name"],
        "city": ["home_city"],
        "state": ["home_state"],
        "updated_at": ["event_timestamp", "event_time"],
    }

    for target_column, source_candidates in rename_map.items():
        if target_column not in result.columns:
            for source_column in source_candidates:
                if source_column in result.columns:
                    result = result.withColumn(target_column, F.col(source_column))
                    break

    return result


def build_scd2_dimension(
    df: DataFrame,
    natural_key: str,
    surrogate_key: str,
    tracked_columns: List[str],
    date_candidates: List[str],
) -> DataFrame:
    df = normalize_dimension_columns(df, natural_key)

    if natural_key not in df.columns:
        available_columns = ", ".join(df.columns)
        raise ValueError(
            f"Missing natural key column: {natural_key}. "
            f"Available columns: {available_columns}"
        )

    effective_column = first_existing_column(df, date_candidates)

    cleaned = df.filter(F.col(natural_key).isNotNull())

    cleaned = add_missing_tracked_columns(cleaned, tracked_columns)

    if effective_column:
        cleaned = cleaned.withColumn(
            "effective_from",
            F.to_timestamp(F.col(effective_column)),
        )
    else:
        cleaned = cleaned.withColumn("effective_from", F.current_timestamp())

    cleaned = cleaned.withColumn(
        "effective_from",
        F.coalesce(F.col("effective_from"), F.current_timestamp()),
    )

    hashed = build_record_hash(cleaned, tracked_columns)

    base_window = Window.partitionBy(natural_key).orderBy(
        F.col("effective_from").asc(),
        F.col("record_hash").asc(),
    )

    with_previous = hashed.withColumn(
        "previous_record_hash",
        F.lag("record_hash").over(base_window),
    )

    changed_only = with_previous.filter(
        F.col("previous_record_hash").isNull()
        | (F.col("previous_record_hash") != F.col("record_hash"))
    )

    version_window = Window.partitionBy(natural_key).orderBy(
        F.col("effective_from").asc(),
        F.col("record_hash").asc(),
    )

    versioned = (
        changed_only
        .withColumn("next_effective_from", F.lead("effective_from").over(version_window))
        .withColumn(
            "effective_to",
            F.coalesce(
                F.col("next_effective_from"),
                F.to_timestamp(F.lit(FAR_FUTURE_DATE)),
            ),
        )
        .withColumn(
            "is_current",
            F.col("next_effective_from").isNull(),
        )
    )

    selected_columns = [
        F.sha2(
            F.concat_ws(
                "||",
                F.col(natural_key).cast("string"),
                F.col("effective_from").cast("string"),
                F.col("record_hash").cast("string"),
            ),
            256,
        ).alias(surrogate_key),
        F.col(natural_key),
    ]

    for column in tracked_columns:
        selected_columns.append(F.col(column))

    selected_columns.extend(
        [
            F.col("effective_from"),
            F.col("effective_to"),
            F.col("is_current"),
            F.col("record_hash"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        ]
    )

    return versioned.select(*selected_columns)


def validate_scd2_dimension(
    df: DataFrame,
    natural_key: str,
    surrogate_key: str,
) -> Dict[str, object]:
    total_rows = df.count()

    natural_key_count = df.select(natural_key).distinct().count()

    current_rows = df.filter(F.col("is_current") == True).count()

    duplicate_current_rows = (
        df.filter(F.col("is_current") == True)
        .groupBy(natural_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_surrogate_keys = df.filter(F.col(surrogate_key).isNull()).count()

    null_natural_keys = df.filter(F.col(natural_key).isNull()).count()

    invalid_date_ranges = df.filter(
        F.col("effective_to") <= F.col("effective_from")
    ).count()

    status = (
        "PASS"
        if duplicate_current_rows == 0
        and null_surrogate_keys == 0
        and null_natural_keys == 0
        and invalid_date_ranges == 0
        and current_rows == natural_key_count
        else "FAIL"
    )

    return {
        "status": status,
        "total_rows": total_rows,
        "natural_key_count": natural_key_count,
        "current_rows": current_rows,
        "duplicate_current_rows": duplicate_current_rows,
        "null_surrogate_keys": null_surrogate_keys,
        "null_natural_keys": null_natural_keys,
        "invalid_date_ranges": invalid_date_ranges,
    }


def write_dimension(df: DataFrame, output_path: Path) -> None:
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def main() -> None:
    spark = create_spark_session()

    report: Dict[str, object] = {
        "job_name": "scd2_dimensions",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "dimensions": {},
    }

    overall_status = "PASS"

    try:
        for dimension_name, config in DIMENSION_CONFIGS.items():
            print(f"Building SCD2 dimension: {dimension_name}")

            source_path = resolve_existing_path(config["source_candidates"])  # type: ignore[arg-type]
            output_path = config["output_path"]  # type: ignore[assignment]
            natural_key = config["natural_key"]  # type: ignore[assignment]
            surrogate_key = config["surrogate_key"]  # type: ignore[assignment]
            tracked_columns = config["tracked_columns"]  # type: ignore[assignment]
            date_candidates = config["date_candidates"]  # type: ignore[assignment]

            source_df = read_table(spark, source_path)

            scd2_df = build_scd2_dimension(
                df=source_df,
                natural_key=natural_key,
                surrogate_key=surrogate_key,
                tracked_columns=tracked_columns,
                date_candidates=date_candidates,
            )

            write_dimension(scd2_df, output_path)

            written_df = read_table(spark, output_path)

            validation = validate_scd2_dimension(
                written_df,
                natural_key=natural_key,
                surrogate_key=surrogate_key,
            )

            if validation["status"] != "PASS":
                overall_status = "FAIL"

            report["dimensions"][dimension_name] = {
                "source_path": str(source_path),
                "output_path": str(output_path),
                "natural_key": natural_key,
                "surrogate_key": surrogate_key,
                "tracked_columns": tracked_columns,
                "validation": validation,
            }

            print(
                f"{dimension_name}: "
                f"status={validation['status']}, "
                f"rows={validation['total_rows']}, "
                f"current_rows={validation['current_rows']}"
            )

    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["overall_status"] = overall_status

        report_path = LOG_DIR / "scd2_quality_report.json"
        with report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

        print(f"SCD2 quality report written to: {report_path}")
        print(f"Overall status: {overall_status}")

        spark.stop()

    if overall_status != "PASS":
        raise SystemExit("SCD2 validation failed. Check logs/scd2_quality_report.json")


if __name__ == "__main__":
    main()
