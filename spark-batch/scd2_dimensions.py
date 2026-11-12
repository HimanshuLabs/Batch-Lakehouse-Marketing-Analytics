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
# The first version of every key is effective from the beginning of time so
# facts dated before the key's first source change still match a version.
# Interpreted in UTC (spark.sql.session.timeZone is UTC).
BEGINNING_OF_TIME = "1900-01-01 00:00:00"

# Per dimension:
#   tracked_columns     attributes whose change opens a new version (hashed into
#                       record_hash and so into the surrogate key)
#   untracked_optional  tracked_columns that may be missing from the source; kept
#                       as NULL and left out of the hash
#   carried_columns     source attributes kept on each version but never hashed:
#                       a change alone opens no version and no key changes. Each
#                       version carries the values of the source row that opened
#                       it. They must exist in the source.


DIMENSION_CONFIGS: Dict[str, Dict[str, object]] = {
    "customers": {
        "source_candidates": [
            SILVER_DIR / "silver_customers_history",
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
        "column_map": {
            "customer_segment": "user_segment",
            "loyalty_tier": "membership_tier",
        },
        "untracked_optional": [],
        "carried_columns": [
            "gender",
            "age",
            "preferred_language",
            "loyalty_points",
            "is_prime_user",
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
            SILVER_DIR / "silver_products_history",
            SILVER_DIR / "silver_products",
            SILVER_DIR / "products",
        ],
        "output_path": GOLD_DIR / "dim_products_scd2",
        "natural_key": "product_id",
        "surrogate_key": "product_sk",
        "tracked_columns": [
            "product_name",
            "category",
            "price",
            # active / discontinued: a status change opens a new version.
            "status",
        ],
        "column_map": {
            "price": "discounted_price",
            "status": "product_status",
        },
        "untracked_optional": [],
        "carried_columns": [
            "brand",
            "original_price",
            "inventory_remaining",
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
            SILVER_DIR / "silver_campaigns_history",
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
        "column_map": {
            "channel": "traffic_source",
        },
        "untracked_optional": [],
        "carried_columns": [
            # The campaign's objective (awareness, consideration, conversion, retention).
            "campaign_type",
            "ab_test_group",
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


def add_missing_tracked_columns(
    df: DataFrame,
    dimension_name: str,
    tracked_columns: List[str],
    untracked_optional: List[str],
) -> DataFrame:
    """
    Fail fast when a hashed tracked column is missing after column mapping.

    Only columns listed in untracked_optional may be absent; they are kept in
    the output contract as NULL and are excluded from the record hash.
    """
    missing_hashed = [
        column
        for column in tracked_columns
        if column not in df.columns and column not in untracked_optional
    ]

    if missing_hashed:
        raise ValueError(
            f"SCD2 dimension '{dimension_name}' is missing tracked columns after "
            f"column mapping: {', '.join(missing_hashed)}. "
            f"Available columns: {', '.join(df.columns)}"
        )

    result = df
    for column in untracked_optional:
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


def normalize_dimension_columns(
    df: DataFrame,
    natural_key: str,
    column_map: Dict[str, str],
) -> DataFrame:
    """
    Normalize source-specific column names into the dimension contract.

    Project 1-style user behavior data uses user_id/home_city/home_state/event_time.
    Project 2-style lakehouse dimensions may use customer_id/city/state/updated_at.
    This function bridges that contract cleanly before SCD2 logic runs.

    column_map holds per-dimension {target: source} copies (for example
    customer_segment <- user_segment). Source columns are kept.
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

    for target_column, source_column in column_map.items():
        if target_column not in result.columns and source_column in result.columns:
            result = result.withColumn(target_column, F.col(source_column))

    return result


def build_scd2_dimension(
    df: DataFrame,
    dimension_name: str,
    natural_key: str,
    surrogate_key: str,
    tracked_columns: List[str],
    date_candidates: List[str],
    column_map: Dict[str, str],
    untracked_optional: List[str],
    carried_columns: List[str] | None = None,
) -> DataFrame:
    carried_columns = list(carried_columns or [])
    df = normalize_dimension_columns(df, natural_key, column_map)

    if natural_key not in df.columns:
        available_columns = ", ".join(df.columns)
        raise ValueError(
            f"Missing natural key column: {natural_key}. "
            f"Available columns: {available_columns}"
        )

    missing_carried = [column for column in carried_columns if column not in df.columns]
    if missing_carried:
        raise ValueError(
            f"SCD2 dimension '{dimension_name}' is missing carried columns: "
            f"{', '.join(missing_carried)}. Available columns: {', '.join(df.columns)}"
        )

    effective_column = first_existing_column(df, date_candidates)

    cleaned = df.filter(F.col(natural_key).isNotNull())

    cleaned = add_missing_tracked_columns(
        cleaned,
        dimension_name,
        tracked_columns,
        untracked_optional,
    )

    hashed_columns = [
        column for column in tracked_columns if column not in untracked_optional
    ]

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

    # Capture the real times before effective_from is rewritten for the
    # first version: source_updated_at is the record's real change time
    # (output column); version_start_at is the version's real start, used
    # internally for the surrogate key so existing key values stay stable.
    cleaned = (
        cleaned
        .withColumn(
            "source_updated_at",
            F.to_timestamp(F.col(effective_column))
            if effective_column
            else F.lit(None).cast("timestamp"),
        )
        .withColumn("version_start_at", F.col("effective_from"))
    )

    hashed = build_record_hash(cleaned, hashed_columns)

    # Carried columns only break exact ties (same time and hash), so the row a
    # version carries is deterministic; they never decide versions or keys.
    base_window = Window.partitionBy(natural_key).orderBy(
        F.col("effective_from").asc(),
        F.col("record_hash").asc(),
        *[F.col(column).asc_nulls_first() for column in carried_columns],
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
        # effective_to and is_current above use the real times. Only now is
        # the first version per key opened back to the beginning of time.
        .withColumn("version_number", F.row_number().over(version_window))
        .withColumn(
            "effective_from",
            F.when(
                F.col("version_number") == 1,
                F.to_timestamp(F.lit(BEGINNING_OF_TIME)),
            ).otherwise(F.col("effective_from")),
        )
    )

    selected_columns = [
        F.sha2(
            F.concat_ws(
                "||",
                F.col(natural_key).cast("string"),
                F.col("version_start_at").cast("string"),
                F.col("record_hash").cast("string"),
            ),
            256,
        ).alias(surrogate_key),
        F.col(natural_key),
    ]

    for column in tracked_columns + carried_columns:
        selected_columns.append(F.col(column))

    selected_columns.extend(
        [
            F.col("effective_from"),
            F.col("effective_to"),
            F.col("is_current"),
            F.col("record_hash"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
            F.col("source_updated_at"),
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
    print(f"spark.sql.session.timeZone={spark.conf.get('spark.sql.session.timeZone')}")

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
            column_map = config["column_map"]  # type: ignore[assignment]
            untracked_optional = config["untracked_optional"]  # type: ignore[assignment]
            carried_columns = config["carried_columns"]  # type: ignore[assignment]

            source_df = read_table(spark, source_path)

            scd2_df = build_scd2_dimension(
                df=source_df,
                dimension_name=dimension_name,
                natural_key=natural_key,
                surrogate_key=surrogate_key,
                tracked_columns=tracked_columns,
                date_candidates=date_candidates,
                column_map=column_map,
                untracked_optional=untracked_optional,
                carried_columns=carried_columns,
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
                "column_map": column_map,
                "untracked_optional": untracked_optional,
                "carried_columns": carried_columns,
                "validation": validation,
            }

            print(
                f"{dimension_name}: "
                f"status={validation['status']}, "
                f"rows={validation['total_rows']}, "
                f"current_rows={validation['current_rows']}"
            )

    except Exception as exc:
        overall_status = "FAIL"
        report["error"] = str(exc)
        raise

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
