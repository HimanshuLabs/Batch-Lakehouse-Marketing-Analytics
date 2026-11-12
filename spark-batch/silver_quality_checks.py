from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, count, expr, greatest, lit, sum as spark_sum


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_BASE_DIR = PROJECT_ROOT / "data" / "silver"
QUARANTINE_BASE_DIR = PROJECT_ROOT / "data" / "quarantine"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()


def int_key(column_name: str) -> str:
    # Same cast as silver_transformations.safe_int, applied to the Bronze string.
    return f"try_cast(try_cast(`{column_name}` as double) as int)"


# Distinct updated_at values stay distinct in any session time zone.
UPDATED_AT_KEY = "try_cast(`updated_at` as timestamp)"


# dedupe_key / history_key: Silver column -> the same value computed from Bronze.
TABLES = {
    "customers": {
        "bronze": BRONZE_BASE_DIR / "bronze_customers",
        "silver": SILVER_BASE_DIR / "silver_customers",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_customers",
        "dedupe_key": {"user_id": int_key("user_id")},
        "history_key": {"user_id": int_key("user_id"), "updated_at": UPDATED_AT_KEY},
        "history": SILVER_BASE_DIR / "silver_customers_history",
        "natural_key": "user_id",
        "required_columns": ["user_id", "email", "age", "processed_date", "silver_record_hash"],
        "not_null_columns": ["user_id", "email", "age"],
    },
    "products": {
        "bronze": BRONZE_BASE_DIR / "bronze_products",
        "silver": SILVER_BASE_DIR / "silver_products",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_products",
        "dedupe_key": {"product_id": int_key("product_id")},
        "history_key": {"product_id": int_key("product_id"), "updated_at": UPDATED_AT_KEY},
        "history": SILVER_BASE_DIR / "silver_products_history",
        "natural_key": "product_id",
        "required_columns": ["product_id", "product_name", "category", "processed_date", "silver_record_hash"],
        "not_null_columns": ["product_id", "product_name", "category"],
    },
    "campaigns": {
        "bronze": BRONZE_BASE_DIR / "bronze_campaigns",
        "silver": SILVER_BASE_DIR / "silver_campaigns",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_campaigns",
        "dedupe_key": {"campaign_id": int_key("campaign_id")},
        "history_key": {"campaign_id": int_key("campaign_id"), "updated_at": UPDATED_AT_KEY},
        "history": SILVER_BASE_DIR / "silver_campaigns_history",
        "natural_key": "campaign_id",
        "required_columns": ["campaign_id", "campaign_name", "traffic_source", "processed_date", "silver_record_hash"],
        "not_null_columns": ["campaign_id", "campaign_name", "traffic_source"],
    },
    "orders": {
        "bronze": BRONZE_BASE_DIR / "bronze_orders",
        "silver": SILVER_BASE_DIR / "silver_orders",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_orders",
        "dedupe_key": {"order_id": int_key("order_id")},
        "required_columns": ["order_id", "user_id", "order_timestamp", "cart_value", "processed_date", "silver_record_hash"],
        "not_null_columns": ["order_id", "user_id", "order_timestamp", "cart_value"],
    },
    "order_items": {
        "bronze": BRONZE_BASE_DIR / "bronze_order_items",
        "silver": SILVER_BASE_DIR / "silver_order_items",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_order_items",
        "dedupe_key": {"order_item_id": int_key("order_item_id")},
        "required_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_amount", "processed_date", "silver_record_hash"],
        "not_null_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_amount"],
    },
    "ad_spend": {
        "bronze": BRONZE_BASE_DIR / "bronze_ad_spend",
        "silver": SILVER_BASE_DIR / "silver_ad_spend",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_ad_spend",
        "dedupe_key": {"spend_id": int_key("spend_id")},
        "required_columns": ["spend_id", "campaign_id", "spend_date", "spend_amount", "processed_date", "silver_record_hash"],
        "not_null_columns": ["spend_id", "campaign_id", "spend_date", "spend_amount"],
    },
    "web_events": {
        "bronze": BRONZE_BASE_DIR / "bronze_web_events",
        "silver": SILVER_BASE_DIR / "silver_web_events",
        "quarantine": QUARANTINE_BASE_DIR / "quarantine_web_events",
        "dedupe_key": {"event_id": "trim(`event_id`)"},
        "required_columns": ["event_id", "session_id", "user_id", "event_type", "event_timestamp", "processed_date", "silver_record_hash"],
        "not_null_columns": ["event_id", "session_id", "user_id", "event_type", "event_timestamp"],
        # Of this batch's Bronze rows, the share that must pass Silver (7 of 8,
        # the old 7,000-of-8,000 floor of the first batch). Per batch, so a
        # small or empty batch, or a start from any batch date, is judged on
        # its own rows.
        "minimum_batch_pass_ratio": 0.875,
    },
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2_Silver_Quality_Checks")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
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


def conservation_check(
    config: dict,
    bronze_df: DataFrame,
    silver_df: DataFrame,
    quarantine_df: DataFrame | None,
) -> dict:
    """
    Every Bronze row (all batches) must be accounted for:
        bronze = passed + quarantined
        passed = silver + dedupe_removed
    Quarantined rows are matched back to their Bronze rows by
    (ingestion_date, record_hash), counting exact duplicates. Silver keeps
    one row per dedupe key, so dedupe_removed = passed rows minus distinct
    dedupe keys among them. It depends only on the data, never on the
    tie-break order, and is not inferred from the other counts.
    """
    group_columns = ["ingestion_date", "record_hash"]
    dedupe_key = config["dedupe_key"]
    history_key = config.get("history_key", {})
    key_columns = {f"key_{name}": sql for name, sql in dedupe_key.items()}
    history_columns = {f"history_{name}": sql for name, sql in history_key.items()}

    bronze_groups = (
        bronze_df
        .select(
            *group_columns,
            *[expr(sql).alias(alias) for alias, sql in {**key_columns, **history_columns}.items()],
        )
        .groupBy(*group_columns, *key_columns, *history_columns)
        .agg(count(lit(1)).alias("bronze_rows"))
    )

    if quarantine_df is None:
        joined = bronze_groups.withColumn("quarantine_rows", lit(0).cast("long"))
    else:
        quarantine_groups = (
            quarantine_df
            .groupBy(*group_columns)
            .agg(count(lit(1)).alias("quarantine_rows"))
        )
        joined = bronze_groups.join(quarantine_groups, on=group_columns, how="full_outer")

    joined = (
        joined
        .withColumn("bronze_rows", coalesce(col("bronze_rows"), lit(0)))
        .withColumn("quarantine_rows", coalesce(col("quarantine_rows"), lit(0)))
        .withColumn("passed_rows", greatest(col("bronze_rows") - col("quarantine_rows"), lit(0)))
        .withColumn("untraceable_rows", greatest(col("quarantine_rows") - col("bronze_rows"), lit(0)))
        .cache()
    )

    totals = joined.agg(
        spark_sum("bronze_rows").alias("bronze"),
        spark_sum("quarantine_rows").alias("quarantine"),
        spark_sum("passed_rows").alias("passed"),
        spark_sum("untraceable_rows").alias("untraceable"),
    ).first()

    passed_groups = joined.filter(col("passed_rows") > 0)
    passed_keys = passed_groups.select(*key_columns).distinct().count()
    expected_history_rows = (
        passed_groups.select(*history_columns).distinct().count()
        if history_columns
        else None
    )
    joined.unpersist()

    bronze_count = int(totals["bronze"] or 0)
    quarantine_count = int(totals["quarantine"] or 0)
    passed_count = int(totals["passed"] or 0)
    untraceable_count = int(totals["untraceable"] or 0)
    silver_count = silver_df.count()
    silver_keys = silver_df.select(*dedupe_key).distinct().count()
    dedupe_removed = passed_count - passed_keys
    unaccounted = passed_count - silver_count - dedupe_removed

    failure_reasons = []

    if untraceable_count != 0:
        failure_reasons.append(
            f"{untraceable_count} quarantine row(s) not traceable to a Bronze row"
        )

    if bronze_count != passed_count + quarantine_count:
        failure_reasons.append(
            f"conservation failed: bronze={bronze_count} != "
            f"passed={passed_count} + quarantine={quarantine_count}"
        )

    if unaccounted != 0:
        failure_reasons.append(
            f"conservation failed: passed={passed_count} != "
            f"silver={silver_count} + dedupe_removed={dedupe_removed} "
            f"(unaccounted={unaccounted})"
        )

    if silver_keys != silver_count:
        failure_reasons.append(
            f"silver {list(dedupe_key)} is not unique: "
            f"{silver_count - silver_keys} duplicate row(s)"
        )

    return {
        "passed_row_count": passed_count,
        "duplicate_dropped_count": dedupe_removed,
        "unaccounted_row_count": unaccounted,
        "untraceable_quarantine_row_count": untraceable_count,
        "history_expected_row_count": expected_history_rows,
        "failure_reasons": failure_reasons,
    }


def validate_history(
    spark: SparkSession,
    config: dict,
    silver_df: DataFrame | None,
) -> dict:
    """
    Checks for a dimension history table (one row per natural key and
    updated_at). Tables without a history config return empty results.
    """
    result = {
        "history_row_count": None,
        "history_duplicate_version_keys": None,
        "current_keys_missing_in_history": None,
        "failure_reasons": [],
    }

    history_path = config.get("history")
    if history_path is None:
        return result

    natural_key = config["natural_key"]
    history_df = read_parquet_if_exists(spark, history_path)

    if history_df is None:
        result["failure_reasons"].append(f"missing silver history path: {history_path}")
        return result

    result["history_row_count"] = history_df.count()

    if result["history_row_count"] <= 0:
        result["failure_reasons"].append("silver history row count is zero")

    duplicate_version_keys = (
        history_df.groupBy(natural_key, "updated_at")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    result["history_duplicate_version_keys"] = duplicate_version_keys

    if duplicate_version_keys != 0:
        result["failure_reasons"].append(
            f"history ({natural_key}, updated_at) is not unique: "
            f"{duplicate_version_keys} duplicated version key(s)"
        )

    if silver_df is not None:
        missing_in_history = (
            silver_df.select(natural_key).distinct()
            .join(history_df.select(natural_key).distinct(), on=natural_key, how="left_anti")
            .count()
        )
        result["current_keys_missing_in_history"] = missing_in_history

        if missing_in_history != 0:
            result["failure_reasons"].append(
                f"{missing_in_history} current {natural_key} value(s) missing from history"
            )

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

    conservation = {
        "passed_row_count": None,
        "duplicate_dropped_count": None,
        "unaccounted_row_count": None,
        "untraceable_quarantine_row_count": None,
        "history_expected_row_count": None,
        "failure_reasons": [],
    }

    if bronze_df is not None and silver_df is not None:
        conservation = conservation_check(config, bronze_df, silver_df, quarantine_df)

    failure_reasons.extend(conservation.pop("failure_reasons"))
    duplicate_dropped_count = conservation["duplicate_dropped_count"]

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

    minimum_batch_pass_ratio = config.get("minimum_batch_pass_ratio")

    if minimum_batch_pass_ratio is not None and bronze_df is not None:
        this_batch = col("ingestion_date") == lit(BATCH_DATE).cast("date")
        batch_bronze_rows = bronze_df.filter(this_batch).count()
        batch_quarantine_rows = (
            quarantine_df.filter(this_batch).count()
            if quarantine_df is not None and "ingestion_date" in quarantine_df.columns
            else 0
        )
        batch_passed_rows = batch_bronze_rows - batch_quarantine_rows
        if batch_bronze_rows > 0 and batch_passed_rows < minimum_batch_pass_ratio * batch_bronze_rows:
            failure_reasons.append(
                f"too few of batch {BATCH_DATE}'s rows passed Silver: passed={batch_passed_rows} of "
                f"{batch_bronze_rows}, minimum share={minimum_batch_pass_ratio}"
            )

    history_result = validate_history(spark, config, silver_df)
    failure_reasons.extend(history_result.pop("failure_reasons"))

    expected_history_rows = conservation["history_expected_row_count"]

    if (
        expected_history_rows is not None
        and history_result["history_row_count"] is not None
        and history_result["history_row_count"] != expected_history_rows
    ):
        failure_reasons.append(
            f"history conservation failed: history={history_result['history_row_count']}, "
            f"expected one row per passed (key, updated_at)={expected_history_rows}"
        )

    status = "PASSED" if not failure_reasons else "FAILED"

    return {
        "table_name": table_name,
        "status": status,
        "bronze_row_count": bronze_count,
        "silver_row_count": silver_count,
        "quarantine_row_count": quarantine_count,
        "reconciled_row_count": (
            silver_count + quarantine_count + duplicate_dropped_count
            if duplicate_dropped_count is not None
            else None
        ),
        **conservation,
        "missing_required_columns": ",".join(required_missing),
        "null_violations": str(null_violations),
        "failure_reason": "; ".join(failure_reasons),
        "batch_date": BATCH_DATE,
        **history_result,
    }


def main() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    print(f"spark.sql.session.timeZone={spark.conf.get('spark.sql.session.timeZone')}")

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
            f"passed={result['passed_row_count']} "
            f"duplicates_dropped={result['duplicate_dropped_count']} "
            f"unaccounted={result['unaccounted_row_count']} "
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