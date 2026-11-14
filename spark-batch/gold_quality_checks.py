from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_BASE_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_BASE_DIR = PROJECT_ROOT / "data" / "gold"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()


GOLD_TABLES = {
    "dim_customers": {
        "gold_path": GOLD_BASE_DIR / "dim_customers",
        "source_silver_path": SILVER_BASE_DIR / "silver_customers",
        "required_columns": ["customer_id", "email", "membership_tier", "gold_processed_date"],
        "not_null_columns": ["customer_id", "email"],
        "expected_count_match": True,
    },
    "dim_products": {
        "gold_path": GOLD_BASE_DIR / "dim_products",
        "source_silver_path": SILVER_BASE_DIR / "silver_products",
        "required_columns": ["product_id", "product_name", "category", "gold_processed_date"],
        "not_null_columns": ["product_id", "product_name", "category"],
        "expected_count_match": True,
    },
    "dim_campaigns": {
        "gold_path": GOLD_BASE_DIR / "dim_campaigns",
        "source_silver_path": SILVER_BASE_DIR / "silver_campaigns",
        "required_columns": ["campaign_id", "campaign_name", "traffic_source", "gold_processed_date"],
        "not_null_columns": ["campaign_id", "campaign_name", "traffic_source"],
        "expected_count_match": True,
    },
    "fact_orders": {
        "gold_path": GOLD_BASE_DIR / "fact_orders",
        "source_silver_path": SILVER_BASE_DIR / "silver_orders",
        "required_columns": ["order_id", "customer_id", "order_timestamp", "order_amount", "gold_processed_date"],
        "not_null_columns": ["order_id", "customer_id", "order_timestamp", "order_amount"],
        "expected_count_match": True,
        "non_negative_columns": ["order_amount", "fraud_score"],
    },
    "fact_order_items": {
        "gold_path": GOLD_BASE_DIR / "fact_order_items",
        "source_silver_path": SILVER_BASE_DIR / "silver_order_items",
        "required_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_amount", "gold_processed_date"],
        "not_null_columns": ["order_item_id", "order_id", "product_id", "quantity", "line_amount"],
        "expected_count_match": True,
        "non_negative_columns": ["quantity", "line_amount", "discounted_price"],
    },
    "fact_ad_spend": {
        "gold_path": GOLD_BASE_DIR / "fact_ad_spend",
        "source_silver_path": SILVER_BASE_DIR / "silver_ad_spend",
        "required_columns": ["spend_id", "campaign_id", "spend_date", "spend_amount", "gold_processed_date"],
        "not_null_columns": ["spend_id", "campaign_id", "spend_date", "spend_amount"],
        "expected_count_match": True,
        "non_negative_columns": ["impressions", "clicks", "spend_amount"],
    },
    "fact_web_events": {
        "gold_path": GOLD_BASE_DIR / "fact_web_events",
        "source_silver_path": SILVER_BASE_DIR / "silver_web_events",
        "required_columns": ["event_id", "session_id", "customer_id", "event_type", "event_timestamp", "gold_processed_date"],
        "not_null_columns": ["event_id", "session_id", "customer_id", "event_type", "event_timestamp"],
        "expected_count_match": True,
        "non_negative_columns": ["cart_value", "api_latency_ms", "page_load_time_ms", "fraud_score"],
    },
    "mart_campaign_performance": {
        "gold_path": GOLD_BASE_DIR / "mart_campaign_performance",
        "source_silver_path": SILVER_BASE_DIR / "silver_campaigns",
        "required_columns": ["campaign_id", "campaign_name", "total_ad_spend", "total_revenue", "roas", "conversion_rate"],
        "not_null_columns": ["campaign_id", "campaign_name"],
        "expected_count_match": True,
        "non_negative_columns": ["total_ad_spend", "total_revenue", "roas", "conversion_rate"],
    },
    "mart_product_performance": {
        "gold_path": GOLD_BASE_DIR / "mart_product_performance",
        "source_silver_path": SILVER_BASE_DIR / "silver_products",
        "required_columns": ["product_id", "product_name", "total_product_revenue", "view_to_cart_rate", "cart_to_purchase_rate"],
        "not_null_columns": ["product_id", "product_name"],
        "expected_count_match": True,
        "non_negative_columns": ["total_product_revenue", "view_to_cart_rate", "cart_to_purchase_rate"],
    },
    "mart_customer_value": {
        "gold_path": GOLD_BASE_DIR / "mart_customer_value",
        "source_silver_path": SILVER_BASE_DIR / "silver_customers",
        "required_columns": ["customer_id", "email", "customer_lifetime_value", "total_orders", "avg_order_value"],
        "not_null_columns": ["customer_id", "email"],
        "expected_count_match": True,
        "non_negative_columns": ["customer_lifetime_value", "total_orders", "avg_order_value"],
    },
    "mart_marketing_funnel": {
        "gold_path": GOLD_BASE_DIR / "mart_marketing_funnel",
        "source_silver_path": None,
        "required_columns": [
            "traffic_source",
            "ab_test_group",
            "device_type",
            "total_events",
            "total_sessions",
            "page_views",
            "product_views",
            "add_to_cart_events",
            "checkout_events",
            "purchase_events",
            "session_conversion_rate",
        ],
        "not_null_columns": ["traffic_source", "ab_test_group", "device_type"],
        "expected_count_match": False,
        "minimum_rows": 1,
        "non_negative_columns": [
            "total_events",
            "total_sessions",
            "page_views",
            "product_views",
            "add_to_cart_events",
            "checkout_events",
            "purchase_events",
            "session_conversion_rate",
        ],
    },
}


# An order's total must equal the sum of its lines (both rounded to cents).
ORDER_TOTAL_TOLERANCE = 0.01
ORDER_TOTALS_CHECK = "order_totals_vs_lines"


def order_total_check(
    orders: DataFrame,
    items: DataFrame,
    bronze_items: DataFrame,
    order_amount_column: str,
) -> dict:
    """Each complete order's total equals the sum of its lines' line_amount.

    An order is complete when every distinct order_item_id that Bronze holds
    for it reached `items`. Orders that lost an item (Silver quarantine, a
    point-in-time reject) are excluded and counted, never failed. FAIL means a
    complete order whose total differs by more than ORDER_TOTAL_TOLERANCE, or
    an order with no item in Bronze at all.
    """
    bronze = (
        bronze_items
        .select(
            F.expr("try_cast(try_cast(order_id as double) as int)").alias("order_id"),
            F.expr("try_cast(try_cast(order_item_id as double) as int)").alias("order_item_id"),
        )
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .agg(
            F.countDistinct("order_item_id").alias("bronze_items"),
            # A Bronze item without a usable ID can never reach the facts.
            F.sum(F.col("order_item_id").isNull().cast("int")).alias("bronze_items_without_id"),
        )
    )
    lines = items.groupBy("order_id").agg(
        F.countDistinct("order_item_id").alias("items"),
        F.sum("line_amount").alias("line_total"),
    )
    per_order = (
        orders.select("order_id", F.col(order_amount_column).alias("order_net"))
        .join(bronze, "order_id", "left")
        .join(lines, "order_id", "left")
        .withColumn(
            "outcome",
            F.when(F.col("bronze_items").isNull(), F.lit("no_bronze_items"))
            .when(
                (F.col("bronze_items_without_id") > 0)
                | (F.coalesce(F.col("items"), F.lit(0)) != F.col("bronze_items")),
                F.lit("incomplete"),
            )
            .when(
                F.col("order_net").isNull()
                | (F.abs(F.col("order_net") - F.col("line_total")) > ORDER_TOTAL_TOLERANCE),
                F.lit("mismatched"),
            )
            .otherwise(F.lit("matched")),
        )
    )

    counts = {row["outcome"]: row["count"] for row in per_order.groupBy("outcome").count().collect()}
    mismatched_examples = [
        row["order_id"]
        for row in per_order.filter(F.col("outcome") == "mismatched").orderBy("order_id").limit(5).collect()
    ]
    mismatched = counts.get("mismatched", 0)
    no_bronze_items = counts.get("no_bronze_items", 0)

    return {
        "checked_orders": counts.get("matched", 0) + mismatched,
        "mismatched_orders": mismatched,
        "excluded_incomplete_orders": counts.get("incomplete", 0),
        "orders_without_bronze_items": no_bronze_items,
        "tolerance": ORDER_TOTAL_TOLERANCE,
        "mismatched_order_examples": mismatched_examples,
        "status": "PASS" if mismatched == 0 and no_bronze_items == 0 else "FAIL",
    }


def validate_order_totals(spark: SparkSession) -> tuple[dict, dict]:
    """Run order_total_check on Gold fact_orders and fact_order_items.

    Returns (the check's counts, a row for the Gold quality report).
    """
    paths = {
        "fact_orders": GOLD_BASE_DIR / "fact_orders",
        "fact_order_items": GOLD_BASE_DIR / "fact_order_items",
        "bronze_order_items": BRONZE_BASE_DIR / "bronze_order_items",
    }
    frames = {name: read_parquet_if_exists(spark, path) for name, path in paths.items()}
    missing = [str(paths[name]) for name, frame in frames.items() if frame is None]

    if missing:
        result = {"status": "FAIL", "error": f"missing input: {missing}"}
        failure_reason = result["error"]
    else:
        result = order_total_check(
            frames["fact_orders"], frames["fact_order_items"], frames["bronze_order_items"], "order_amount"
        )
        failure_reason = (
            ""
            if result["status"] == "PASS"
            else (
                f"{result['mismatched_orders']} order(s) whose total differs from the sum of their lines "
                f"(e.g. {result['mismatched_order_examples']}); "
                f"{result['orders_without_bronze_items']} order(s) with no item in Bronze"
            )
        )

    report_row = {
        "table_name": ORDER_TOTALS_CHECK,
        "status": "PASSED" if result["status"] == "PASS" else "FAILED",
        "gold_row_count": count_df(frames["fact_orders"]),
        "source_silver_row_count": count_df(read_parquet_if_exists(spark, SILVER_BASE_DIR / "silver_orders")),
        "missing_required_columns": "",
        "null_violations": "{}",
        "negative_violations": "{}",
        "failure_reason": failure_reason,
        "batch_date": BATCH_DATE,
    }
    return result, report_row


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2_Gold_Quality_Checks")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def read_parquet_if_exists(spark: SparkSession, path: Path | None) -> DataFrame | None:
    if path is None:
        return None

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
    results = {}

    for column_name in columns:
        if column_name in df.columns:
            results[column_name] = df.filter(col(column_name).isNull()).count()
        else:
            results[column_name] = -1

    return results


def count_negative_values(df: DataFrame, columns: list[str]) -> dict[str, int]:
    results = {}

    for column_name in columns:
        if column_name in df.columns:
            results[column_name] = df.filter(col(column_name) < 0).count()
        else:
            results[column_name] = -1

    return results


def validate_gold_table(spark: SparkSession, table_name: str, config: dict) -> dict:
    gold_df = read_parquet_if_exists(spark, config["gold_path"])
    source_silver_df = read_parquet_if_exists(spark, config.get("source_silver_path"))

    gold_count = count_df(gold_df)
    source_silver_count = count_df(source_silver_df)

    failure_reasons = []

    if gold_df is None:
        failure_reasons.append(f"missing gold path: {config['gold_path']}")

    if gold_count <= 0:
        failure_reasons.append("gold row count is zero")

    if config.get("source_silver_path") is not None and source_silver_df is None:
        failure_reasons.append(f"missing source silver path: {config['source_silver_path']}")

    if config.get("expected_count_match") and gold_count != source_silver_count:
        failure_reasons.append(
            f"row count mismatch: gold={gold_count}, source_silver={source_silver_count}"
        )

    minimum_rows = config.get("minimum_rows")
    if minimum_rows is not None and gold_count < minimum_rows:
        failure_reasons.append(
            f"gold row count below minimum threshold: actual={gold_count}, minimum={minimum_rows}"
        )

    required_missing = missing_columns(gold_df, config["required_columns"])

    if required_missing:
        failure_reasons.append(f"missing required columns: {required_missing}")

    null_violations = {}
    negative_violations = {}

    if gold_df is not None:
        null_violations = count_nulls(gold_df, config.get("not_null_columns", []))
        bad_nulls = {
            column_name: null_count
            for column_name, null_count in null_violations.items()
            if null_count != 0
        }

        if bad_nulls:
            failure_reasons.append(f"not-null violations: {bad_nulls}")

        negative_violations = count_negative_values(gold_df, config.get("non_negative_columns", []))
        bad_negatives = {
            column_name: negative_count
            for column_name, negative_count in negative_violations.items()
            if negative_count != 0
        }

        if bad_negatives:
            failure_reasons.append(f"negative metric violations: {bad_negatives}")

    status = "PASSED" if not failure_reasons else "FAILED"

    return {
        "table_name": table_name,
        "status": status,
        "gold_row_count": gold_count,
        "source_silver_row_count": source_silver_count,
        "missing_required_columns": ",".join(required_missing),
        "null_violations": str(null_violations),
        "negative_violations": str(negative_violations),
        "failure_reason": "; ".join(failure_reasons),
        "batch_date": BATCH_DATE,
    }


def main() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    print(f"spark.sql.session.timeZone={spark.conf.get('spark.sql.session.timeZone')}")

    results = []

    print("Starting Gold quality checks")
    print(f"Batch date: {BATCH_DATE}")

    for table_name, config in GOLD_TABLES.items():
        result = validate_gold_table(spark, table_name, config)
        results.append(result)

        print(
            f"{table_name}: {result['status']} "
            f"gold={result['gold_row_count']} "
            f"source_silver={result['source_silver_row_count']}"
        )

        if result["failure_reason"]:
            print(f"  reason: {result['failure_reason']}")

    order_totals, order_totals_row = validate_order_totals(spark)
    results.append(order_totals_row)
    print(
        f"{ORDER_TOTALS_CHECK}: {order_totals_row['status']} "
        + " ".join(f"{key}={value}" for key, value in order_totals.items() if key != "status")
    )
    if order_totals_row["failure_reason"]:
        print(f"  reason: {order_totals_row['failure_reason']}")

    order_totals_path = LOGS_DIR / f"gold_order_totals_report_{BATCH_DATE}.json"
    order_totals_path.write_text(
        json.dumps({"check_name": ORDER_TOTALS_CHECK, "batch_date": BATCH_DATE, **order_totals}, indent=2),
        encoding="utf-8",
    )
    print(f"Order totals report written to: {order_totals_path}")

    report_df = spark.createDataFrame(results)
    report_path = LOGS_DIR / f"gold_quality_report_{BATCH_DATE}"

    (
        report_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(report_path))
    )

    print(f"Gold quality report written to: {report_path}")

    spark.stop()

    failed = [result for result in results if result["status"] == "FAILED"]

    if failed:
        raise RuntimeError(f"Gold quality checks failed for {len(failed)} table(s).")

    print("All Gold quality checks passed.")


if __name__ == "__main__":
    main()