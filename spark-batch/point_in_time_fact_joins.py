from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


SOURCE_PATHS = {
    "orders": [
        SILVER_DIR / "silver_orders",
        SILVER_DIR / "orders",
    ],
    "order_items": [
        SILVER_DIR / "silver_order_items",
        SILVER_DIR / "order_items",
    ],
    "ad_spend": [
        SILVER_DIR / "silver_ad_spend",
        SILVER_DIR / "ad_spend",
    ],
}

DIMENSION_PATHS = {
    "customers": GOLD_DIR / "dim_customers_scd2",
    "products": GOLD_DIR / "dim_products_scd2",
    "campaigns": GOLD_DIR / "dim_campaigns_scd2",
}

OUTPUT_PATHS = {
    "fact_orders": GOLD_DIR / "fact_orders_scd2",
    "fact_order_items": GOLD_DIR / "fact_order_items_scd2",
    "fact_campaign_spend": GOLD_DIR / "fact_campaign_spend_scd2",
}

# Every fact key is resolved into exactly one of these. Only in_range facts
# are valid; the others are rejected with an exact per-key reason.
#   in_range              a dimension version covers the fact time
#   no_key                the fact key is NULL
#   key_not_in_dimension  the key is in no dimension version
#   no_version_for_time   the key exists but no version covers the fact time.
#                         Versions run from 1900-01-01 to 9999-12-31 without
#                         gaps, so this means a corrupt dimension: the job fails.
RESOLUTIONS = ["in_range", "no_key", "key_not_in_dimension", "no_version_for_time"]

ORDER_NOT_IN_FACT_REASON = "order not in fact_orders_scd2"

QUARANTINE_PATHS = {
    "fact_orders": QUARANTINE_DIR / "fact_orders_scd2_rejects",
    "fact_order_items": QUARANTINE_DIR / "fact_order_items_scd2_rejects",
    "fact_campaign_spend": QUARANTINE_DIR / "fact_campaign_spend_scd2_rejects",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2-Point-In-Time-Fact-Joins")
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


def read_parquet(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required path: {path}")
    return spark.read.parquet(str(path))


def first_existing_column(df: DataFrame, candidates: List[str]) -> str | None:
    available = set(df.columns)
    for column in candidates:
        if column in available:
            return column
    return None


def add_column_from_candidates(
    df: DataFrame,
    target_column: str,
    source_candidates: List[str],
    default_value=None,
) -> DataFrame:
    if target_column in df.columns:
        return df

    for source_column in source_candidates:
        if source_column in df.columns:
            return df.withColumn(target_column, F.col(source_column))

    return df.withColumn(target_column, F.lit(default_value))


def normalize_orders(df: DataFrame) -> DataFrame:
    result = df

    result = add_column_from_candidates(result, "order_id", ["event_id"])
    result = add_column_from_candidates(result, "customer_id", ["user_id"])
    result = add_column_from_candidates(result, "order_date", ["order_timestamp", "event_timestamp", "event_time", "processed_timestamp", "ingestion_timestamp"])
    result = add_column_from_candidates(result, "campaign_id", ["campaign"])
    result = add_column_from_candidates(result, "total_amount", ["cart_value", "discounted_price"], 0.0)
    result = add_column_from_candidates(result, "order_status", ["event_type"], "completed")
    result = add_column_from_candidates(result, "payment_method", [], "unknown")
    result = add_column_from_candidates(result, "region", ["state", "home_state", "city", "home_city"], "unknown")

    result = result.withColumn("order_date", F.to_timestamp(F.col("order_date")))
    result = result.withColumn("total_amount", F.col("total_amount").cast("double"))

    return result.filter(F.col("order_id").isNotNull() & F.col("order_date").isNotNull())


def normalize_order_items(df: DataFrame) -> DataFrame:
    result = df

    result = add_column_from_candidates(result, "order_id", ["event_id"])
    result = add_column_from_candidates(result, "product_id", ["product"])
    result = add_column_from_candidates(result, "quantity", [], 1)
    result = add_column_from_candidates(result, "unit_price", ["discounted_price", "original_price"], 0.0)

    if "order_item_id" not in result.columns:
        result = result.withColumn(
            "order_item_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("order_id").cast("string"),
                    F.col("product_id").cast("string"),
                    F.col("quantity").cast("string"),
                    F.col("unit_price").cast("string"),
                ),
                256,
            ),
        )

    result = result.withColumn("quantity", F.col("quantity").cast("long"))
    result = result.withColumn("unit_price", F.col("unit_price").cast("double"))

    if "line_amount" not in result.columns:
        result = result.withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
    else:
        result = result.withColumn("line_amount", F.col("line_amount").cast("double"))

    return result.filter(
        F.col("order_item_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
    )


def normalize_ad_spend(df: DataFrame) -> DataFrame:
    result = df

    result = add_column_from_candidates(result, "spend_id", ["event_id"])
    result = add_column_from_candidates(result, "campaign_id", ["campaign"])
    result = add_column_from_candidates(result, "spend_date", ["event_timestamp", "event_time"])
    result = add_column_from_candidates(result, "channel", ["traffic_source"], "unknown")
    result = add_column_from_candidates(result, "impressions", [], 0)
    result = add_column_from_candidates(result, "clicks", [], 0)
    result = add_column_from_candidates(result, "spend_amount", ["total_amount", "cart_value"], 0.0)

    result = result.withColumn("spend_date", F.to_timestamp(F.col("spend_date")))
    result = result.withColumn("impressions", F.col("impressions").cast("long"))
    result = result.withColumn("clicks", F.col("clicks").cast("long"))
    result = result.withColumn("spend_amount", F.col("spend_amount").cast("double"))

    return result.filter(F.col("spend_id").isNotNull() & F.col("spend_date").isNotNull())


def resolution_column(surrogate_key: str) -> str:
    return f"{surrogate_key}_resolution"


def point_in_time_join(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_key: str,
    dim_key: str,
    fact_timestamp: str,
    surrogate_key: str,
) -> DataFrame:
    # Only a version whose range covers the fact time is used; there is no
    # fallback to the current version. <surrogate_key>_resolution records
    # how the key resolved (see RESOLUTIONS).
    prepared_fact = fact_df.drop(surrogate_key)

    prepared_dim = dim_df.select(
        F.col(dim_key).alias(f"dim_{dim_key}"),
        F.col(surrogate_key),
        F.col("effective_from"),
        F.col("effective_to"),
    )

    known_keys = dim_df.select(F.col(dim_key).alias(f"known_{dim_key}")).distinct()

    historical_join = (
        prepared_fact.alias("f")
        .join(
            prepared_dim.alias("d"),
            (
                (F.col(f"f.{fact_key}") == F.col(f"d.dim_{dim_key}"))
                & (F.col(f"f.{fact_timestamp}") >= F.col("d.effective_from"))
                & (F.col(f"f.{fact_timestamp}") < F.col("d.effective_to"))
            ),
            "left",
        )
        .select("f.*", F.col(f"d.{surrogate_key}").alias(surrogate_key))
    )

    return (
        historical_join.alias("f")
        .join(
            known_keys.alias("k"),
            F.col(f"f.{fact_key}") == F.col(f"k.known_{dim_key}"),
            "left",
        )
        .withColumn(
            resolution_column(surrogate_key),
            F.when(F.col(f"f.{fact_key}").isNull(), F.lit("no_key"))
            .when(F.col(surrogate_key).isNotNull(), F.lit("in_range"))
            .when(F.col(f"k.known_{dim_key}").isNotNull(), F.lit("no_version_for_time"))
            .otherwise(F.lit("key_not_in_dimension")),
        )
        .drop(f"known_{dim_key}")
    )


def resolution_reason(surrogate_key: str, fact_key: str, fact_timestamp: str, dim_name: str):
    resolution = F.col(resolution_column(surrogate_key))
    return (
        F.when(resolution == "no_key", F.lit(f"{fact_key} is NULL"))
        .when(resolution == "key_not_in_dimension", F.lit(f"{fact_key} not in {dim_name}"))
        .when(resolution == "no_version_for_time", F.lit(f"no {dim_name} version covers {fact_timestamp}"))
    )


def resolution_counts(df: DataFrame, surrogate_key: str) -> Dict[str, int]:
    counts = {resolution: 0 for resolution in RESOLUTIONS}
    for row in df.groupBy(resolution_column(surrogate_key)).count().collect():
        counts[row[0]] = row["count"]
    return counts


def write_table(df: DataFrame, output_path: Path, partition_column: str | None = None) -> None:
    writer = df.write.mode("overwrite")

    if partition_column and partition_column in df.columns:
        writer = writer.partitionBy(partition_column)

    writer.parquet(str(output_path))


def split_valid_and_rejected_facts(
    df: DataFrame,
    resolved_keys: List[tuple[str, str, str, str]],
) -> tuple[DataFrame, DataFrame]:
    # resolved_keys: (surrogate_key, fact_key, fact_timestamp, dim_name).
    # Valid facts have every key in_range and keep their original schema;
    # rejects keep the resolution columns and name every failing key.
    resolution_columns = [resolution_column(key[0]) for key in resolved_keys]

    all_in_range = F.lit(True)
    for column in resolution_columns:
        all_in_range = all_in_range & (F.col(column) == "in_range")

    valid = df.filter(all_in_range).drop(*resolution_columns)

    rejected = df.filter(~all_in_range).withColumn(
        "reject_reason",
        F.concat_ws("; ", *[resolution_reason(*key) for key in resolved_keys]),
    )

    return valid, rejected


def conservation(silver_rows: int, valid_rows: int, rejected_rows: int) -> Dict[str, object]:
    unaccounted = silver_rows - valid_rows - rejected_rows
    return {
        "silver_rows": silver_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "unaccounted_rows": unaccounted,
        "status": "PASS" if unaccounted == 0 else "FAIL",
    }


def validate_fact_table(
    df: DataFrame,
    fact_name: str,
    primary_key: str,
    required_surrogate_keys: List[str],
) -> Dict[str, object]:
    total_rows = df.count()

    duplicate_primary_keys = (
        df.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
        if primary_key in df.columns
        else total_rows
    )

    null_checks = {
        key: df.filter(F.col(key).isNull()).count()
        for key in required_surrogate_keys
        if key in df.columns
    }

    missing_required_columns = [
        key for key in required_surrogate_keys if key not in df.columns
    ]

    status = (
        "PASS"
        if total_rows > 0
        and duplicate_primary_keys == 0
        and not missing_required_columns
        and all(value == 0 for value in null_checks.values())
        else "FAIL"
    )

    return {
        "fact_name": fact_name,
        "status": status,
        "total_rows": total_rows,
        "primary_key": primary_key,
        "duplicate_primary_keys": duplicate_primary_keys,
        "required_surrogate_keys": required_surrogate_keys,
        "missing_required_columns": missing_required_columns,
        "null_surrogate_key_counts": null_checks,
    }


def main() -> None:
    spark = create_spark_session()
    print(f"spark.sql.session.timeZone={spark.conf.get('spark.sql.session.timeZone')}")

    report: Dict[str, object] = {
        "job_name": "point_in_time_fact_joins",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "facts": {},
    }

    overall_status = "PASS"

    try:
        orders_source_path = resolve_existing_path(SOURCE_PATHS["orders"])
        order_items_source_path = resolve_existing_path(SOURCE_PATHS["order_items"])
        ad_spend_source_path = resolve_existing_path(SOURCE_PATHS["ad_spend"])

        print(f"Reading orders from: {orders_source_path}")
        print(f"Reading order_items from: {order_items_source_path}")
        print(f"Reading ad_spend from: {ad_spend_source_path}")

        orders_silver = read_parquet(spark, orders_source_path)
        order_items_silver = read_parquet(spark, order_items_source_path)
        ad_spend_silver = read_parquet(spark, ad_spend_source_path)

        silver_rows = {
            "fact_orders_scd2": orders_silver.count(),
            "fact_order_items_scd2": order_items_silver.count(),
            "fact_campaign_spend_scd2": ad_spend_silver.count(),
        }

        orders = normalize_orders(orders_silver)
        order_items = normalize_order_items(order_items_silver)
        ad_spend = normalize_ad_spend(ad_spend_silver)

        customers_dim = read_parquet(spark, DIMENSION_PATHS["customers"])
        products_dim = read_parquet(spark, DIMENSION_PATHS["products"])
        campaigns_dim = read_parquet(spark, DIMENSION_PATHS["campaigns"])

        print("Building fact_orders_scd2")

        fact_orders = point_in_time_join(
            fact_df=orders,
            dim_df=customers_dim,
            fact_key="customer_id",
            dim_key="customer_id",
            fact_timestamp="order_date",
            surrogate_key="customer_sk",
        )

        fact_orders = point_in_time_join(
            fact_df=fact_orders,
            dim_df=campaigns_dim,
            fact_key="campaign_id",
            dim_key="campaign_id",
            fact_timestamp="order_date",
            surrogate_key="campaign_sk",
        )

        fact_orders = fact_orders.withColumn("order_year", F.year(F.col("order_date"))).cache()

        orders_resolutions = {
            "customer_sk": resolution_counts(fact_orders, "customer_sk"),
            "campaign_sk": resolution_counts(fact_orders, "campaign_sk"),
        }

        valid_fact_orders, rejected_fact_orders = split_valid_and_rejected_facts(
            fact_orders,
            resolved_keys=[
                ("customer_sk", "customer_id", "order_date", "dim_customers_scd2"),
                ("campaign_sk", "campaign_id", "order_date", "dim_campaigns_scd2"),
            ],
        )

        orders_validation = validate_fact_table(
            valid_fact_orders,
            fact_name="fact_orders_scd2",
            primary_key="order_id",
            required_surrogate_keys=["customer_sk", "campaign_sk"],
        )

        write_table(
            valid_fact_orders,
            OUTPUT_PATHS["fact_orders"],
            partition_column="order_year",
        )

        write_table(
            rejected_fact_orders,
            QUARANTINE_PATHS["fact_orders"],
            partition_column="order_year",
        )

        print(
            f"fact_orders_scd2: "
            f"status={orders_validation['status']}, "
            f"rows={orders_validation['total_rows']}"
        )

        print("Building fact_order_items_scd2")

        orders_for_item_dates = valid_fact_orders.select(
            "order_id",
            "order_date",
        ).dropDuplicates(["order_id"])

        order_items_joined = order_items.join(
            orders_for_item_dates,
            on="order_id",
            how="left",
        ).cache()

        # Items whose order is not a valid fact are rejected, not dropped.
        order_items_with_dates = order_items_joined.filter(F.col("order_date").isNotNull())
        order_items_without_order = (
            order_items_joined
            .filter(F.col("order_date").isNull())
            .withColumn("order_year", F.year(F.col("order_date")))
            .withColumn("reject_reason", F.lit(ORDER_NOT_IN_FACT_REASON))
        )

        fact_order_items = point_in_time_join(
            fact_df=order_items_with_dates,
            dim_df=products_dim,
            fact_key="product_id",
            dim_key="product_id",
            fact_timestamp="order_date",
            surrogate_key="product_sk",
        )

        fact_order_items = fact_order_items.withColumn(
            "order_year",
            F.year(F.col("order_date")),
        ).cache()

        order_items_resolutions = {
            "order": {
                "valid_order": order_items_with_dates.count(),
                "order_not_in_fact_orders_scd2": order_items_without_order.count(),
            },
            "product_sk": resolution_counts(fact_order_items, "product_sk"),
        }

        valid_fact_order_items, rejected_fact_order_items = split_valid_and_rejected_facts(
            fact_order_items,
            resolved_keys=[("product_sk", "product_id", "order_date", "dim_products_scd2")],
        )
        rejected_fact_order_items = rejected_fact_order_items.unionByName(
            order_items_without_order,
            allowMissingColumns=True,
        )

        order_items_validation = validate_fact_table(
            valid_fact_order_items,
            fact_name="fact_order_items_scd2",
            primary_key="order_item_id",
            required_surrogate_keys=["product_sk"],
        )

        write_table(
            valid_fact_order_items,
            OUTPUT_PATHS["fact_order_items"],
            partition_column="order_year",
        )

        write_table(
            rejected_fact_order_items,
            QUARANTINE_PATHS["fact_order_items"],
            partition_column="order_year",
        )

        print(
            f"fact_order_items_scd2: "
            f"status={order_items_validation['status']}, "
            f"rows={order_items_validation['total_rows']}"
        )

        print("Building fact_campaign_spend_scd2")

        fact_campaign_spend = point_in_time_join(
            fact_df=ad_spend,
            dim_df=campaigns_dim,
            fact_key="campaign_id",
            dim_key="campaign_id",
            fact_timestamp="spend_date",
            surrogate_key="campaign_sk",
        )

        fact_campaign_spend = fact_campaign_spend.withColumn(
            "spend_year",
            F.year(F.col("spend_date")),
        ).cache()

        campaign_spend_resolutions = {
            "campaign_sk": resolution_counts(fact_campaign_spend, "campaign_sk"),
        }

        valid_fact_campaign_spend, rejected_fact_campaign_spend = split_valid_and_rejected_facts(
            fact_campaign_spend,
            resolved_keys=[("campaign_sk", "campaign_id", "spend_date", "dim_campaigns_scd2")],
        )

        campaign_spend_validation = validate_fact_table(
            valid_fact_campaign_spend,
            fact_name="fact_campaign_spend_scd2",
            primary_key="spend_id",
            required_surrogate_keys=["campaign_sk"],
        )

        write_table(
            valid_fact_campaign_spend,
            OUTPUT_PATHS["fact_campaign_spend"],
            partition_column="spend_year",
        )

        write_table(
            rejected_fact_campaign_spend,
            QUARANTINE_PATHS["fact_campaign_spend"],
            partition_column="spend_year",
        )

        print(
            f"fact_campaign_spend_scd2: "
            f"status={campaign_spend_validation['status']}, "
            f"rows={campaign_spend_validation['total_rows']}"
        )

        validations = [
            orders_validation,
            order_items_validation,
            campaign_spend_validation,
        ]

        if any(validation["status"] != "PASS" for validation in validations):
            overall_status = "FAIL"

        facts = {
            "fact_orders_scd2": (
                orders_source_path, "fact_orders", orders_validation,
                rejected_fact_orders, orders_resolutions,
            ),
            "fact_order_items_scd2": (
                order_items_source_path, "fact_order_items", order_items_validation,
                rejected_fact_order_items, order_items_resolutions,
            ),
            "fact_campaign_spend_scd2": (
                ad_spend_source_path, "fact_campaign_spend", campaign_spend_validation,
                rejected_fact_campaign_spend, campaign_spend_resolutions,
            ),
        }

        for fact_name, (source_path, output_key, validation, rejected, resolutions) in facts.items():
            rejected_rows = rejected.count()
            fact_conservation = conservation(
                silver_rows[fact_name],
                validation["total_rows"],
                rejected_rows,
            )
            corrupt_dimension_rows = sum(
                counts.get("no_version_for_time", 0) for counts in resolutions.values()
            )

            report["facts"][fact_name] = {
                "source_path": str(source_path),
                "output_path": str(OUTPUT_PATHS[output_key]),
                "validation": validation,
                "rejected_path": str(QUARANTINE_PATHS[output_key]),
                "rejected_rows": rejected_rows,
                "resolutions": resolutions,
                "conservation": fact_conservation,
            }

            print(
                f"{fact_name}: silver={fact_conservation['silver_rows']} "
                f"valid={fact_conservation['valid_rows']} "
                f"rejected={rejected_rows} "
                f"unaccounted={fact_conservation['unaccounted_rows']} "
                f"resolutions={resolutions}"
            )

            if fact_conservation["status"] != "PASS":
                overall_status = "FAIL"
                print(f"  FAIL: {fact_name} conservation: silver != valid + rejected")

            if corrupt_dimension_rows:
                overall_status = "FAIL"
                print(
                    f"  FAIL: {fact_name}: {corrupt_dimension_rows} fact key(s) exist in "
                    "their dimension but no version covers the fact time (corrupt dimension)"
                )

    except Exception as exc:
        overall_status = "FAIL"
        report["error"] = str(exc)
        raise

    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["overall_status"] = overall_status

        report_path = LOG_DIR / "point_in_time_fact_join_report.json"
        with report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

        print(f"Point-in-time fact join report written to: {report_path}")
        print(f"Overall status: {overall_status}")

        spark.stop()

    if overall_status != "PASS":
        raise SystemExit(
            "Point-in-time fact join validation failed. "
            "Check logs/point_in_time_fact_join_report.json"
        )


if __name__ == "__main__":
    main()
