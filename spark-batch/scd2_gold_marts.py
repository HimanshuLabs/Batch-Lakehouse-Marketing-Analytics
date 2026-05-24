from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
LOG_DIR = PROJECT_ROOT / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)


INPUT_PATHS = {
    "fact_orders": GOLD_DIR / "fact_orders_scd2",
    "fact_order_items": GOLD_DIR / "fact_order_items_scd2",
    "fact_campaign_spend": GOLD_DIR / "fact_campaign_spend_scd2",
    "dim_customers": GOLD_DIR / "dim_customers_scd2",
    "dim_products": GOLD_DIR / "dim_products_scd2",
    "dim_campaigns": GOLD_DIR / "dim_campaigns_scd2",
    "silver_web_events": SILVER_DIR / "silver_web_events",
}

OUTPUT_PATHS = {
    "mart_campaign_performance": GOLD_DIR / "mart_campaign_performance_scd2",
    "mart_customer_lifetime_value": GOLD_DIR / "mart_customer_lifetime_value_scd2",
    "mart_product_performance": GOLD_DIR / "mart_product_performance_scd2",
    "mart_marketing_funnel": GOLD_DIR / "mart_marketing_funnel_scd2",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2-SCD2-Gold-Marts")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_required_parquet(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input path: {path}")
    return spark.read.parquet(str(path))


def read_optional_parquet(spark: SparkSession, path: Path) -> DataFrame | None:
    if not path.exists():
        return None
    return spark.read.parquet(str(path))


def safe_divide(numerator: F.Column, denominator: F.Column) -> F.Column:
    return F.when(
        denominator.isNull() | (denominator == 0),
        F.lit(0.0),
    ).otherwise(numerator / denominator)


def write_mart(df: DataFrame, output_path: Path) -> None:
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def build_campaign_performance_mart(
    fact_orders: DataFrame,
    fact_campaign_spend: DataFrame,
    dim_campaigns: DataFrame,
) -> DataFrame:
    campaign_dim = dim_campaigns.select(
        "campaign_sk",
        "campaign_id",
        "campaign_name",
        "channel",
        "target_segment",
        "budget",
        "campaign_status",
        "start_date",
        "end_date",
        "is_current",
    )

    order_metrics = (
        fact_orders
        .groupBy("campaign_sk")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.countDistinct("customer_sk").alias("customers_count"),
            F.sum(F.col("total_amount")).alias("total_revenue"),
            F.avg(F.col("total_amount")).alias("avg_order_value"),
            F.min(F.col("order_date")).alias("first_order_date"),
            F.max(F.col("order_date")).alias("last_order_date"),
        )
    )

    spend_metrics = (
        fact_campaign_spend
        .groupBy("campaign_sk")
        .agg(
            F.sum(F.col("spend_amount")).alias("total_spend"),
            F.sum(F.col("impressions")).alias("impressions"),
            F.sum(F.col("clicks")).alias("clicks"),
            F.countDistinct("spend_id").alias("spend_records_count"),
            F.min(F.col("spend_date")).alias("first_spend_date"),
            F.max(F.col("spend_date")).alias("last_spend_date"),
        )
    )

    return (
        campaign_dim
        .join(order_metrics, on="campaign_sk", how="left")
        .join(spend_metrics, on="campaign_sk", how="left")
        .fillna(
            {
                "orders_count": 0,
                "customers_count": 0,
                "total_revenue": 0.0,
                "avg_order_value": 0.0,
                "total_spend": 0.0,
                "impressions": 0,
                "clicks": 0,
                "spend_records_count": 0,
            }
        )
        .withColumn("roas", safe_divide(F.col("total_revenue"), F.col("total_spend")))
        .withColumn("ctr", safe_divide(F.col("clicks"), F.col("impressions")))
        .withColumn("cost_per_click", safe_divide(F.col("total_spend"), F.col("clicks")))
        .withColumn("cost_per_acquisition", safe_divide(F.col("total_spend"), F.col("orders_count")))
        .withColumn("revenue_per_click", safe_divide(F.col("total_revenue"), F.col("clicks")))
        .withColumn("built_at", F.current_timestamp())
    )


def build_customer_lifetime_value_mart(
    fact_orders: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    clean_fact_orders = fact_orders.drop(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "country",
        "customer_segment",
        "loyalty_tier",
    )

    customer_dim = dim_customers.select(
        "customer_sk",
        F.col("customer_id").alias("dim_customer_id"),
        "customer_name",
        "email",
        "city",
        "state",
        "country",
        "customer_segment",
        "loyalty_tier",
    )

    return (
        clean_fact_orders
        .join(
            customer_dim,
            on="customer_sk",
            how="left",
        )
        .groupBy(
            "customer_sk",
            "dim_customer_id",
            "customer_name",
            "email",
            "city",
            "state",
            "country",
            "customer_segment",
            "loyalty_tier",
        )
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum(F.col("total_amount")).alias("total_revenue"),
            F.avg(F.col("total_amount")).alias("avg_order_value"),
            F.min(F.col("order_date")).alias("first_order_date"),
            F.max(F.col("order_date")).alias("last_order_date"),
            F.countDistinct("campaign_sk").alias("campaigns_touched"),
        )
        .withColumnRenamed("dim_customer_id", "customer_id")
        .withColumn(
            "customer_lifetime_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")),
        )
        .withColumn("built_at", F.current_timestamp())
    )


def build_product_performance_mart(
    fact_order_items: DataFrame,
    dim_products: DataFrame,
) -> DataFrame:
    clean_fact_order_items = fact_order_items.drop(
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "status",
    )

    product_dim = dim_products.select(
        "product_sk",
        F.col("product_id").alias("dim_product_id"),
        "product_name",
        "category",
        "brand",
        "price",
        "status",
    )

    return (
        clean_fact_order_items
        .join(
            product_dim,
            on="product_sk",
            how="left",
        )
        .groupBy(
            "product_sk",
            "dim_product_id",
            "product_name",
            "category",
            "brand",
            "price",
            "status",
        )
        .agg(
            F.sum(F.col("quantity")).alias("units_sold"),
            F.sum(F.col("line_amount")).alias("total_revenue"),
            F.avg(F.col("unit_price")).alias("avg_selling_price"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("order_item_id").alias("order_item_count"),
            F.min(F.col("order_date")).alias("first_sold_date"),
            F.max(F.col("order_date")).alias("last_sold_date"),
        )
        .withColumnRenamed("dim_product_id", "product_id")
        .withColumn(
            "revenue_per_unit",
            safe_divide(F.col("total_revenue"), F.col("units_sold")),
        )
        .withColumn("built_at", F.current_timestamp())
    )


def normalize_web_events(web_events: DataFrame) -> DataFrame:
    result = web_events

    if "event_timestamp" not in result.columns and "event_time" in result.columns:
        result = result.withColumn("event_timestamp", F.col("event_time"))

    if "event_date" not in result.columns:
        result = result.withColumn("event_date", F.to_date(F.col("event_timestamp")))

    if "channel" not in result.columns:
        if "traffic_source" in result.columns:
            result = result.withColumn("channel", F.col("traffic_source"))
        else:
            result = result.withColumn("channel", F.lit("unknown"))

    if "campaign_id" not in result.columns:
        result = result.withColumn("campaign_id", F.lit(None).cast("string"))

    return result


def build_marketing_funnel_mart(
    web_events: DataFrame | None,
    dim_campaigns: DataFrame,
) -> DataFrame:
    if web_events is None:
        spark = dim_campaigns.sparkSession
        return spark.createDataFrame(
            [],
            "campaign_id string, campaign_sk string, campaign_name string, channel string, target_segment string, campaign_status string, page_views long, product_views long, add_to_cart long, checkout long, purchases long, sessions_count long, users_count long, view_to_cart_rate double, cart_to_purchase_rate double, overall_conversion_rate double, built_at timestamp",
        )

    normalized = normalize_web_events(web_events)

    current_campaigns = dim_campaigns.filter(F.col("is_current") == True).select(
        "campaign_id",
        "campaign_sk",
        "campaign_name",
        "target_segment",
        "campaign_status",
    )

    return (
        normalized
        .join(current_campaigns, on="campaign_id", how="left")
        .groupBy(
            "campaign_id",
            "campaign_sk",
            "campaign_name",
            "channel",
            "target_segment",
            "campaign_status",
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
            F.sum(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("checkout"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.countDistinct("session_id").alias("sessions_count"),
            F.countDistinct("user_id").alias("users_count"),
            F.min(F.col("event_timestamp")).alias("first_event_timestamp"),
            F.max(F.col("event_timestamp")).alias("last_event_timestamp"),
        )
        .withColumn("view_to_cart_rate", safe_divide(F.col("add_to_cart"), F.col("page_views")))
        .withColumn("cart_to_purchase_rate", safe_divide(F.col("purchases"), F.col("add_to_cart")))
        .withColumn("overall_conversion_rate", safe_divide(F.col("purchases"), F.col("page_views")))
        .withColumn("built_at", F.current_timestamp())
    )


def get_sum(df: DataFrame, column: str) -> float:
    value = df.agg(F.sum(F.col(column)).alias("value")).collect()[0]["value"]
    return float(value or 0.0)


def get_count(df: DataFrame) -> int:
    return int(df.count())


def validate_marts(
    fact_orders: DataFrame,
    fact_order_items: DataFrame,
    fact_campaign_spend: DataFrame,
    mart_campaign_performance: DataFrame,
    mart_customer_lifetime_value: DataFrame,
    mart_product_performance: DataFrame,
    mart_marketing_funnel: DataFrame,
) -> Dict[str, object]:
    fact_order_revenue = get_sum(fact_orders, "total_amount")
    campaign_mart_revenue = get_sum(mart_campaign_performance, "total_revenue")
    clv_mart_revenue = get_sum(mart_customer_lifetime_value, "total_revenue")

    fact_item_revenue = get_sum(fact_order_items, "line_amount")
    product_mart_revenue = get_sum(mart_product_performance, "total_revenue")

    fact_spend = get_sum(fact_campaign_spend, "spend_amount")
    campaign_mart_spend = get_sum(mart_campaign_performance, "total_spend")

    tolerance = 0.01

    checks = {
        "campaign_revenue_reconciles_to_fact_orders": abs(fact_order_revenue - campaign_mart_revenue) <= tolerance,
        "clv_revenue_reconciles_to_fact_orders": abs(fact_order_revenue - clv_mart_revenue) <= tolerance,
        "product_revenue_reconciles_to_fact_order_items": abs(fact_item_revenue - product_mart_revenue) <= tolerance,
        "campaign_spend_reconciles_to_fact_campaign_spend": abs(fact_spend - campaign_mart_spend) <= tolerance,
        "campaign_mart_has_rows": get_count(mart_campaign_performance) > 0,
        "clv_mart_has_rows": get_count(mart_customer_lifetime_value) > 0,
        "product_mart_has_rows": get_count(mart_product_performance) > 0,
        "funnel_mart_has_rows": get_count(mart_marketing_funnel) > 0,
    }

    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "metrics": {
            "fact_order_revenue": fact_order_revenue,
            "campaign_mart_revenue": campaign_mart_revenue,
            "clv_mart_revenue": clv_mart_revenue,
            "fact_item_revenue": fact_item_revenue,
            "product_mart_revenue": product_mart_revenue,
            "fact_campaign_spend": fact_spend,
            "campaign_mart_spend": campaign_mart_spend,
            "campaign_mart_rows": get_count(mart_campaign_performance),
            "clv_mart_rows": get_count(mart_customer_lifetime_value),
            "product_mart_rows": get_count(mart_product_performance),
            "funnel_mart_rows": get_count(mart_marketing_funnel),
        },
    }


def main() -> None:
    spark = create_spark_session()

    report: Dict[str, object] = {
        "job_name": "scd2_gold_marts",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "marts": {},
    }

    overall_status = "PASS"

    try:
        print("Reading SCD2-aware Gold facts and dimensions")

        fact_orders = read_required_parquet(spark, INPUT_PATHS["fact_orders"])
        fact_order_items = read_required_parquet(spark, INPUT_PATHS["fact_order_items"])
        fact_campaign_spend = read_required_parquet(spark, INPUT_PATHS["fact_campaign_spend"])

        dim_customers = read_required_parquet(spark, INPUT_PATHS["dim_customers"])
        dim_products = read_required_parquet(spark, INPUT_PATHS["dim_products"])
        dim_campaigns = read_required_parquet(spark, INPUT_PATHS["dim_campaigns"])

        web_events = read_optional_parquet(spark, INPUT_PATHS["silver_web_events"])

        print("Building mart_campaign_performance_scd2")
        mart_campaign_performance = build_campaign_performance_mart(
            fact_orders=fact_orders,
            fact_campaign_spend=fact_campaign_spend,
            dim_campaigns=dim_campaigns,
        )

        print("Building mart_customer_lifetime_value_scd2")
        mart_customer_lifetime_value = build_customer_lifetime_value_mart(
            fact_orders=fact_orders,
            dim_customers=dim_customers,
        )

        print("Building mart_product_performance_scd2")
        mart_product_performance = build_product_performance_mart(
            fact_order_items=fact_order_items,
            dim_products=dim_products,
        )

        print("Building mart_marketing_funnel_scd2")
        mart_marketing_funnel = build_marketing_funnel_mart(
            web_events=web_events,
            dim_campaigns=dim_campaigns,
        )

        validation = validate_marts(
            fact_orders=fact_orders,
            fact_order_items=fact_order_items,
            fact_campaign_spend=fact_campaign_spend,
            mart_campaign_performance=mart_campaign_performance,
            mart_customer_lifetime_value=mart_customer_lifetime_value,
            mart_product_performance=mart_product_performance,
            mart_marketing_funnel=mart_marketing_funnel,
        )

        if validation["status"] != "PASS":
            overall_status = "FAIL"

        write_mart(mart_campaign_performance, OUTPUT_PATHS["mart_campaign_performance"])
        write_mart(mart_customer_lifetime_value, OUTPUT_PATHS["mart_customer_lifetime_value"])
        write_mart(mart_product_performance, OUTPUT_PATHS["mart_product_performance"])
        write_mart(mart_marketing_funnel, OUTPUT_PATHS["mart_marketing_funnel"])

        report["marts"] = {
            "mart_campaign_performance_scd2": {
                "output_path": str(OUTPUT_PATHS["mart_campaign_performance"]),
                "rows": get_count(mart_campaign_performance),
            },
            "mart_customer_lifetime_value_scd2": {
                "output_path": str(OUTPUT_PATHS["mart_customer_lifetime_value"]),
                "rows": get_count(mart_customer_lifetime_value),
            },
            "mart_product_performance_scd2": {
                "output_path": str(OUTPUT_PATHS["mart_product_performance"]),
                "rows": get_count(mart_product_performance),
            },
            "mart_marketing_funnel_scd2": {
                "output_path": str(OUTPUT_PATHS["mart_marketing_funnel"]),
                "rows": get_count(mart_marketing_funnel),
            },
        }

        report["validation"] = validation

        print(f"mart_campaign_performance_scd2: rows={get_count(mart_campaign_performance)}")
        print(f"mart_customer_lifetime_value_scd2: rows={get_count(mart_customer_lifetime_value)}")
        print(f"mart_product_performance_scd2: rows={get_count(mart_product_performance)}")
        print(f"mart_marketing_funnel_scd2: rows={get_count(mart_marketing_funnel)}")
        print(f"Validation status: {validation['status']}")

    except Exception as exc:
        overall_status = "FAIL"
        report["error"] = str(exc)
        raise

    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["overall_status"] = overall_status

        report_path = LOG_DIR / "scd2_gold_mart_report.json"
        with report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

        print(f"SCD2 Gold mart report written to: {report_path}")
        print(f"Overall status: {overall_status}")

        spark.stop()

    if overall_status != "PASS":
        raise SystemExit(
            "SCD2 Gold mart validation failed. "
            "Check logs/scd2_gold_mart_report.json"
        )


if __name__ == "__main__":
    main()
