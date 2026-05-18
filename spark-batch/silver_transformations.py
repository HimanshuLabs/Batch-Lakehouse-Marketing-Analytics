from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    row_number,
    sha2,
    concat_ws,
    to_date,
    to_timestamp,
    trim,
    lower,
    when,
    regexp_extract,
    expr,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_BASE_DIR = PROJECT_ROOT / "data" / "silver"
QUARANTINE_BASE_DIR = PROJECT_ROOT / "data" / "quarantine"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.today().isoformat()


def safe_int(column_name: str):
    return expr(f"try_cast(try_cast(`{column_name}` as double) as int)")


def safe_long(column_name: str):
    return expr(f"try_cast(try_cast(`{column_name}` as double) as bigint)")


def safe_double(column_name: str):
    return expr(f"try_cast(`{column_name}` as double)")


def safe_boolean(column_name: str):
    return expr(f"try_cast(`{column_name}` as boolean)")


def safe_timestamp(column_name: str):
    return expr(f"try_cast(`{column_name}` as timestamp)")


def safe_date(column_name: str):
    return expr(f"try_cast(`{column_name}` as date)")


def create_spark_session() -> SparkSession:
    checkpoint_dir = PROJECT_ROOT / "checkpoints" / "silver"

    return (
        SparkSession.builder
        .appName("Project2_Silver_Transformations")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.debug.maxToStringFields", "200")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, table_name: str) -> DataFrame:
    path = BRONZE_BASE_DIR / f"bronze_{table_name}"

    if not path.exists():
        raise FileNotFoundError(f"Missing Bronze table: {path}")

    return spark.read.parquet(str(path))


def write_table(df: DataFrame, output_path: Path) -> None:
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("processed_date")
        .parquet(str(output_path))
    )


def add_silver_metadata(df: DataFrame, table_name: str) -> DataFrame:
    business_columns = [
        column
        for column in df.columns
        if column not in {
            "ingestion_timestamp",
            "ingestion_date",
            "batch_id",
            "source_system",
            "source_file_name",
            "record_hash",
        }
    ]

    return (
        df
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("processed_date", to_date(lit(BATCH_DATE)))
        .withColumn("silver_table", lit(table_name))
        .withColumn(
            "silver_record_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in business_columns]), 256),
        )
    )


def split_valid_invalid(df: DataFrame, invalid_condition, reason: str) -> tuple[DataFrame, DataFrame]:
    valid_df = df.filter(~invalid_condition)
    invalid_df = df.filter(invalid_condition).withColumn("quarantine_reason", lit(reason))
    return valid_df, invalid_df


def deduplicate_latest(df: DataFrame, key_columns: list[str], order_column: str) -> DataFrame:
    window_spec = Window.partitionBy(*key_columns).orderBy(col(order_column).desc_nulls_last())

    return (
        df
        .withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def clean_customers(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "customers")

    cleaned = (
        df
        .withColumn("user_id", safe_int("user_id"))
        .withColumn("user_name", trim(col("user_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("gender", trim(col("gender")))
        .withColumn("age", safe_int("age"))
        .withColumn("membership_tier", trim(col("membership_tier")))
        .withColumn("loyalty_points", safe_int("loyalty_points"))
        .withColumn("preferred_language", trim(col("preferred_language")))
        .withColumn("home_city", trim(col("home_city")))
        .withColumn("home_state", trim(col("home_state")))
        .withColumn("country", trim(col("country")))
        .withColumn("user_segment", trim(col("user_segment")))
        .withColumn("is_prime_user", safe_boolean("is_prime_user"))
        .withColumn("updated_at", safe_timestamp("updated_at"))
    )

    invalid_condition = (
        col("user_id").isNull()
        | col("email").isNull()
        | (~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
        | col("age").isNull()
        | (col("age") < 18)
        | (col("age") > 100)
        | col("home_city").isNull()
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid customer record: bad id/email/age/city",
    )

    valid = deduplicate_latest(valid, ["user_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_customers")
    invalid = add_silver_metadata(invalid, "quarantine_customers")

    return valid, invalid


def clean_products(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "products")

    cleaned = (
        df
        .withColumn("product_id", safe_int("product_id"))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("original_price", safe_double("original_price"))
        .withColumn("discount_percent", safe_double("discount_percent"))
        .withColumn("discounted_price", safe_double("discounted_price"))
        .withColumn("inventory_remaining", safe_int("inventory_remaining"))
        .withColumn("updated_at", safe_timestamp("updated_at"))
    )

    invalid_condition = (
        col("product_id").isNull()
        | col("product_name").isNull()
        | col("category").isNull()
        | col("original_price").isNull()
        | (col("original_price") <= 0)
        | col("discount_percent").isNull()
        | (col("discount_percent") < 0)
        | (col("discount_percent") > 100)
        | col("discounted_price").isNull()
        | (col("discounted_price") < 0)
        | col("inventory_remaining").isNull()
        | (col("inventory_remaining") < 0)
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid product record: bad id/category/price/discount/inventory",
    )

    valid = deduplicate_latest(valid, ["product_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_products")
    invalid = add_silver_metadata(invalid, "quarantine_products")

    return valid, invalid


def clean_campaigns(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "campaigns")

    cleaned = (
        df
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("campaign_name", trim(col("campaign_name")))
        .withColumn("traffic_source", lower(trim(col("traffic_source"))))
        .withColumn("ab_test_group", trim(col("ab_test_group")))
        .withColumn("target_segment", trim(col("target_segment")))
        .withColumn("budget", safe_double("budget"))
        .withColumn("campaign_status", lower(trim(col("campaign_status"))))
        .withColumn("start_date", safe_date("start_date"))
        .withColumn("end_date", safe_date("end_date"))
        .withColumn("updated_at", safe_timestamp("updated_at"))
    )

    valid_sources = ["google", "facebook", "instagram", "email", "organic", "affiliate"]
    valid_statuses = ["planned", "active", "paused", "completed"]

    invalid_condition = (
        col("campaign_id").isNull()
        | col("campaign_name").isNull()
        | (~col("traffic_source").isin(valid_sources))
        | col("budget").isNull()
        | (col("budget") <= 0)
        | (~col("campaign_status").isin(valid_statuses))
        | col("start_date").isNull()
        | col("end_date").isNull()
        | (col("end_date") < col("start_date"))
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid campaign record: bad id/source/budget/status/date range",
    )

    valid = deduplicate_latest(valid, ["campaign_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_campaigns")
    invalid = add_silver_metadata(invalid, "quarantine_campaigns")

    return valid, invalid


def clean_orders(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "orders")

    cleaned = (
        df
        .withColumn("order_id", safe_int("order_id"))
        .withColumn("user_id", safe_int("user_id"))
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("order_timestamp", safe_timestamp("order_timestamp"))
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("cart_value", safe_double("cart_value"))
        .withColumn("fraud_score", safe_double("fraud_score"))
        .withColumn("country", trim(col("country")))
        .withColumn("city", trim(col("city")))
    )

    invalid_condition = (
        col("order_id").isNull()
        | col("user_id").isNull()
        | col("order_timestamp").isNull()
        | col("payment_method").isNull()
        | col("cart_value").isNull()
        | (col("cart_value") <= 0)
        | col("fraud_score").isNull()
        | (col("fraud_score") < 0)
        | (col("fraud_score") > 1)
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid order record: bad id/timestamp/payment/cart/fraud score",
    )

    valid = deduplicate_latest(valid, ["order_id"], "order_timestamp")
    valid = add_silver_metadata(valid, "silver_orders")
    invalid = add_silver_metadata(invalid, "quarantine_orders")

    return valid, invalid


def clean_order_items(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "order_items")

    cleaned = (
        df
        .withColumn("order_item_id", safe_int("order_item_id"))
        .withColumn("order_id", safe_int("order_id"))
        .withColumn("product_id", safe_int("product_id"))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("quantity", safe_int("quantity"))
        .withColumn("original_price", safe_double("original_price"))
        .withColumn("discount_percent", safe_double("discount_percent"))
        .withColumn("discounted_price", safe_double("discounted_price"))
        .withColumn("line_amount", col("line_amount").cast(DoubleType()))
        .withColumn(
            "expected_line_amount",
            (col("quantity") * col("discounted_price")).cast(DoubleType()),
        )
    )

    invalid_condition = (
        col("order_item_id").isNull()
        | col("order_id").isNull()
        | col("product_id").isNull()
        | col("quantity").isNull()
        | (col("quantity") <= 0)
        | col("discounted_price").isNull()
        | (col("discounted_price") < 0)
        | col("line_amount").isNull()
        | (col("line_amount") <= 0)
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid order item record: bad id/quantity/price/line amount",
    )

    valid = deduplicate_latest(valid, ["order_item_id"], "ingestion_timestamp")
    valid = add_silver_metadata(valid, "silver_order_items")
    invalid = add_silver_metadata(invalid, "quarantine_order_items")

    return valid, invalid


def clean_ad_spend(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "ad_spend")

    cleaned = (
        df
        .withColumn("spend_id", safe_int("spend_id"))
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("traffic_source", lower(trim(col("traffic_source"))))
        .withColumn("spend_date", safe_date("spend_date"))
        .withColumn("impressions", safe_long("impressions"))
        .withColumn("clicks", safe_long("clicks"))
        .withColumn("spend_amount", safe_double("spend_amount"))
    )

    invalid_condition = (
        col("spend_id").isNull()
        | col("campaign_id").isNull()
        | col("spend_date").isNull()
        | col("impressions").isNull()
        | (col("impressions") <= 0)
        | col("clicks").isNull()
        | (col("clicks") < 0)
        | (col("clicks") > col("impressions"))
        | col("spend_amount").isNull()
        | (col("spend_amount") <= 0)
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid ad spend record: bad id/date/impressions/clicks/spend",
    )

    valid = deduplicate_latest(valid, ["spend_id"], "ingestion_timestamp")
    valid = add_silver_metadata(valid, "silver_ad_spend")
    invalid = add_silver_metadata(invalid, "quarantine_ad_spend")

    return valid, invalid


def clean_web_events(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = read_bronze(spark, "web_events")

    cleaned = (
        df
        .withColumn("event_id", trim(col("event_id")))
        .withColumn("session_id", trim(col("session_id")))
        .withColumn("user_id", safe_int("user_id"))
        .withColumn("user_name", trim(col("user_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("age", safe_int("age"))
        .withColumn("loyalty_points", safe_int("loyalty_points"))
        .withColumn("is_prime_user", safe_boolean("is_prime_user"))
        .withColumn("event_time", safe_timestamp("event_time"))
        .withColumn("event_timestamp", safe_timestamp("event_timestamp"))
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("product_id", safe_int("product_id"))
        .withColumn("quantity", safe_int("quantity"))
        .withColumn("original_price", safe_double("original_price"))
        .withColumn("discount_percent", safe_double("discount_percent"))
        .withColumn("discounted_price", safe_double("discounted_price"))
        .withColumn("cart_value", safe_double("cart_value"))
        .withColumn("inventory_remaining", safe_int("inventory_remaining"))
        .withColumn("time_on_page_sec", safe_int("time_on_page_sec"))
        .withColumn("scroll_depth_percent", safe_int("scroll_depth_percent"))
        .withColumn("hover_duration_ms", safe_int("hover_duration_ms"))
        .withColumn("session_duration_sec", safe_int("session_duration_sec"))
        .withColumn("items_viewed_in_session", safe_int("items_viewed_in_session"))
        .withColumn("repeat_product_view_count", safe_int("repeat_product_view_count"))
        .withColumn("time_since_last_event_ms", safe_long("time_since_last_event_ms"))
        .withColumn("recommendation_rank", safe_int("recommendation_rank"))
        .withColumn("recommendation_clicked", safe_boolean("recommendation_clicked"))
        .withColumn("click_position", safe_int("click_position"))
        .withColumn("engagement_score", safe_double("engagement_score"))
        .withColumn("purchase_probability", safe_double("purchase_probability"))
        .withColumn("cart_abandonment_probability", safe_double("cart_abandonment_probability"))
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("api_latency_ms", safe_int("api_latency_ms"))
        .withColumn("page_load_time_ms", safe_int("page_load_time_ms"))
        .withColumn("fraud_score", safe_double("fraud_score"))
        .withColumn("traffic_source", lower(trim(col("traffic_source"))))
        .withColumn("device_type", lower(trim(col("device_type"))))
        .withColumn("operating_system", trim(col("operating_system")))
        .withColumn("browser", trim(col("browser")))
        .withColumn("network_type", trim(col("network_type")))
        .withColumn("schema_version", trim(col("schema_version")))
        .withColumn("source", trim(col("source")))
        .withColumn("ip_octet_check", expr("try_cast(regexp_extract(ip_address, '^(\\\\d{1,3})\\\\.', 1) as int)"))
    )

    valid_event_types = ["page_view", "product_view", "search", "add_to_cart", "checkout", "purchase"]
    valid_device_types = ["mobile", "desktop", "tablet"]

    # Keep this validation intentionally practical.
    # The first version checked too many fields in one giant OR expression,
    # which created a huge Spark logical plan and caused Java heap pressure locally.
    # This still catches the intentionally dirty web event using critical business rules.
    invalid_condition = (
        col("event_id").isNull()
        | col("session_id").isNull()
        | col("user_id").isNull()
        | col("event_timestamp").isNull()
        | (~col("event_type").isin(valid_event_types))
        | col("product_id").isNull()
        | col("original_price").isNull()
        | (col("original_price") <= 0)
        | col("discount_percent").isNull()
        | (col("discount_percent") < 0)
        | (col("discount_percent") > 100)
        | col("discounted_price").isNull()
        | (col("discounted_price") < 0)
        | (~col("device_type").isin(valid_device_types))
        | col("fraud_score").isNull()
        | (col("fraud_score") < 0)
        | (col("fraud_score") > 1)
        | col("ip_octet_check").isNull()
        | (col("ip_octet_check") < 0)
        | (col("ip_octet_check") > 255)
    )

    valid, invalid = split_valid_invalid(
        cleaned,
        invalid_condition,
        "Invalid web event record: bad ids/timestamps/types/numeric ranges/device/ip",
    )

    valid = valid.drop("ip_octet_check")
    invalid = invalid.drop("ip_octet_check")

    valid = deduplicate_latest(valid, ["event_id"], "event_timestamp")
    valid = add_silver_metadata(valid, "silver_web_events")
    invalid = add_silver_metadata(invalid, "quarantine_web_events")

    return valid, invalid


def run_table(
    table_name: str,
    clean_function,
    spark: SparkSession,
    report_rows: list[dict],
) -> None:
    valid_df, invalid_df = clean_function(spark)

    silver_path = SILVER_BASE_DIR / f"silver_{table_name}"
    quarantine_path = QUARANTINE_BASE_DIR / f"quarantine_{table_name}"

    valid_df = valid_df.cache()
    invalid_df = invalid_df.cache()

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    write_table(valid_df, silver_path)

    if invalid_count > 0:
        write_table(invalid_df, quarantine_path)

    valid_df.unpersist()
    invalid_df.unpersist()

    report_rows.append(
        {
            "table_name": table_name,
            "silver_row_count": valid_count,
            "quarantine_row_count": invalid_count,
            "silver_path": str(silver_path),
            "quarantine_path": str(quarantine_path),
            "batch_date": BATCH_DATE,
        }
    )

    print(
        f"{table_name}: silver={valid_count}, "
        f"quarantine={invalid_count}"
    )


def main() -> None:
    SILVER_BASE_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    report_rows: list[dict] = []

    table_jobs = {
        "customers": clean_customers,
        "products": clean_products,
        "campaigns": clean_campaigns,
        "orders": clean_orders,
        "order_items": clean_order_items,
        "ad_spend": clean_ad_spend,
        "web_events": clean_web_events,
    }

    print("Starting Silver transformations")
    print(f"Batch date: {BATCH_DATE}")

    for table_name, clean_function in table_jobs.items():
        run_table(
            table_name=table_name,
            clean_function=clean_function,
            spark=spark,
            report_rows=report_rows,
        )

    report_df = spark.createDataFrame(report_rows)
    report_path = LOGS_DIR / f"silver_transformation_report_{BATCH_DATE}"

    (
        report_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(report_path))
    )

    print(f"Silver transformation report written to: {report_path}")
    print("Silver transformations completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()