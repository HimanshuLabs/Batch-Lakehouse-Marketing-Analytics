from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    round,
    sum,
    to_date,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_BASE_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_BASE_DIR = PROJECT_ROOT / "data" / "gold"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.today().isoformat()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project2_Gold_Transformations")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def read_silver(spark: SparkSession, table_name: str) -> DataFrame:
    path = SILVER_BASE_DIR / f"silver_{table_name}"

    if not path.exists():
        raise FileNotFoundError(f"Missing Silver table: {path}")

    return spark.read.parquet(str(path))


def add_gold_metadata(df: DataFrame, table_name: str) -> DataFrame:
    return (
        df
        .withColumn("gold_table", lit(table_name))
        .withColumn("gold_processed_timestamp", current_timestamp())
        .withColumn("gold_processed_date", to_date(lit(BATCH_DATE)))
    )


def write_gold_table(df: DataFrame, table_name: str) -> int:
    output_path = GOLD_BASE_DIR / table_name

    count_rows = df.count()

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("gold_processed_date")
        .parquet(str(output_path))
    )

    return count_rows


def build_dim_customers(customers: DataFrame) -> DataFrame:
    return add_gold_metadata(
        customers.select(
            col("user_id").alias("customer_id"),
            "user_name",
            "email",
            "gender",
            "age",
            "membership_tier",
            "loyalty_points",
            "preferred_language",
            "home_city",
            "home_state",
            "country",
            "user_segment",
            "is_prime_user",
            "updated_at",
        ),
        "dim_customers",
    )


def build_dim_products(products: DataFrame) -> DataFrame:
    return add_gold_metadata(
        products.select(
            "product_id",
            "product_name",
            "category",
            "original_price",
            "discount_percent",
            "discounted_price",
            "inventory_remaining",
            "updated_at",
        ),
        "dim_products",
    )


def build_dim_campaigns(campaigns: DataFrame) -> DataFrame:
    return add_gold_metadata(
        campaigns.select(
            "campaign_id",
            "campaign_name",
            "traffic_source",
            "ab_test_group",
            "target_segment",
            "budget",
            "campaign_status",
            "start_date",
            "end_date",
            "updated_at",
        ),
        "dim_campaigns",
    )


def build_fact_orders(orders: DataFrame) -> DataFrame:
    return add_gold_metadata(
        orders.select(
            "order_id",
            col("user_id").alias("customer_id"),
            "campaign_id",
            "order_timestamp",
            to_date(col("order_timestamp")).alias("order_date"),
            "payment_method",
            col("cart_value").alias("order_amount"),
            "fraud_score",
            "country",
            "city",
        ),
        "fact_orders",
    )


def build_fact_order_items(order_items: DataFrame) -> DataFrame:
    return add_gold_metadata(
        order_items.select(
            "order_item_id",
            "order_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "original_price",
            "discount_percent",
            "discounted_price",
            "line_amount",
            "expected_line_amount",
        ),
        "fact_order_items",
    )


def build_fact_ad_spend(ad_spend: DataFrame) -> DataFrame:
    return add_gold_metadata(
        ad_spend.select(
            "spend_id",
            "campaign_id",
            "traffic_source",
            "spend_date",
            "impressions",
            "clicks",
            "spend_amount",
        ),
        "fact_ad_spend",
    )


def build_fact_web_events(web_events: DataFrame) -> DataFrame:
    return add_gold_metadata(
        web_events.select(
            "event_id",
            "session_id",
            col("user_id").alias("customer_id"),
            "event_timestamp",
            to_date(col("event_timestamp")).alias("event_date"),
            "event_type",
            "user_journey_stage",
            "user_segment",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "cart_value",
            "traffic_source",
            "campaign_id",
            "ab_test_group",
            "device_type",
            "operating_system",
            "browser",
            "network_type",
            "engagement_score",
            "purchase_probability",
            "cart_abandonment_probability",
            "api_latency_ms",
            "page_load_time_ms",
            "fraud_score",
            "city",
            "country",
        ),
        "fact_web_events",
    )


def build_mart_campaign_performance(
    campaigns: DataFrame,
    orders: DataFrame,
    ad_spend: DataFrame,
    web_events: DataFrame,
) -> DataFrame:
    campaign_dim = campaigns.select(
        "campaign_id",
        "campaign_name",
        "traffic_source",
        "ab_test_group",
        "target_segment",
        "budget",
        "campaign_status",
    )

    spend_agg = (
        ad_spend
        .groupBy("campaign_id")
        .agg(
            sum("impressions").alias("total_impressions"),
            sum("clicks").alias("total_clicks"),
            round(sum("spend_amount"), 2).alias("total_ad_spend"),
        )
    )

    order_agg = (
        orders
        .groupBy("campaign_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            round(sum("cart_value"), 2).alias("total_revenue"),
            round(avg("cart_value"), 2).alias("avg_order_value"),
        )
    )

    event_agg = (
        web_events
        .groupBy("campaign_id")
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("user_id").alias("unique_customers"),
            sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
            sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_events"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_events"),
            round(avg("engagement_score"), 4).alias("avg_engagement_score"),
            round(avg("purchase_probability"), 4).alias("avg_purchase_probability"),
            round(avg("cart_abandonment_probability"), 4).alias("avg_cart_abandonment_probability"),
        )
    )

    mart = (
        campaign_dim
        .join(spend_agg, "campaign_id", "left")
        .join(order_agg, "campaign_id", "left")
        .join(event_agg, "campaign_id", "left")
        .fillna(
            {
                "total_impressions": 0,
                "total_clicks": 0,
                "total_ad_spend": 0.0,
                "total_orders": 0,
                "total_revenue": 0.0,
                "avg_order_value": 0.0,
                "total_events": 0,
                "total_sessions": 0,
                "unique_customers": 0,
                "page_views": 0,
                "product_views": 0,
                "add_to_cart_events": 0,
                "checkout_events": 0,
                "purchase_events": 0,
                "avg_engagement_score": 0.0,
                "avg_purchase_probability": 0.0,
                "avg_cart_abandonment_probability": 0.0,
            }
        )
        .withColumn(
            "ctr",
            when(col("total_impressions") > 0, round(col("total_clicks") / col("total_impressions"), 6)).otherwise(0.0),
        )
        .withColumn(
            "cpc",
            when(col("total_clicks") > 0, round(col("total_ad_spend") / col("total_clicks"), 2)).otherwise(0.0),
        )
        .withColumn(
            "roas",
            when(col("total_ad_spend") > 0, round(col("total_revenue") / col("total_ad_spend"), 4)).otherwise(0.0),
        )
        .withColumn(
            "conversion_rate",
            when(col("total_sessions") > 0, round(col("purchase_events") / col("total_sessions"), 6)).otherwise(0.0),
        )
    )

    return add_gold_metadata(mart, "mart_campaign_performance")


def build_mart_product_performance(
    products: DataFrame,
    order_items: DataFrame,
    web_events: DataFrame,
) -> DataFrame:
    product_dim = products.select(
        "product_id",
        "product_name",
        "category",
        "original_price",
        "discount_percent",
        "discounted_price",
        "inventory_remaining",
    )

    sales_agg = (
        order_items
        .groupBy("product_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            round(sum("line_amount"), 2).alias("total_product_revenue"),
            round(avg("line_amount"), 2).alias("avg_line_amount"),
        )
    )

    event_agg = (
        web_events
        .groupBy("product_id")
        .agg(
            count("*").alias("total_product_events"),
            countDistinct("session_id").alias("total_product_sessions"),
            countDistinct("user_id").alias("unique_product_customers"),
            sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_events"),
            round(avg("engagement_score"), 4).alias("avg_engagement_score"),
        )
    )

    mart = (
        product_dim
        .join(sales_agg, "product_id", "left")
        .join(event_agg, "product_id", "left")
        .fillna(
            {
                "total_orders": 0,
                "total_units_sold": 0,
                "total_product_revenue": 0.0,
                "avg_line_amount": 0.0,
                "total_product_events": 0,
                "total_product_sessions": 0,
                "unique_product_customers": 0,
                "product_views": 0,
                "add_to_cart_events": 0,
                "purchase_events": 0,
                "avg_engagement_score": 0.0,
            }
        )
        .withColumn(
            "view_to_cart_rate",
            when(col("product_views") > 0, round(col("add_to_cart_events") / col("product_views"), 6)).otherwise(0.0),
        )
        .withColumn(
            "cart_to_purchase_rate",
            when(col("add_to_cart_events") > 0, round(col("purchase_events") / col("add_to_cart_events"), 6)).otherwise(0.0),
        )
    )

    return add_gold_metadata(mart, "mart_product_performance")


def build_mart_customer_value(
    customers: DataFrame,
    orders: DataFrame,
    web_events: DataFrame,
) -> DataFrame:
    customer_dim = customers.select(
        col("user_id").alias("customer_id"),
        "user_name",
        "email",
        "membership_tier",
        "loyalty_points",
        "home_city",
        "home_state",
        "user_segment",
        "is_prime_user",
    )

    order_agg = (
        orders
        .groupBy(col("user_id").alias("customer_id"))
        .agg(
            countDistinct("order_id").alias("total_orders"),
            round(sum("cart_value"), 2).alias("customer_lifetime_value"),
            round(avg("cart_value"), 2).alias("avg_order_value"),
        )
    )

    event_agg = (
        web_events
        .groupBy(col("user_id").alias("customer_id"))
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("product_id").alias("unique_products_interacted"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_events"),
            round(avg("engagement_score"), 4).alias("avg_engagement_score"),
            round(avg("purchase_probability"), 4).alias("avg_purchase_probability"),
            round(avg("cart_abandonment_probability"), 4).alias("avg_cart_abandonment_probability"),
        )
    )

    mart = (
        customer_dim
        .join(order_agg, "customer_id", "left")
        .join(event_agg, "customer_id", "left")
        .fillna(
            {
                "total_orders": 0,
                "customer_lifetime_value": 0.0,
                "avg_order_value": 0.0,
                "total_events": 0,
                "total_sessions": 0,
                "unique_products_interacted": 0,
                "purchase_events": 0,
                "avg_engagement_score": 0.0,
                "avg_purchase_probability": 0.0,
                "avg_cart_abandonment_probability": 0.0,
            }
        )
    )

    return add_gold_metadata(mart, "mart_customer_value")


def build_mart_marketing_funnel(web_events: DataFrame) -> DataFrame:
    mart = (
        web_events
        .groupBy("traffic_source", "ab_test_group", "device_type")
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("user_id").alias("unique_customers"),
            sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            sum(when(col("event_type") == "search", 1).otherwise(0)).alias("search_events"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
            sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_events"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_events"),
            round(avg("engagement_score"), 4).alias("avg_engagement_score"),
            round(avg("api_latency_ms"), 2).alias("avg_api_latency_ms"),
            round(avg("page_load_time_ms"), 2).alias("avg_page_load_time_ms"),
        )
        .withColumn(
            "view_to_cart_rate",
            when(col("product_views") > 0, round(col("add_to_cart_events") / col("product_views"), 6)).otherwise(0.0),
        )
        .withColumn(
            "cart_to_checkout_rate",
            when(col("add_to_cart_events") > 0, round(col("checkout_events") / col("add_to_cart_events"), 6)).otherwise(0.0),
        )
        .withColumn(
            "checkout_to_purchase_rate",
            when(col("checkout_events") > 0, round(col("purchase_events") / col("checkout_events"), 6)).otherwise(0.0),
        )
        .withColumn(
            "session_conversion_rate",
            when(col("total_sessions") > 0, round(col("purchase_events") / col("total_sessions"), 6)).otherwise(0.0),
        )
    )

    return add_gold_metadata(mart, "mart_marketing_funnel")


def main() -> None:
    GOLD_BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    print("Starting Gold transformations")
    print(f"Batch date: {BATCH_DATE}")

    customers = read_silver(spark, "customers").cache()
    products = read_silver(spark, "products").cache()
    campaigns = read_silver(spark, "campaigns").cache()
    orders = read_silver(spark, "orders").cache()
    order_items = read_silver(spark, "order_items").cache()
    ad_spend = read_silver(spark, "ad_spend").cache()
    web_events = read_silver(spark, "web_events").cache()

    gold_tables = {
        "dim_customers": build_dim_customers(customers),
        "dim_products": build_dim_products(products),
        "dim_campaigns": build_dim_campaigns(campaigns),
        "fact_orders": build_fact_orders(orders),
        "fact_order_items": build_fact_order_items(order_items),
        "fact_ad_spend": build_fact_ad_spend(ad_spend),
        "fact_web_events": build_fact_web_events(web_events),
        "mart_campaign_performance": build_mart_campaign_performance(campaigns, orders, ad_spend, web_events),
        "mart_product_performance": build_mart_product_performance(products, order_items, web_events),
        "mart_customer_value": build_mart_customer_value(customers, orders, web_events),
        "mart_marketing_funnel": build_mart_marketing_funnel(web_events),
    }

    report_rows = []

    for table_name, dataframe in gold_tables.items():
        row_count = write_gold_table(dataframe, table_name)

        report_rows.append(
            {
                "table_name": table_name,
                "row_count": row_count,
                "gold_path": str(GOLD_BASE_DIR / table_name),
                "batch_date": BATCH_DATE,
            }
        )

        print(f"{table_name}: rows={row_count}")

    report_df = spark.createDataFrame(report_rows)
    report_path = LOGS_DIR / f"gold_transformation_report_{BATCH_DATE}"

    (
        report_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(report_path))
    )

    print(f"Gold transformation report written to: {report_path}")
    print("Gold transformations completed successfully.")

    customers.unpersist()
    products.unpersist()
    campaigns.unpersist()
    orders.unpersist()
    order_items.unpersist()
    ad_spend.unpersist()
    web_events.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()