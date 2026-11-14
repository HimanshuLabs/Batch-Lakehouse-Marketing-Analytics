from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
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
    isnan,
    collect_set,
    size,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_BASE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_BASE_DIR = PROJECT_ROOT / "data" / "silver"
QUARANTINE_BASE_DIR = PROJECT_ROOT / "data" / "quarantine"
LOGS_DIR = PROJECT_ROOT / "logs"

BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()

BRONZE_METADATA_COLUMNS = {
    "ingestion_timestamp",
    "ingestion_date",
    "batch_id",
    "source_system",
    "source_file_name",
    "record_hash",
}

# Text that means "missing": how Python and pandas print a missing value
# (json.dumps writes a float NaN as NaN), plus blank strings. Matched exactly
# after trim, so real values such as the name "Nan" are kept.
MISSING_TEXT_TOKENS = ["NaN", "nan", "None", "null", "NULL", "NaT", "<NA>", ""]

# Free text typed by users, where these words can be genuine.
FREE_TEXT_COLUMNS = {"search_query"}

# Double columns that no validity rule checks. A NaN or +/-Infinity there
# becomes NULL, so averages can never become NaN. Rule columns are not listed:
# a non-finite value there is quarantined by not_finite instead.
#   customers, products, campaigns, orders, ad_spend: none
#   order_items: original_price, discount_percent
# original_price is normalized first, then its NULL price rule quarantines it.
#   web_events:  cart_value, engagement_score, purchase_probability,
#                cart_abandonment_probability
ORDER_TOTAL_MISMATCH_REASON = "Invalid order record: total_amount is not cart_value + tax_amount + shipping_amount"
LINE_AMOUNT_MISMATCH_REASON = "Invalid order item record: line_amount is not quantity x discounted_price"

NON_RULE_DOUBLE_COLUMNS = {
    "order_items": {"original_price", "discount_percent"},
    "web_events": {
        "cart_value",
        "engagement_score",
        "purchase_probability",
        "cart_abandonment_probability",
    },
}


def _source_decimal(column_name: str) -> str:
    # The value as an exact decimal (whitespace around it allowed), NULL when
    # it is not a number. Decimal, not double, so 3100227.9 or 33.335 are seen
    # exactly as written.
    return f"try_cast(trim(cast(`{column_name}` as string)) as decimal(38, 18))"


def safe_int(column_name: str):
    # NULL unless the value is a whole number that fits an int: '3100227.9'
    # is never truncated to 3100227 (the row is quarantined by INTEGER_COLUMNS).
    value = _source_decimal(column_name)
    return expr(f"CASE WHEN {value} = floor({value}) THEN try_cast({value} as int) END")


def safe_long(column_name: str):
    value = _source_decimal(column_name)
    return expr(f"CASE WHEN {value} = floor({value}) THEN try_cast({value} as bigint) END")


def safe_double(column_name: str):
    return expr(f"try_cast(`{column_name}` as double)")


def safe_boolean(column_name: str):
    return expr(f"try_cast(`{column_name}` as boolean)")


def safe_timestamp(column_name: str):
    return expr(f"try_cast(`{column_name}` as timestamp)")


def safe_date(column_name: str):
    return expr(f"try_cast(`{column_name}` as date)")


# Exact-value rules, checked on the source text before casting. A value that is
# present but is not a whole number (integer columns), or has more than two
# decimal places (money columns), quarantines the row with an exact reason.
RAW_PREFIX = "_source_text_"

INTEGER_COLUMNS = {
    "customers": {"user_id": "int", "age": "int", "loyalty_points": "int"},
    "products": {"product_id": "int", "inventory_remaining": "int"},
    "campaigns": {"campaign_id": "int"},
    "orders": {"order_id": "int", "user_id": "int", "campaign_id": "int"},
    "order_items": {"order_item_id": "int", "order_id": "int", "product_id": "int", "quantity": "int"},
    "ad_spend": {"spend_id": "int", "campaign_id": "int", "impressions": "long", "clicks": "long",
                 "conversions": "long"},
    "web_events": {
        "user_id": "int", "age": "int", "loyalty_points": "int", "product_id": "int", "quantity": "int",
        "inventory_remaining": "int", "time_on_page_sec": "int", "scroll_depth_percent": "int",
        "hover_duration_ms": "int", "session_duration_sec": "int", "items_viewed_in_session": "int",
        "repeat_product_view_count": "int", "time_since_last_event_ms": "long", "recommendation_rank": "int",
        "click_position": "int", "campaign_id": "int", "api_latency_ms": "int", "page_load_time_ms": "int",
    },
}

MONEY_COLUMNS = {
    "products": ["original_price", "discounted_price"],
    "campaigns": ["budget"],
    "orders": ["cart_value", "tax_amount", "shipping_amount", "total_amount", "refund_amount"],
    "order_items": ["original_price", "discounted_price", "line_amount"],
    "ad_spend": ["spend_amount", "attributed_revenue", "campaign_day_spend"],
    "web_events": ["original_price", "discounted_price", "cart_value"],
}

# The values the source may send for its coded attributes.
ORDER_STATUSES = ["completed", "cancelled", "returned"]
PRODUCT_STATUSES = ["active", "discontinued"]
CAMPAIGN_TYPES = ["awareness", "consideration", "conversion", "retention"]

INTEGER_RANGES = {"int": (-(2 ** 31), 2 ** 31 - 1), "long": (-(2 ** 63), 2 ** 63 - 1)}


def keep_source_text(df: DataFrame, table_name: str) -> DataFrame:
    # Copy each integer and money column's source value before it is cast.
    columns = list(INTEGER_COLUMNS.get(table_name, {})) + MONEY_COLUMNS.get(table_name, [])
    return df.select("*", *[col(c).cast("string").alias(RAW_PREFIX + c) for c in columns if c in df.columns])


def drop_source_text(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in df.columns if c.startswith(RAW_PREFIX)])


def exact_value_rules(table_name: str, entity: str) -> list:
    rules = []
    for column_name, kind in INTEGER_COLUMNS.get(table_name, {}).items():
        value = expr(_source_decimal(RAW_PREFIX + column_name))
        low, high = INTEGER_RANGES[kind]
        rules.append((
            col(RAW_PREFIX + column_name).isNotNull()
            & (value.isNull() | (value != expr(f"floor({_source_decimal(RAW_PREFIX + column_name)})"))
               | (value < low) | (value > high)),
            f"Invalid {entity} record: {column_name} is not an integer",
        ))
    for column_name in MONEY_COLUMNS.get(table_name, []):
        value = expr(_source_decimal(RAW_PREFIX + column_name))
        rules.append((
            value.isNotNull() & (value != expr(f"round({_source_decimal(RAW_PREFIX + column_name)}, 2)")),
            f"Invalid {entity} record: {column_name} has more than 2 decimal places",
        ))
    return rules


def after_batch_day(column_name: str):
    # At or after midnight that ends the row's batch day (its Bronze
    # ingestion_date). The session time zone is SOURCE_TIMEZONE (Asia/Kolkata),
    # so the cast gives midnight IST.
    return col(column_name) >= expr("cast(date_add(ingestion_date, 1) as timestamp)")


def split_rules(df: DataFrame, rules: list, invalid: DataFrame | None = None) -> tuple[DataFrame, DataFrame]:
    # Rules in order: a row is quarantined with the reason of the first rule it
    # breaks, so every reason is exact. One CASE expression and one split, so
    # the plan stays small however many rules a table has. As in
    # split_valid_invalid, a rule that evaluates to NULL counts as broken.
    reason = None
    for condition, text in rules:
        broken = coalesce(condition, lit(True))
        reason = when(broken, lit(text)) if reason is None else reason.when(broken, lit(text))
    tagged = df.withColumn("_quarantine_reason", reason)
    valid = tagged.filter(col("_quarantine_reason").isNull()).drop("_quarantine_reason")
    rejected = (
        tagged.filter(col("_quarantine_reason").isNotNull())
        .withColumn("quarantine_reason", col("_quarantine_reason"))
        .drop("_quarantine_reason")
    )
    return valid, rejected if invalid is None else invalid.unionByName(rejected)


def quarantine_conflicting_versions(
    valid: DataFrame,
    invalid: DataFrame,
    key_column: str,
    entity: str,
) -> tuple[DataFrame, DataFrame]:
    # Two rows of one key with the same updated_at in one batch but different
    # content are conflicting versions: no rule can say which is true, so both
    # are quarantined (and counted) instead of one being dropped silently.
    # Identical rows stay and are deduplicated as before.
    business_columns = [
        c for c in valid.columns if c not in BRONZE_METADATA_COLUMNS and not c.startswith(RAW_PREFIX)
    ]
    content = sha2(
        concat_ws("||", *[coalesce(col(c).cast("string"), lit("<NULL>")) for c in business_columns]), 256,
    )
    window = Window.partitionBy(key_column, "updated_at", "ingestion_date")
    flagged = valid.withColumn(
        "_versions", size(collect_set(content).over(window)),
    )
    valid, conflicts = split_valid_invalid(
        flagged,
        col("_versions") > 1,
        f"Invalid {entity} record: conflicting versions with the same updated_at in one batch",
    )
    return valid.drop("_versions"), invalid.unionByName(conflicts.drop("_versions"))


def not_finite(column_name: str):
    # NULL, NaN or +/-Infinity. NaN compares greater than every number in
    # Spark, so a NaN would pass every "<= 0" or "< 0" rule.
    value = col(column_name)
    return value.isNull() | isnan(value) | value.isin(float("inf"), float("-inf"))


def null_missing_values(df: DataFrame, table_name: str) -> DataFrame:
    # One select over the Bronze columns: missing-value text becomes NULL in
    # string columns, and non-finite values become NULL in the table's
    # non-rule double columns (typed doubles in JSON, text in CSV).
    non_rule_doubles = NON_RULE_DOUBLE_COLUMNS.get(table_name, set())

    def cleaned(field):
        name = field.name
        missing = None

        if (
            isinstance(field.dataType, StringType)
            and name not in BRONZE_METADATA_COLUMNS
            and name not in FREE_TEXT_COLUMNS
        ):
            missing = trim(col(name)).isin(MISSING_TEXT_TOKENS)

        if name in non_rule_doubles:
            as_double = expr(f"try_cast(`{name}` as double)")
            non_finite = isnan(as_double) | as_double.isin(float("inf"), float("-inf"))
            missing = non_finite if missing is None else (missing | non_finite)

        if missing is None:
            return col(name)

        return (
            when(missing, lit(None).cast(field.dataType))
            .otherwise(col(name))
            .alias(name)
        )

    return df.select(*[cleaned(field) for field in df.schema.fields])


def create_spark_session() -> SparkSession:
    checkpoint_dir = PROJECT_ROOT / "checkpoints" / "silver"

    return (
        SparkSession.builder
        .appName("Project2_Silver_Transformations")
        .master("local[2]")
        .config("spark.sql.session.timeZone", os.environ.get("SOURCE_TIMEZONE", "Asia/Kolkata"))
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

    return null_missing_values(spark.read.parquet(str(path)), table_name)


def write_table(df: DataFrame, output_path: Path) -> None:
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("processed_date")
        .parquet(str(output_path))
    )


def write_empty_table(df: DataFrame, output_path: Path) -> None:
    # A partitioned write of zero rows leaves only _SUCCESS, which nothing can
    # read back. Clear the old table with that write, then put one empty file
    # carrying the schema into this batch's partition.
    (
        df.limit(0)
        .write
        .mode("overwrite")
        .option("partitionOverwriteMode", "static")
        .partitionBy("processed_date")
        .parquet(str(output_path))
    )
    (
        df.limit(0)
        .drop("processed_date")
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(output_path / f"processed_date={BATCH_DATE}"))
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
    # Every rule below tests NULL explicitly. As a safety net, a rule that
    # still evaluates to NULL counts as invalid, so every row lands in
    # exactly one of the two outputs and none can vanish.
    is_invalid = coalesce(invalid_condition, lit(True))
    valid_df = df.filter(~is_invalid)
    invalid_df = df.filter(is_invalid).withColumn("quarantine_reason", lit(reason))
    return valid_df, invalid_df


DEDUPE_TIEBREAKS = [
    ("ingestion_date", "desc"),
    ("ingestion_timestamp", "desc"),
    ("record_hash", "asc"),
]


def quarantine_missing_updated_at(
    valid: DataFrame,
    invalid: DataFrame,
    entity: str,
) -> tuple[DataFrame, DataFrame]:
    # Dimension versions are keyed by updated_at, so a NULL (or unparseable,
    # which safe_timestamp turns into NULL) updated_at cannot become a version.
    valid, missing_updated_at = split_valid_invalid(
        valid,
        col("updated_at").isNull(),
        f"Invalid {entity} record: null or unparseable updated_at",
    )
    return valid, invalid.unionByName(missing_updated_at)


def deduplicate_latest(df: DataFrame, key_columns: list[str], order_column: str) -> DataFrame:
    # After the table's own order column, break ties deterministically:
    # latest batch first, then the Bronze record hash. Only columns that
    # exist in the table (and are not already the order column) are used.
    order_spec = [col(order_column).desc_nulls_last()]
    for column_name, direction in DEDUPE_TIEBREAKS:
        if column_name == order_column or column_name not in df.columns:
            continue
        order_spec.append(
            col(column_name).desc_nulls_last()
            if direction == "desc"
            else col(column_name).asc_nulls_last()
        )

    window_spec = Window.partitionBy(*key_columns).orderBy(*order_spec)

    return (
        df
        .withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def clean_customers(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "customers"), "customers")

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
        .withColumn("signup_date", safe_date("signup_date"))
        .withColumn("updated_at", safe_timestamp("updated_at"))
    )

    # NULL allowed: user_name, gender, membership_tier, loyalty_points,
    # preferred_language, home_state, country, user_segment, is_prime_user,
    # signup_date.
    invalid_condition = (
        col("user_id").isNull()
        | col("email").isNull()
        | (~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
        | col("age").isNull()
        | (col("age") < 18)
        | (col("age") > 100)
        | col("home_city").isNull()
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("customers", "customer"))
    valid, invalid = split_rules(valid, [(
        invalid_condition,
        "Invalid customer record: bad id/email/age/city",
    )], invalid)

    valid, invalid = quarantine_missing_updated_at(valid, invalid, "customer")

    valid, invalid = quarantine_conflicting_versions(valid, invalid, "user_id", "customer")
    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

    history = deduplicate_latest(valid, ["user_id", "updated_at"], "updated_at")
    valid = deduplicate_latest(valid, ["user_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_customers")
    history = add_silver_metadata(history, "silver_customers_history")
    invalid = add_silver_metadata(invalid, "quarantine_customers")

    return valid, invalid, history


def clean_products(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "products"), "products")

    cleaned = (
        df
        .withColumn("product_id", safe_int("product_id"))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("brand", trim(col("brand")))
        .withColumn("product_status", lower(trim(col("product_status"))))
        .withColumn("original_price", safe_double("original_price"))
        .withColumn("discount_percent", safe_double("discount_percent"))
        .withColumn("discounted_price", safe_double("discounted_price"))
        .withColumn("inventory_remaining", safe_int("inventory_remaining"))
        .withColumn("updated_at", safe_timestamp("updated_at"))
    )

    # NULL allowed: none of the business columns (updated_at is checked below).
    invalid_condition = (
        col("product_id").isNull()
        | col("product_name").isNull()
        | col("category").isNull()
        | not_finite("original_price")
        | (col("original_price") <= 0)
        | not_finite("discount_percent")
        | (col("discount_percent") < 0)
        | (col("discount_percent") > 100)
        | not_finite("discounted_price")
        | (col("discounted_price") < 0)
        | col("inventory_remaining").isNull()
        | (col("inventory_remaining") < 0)
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("products", "product"))
    valid, invalid = split_rules(valid, [
        (invalid_condition, "Invalid product record: bad id/category/price/discount/inventory"),
        (col("brand").isNull(), "Invalid product record: brand is missing"),
        (col("product_status").isNull() | (~col("product_status").isin(PRODUCT_STATUSES)),
         "Invalid product record: product_status must be active or discontinued"),
    ], invalid)

    valid, invalid = quarantine_missing_updated_at(valid, invalid, "product")

    valid, invalid = quarantine_conflicting_versions(valid, invalid, "product_id", "product")
    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

    history = deduplicate_latest(valid, ["product_id", "updated_at"], "updated_at")
    valid = deduplicate_latest(valid, ["product_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_products")
    history = add_silver_metadata(history, "silver_products_history")
    invalid = add_silver_metadata(invalid, "quarantine_products")

    return valid, invalid, history


def clean_campaigns(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "campaigns"), "campaigns")

    cleaned = (
        df
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("campaign_name", trim(col("campaign_name")))
        .withColumn("traffic_source", lower(trim(col("traffic_source"))))
        .withColumn("campaign_type", lower(trim(col("campaign_type"))))
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

    # NULL allowed: ab_test_group, target_segment.
    invalid_condition = (
        col("campaign_id").isNull()
        | col("campaign_name").isNull()
        | col("traffic_source").isNull()
        | (~col("traffic_source").isin(valid_sources))
        | not_finite("budget")
        | (col("budget") <= 0)
        | col("campaign_status").isNull()
        | (~col("campaign_status").isin(valid_statuses))
        | col("start_date").isNull()
        | col("end_date").isNull()
        | (col("end_date") < col("start_date"))
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("campaigns", "campaign"))
    valid, invalid = split_rules(valid, [
        (invalid_condition, "Invalid campaign record: bad id/source/budget/status/date range"),
        (col("campaign_type").isNull() | (~col("campaign_type").isin(CAMPAIGN_TYPES)),
         "Invalid campaign record: campaign_type must be awareness, consideration, conversion or retention"),
    ], invalid)

    valid, invalid = quarantine_missing_updated_at(valid, invalid, "campaign")

    valid, invalid = quarantine_conflicting_versions(valid, invalid, "campaign_id", "campaign")
    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

    history = deduplicate_latest(valid, ["campaign_id", "updated_at"], "updated_at")
    valid = deduplicate_latest(valid, ["campaign_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_campaigns")
    history = add_silver_metadata(history, "silver_campaigns_history")
    invalid = add_silver_metadata(invalid, "quarantine_campaigns")

    return valid, invalid, history


def clean_orders(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "orders"), "orders")

    cleaned = (
        df
        .withColumn("order_id", safe_int("order_id"))
        .withColumn("user_id", safe_int("user_id"))
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("order_timestamp", safe_timestamp("order_timestamp"))
        .withColumn("order_status", lower(trim(col("order_status"))))
        .withColumn("status_timestamp", safe_timestamp("status_timestamp"))
        .withColumn("refund_amount", safe_double("refund_amount"))
        .withColumn("updated_at", safe_timestamp("updated_at"))
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("cart_value", safe_double("cart_value"))
        .withColumn("tax_amount", safe_double("tax_amount"))
        .withColumn("shipping_amount", safe_double("shipping_amount"))
        .withColumn("total_amount", safe_double("total_amount"))
        .withColumn("fraud_score", safe_double("fraud_score"))
        .withColumn("country", trim(col("country")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
    )

    # NULL allowed: campaign_id (no attributed campaign), country, state, city.
    invalid_condition = (
        col("order_id").isNull()
        | col("user_id").isNull()
        | col("order_timestamp").isNull()
        | col("payment_method").isNull()
        | not_finite("cart_value")
        | (col("cart_value") <= 0)
        | not_finite("fraud_score")
        | (col("fraud_score") < 0)
        | (col("fraud_score") > 1)
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("orders", "order"))
    # cart_value is the net value (the sum of the lines). What the customer was
    # charged is total_amount = cart_value + tax_amount + shipping_amount, to the
    # cent: every amount is whole cents (MONEY_COLUMNS).
    valid, invalid = split_rules(valid, [
        (invalid_condition, "Invalid order record: bad id/timestamp/payment/cart/fraud score"),
        (col("order_status").isNull() | (~col("order_status").isin(ORDER_STATUSES)),
         "Invalid order record: order_status must be completed, cancelled or returned"),
        (not_finite("tax_amount") | (col("tax_amount") < 0), "Invalid order record: tax_amount is missing or negative"),
        (not_finite("shipping_amount") | (col("shipping_amount") < 0),
         "Invalid order record: shipping_amount is missing or negative"),
        (not_finite("total_amount")
         | (expr(_source_decimal(RAW_PREFIX + "total_amount"))
            != expr(_source_decimal(RAW_PREFIX + "cart_value")) + expr(_source_decimal(RAW_PREFIX + "tax_amount"))
            + expr(_source_decimal(RAW_PREFIX + "shipping_amount"))),
         ORDER_TOTAL_MISMATCH_REASON),
        # The order's lifecycle: its status took effect at status_timestamp, never
        # before the order was placed; only a returned order has a refund, and a
        # refund is more than nothing and at most the order's net value.
        (col("status_timestamp").isNull() | col("updated_at").isNull(),
         "Invalid order record: status_timestamp or updated_at is missing"),
        (col("status_timestamp") < col("order_timestamp"),
         "Invalid order record: status_timestamp is before order_timestamp"),
        (not_finite("refund_amount") | (col("refund_amount") < 0), "Invalid order record: refund_amount is missing or negative"),
        ((col("order_status") != "returned") & (col("refund_amount") != 0),
         "Invalid order record: refund_amount on an order that was not returned"),
        ((col("order_status") == "returned") & (col("refund_amount") <= 0),
         "Invalid order record: a returned order has no refund_amount"),
        (expr(_source_decimal(RAW_PREFIX + "refund_amount")) > expr(_source_decimal(RAW_PREFIX + "cart_value")),
         "Invalid order record: refund_amount is more than the order's net value"),
    ], invalid)

    valid, invalid = split_rules(valid, [
        (after_batch_day("order_timestamp"), "Invalid order record: order_timestamp is after the end of the batch day"),
        (after_batch_day("status_timestamp"), "Invalid order record: status_timestamp is after the end of the batch day"),
    ], invalid)
    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

    # The source delivers an order again when it is cancelled or returned: the
    # latest version (by updated_at, then the latest batch) is the order.
    valid = deduplicate_latest(valid, ["order_id"], "updated_at")
    valid = add_silver_metadata(valid, "silver_orders")
    invalid = add_silver_metadata(invalid, "quarantine_orders")

    return valid, invalid


def clean_order_items(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "order_items"), "order_items")

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

    # NULL allowed: product_name, category, discount_percent.
    invalid_condition = (
        col("order_item_id").isNull()
        | col("order_id").isNull()
        | col("product_id").isNull()
        | col("quantity").isNull()
        | (col("quantity") <= 0)
        | not_finite("discounted_price")
        | (col("discounted_price") < 0)
        | not_finite("line_amount")
        # A free item (discounted_price 0, line_amount 0) is a valid line.
        | (col("line_amount") < 0)
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("order_items", "order item"))
    valid, invalid = split_rules(valid, [(
        invalid_condition,
        "Invalid order item record: bad id/quantity/price/line amount",
    )], invalid)

    for condition, reason in (
        (col("original_price").isNull(), "original_price is NULL"),
        (col("original_price") <= 0, "original_price must be positive"),
        (col("discounted_price") > col("original_price"), "discounted_price exceeds original_price"),
    ):
        valid, rejected = split_valid_invalid(
            valid, condition, f"Invalid order item record: {reason}",
        )
        invalid = invalid.unionByName(rejected)

    # An item of an order dated after the end of its batch day (that order is
    # quarantined too). Items carry no timestamp of their own.
    # (clean_orders fails first when Bronze orders are missing; alone, as in a
    # unit test of this function, no order means no future order.)
    if (BRONZE_BASE_DIR / "bronze_orders").exists():
        future_orders = (
            read_bronze(spark, "orders")
            .select(safe_int("order_id").alias("order_id"),
                    safe_timestamp("order_timestamp").alias("order_timestamp"), "ingestion_date")
            .filter(after_batch_day("order_timestamp"))
            .select("order_id").distinct()
            .withColumn("_future_order", lit(True))
        )
    else:
        future_orders = spark.createDataFrame([], "order_id int, _future_order boolean")
    valid, invalid = split_rules(
        valid.join(future_orders, on="order_id", how="left"),
        [(col("_future_order").isNotNull(),
          "Invalid order item record: its order's order_timestamp is after the end of the batch day")],
        invalid.withColumn("_future_order", lit(None).cast(BooleanType())),
    )
    valid, invalid = valid.drop("_future_order"), invalid.drop("_future_order")

    # line_amount must be exactly quantity x discounted_price: every amount is
    # whole cents (MONEY_COLUMNS), so a line that differs by even one cent is
    # wrong data, and the order total, product revenue and the warehouse's
    # gross - discount = net check would disagree.
    valid, invalid = split_rules(valid, [(
        expr(_source_decimal(RAW_PREFIX + "line_amount"))
        != col("quantity").cast("decimal(38, 0)") * expr(_source_decimal(RAW_PREFIX + "discounted_price")),
        LINE_AMOUNT_MISMATCH_REASON,
    )], invalid)
    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

    valid = deduplicate_latest(valid, ["order_item_id"], "ingestion_timestamp")
    valid = add_silver_metadata(valid, "silver_order_items")
    invalid = add_silver_metadata(invalid, "quarantine_order_items")

    return valid, invalid


def clean_ad_spend(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "ad_spend"), "ad_spend")

    cleaned = (
        df
        .withColumn("spend_id", safe_int("spend_id"))
        .withColumn("campaign_id", safe_int("campaign_id"))
        .withColumn("traffic_source", lower(trim(col("traffic_source"))))
        .withColumn("spend_date", safe_date("spend_date"))
        .withColumn("country", trim(col("country")))
        .withColumn("state", trim(col("state")))
        .withColumn("impressions", safe_long("impressions"))
        .withColumn("clicks", safe_long("clicks"))
        .withColumn("conversions", safe_long("conversions"))
        .withColumn("attributed_revenue", safe_double("attributed_revenue"))
        .withColumn("spend_amount", safe_double("spend_amount"))
        .withColumn("campaign_day_spend", safe_double("campaign_day_spend"))
    )

    # NULL allowed: traffic_source.
    invalid_condition = (
        col("spend_id").isNull()
        | col("campaign_id").isNull()
        | col("spend_date").isNull()
        | col("impressions").isNull()
        | (col("impressions") <= 0)
        | col("clicks").isNull()
        | (col("clicks") < 0)
        | (col("clicks") > col("impressions"))
        | not_finite("spend_amount")
        | (col("spend_amount") <= 0)
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("ad_spend", "ad spend"))
    # What the ad platform reports for the day: it cannot convert more often
    # than it was clicked, and attributed revenue is never negative.
    valid, invalid = split_rules(valid, [
        (invalid_condition, "Invalid ad spend record: bad id/date/impressions/clicks/spend"),
        (col("conversions").isNull() | (col("conversions") < 0) | (col("conversions") > col("clicks")),
         "Invalid ad spend record: conversions are missing, negative or more than the clicks"),
        (not_finite("attributed_revenue") | (col("attributed_revenue") < 0),
         "Invalid ad spend record: attributed_revenue is missing or negative"),
        # The feed's grain is campaign, day and region: every row names its
        # region, and carries the platform's total for the campaign-day.
        (col("state").isNull() | col("country").isNull(), "Invalid ad spend record: region (country, state) is missing"),
        (not_finite("campaign_day_spend") | (col("campaign_day_spend") < col("spend_amount")),
         "Invalid ad spend record: campaign_day_spend is missing or less than the row's spend"),
    ], invalid)

    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

    valid = deduplicate_latest(valid, ["spend_id"], "ingestion_timestamp")
    valid = add_silver_metadata(valid, "silver_ad_spend")
    invalid = add_silver_metadata(invalid, "quarantine_ad_spend")

    return valid, invalid


def clean_web_events(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df = keep_source_text(read_bronze(spark, "web_events"), "web_events")

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
    # NULL allowed: every column not tested here, e.g. campaign_id,
    # payment_method, search_query, city.
    invalid_condition = (
        col("event_id").isNull()
        | col("session_id").isNull()
        | col("user_id").isNull()
        | col("event_timestamp").isNull()
        | col("event_type").isNull()
        | (~col("event_type").isin(valid_event_types))
        | col("product_id").isNull()
        | not_finite("original_price")
        | (col("original_price") <= 0)
        | not_finite("discount_percent")
        | (col("discount_percent") < 0)
        | (col("discount_percent") > 100)
        | not_finite("discounted_price")
        | (col("discounted_price") < 0)
        | col("device_type").isNull()
        | (~col("device_type").isin(valid_device_types))
        | not_finite("fraud_score")
        | (col("fraud_score") < 0)
        | (col("fraud_score") > 1)
        | col("ip_octet_check").isNull()
        | (col("ip_octet_check") < 0)
        | (col("ip_octet_check") > 255)
    )

    valid, invalid = split_rules(cleaned, exact_value_rules("web_events", "web event"))
    valid, invalid = split_rules(valid, [(
        invalid_condition,
        "Invalid web event record: bad ids/timestamps/types/numeric ranges/device/ip",
    )], invalid)

    valid = valid.drop("ip_octet_check")
    invalid = invalid.drop("ip_octet_check")

    valid, invalid = split_rules(valid, [(
        after_batch_day("event_timestamp"),
        "Invalid web event record: event_timestamp is after the end of the batch day",
    )], invalid)
    valid, invalid = drop_source_text(valid), drop_source_text(invalid)

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
    # Dimension cleaners also return a history frame (one row per key and
    # updated_at); fact cleaners return only (valid, invalid).
    result = clean_function(spark)
    valid_df, invalid_df = result[0], result[1]
    history_df = result[2] if len(result) > 2 else None

    silver_path = SILVER_BASE_DIR / f"silver_{table_name}"
    history_path = SILVER_BASE_DIR / f"silver_{table_name}_history"
    quarantine_path = QUARANTINE_BASE_DIR / f"quarantine_{table_name}"

    valid_df = valid_df.cache()
    invalid_df = invalid_df.cache()

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    write_table(valid_df, silver_path)

    history_count = None
    if history_df is not None:
        history_df = history_df.cache()
        history_count = history_df.count()
        write_table(history_df, history_path)
        history_df.unpersist()

    # Always rewrite quarantine so last run's rows can never survive into this run.
    if invalid_count > 0:
        write_table(invalid_df, quarantine_path)
    else:
        write_empty_table(invalid_df, quarantine_path)

    valid_df.unpersist()
    invalid_df.unpersist()

    report_rows.append(
        {
            "table_name": table_name,
            "silver_row_count": valid_count,
            "quarantine_row_count": invalid_count,
            "history_row_count": history_count,
            "silver_path": str(silver_path),
            "history_path": str(history_path) if history_df is not None else None,
            "quarantine_path": str(quarantine_path),
            "batch_date": BATCH_DATE,
        }
    )

    history_message = f", history={history_count}" if history_df is not None else ""
    print(
        f"{table_name}: silver={valid_count}, "
        f"quarantine={invalid_count}{history_message}"
    )


def main() -> None:
    SILVER_BASE_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()
    print(f"spark.sql.session.timeZone={spark.conf.get('spark.sql.session.timeZone')}")

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