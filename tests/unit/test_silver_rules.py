"""Silver validity rules.

P2-03 (fixed in 1831e6b): a rule that evaluates to NULL put a row in neither
Silver nor quarantine, so the row vanished.
N-13 (fixed in 62b273d): 'NaN'-like text stayed as text, and NaN / Infinity
passed every "<= 0" rule; non-rule doubles could carry NaN into averages.
p2-data-v2: an order item whose line_amount is not quantity x discounted_price
(the generator's dirty line_amount = 1) passed Silver and reached Gold.
"""
from __future__ import annotations

import math

import pytest
from pyspark.sql import functions as F

SILVER_JOB = "spark-batch/silver_transformations.py"
BATCH_DATE = "2026-06-18"

# Silver key column per table (web events keep the string event_id).
SILVER_KEYS = {
    "customers": "user_id", "products": "product_id", "campaigns": "campaign_id", "orders": "order_id",
    "order_items": "order_item_id", "ad_spend": "spend_id", "web_events": "event_id",
}


def key_values(table: str, n: int) -> list:
    return [f"event-{i}" for i in range(n)] if table == "web_events" else [str(1000 + i) for i in range(n)]


def silver_key(table: str, raw):
    return raw if table == "web_events" else int(float(raw))


@pytest.fixture
def run_silver(spark, load_job, write_bronze):
    """Write one Bronze table, run the real clean_<table>, return (silver rows, quarantine keys)."""

    def _run(table: str, rows: list[dict]):
        silver = load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)
        write_bronze(table, rows)
        session = silver.create_spark_session()  # the job's own session settings
        result = getattr(silver, f"clean_{table}")(session)
        valid, invalid = result[0], result[1]
        key = SILVER_KEYS[table]
        valid_rows = {row[key]: row.asDict() for row in valid.collect()}
        invalid_keys = {row[key] for row in invalid.collect()}
        return valid_rows, invalid_keys

    return _run


def two_rows(table: str, column: str, value) -> tuple[list[dict], object, object]:
    """A valid control row and a row with column=value, with distinct keys."""
    good, bad = key_values(table, 2)
    key = SILVER_KEYS[table]
    return [{key: good}, {key: bad, column: value}], silver_key(table, good), silver_key(table, bad)


# --- P2-03 -----------------------------------------------------------------

@pytest.mark.parametrize(
    "table, column",
    [
        ("campaigns", "traffic_source"),
        ("campaigns", "campaign_status"),
        ("web_events", "event_type"),
        ("web_events", "device_type"),
    ],
)
def test_null_in_a_value_list_rule_is_quarantined(run_silver, table, column):
    rows, good, bad = two_rows(table, column, None)
    valid, invalid = run_silver(table, rows)
    assert good in valid
    assert bad in invalid, f"row with NULL {column} vanished: not in Silver or quarantine"
    assert bad not in valid


def test_a_rule_that_evaluates_to_null_counts_as_invalid(spark, load_job):
    silver = load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)
    frame = spark.createDataFrame([(1,), (2,)], "id int")
    valid, invalid = silver.split_valid_invalid(frame, F.lit(None).cast("boolean"), "unevaluable rule")
    assert valid.count() == 0
    assert invalid.count() == 2, "rows whose rule is NULL must be quarantined, not dropped"


# --- N-13: missing-value text ---------------------------------------------

@pytest.mark.parametrize(
    "table, column, token",
    [
        ("web_events", "category", "NaN"),
        ("web_events", "city", "<NA>"),
        ("web_events", "home_city", "NaT"),
        ("customers", "home_state", "None"),
        ("orders", "city", "  nan "),
        ("campaigns", "ab_test_group", "NULL"),
        ("ad_spend", "traffic_source", "null"),
    ],
)
def test_missing_value_text_becomes_null(run_silver, table, column, token):
    rows, good, bad = two_rows(table, column, token)
    valid, _ = run_silver(table, rows)
    assert bad in valid, "a descriptive column must not reject the row"
    assert valid[bad][column] is None, f"{token!r} in {column} must become NULL, got {valid[bad][column]!r}"


def test_real_values_are_kept_while_tokens_become_null(run_silver):
    rows = [{"event_id": "event-0", "category": "NaN", "user_name": "Nan", "search_query": "nan"}]
    valid, _ = run_silver("web_events", rows)
    row = valid["event-0"]
    assert row["category"] is None
    assert row["user_name"] == "Nan", "only exact tokens are missing values; the name 'Nan' is real"
    assert row["search_query"] == "nan", "search_query is free text and keeps every word"


@pytest.mark.parametrize(
    "table, column",
    [
        ("customers", "home_city"),
        ("products", "product_name"),
        ("campaigns", "campaign_name"),
        ("orders", "payment_method"),
        ("web_events", "session_id"),
    ],
)
def test_missing_value_text_in_a_rule_column_is_quarantined(run_silver, table, column):
    rows, good, bad = two_rows(table, column, "NaN")
    valid, invalid = run_silver(table, rows)
    assert good in valid
    assert bad in invalid, f"'NaN' in required column {column} must be quarantined"


# --- N-13: non-finite numbers ---------------------------------------------

@pytest.mark.parametrize(
    "table, column, value",
    [
        ("products", "original_price", "NaN"),
        ("products", "discounted_price", "Infinity"),
        ("campaigns", "budget", "Infinity"),
        ("orders", "cart_value", "NaN"),
        ("order_items", "line_amount", "Infinity"),
        ("ad_spend", "spend_amount", "NaN"),
        ("web_events", "original_price", float("nan")),
        ("web_events", "discounted_price", float("inf")),
    ],
)
def test_non_finite_value_in_a_rule_column_is_quarantined(run_silver, table, column, value):
    rows, good, bad = two_rows(table, column, value)
    valid, invalid = run_silver(table, rows)
    assert good in valid
    assert bad in invalid, f"{value!r} in {column} passed a '<= 0' rule"


@pytest.mark.parametrize(
    "table, column, value",
    [
        ("order_items", "discount_percent", "-Infinity"),
        ("web_events", "engagement_score", float("nan")),
        ("web_events", "cart_value", float("inf")),
        ("web_events", "purchase_probability", float("-inf")),
    ],
)
def test_non_finite_value_in_a_non_rule_double_becomes_null(run_silver, table, column, value):
    rows, good, bad = two_rows(table, column, value)
    valid, _ = run_silver(table, rows)
    assert bad in valid, "a non-rule column must not reject the row"
    stored = valid[bad][column]
    assert stored is None, f"{value!r} in {column} must become NULL, got {stored!r}"
    assert not (isinstance(stored, float) and (math.isnan(stored) or math.isinf(stored)))


# --- p2-data-v2: line_amount must be quantity x discounted_price ------------

def test_a_line_amount_that_is_not_quantity_times_price_is_quarantined(spark, load_job, write_bronze):
    # Every row is 5 x 13673.69 = 68368.45.
    line_amounts = {
        "1000": "68368.45",   # exact: valid
        "1001": "68368.455",  # not whole cents: quarantined as such (Part G)
        "1002": "68368.47",   # 0.02 off: quarantined
        "1003": "1",          # the generator's dirty line_amount: quarantined
        "1004": "68368.46",   # one cent off: quarantined (Part G: exact, not within 0.01)
    }
    silver = load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)
    write_bronze("order_items", [{"order_item_id": key, "line_amount": value} for key, value in line_amounts.items()])
    valid, invalid = silver.clean_order_items(silver.create_spark_session())

    reasons = {row["order_item_id"]: row["quarantine_reason"] for row in invalid.collect()}
    assert {row["order_item_id"] for row in valid.collect()} == {1000}
    reason = "Invalid order item record: line_amount is not quantity x discounted_price"
    assert reasons == {1001: "Invalid order item record: line_amount has more than 2 decimal places",
                       1002: reason, 1003: reason, 1004: reason}


# --- Part J: the new source attributes -------------------------------------------
# Each has a validity rule with an exact reason; these are the generator's dirty
# rows (data-generator/generate_data.py, add_attribute_fixtures) and their neighbours.

NEW_ATTRIBUTE_RULES = [
    ("products", "product_status", "recalled", "Invalid product record: product_status must be active or discontinued"),
    ("products", "product_status", None, "Invalid product record: product_status must be active or discontinued"),
    ("products", "brand", None, "Invalid product record: brand is missing"),
    ("campaigns", "campaign_type", "brand_lift",
     "Invalid campaign record: campaign_type must be awareness, consideration, conversion or retention"),
    ("campaigns", "campaign_type", None,
     "Invalid campaign record: campaign_type must be awareness, consideration, conversion or retention"),
    ("orders", "order_status", "shipped", "Invalid order record: order_status must be completed, cancelled or returned"),
    ("orders", "order_status", None, "Invalid order record: order_status must be completed, cancelled or returned"),
    ("orders", "tax_amount", "-1.0", "Invalid order record: tax_amount is missing or negative"),
    ("orders", "tax_amount", None, "Invalid order record: tax_amount is missing or negative"),
    ("orders", "shipping_amount", "-499.0", "Invalid order record: shipping_amount is missing or negative"),
    ("orders", "total_amount", "163913.31", "Invalid order record: total_amount is not cart_value + tax_amount + shipping_amount"),
    ("orders", "total_amount", "163903.30", "Invalid order record: total_amount is not cart_value + tax_amount + shipping_amount"),
    ("orders", "tax_amount", "25002.205", "Invalid order record: tax_amount has more than 2 decimal places"),
    ("ad_spend", "conversions", "153714", "Invalid ad spend record: conversions are missing, negative or more than the clicks"),
    ("ad_spend", "conversions", "-1", "Invalid ad spend record: conversions are missing, negative or more than the clicks"),
    ("ad_spend", "conversions", "12.5", "Invalid ad spend record: conversions is not an integer"),
    ("ad_spend", "attributed_revenue", "-1.0", "Invalid ad spend record: attributed_revenue is missing or negative"),
    ("ad_spend", "attributed_revenue", None, "Invalid ad spend record: attributed_revenue is missing or negative"),
    # J3: the feed is per campaign, day and region.
    ("ad_spend", "state", None, "Invalid ad spend record: region (country, state) is missing"),
    ("ad_spend", "country", None, "Invalid ad spend record: region (country, state) is missing"),
    ("ad_spend", "campaign_day_spend", "88287.73",
     "Invalid ad spend record: campaign_day_spend is missing or less than the row's spend"),
    ("ad_spend", "campaign_day_spend", None,
     "Invalid ad spend record: campaign_day_spend is missing or less than the row's spend"),
]


@pytest.mark.parametrize("table,column,value,reason", NEW_ATTRIBUTE_RULES,
                         ids=[f"{t}.{c}={v}" for t, c, v, _ in NEW_ATTRIBUTE_RULES])
def test_a_bad_value_of_a_new_attribute_is_quarantined_with_its_reason(spark, load_job, write_bronze, table, column, value, reason):
    silver = load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)
    rows, good, bad = two_rows(table, column, value)
    write_bronze(table, rows)
    result = getattr(silver, f"clean_{table}")(silver.create_spark_session())
    key = SILVER_KEYS[table]
    assert [row[key] for row in result[0].collect()] == [good], "the valid row passes with its new attributes"
    assert [(row[key], row["quarantine_reason"]) for row in result[1].collect()] == [(bad, reason)]


def test_the_new_attributes_reach_silver_typed_and_normalized(run_silver):
    orders, _ = run_silver("orders", [{"order_id": "1", "order_status": " Returned ", "cart_value": "100.00",
                                       "tax_amount": "18.00", "shipping_amount": "499.00", "total_amount": "617.00",
                                       "status_timestamp": "2026-06-14T10:00:00", "refund_amount": "40.50"}])
    assert [(row["order_status"], row["cart_value"], row["tax_amount"], row["shipping_amount"], row["total_amount"],
             row["refund_amount"]) for row in orders.values()] == [("returned", 100.0, 18.0, 499.0, 617.0, 40.5)]
    products, _ = run_silver("products", [{"product_id": "1", "brand": " Voltix ", "product_status": "Discontinued"}])
    assert [(row["brand"], row["product_status"]) for row in products.values()] == [("Voltix", "discontinued")]
    campaigns, _ = run_silver("campaigns", [{"campaign_id": "1", "campaign_type": "Retention"}])
    assert [row["campaign_type"] for row in campaigns.values()] == ["retention"]
    spend, _ = run_silver("ad_spend", [{"spend_id": "1", "campaign_id": "5", "clicks": "100", "conversions": "0",
                                        "attributed_revenue": "0.0"}])
    assert [(row["conversions"], row["attributed_revenue"]) for row in spend.values()] == [(0, 0.0)], \
        "a campaign-day without conversions is valid"
