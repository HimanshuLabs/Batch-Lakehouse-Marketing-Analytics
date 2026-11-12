"""Unknown members: organic orders and one label (8b, p2-data-v2).

An organic order (no campaign) is a valid fact on the unknown campaign member,
so the SCD2 campaign mart reconciles against orders with a known campaign only.

The unknown member used to be spelled 'Unknown', 'UNKNOWN', 'unknown',
'Unknown Campaign', 'Unknown Customer' and 'Unknown Product'. Now IDs and keys
are 'UNKNOWN' and display names and labels 'Unknown'. The only lowercase
'unknown' left is normalized text inside hashed natural keys (and the input
token that maps the text 'unknown' to NULL): changing it would change existing
region, channel and fact keys. Those lines are listed below, exactly.
"""
from __future__ import annotations

import re

import pytest
from pyspark.sql import functions as F

from conftest import CODE_ROOT

MARTS_JOB = "spark-batch/scd2_gold_marts.py"

LABEL_FILES = [
    *sorted((CODE_ROOT / "sql").glob("*.sql")),
    *sorted((CODE_ROOT / "sql" / "warehouse").glob("*.sql")),
    *sorted((CODE_ROOT / "spark-batch").glob("*.py")),
    *sorted((CODE_ROOT / "dbt" / "models").rglob("*.sql")),
    *sorted((CODE_ROOT / "dbt" / "tests").glob("*.sql")),
    CODE_ROOT / "sql" / "export_power_bi_dataset.py",
]

# A quoted literal that starts with "unknown" in any case.
UNKNOWN_LITERAL = re.compile(r"""(['"])(unknown[^'"]*)\1""", re.IGNORECASE)
# snake_case codes such as unknown_member are identifiers, not labels.
IDENTIFIER = re.compile(r"^unknown_[a-z_]+$")

# The lowercase key text that stays, by file and exact line.
KEY_TEXT_EXCEPTIONS = {
    ("sql/warehouse/create_warehouse_tables.sql", line) for line in (
        # input tokens that mean NULL
        "WHEN lower(btrim(value)) IN ('null', 'none', 'nan', 'unknown') THEN NULL",
        # deterministic_sk's NULL fallback (hash input)
        "SELECT warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown'));",
        # region_nk and channel_nk (hashed into region_sk and channel_sk)
        "coalesce(nullif(btrim(city_value), ''), 'unknown')",
        "|| coalesce(nullif(btrim(state_value), ''), 'unknown')",
        "|| coalesce(nullif(btrim(country_value), ''), 'unknown')",
        "SELECT lower(coalesce(nullif(btrim(channel_value), ''), 'unknown'));",
        # the comment that explains these exceptions
        "-- labels are 'Unknown'. The lowercase 'unknown' left inside hashed natural",
        # the unknown region and channel members' natural keys, and the filters that skip them
        "VALUES (0, 'unknown|unknown|unknown', 'Unknown', 'Unknown', 'Unknown')",
        "VALUES (0, 'unknown', 'Unknown', 'Unknown')",
        "WHERE region_nk <> 'unknown|unknown|unknown'",
        "WHERE channel_nk <> 'unknown'",
        # the campaign spend natural key (hashed into campaign_spend_sk)
        "|| coalesce(nullif(r->>'spend_date', ''), nullif(r->>'date', ''), 'unknown-date')",
        "|| coalesce(nullif(r->>'channel', ''), nullif(r->>'traffic_source', ''), 'unknown-channel'),",
    )
} | {
    ("sql/warehouse/create_marts.sql", line) for line in (
        # compared after lower(): normalization, not a label
        "lower(COALESCE(fwe.event_type, 'unknown')) AS event_type_norm,",
        "lower(COALESCE(fwe.user_journey_stage, 'unknown')) AS stage_norm",
    )
}


def unknown_literals():
    for path in LABEL_FILES:
        relative = path.relative_to(CODE_ROOT).as_posix()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in UNKNOWN_LITERAL.finditer(line):
                yield relative, number, line.strip(), match.group(2)


def test_unknown_is_spelled_unknown_for_ids_and_unknown_for_labels():
    wrong = [
        f"{relative}:{number}: {value!r}"
        for relative, number, line, value in unknown_literals()
        if value not in ("UNKNOWN", "Unknown")
        and not IDENTIFIER.match(value)
        and (relative, line) not in KEY_TEXT_EXCEPTIONS
    ]
    assert wrong == []


def test_every_key_text_exception_is_still_needed():
    # An exception that no longer matches a line is stale; remove it.
    seen = {(relative, line) for relative, _, line, value in unknown_literals()}
    assert sorted(KEY_TEXT_EXCEPTIONS - seen) == []


def test_the_spark_unknown_member_matches_the_warehouse(load_job):
    pit = load_job("spark-batch/point_in_time_fact_joins.py")
    marts = load_job(MARTS_JOB)
    assert (pit.UNKNOWN_KEY, pit.UNKNOWN_LABEL) == ("UNKNOWN", "Unknown")
    assert (marts.UNKNOWN_KEY, marts.UNKNOWN_LABEL) == ("UNKNOWN", "Unknown")
    assert pit.INVALID_KEY == marts.INVALID_KEY == "INVALID"


def test_the_campaign_mart_reconciles_to_orders_with_a_known_campaign(spark, load_job):
    marts = load_job(MARTS_JOB)
    # Completed orders: 100 on a real campaign, 40 organic (unknown member), 25
    # on a campaign with no valid dimension row (invalid-reference member).
    # Order 4 was cancelled: not a sale. Order 5 was returned and refunded in
    # full (K1): a sale of 30 and a refund of 30, so no revenue either.
    fact_orders = spark.createDataFrame(
        [(1, "c-1", "campaign-4-v1", 100.0, "completed", 0.0), (2, "c-1", "UNKNOWN", 40.0, "completed", 0.0),
         (3, "INVALID", "INVALID", 25.0, "completed", 0.0), (4, "c-1", "campaign-4-v1", 70.0, "cancelled", 0.0),
         (5, "c-1", "campaign-4-v1", 30.0, "returned", 30.0)],
        "order_id int, customer_sk string, campaign_sk string, cart_value double, order_status string, refund_amount double")
    items = spark.createDataFrame(
        [(10, 1, "p-1", 100.0, 1, 0.0), (20, 2, "p-1", 40.0, 2, 0.0), (30, 3, "INVALID", 25.0, 1, 0.0),
         (40, 4, "p-1", 70.0, 1, 0.0), (50, 5, "p-1", 30.0, 1, 30.0)],
        "order_item_id int, order_id int, product_sk string, line_amount double, quantity int, refund_amount double")
    spend = spark.createDataFrame([(1, "campaign-4-v1", 25.0), (2, "INVALID", 9.0)],
                                  "spend_id int, campaign_sk string, spend_amount double")
    # J3: each mart has a row for the unknown and for the invalid-reference
    # member, so its totals are the fact totals.
    campaign_mart = spark.createDataFrame(
        [("campaign-4-v1", 100.0, 25.0, 30.0), ("UNKNOWN", 40.0, 0.0, 0.0), ("INVALID", 25.0, 9.0, 0.0)],
        "campaign_sk string, total_revenue double, total_spend double, refund_amount double")
    clv_mart = spark.createDataFrame(
        [("c-1", 140.0, 70.0, 30.0, 30.0), ("INVALID", 25.0, 0.0, 0.0, 0.0)],
        "customer_sk string, total_revenue double, cancelled_amount double, returned_amount double, refund_amount double")
    # Units sold are those of the orders that were not cancelled: the returned order's unit is one of them.
    product_mart = spark.createDataFrame([("p-1", 140.0, 4, 30.0), ("INVALID", 25.0, 1, 0.0)],
                                         "product_sk string, total_revenue double, units_sold int, refund_amount double")
    funnel_mart = spark.createDataFrame([("facebook",)], "channel string")

    result = marts.validate_marts(fact_orders, items, spend, campaign_mart, clv_mart, product_mart, funnel_mart)

    assert result["status"] == "PASS", result["checks"]
    assert result["checks"]["campaign_revenue_reconciles_to_campaign_fact_orders"] is True
    assert result["checks"]["clv_revenue_reconciles_to_fact_orders"] is True
    assert result["checks"]["clv_cancelled_and_returned_reconcile_to_fact_orders"] is True
    for name in ("campaign_refunds_reconcile_to_fact_orders", "clv_refunds_reconcile_to_fact_orders",
                 "product_refunds_reconcile_to_fact_order_items"):
        assert result["checks"][name] is True, name
    metrics = result["metrics"]
    assert metrics["completed_order_revenue"] == pytest.approx(165.0)
    assert metrics["cancelled_and_returned_order_amount"] == pytest.approx(100.0)
    assert metrics["unknown_or_invalid_campaign_order_revenue"] == pytest.approx(65.0)
    assert metrics["invalid_customer_order_revenue"] == pytest.approx(25.0)

    # A mart that counted the cancelled and the returned order as revenue fails.
    inflated = spark.createDataFrame(
        [("campaign-4-v1", 200.0, 25.0, 30.0), ("UNKNOWN", 40.0, 0.0, 0.0), ("INVALID", 25.0, 9.0, 0.0)],
        "campaign_sk string, total_revenue double, total_spend double, refund_amount double")
    assert marts.validate_marts(fact_orders, items, spend, inflated, clv_mart, product_mart, funnel_mart)["status"] == "FAIL"
    # A mart that lost the refund fails the refund check.
    no_refund = clv_mart.withColumn("refund_amount", F.lit(0.0))
    lost = marts.validate_marts(fact_orders, items, spend, campaign_mart, no_refund, product_mart, funnel_mart)
    assert lost["status"] == "FAIL" and lost["checks"]["clv_refunds_reconcile_to_fact_orders"] is False
    assert (result["metrics"]["order_refunds"], result["metrics"]["item_refunds"]) == (30.0, 30.0)
    # A mart without its member rows no longer ties out to the facts.
    known_only = campaign_mart.filter("campaign_sk not in ('UNKNOWN', 'INVALID')")
    without = marts.validate_marts(fact_orders, items, spend, known_only, clv_mart, product_mart, funnel_mart)
    assert without["checks"]["campaign_revenue_reconciles_to_campaign_fact_orders"] is False
    assert without["checks"]["campaign_spend_reconciles_to_fact_campaign_spend"] is False
    assert result["checks"]["product_units_reconcile_to_fact_order_items"] is True


def test_the_gold_marts_carry_flagged_rows_for_the_unknown_and_invalid_members(spark, load_job):
    marts = load_job(MARTS_JOB)
    orders = spark.createDataFrame(
        [(1, "c-1", "k-1", "2026-06-01 10:00:00", 100.0, "completed", 0.0),
         (2, "INVALID", "UNKNOWN", "2026-06-02 10:00:00", 40.0, "completed", 0.0),
         (3, "c-1", "INVALID", "2026-06-03 10:00:00", 25.0, "returned", 25.0)],
        "order_id int, customer_sk string, campaign_sk string, order_date string, cart_value double, order_status string, "
        "refund_amount double",
    ).withColumn("order_date", F.to_timestamp("order_date"))
    customers = spark.createDataFrame(
        [("c-1", 7, "Asha", "asha@example.org", "Pune", "MH", "India", "Student", "Gold")],
        "customer_sk string, customer_id int, customer_name string, email string, city string, state string, "
        "country string, customer_segment string, loyalty_tier string")
    clv = {row["customer_sk"]: row.asDict() for row in marts.build_customer_lifetime_value_mart(orders, customers).collect()}
    assert {key: (row["member_type"], row["customer_name"], row["total_revenue"], row["returned_amount"])
            for key, row in clv.items()} == {
        "c-1": ("known", "Asha", 100.0, 25.0), "INVALID": ("invalid_reference", "Invalid reference", 40.0, 0.0)}

    campaigns = spark.createDataFrame(
        [("k-1", 5, "Summer", "conversion", "facebook", "Student", 1000.0, "active", "2026-05-01", "2026-07-01", True)],
        "campaign_sk string, campaign_id int, campaign_name string, campaign_type string, channel string, "
        "target_segment string, budget double, campaign_status string, start_date string, end_date string, is_current boolean")
    spend = spark.createDataFrame(
        [(1, "k-1", "2026-06-01", 30.0, 600, 6, 2, 300.0), (2, "k-1", "2026-06-01", 20.0, 400, 4, 1, 150.0),
         (3, "INVALID", "2026-06-01", 9.0, 100, 2, 0, 0.0)],
        "spend_id int, campaign_sk string, spend_date string, spend_amount double, impressions bigint, clicks bigint, "
        "conversions bigint, attributed_revenue double")
    mart = {row["campaign_sk"]: row.asDict() for row in marts.build_campaign_performance_mart(orders, spend, campaigns).collect()}
    assert {key: (row["member_type"], row["campaign_name"], row["total_revenue"], row["total_spend"])
            for key, row in mart.items()} == {
        # The regional rows of the campaign-day are summed: 30 + 20.
        "k-1": ("known", "Summer", 100.0, 50.0), "UNKNOWN": ("unknown_member", "Unknown", 40.0, 0.0),
        "INVALID": ("invalid_reference", "Invalid reference", 0.0, 9.0)}
    assert (mart["k-1"]["ad_platform_conversions"], mart["k-1"]["platform_attributed_revenue"], mart["k-1"]["platform_roas"]) == (3, 450.0, 9.0)
    assert mart["INVALID"]["returned_amount"] == 25.0
