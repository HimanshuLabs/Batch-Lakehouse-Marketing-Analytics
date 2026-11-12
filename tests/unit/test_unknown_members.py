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


def test_the_campaign_mart_reconciles_to_orders_with_a_known_campaign(spark, load_job):
    marts = load_job(MARTS_JOB)
    fact_orders = spark.createDataFrame(
        [(1, "campaign-4-v1", 100.0), (2, "UNKNOWN", 40.0)], "order_id int, campaign_sk string, total_amount double")
    items = spark.createDataFrame([(10, 140.0)], "order_item_id int, line_amount double")
    spend = spark.createDataFrame([(1, 25.0)], "spend_id int, spend_amount double")
    campaign_mart = spark.createDataFrame([("campaign-4-v1", 100.0, 25.0)],
                                          "campaign_sk string, total_revenue double, total_spend double")
    clv_mart = spark.createDataFrame([(1, 140.0)], "customer_sk int, total_revenue double")
    product_mart = spark.createDataFrame([(1, 140.0)], "product_sk int, total_revenue double")
    funnel_mart = spark.createDataFrame([("facebook",)], "channel string")

    result = marts.validate_marts(fact_orders, items, spend, campaign_mart, clv_mart, product_mart, funnel_mart)

    assert result["status"] == "PASS", result["checks"]
    assert result["checks"]["campaign_revenue_reconciles_to_campaign_fact_orders"] is True
    assert result["checks"]["clv_revenue_reconciles_to_fact_orders"] is True
    assert result["metrics"]["unknown_campaign_order_revenue"] == pytest.approx(40.0)
