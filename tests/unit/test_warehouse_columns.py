"""Warehouse columns carry real source values (E1, p2-data-v2).

A sweep of the warehouse found columns that were empty or invented:
  - campaign_type: no source has it, so dim_campaign and both campaign marts
    showed 'Unknown' for every campaign; it is gone, and the campaign's real
    campaign_status and ab_test_group are carried instead.
  - inferred members: web events whose customer, product or campaign was
    quarantined in Silver got invented dimension members ('Inferred Campaign 2'),
    while orders, items and spend with the same keys were rejected. Now one rule
    holds for every fact: a NULL key is the unknown member, a present key with no
    dimension version is rejected and counted (audit.fact_web_events_rejects).
  - coalesce defaults turned missing loyalty_points / is_prime_user into 0 / false.
  - every order had channel_sk 0: its channel came from the lineage column
    'source' ('orders_from_purchase_events'); it now comes from the order's
    point-in-time campaign.
  - Gold fact_web_events dropped search_query, time_on_page_sec,
    scroll_depth_percent, schema_version and source_system.
  - fact_orders.discount_amount was always 0; it is now the sum of its items'
    (original_price - discounted_price) x quantity.
  - E2: gross_amount equalled net revenue, so gross - discount did not; it is now
    the sum of original_price x quantity over the order's loaded items, and
    reconciliation checks gross - discount = net for every complete order.

No Postgres runs in the unit tests, so the SQL is checked as text; the
behaviour is checked on a real build by tests/integration/test_warehouse.py.
"""
from __future__ import annotations

import re

from pyspark.sql import functions as F

from conftest import BRONZE_TEMPLATES, CODE_ROOT

WAREHOUSE_DIR = CODE_ROOT / "sql" / "warehouse"
TABLES_SQL = WAREHOUSE_DIR / "create_warehouse_tables.sql"
MARTS_SQL = WAREHOUSE_DIR / "create_marts.sql"
GOLD_JOB = "spark-batch/gold_transformations.py"


def sql_code(path) -> str:
    """The file without -- comments."""
    return "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())


def view_body(name: str) -> str:
    code = sql_code(MARTS_SQL)
    match = re.search(rf"CREATE OR REPLACE VIEW {re.escape(name)} AS(.*?);\s*\n", code, re.S)
    assert match, f"{name} is not defined"
    return match.group(1)


def table_ddl(name: str) -> str:
    match = re.search(rf"CREATE TABLE {re.escape(name)} \((.*?)\n\);", sql_code(TABLES_SQL), re.S)
    assert match, f"{name} is not defined"
    return match.group(1)


def test_campaign_type_is_gone_and_the_real_campaign_attributes_are_carried():
    files = [*WAREHOUSE_DIR.glob("*.sql"), *(CODE_ROOT / "dbt" / "models").rglob("*.sql"),
             *(CODE_ROOT / "dbt" / "tests").rglob("*.sql")]
    assert [p.name for p in files if "campaign_type" in p.read_text(encoding="utf-8")] == []

    ddl = table_ddl("warehouse.dim_campaign")
    assert re.search(r"\bcampaign_status TEXT,", ddl) and re.search(r"\bab_test_group TEXT,", ddl)
    performance = view_body("marts.mart_campaign_performance")
    assert "cc.campaign_status" in performance and "cc.ab_test_group" in performance


def test_no_dimension_member_is_invented_for_a_web_event():
    code = sql_code(TABLES_SQL)
    assert re.search(r"inferred", code, re.IGNORECASE) is None
    # Each SCD2 dimension gets rows from two places only: its unknown member
    # and its staging loader.
    for dimension in ("dim_customer", "dim_product", "dim_campaign"):
        assert code.count(f"INSERT INTO warehouse.{dimension} (") == 2, dimension
    assert not (WAREHOUSE_DIR / "repair_web_event_surrogate_keys.sql").exists()

    # Web events with a present key and no covering version are rejected and counted.
    assert "CREATE TABLE audit.fact_web_events_rejects (" in code
    assert re.search(r"INSERT INTO audit\.fact_web_events_rejects .*?WHERE reject_reason IS NOT NULL", code, re.S)
    assert re.search(r"INSERT INTO warehouse\.fact_web_events .*?WHERE b\.reject_reason IS NULL", code, re.S)
    for dimension, key in (("dim_customer", "customer_id"), ("dim_product", "product_id"),
                           ("dim_campaign", "campaign_id")):
        assert f"'{key} not in {dimension}'" in code
        assert f"'no {dimension} version covers event_timestamp'" in code


def test_missing_customer_attributes_stay_null():
    code = sql_code(TABLES_SQL)
    assert "warehouse.safe_int(r->>'loyalty_points') AS loyalty_points," in code
    assert "warehouse.safe_bool(r->>'is_prime_user') AS is_prime_user," in code
    ddl = table_ddl("warehouse.dim_customer")
    assert re.search(r"\bloyalty_points INTEGER,", ddl) and re.search(r"\bis_prime_user BOOLEAN,", ddl)


def test_no_channel_comes_from_the_lineage_source_column():
    for path in (TABLES_SQL, WAREHOUSE_DIR / "reconciliation_report.sql"):
        for line in sql_code(path).splitlines():
            if "r->>'source'" in line:
                # The only use left: the lineage column itself, as source_system.
                assert line.strip() == (
                    "coalesce(nullif(r->>'source', ''), nullif(r->>'source_system', '')) AS source_system,"
                ), f"{path.name}: {line.strip()}"
    code = sql_code(TABLES_SQL)
    assert "ON dch.channel_nk = warehouse.channel_nk(dcamp.channel)" in code, \
        "an order's channel must be its point-in-time campaign's channel"


def test_order_gross_and_discount_are_the_sums_of_their_items():
    code = re.sub(r"\s+", " ", sql_code(TABLES_SQL))
    assert ("warehouse.safe_numeric(r->>'original_price') "
            "* coalesce(warehouse.safe_int(r->>'quantity'), 1) AS gross_amount") in code
    assert ("(warehouse.safe_numeric(r->>'original_price') - warehouse.safe_numeric(r->>'discounted_price')) "
            "* coalesce(warehouse.safe_int(r->>'quantity'), 1) AS discount_amount") in code
    # Every order, with or without loaded items, takes both sums from its items.
    assert "SET gross_amount = order_amounts.gross_amount, discount_amount = order_amounts.discount_amount," in code
    assert ("COALESCE(SUM(i.gross_amount), 0) AS gross_amount, COALESCE(SUM(i.discount_amount), 0) AS discount_amount "
            "FROM warehouse.fact_orders o LEFT JOIN warehouse.fact_order_items i") in code


def test_reconciliation_checks_gross_minus_discount_equals_net():
    code = re.sub(r"\s+", " ", sql_code(WAREHOUSE_DIR / "reconciliation_report.sql"))
    assert "'Order gross minus discount equals net revenue for every complete order'" in code
    assert "WHERE o.line_total = o.net_revenue AND o.gross_amount - o.discount_amount <> o.net_revenue" in code


def test_gold_web_events_keep_the_engagement_and_lineage_fields(spark, load_job):
    gold = load_job(GOLD_JOB, BATCH_DATE="2026-06-18")
    silver_row = {**BRONZE_TEMPLATES["web_events"], "source_system": "web_events"}
    events = spark.createDataFrame([silver_row]).withColumn("event_timestamp", F.to_timestamp("event_timestamp")) \
        .withColumn("ingestion_date", F.to_date(F.lit("2026-06-18")))

    row = gold.build_fact_web_events(events).collect()[0].asDict()

    assert {key: row.get(key) for key in (
        "search_query", "time_on_page_sec", "scroll_depth_percent", "schema_version", "source_system"
    )} == {"search_query": "enim", "time_on_page_sec": 783, "scroll_depth_percent": 40,
           "schema_version": "v1", "source_system": "web_events"}
