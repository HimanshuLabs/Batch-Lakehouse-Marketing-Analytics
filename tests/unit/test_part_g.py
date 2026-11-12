"""Part G (p2-data-v2): the remaining correctness gaps found by the edge-case audit.

C3  Bronze read CSV without multiLine and with Spark's default backslash escape,
    so a doubled quote kept its quote characters and a quoted line break split
    one record into two broken rows.
C4  safe_int cast through double, so '3100227.9' became 3100227 and '-0.4'
    became 0 silently.
C5  Two versions of one dimension key with the same updated_at in one batch:
    one was kept by record hash, the other dropped without a trace.
C6  Amounts with more than two decimal places were accepted, so the warehouse's
    gross - discount = net check could fail on rounding.
C7  A free item (discounted_price 0, line_amount 0) was quarantined.
C8  Orders, their items and web events dated after the end of their batch day
    (Asia/Kolkata) were accepted.
7   A missing expected staging table only raised a NOTICE, and the warehouse was
    built without it.
"""
from __future__ import annotations

import os

import pytest

from conftest import CODE_ROOT, begin_build, build_step, require_sandbox_database, reset_sandbox

SILVER_JOB = "spark-batch/silver_transformations.py"
BRONZE_JOB = "spark-batch/bronze_ingestion.py"
BRONZE_QC_JOB = "spark-batch/bronze_quality_checks.py"
BATCH_DATE = "2026-06-18"


@pytest.fixture
def silver(load_job):
    return load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)


def reasons_by(invalid, key):
    return sorted((row[key], row["quarantine_reason"]) for row in invalid.collect())


# --- C3 ----------------------------------------------------------------------

def test_csv_fields_with_commas_quotes_and_line_breaks_round_trip(spark, load_job, project_root, write_bronze):
    names = ['Speaker, "Pro"', "Two-line\nname", "Café ☃ Ünïcode"]
    raw = project_root / "products.csv"
    import csv
    with raw.open("w", encoding="utf-8", newline="") as handle:  # how pandas writes it (RFC 4180)
        writer = csv.writer(handle)
        writer.writerow(["product_id", "product_name", "category"])
        writer.writerows([[str(i + 1), name, "Audio"] for i, name in enumerate(names)])

    bronze = load_job(BRONZE_JOB, BATCH_DATE=BATCH_DATE)
    rows = bronze.read_source(spark, "csv", raw).orderBy("product_id").collect()
    assert [(r["product_id"], r["product_name"], r["category"]) for r in rows] == [
        (str(i + 1), name, "Audio") for i, name in enumerate(names)]

    quality = load_job(BRONZE_QC_JOB, BATCH_DATE=BATCH_DATE)
    assert quality.count_raw_file_lines(raw, has_header=True) == 3, "raw rows are records, not text lines"

    silver = load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)
    write_bronze("products", [{"product_id": str(i + 1), "product_name": name} for i, name in enumerate(names)])
    valid, invalid, _ = silver.clean_products(silver.create_spark_session())
    assert invalid.count() == 0
    assert sorted(r["product_name"] for r in valid.collect()) == sorted(names), "Silver keeps the exact text"


# --- C4 ----------------------------------------------------------------------

def test_non_integer_ids_are_quarantined_not_truncated(spark, silver, write_bronze):
    write_bronze("order_items", [
        {"order_item_id": "1", "order_id": "3100227.9"},
        {"order_item_id": "-0.4"},
        {"order_item_id": " 8 "},
        {"order_item_id": "9", "quantity": "2.5", "line_amount": "34184.225"},
    ])
    valid, invalid = silver.clean_order_items(silver.create_spark_session())

    assert [(r["order_item_id"], r["order_id"]) for r in valid.collect()] == [(8, 3100227)]
    assert sorted(r["quarantine_reason"] for r in invalid.collect()) == sorted([
        "Invalid order item record: order_id is not an integer",
        "Invalid order item record: order_item_id is not an integer",
        "Invalid order item record: quantity is not an integer",
    ])
    probe = spark.createDataFrame([("3100227.9",), ("-0.4",), (" 8 ",), ("5.0",), ("abc",)], "v string")
    cast = [r[0] for r in probe.select(silver.safe_int("v")).collect()]
    assert cast == [None, None, 8, 5, None]


# --- C5 ----------------------------------------------------------------------

def test_conflicting_versions_with_the_same_updated_at_are_quarantined(silver, write_bronze):
    write_bronze("customers", [
        {"user_id": "4", "membership_tier": "Gold"},
        {"user_id": "4", "membership_tier": "Platinum"},   # same key, same updated_at, other content
        {"user_id": "5", "membership_tier": "Silver"},
        {"user_id": "5", "membership_tier": "Silver"},     # identical duplicate: deduplicated as before
    ])
    valid, invalid, history = silver.clean_customers(silver.create_spark_session())

    reason = "Invalid customer record: conflicting versions with the same updated_at in one batch"
    assert reasons_by(invalid, "user_id") == [(4, reason), (4, reason)]
    assert [r["user_id"] for r in valid.collect()] == [5]
    assert [r["user_id"] for r in history.collect()] == [5]


# --- C6 ----------------------------------------------------------------------

def test_amounts_with_more_than_two_decimals_are_quarantined(silver, write_bronze):
    session = silver.create_spark_session()
    write_bronze("order_items", [
        {"order_item_id": "1", "original_price": "40", "discounted_price": "33.335", "quantity": "3",
         "line_amount": "100.01"},
        {"order_item_id": "2", "original_price": "40", "discounted_price": "33.33", "quantity": "3",
         "line_amount": "99.99"},
    ])
    valid, invalid = silver.clean_order_items(session)
    assert [r["order_item_id"] for r in valid.collect()] == [2]
    assert reasons_by(invalid, "order_item_id") == [
        (1, "Invalid order item record: discounted_price has more than 2 decimal places")]

    write_bronze("orders", [{"order_id": "1", "cart_value": "100.005"}, {"order_id": "2"}])
    valid, invalid = silver.clean_orders(session)
    assert [r["order_id"] for r in valid.collect()] == [2]
    assert reasons_by(invalid, "order_id") == [(1, "Invalid order record: cart_value has more than 2 decimal places")]


# --- C7 ----------------------------------------------------------------------

def test_a_free_item_is_accepted_and_a_negative_line_is_not(silver, write_bronze):
    write_bronze("order_items", [
        {"order_item_id": "1", "original_price": "100", "discount_percent": "100", "discounted_price": "0",
         "quantity": "1", "line_amount": "0"},
        {"order_item_id": "2", "line_amount": "-5"},
    ])
    valid, invalid = silver.clean_order_items(silver.create_spark_session())
    assert [(r["order_item_id"], r["line_amount"]) for r in valid.collect()] == [(1, 0.0)]
    assert [r["order_item_id"] for r in invalid.collect()] == [2]


# --- C8 ----------------------------------------------------------------------

def test_records_after_the_end_of_their_batch_day_are_quarantined(silver, write_bronze):
    # The batch day is 2026-06-18 (ingestion_date) in Asia/Kolkata: it ends at
    # 2026-06-19 00:00 IST.
    write_bronze("orders", [
        {"order_id": "1", "order_timestamp": "2026-06-18T23:59:59"},
        {"order_id": "2", "order_timestamp": "2026-06-19T00:00:00"},
    ])
    write_bronze("order_items", [{"order_item_id": "10", "order_id": "1"}, {"order_item_id": "20", "order_id": "2"}])
    write_bronze("web_events", [
        {"event_id": "e1", "event_timestamp": "2026-06-18T23:59:59"},
        {"event_id": "e2", "event_timestamp": "2026-06-19T00:00:00", "event_time": "2026-06-19T00:00:00"},
    ])
    session = silver.create_spark_session()

    valid, invalid = silver.clean_orders(session)
    assert [r["order_id"] for r in valid.collect()] == [1]
    assert reasons_by(invalid, "order_id") == [
        (2, "Invalid order record: order_timestamp is after the end of the batch day")]

    valid, invalid = silver.clean_order_items(session)
    assert [r["order_item_id"] for r in valid.collect()] == [10]
    assert reasons_by(invalid, "order_item_id") == [
        (20, "Invalid order item record: its order's order_timestamp is after the end of the batch day")]

    valid, invalid = silver.clean_web_events(session)
    assert [r["event_id"] for r in valid.collect()] == ["e1"]
    assert reasons_by(invalid, "event_id") == [
        ("e2", "Invalid web event record: event_timestamp is after the end of the batch day")]


# --- 7 -----------------------------------------------------------------------

def sql_code(path) -> str:
    return "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())


def test_a_missing_staging_table_fails_the_warehouse_build():
    code = sql_code(CODE_ROOT / "sql" / "warehouse" / "create_warehouse_tables.sql")
    assert "RAISE NOTICE" not in code
    assert code.count("RAISE EXCEPTION 'Missing staging table:") == 7


def test_a_missing_staging_table_fails_the_build_in_postgres():
    require_sandbox_database()
    import psycopg
    with psycopg.connect(host="127.0.0.1", port=5546, user="project2", password="project2",
                         dbname="marketing_analytics", autocommit=True) as conn:
        reset_sandbox(conn)
        conn.execute(""
                     "CREATE SCHEMA staging")
        sql = begin_build() + build_step("create_warehouse_tables") + "COMMIT;"
        with pytest.raises(psycopg.errors.RaiseException, match="Missing staging table: no customer dimension"):
            conn.execute(sql)
        conn.execute("ROLLBACK")  # the build transaction was aborted
        assert conn.execute("SELECT to_regclass('warehouse.dim_customer')").fetchone()[0] is None, \
            "nothing is left behind: the build is one transaction"
