"""Part K1 (p2-data-v2): orders have a lifecycle, revenue has refunds, and the
marts have leak-free daily snapshots.

1  An order is placed at order_timestamp. A cancelled order has a cancellation
   time shortly after, a returned order a return time some days later and a
   refund (full or partial). When that falls after the batch that delivered
   the order, a later batch delivers the order again with its new status and
   a later updated_at; Silver keeps the latest version.
2  A sale is an order that was not cancelled, recognised on its order date; a
   refund is recognised on the date of the return. Every mart uses that.
3  marts.mart_customer_daily_snapshot, mart_product_daily_snapshot and
   mart_campaign_daily_snapshot hold the lifetime marts as they were at the
   end of each business date: nothing after snapshot_date reaches a row.

The PostgreSQL cases need the disposable sandbox on localhost:5546 (P2_F_SANDBOX=1).
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

import pandas as pd
import pytest

from conftest import require_sandbox_database, reset_sandbox
from test_part_h import STAGING_ROWS
from test_part_h2 import build

GENERATOR = "data-generator/generate_data.py"
SILVER_JOB = "spark-batch/silver_transformations.py"
PIT_JOB = "spark-batch/point_in_time_fact_joins.py"
BATCHES = ("2026-05-18", "2026-06-18", "2026-06-25")


# --- 1: the lifecycle in the source ---------------------------------------------

@pytest.fixture(scope="module")
def three_batches(tmp_path_factory):
    """Three batches generated in order into one raw folder: {batch: orders}."""
    import importlib.util
    import os
    import shutil

    from conftest import CODE_ROOT

    root = tmp_path_factory.mktemp("lifecycle")
    target = root / GENERATOR
    target.parent.mkdir(parents=True)
    shutil.copy(CODE_ROOT / GENERATOR, target)
    saved = os.environ.get("BATCH_DATE")
    frames = {}
    try:
        for batch in BATCHES:
            os.environ["BATCH_DATE"] = batch
            spec = importlib.util.spec_from_file_location(f"p2_lifecycle_generator_{batch.replace('-', '')}", target)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.main()
            folder = root / "data" / "raw" / f"batch_date={batch}"
            frames[batch] = (pd.read_csv(folder / "orders.csv"),
                             [json.loads(line) for line in (folder / "generation_truth.json").read_text().splitlines()])
    finally:
        if saved is None:
            os.environ.pop("BATCH_DATE", None)
        else:
            os.environ["BATCH_DATE"] = saved
    return frames


def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    # Without the dirty rows: 0, 2, 3, 4 and the appended fixtures (top of the ID block).
    orders = orders.drop(index=[0, 2, 3, 4])
    return orders[orders["order_id"] % 100_000 < 99_900]


def test_an_order_is_cancelled_shortly_after_or_returned_days_later_with_a_refund(three_batches):
    seen = set()
    for batch, (orders, _) in three_batches.items():
        assert {"status_timestamp", "refund_amount", "updated_at"} <= set(orders.columns), batch
        clean = clean_orders(orders)
        placed = pd.to_datetime(clean["order_timestamp"])
        changed = pd.to_datetime(clean["status_timestamp"])
        delay = (changed - placed).dt.total_seconds()
        completed, cancelled, returned = (clean["order_status"] == status for status in ("completed", "cancelled", "returned"))
        seen |= set(clean["order_status"])
        assert (delay[completed] == 0).all() and (clean.loc[completed, "refund_amount"] == 0).all(), batch
        assert delay[cancelled].between(5 * 60, 6 * 3600).all(), "a cancellation follows the order within hours"
        assert (clean.loc[cancelled, "refund_amount"] == 0).all(), batch
        assert delay[returned].between(2 * 86400, 20 * 86400).all(), "a return follows the order by days"
        refund, net = clean.loc[returned, "refund_amount"], clean.loc[returned, "cart_value"]
        assert ((refund > 0) & (refund <= net)).all(), batch
        # The batch knows nothing that happens after its own day.
        assert (changed.dt.date <= datetime.fromisoformat(batch).date()).all(), batch
        assert (pd.to_datetime(clean["updated_at"]) == changed).all(), batch
    assert seen == {"completed", "cancelled", "returned"}
    anchor = clean_orders(three_batches[BATCHES[0]][0])
    returned = anchor[anchor["order_status"] == "returned"]
    assert (returned["refund_amount"] == returned["cart_value"]).any(), "some refunds are full"
    assert (returned["refund_amount"] < returned["cart_value"]).any(), "some refunds are partial"


def test_a_later_batch_delivers_the_order_again_when_its_status_changes(three_batches):
    first_seen, delivered_again = {}, {}
    for batch, (orders, _) in three_batches.items():
        block = (datetime.fromisoformat(batch) - datetime.fromisoformat(BATCHES[0])).days * 100_000
        clean = clean_orders(orders)
        own = clean[clean["order_id"].between(block + 1, block + 99_899)]
        again = clean[~clean["order_id"].isin(own["order_id"])]
        for row in own.to_dict("records"):
            first_seen[row["order_id"]] = row
        for row in again.to_dict("records"):
            assert row["order_id"] not in delivered_again, "an order is delivered again once"
            delivered_again[row["order_id"]] = (batch, row)
    assert len(delivered_again) >= 5, "orders of an earlier batch change status in a later one"
    assert {batch for batch, _ in delivered_again.values()} == set(BATCHES[1:])
    for order_id, (batch, row) in delivered_again.items():
        before = first_seen[order_id]
        assert before["order_status"] == "completed" and before["refund_amount"] == 0
        assert row["order_status"] in ("cancelled", "returned")
        assert row["updated_at"] > before["updated_at"], "the new version is the later one"
        assert row["status_timestamp"] > before["order_timestamp"]
        # Only the lifecycle changed.
        same = [c for c in row if c not in ("order_status", "status_timestamp", "refund_amount", "updated_at")]
        assert {c: row[c] for c in same if not pd.isna(row[c])} == {c: before[c] for c in same if not pd.isna(before[c])}
        # It comes with the first batch on or after the day of the change.
        changed = row["status_timestamp"][:10]
        earlier = [b for b in BATCHES if b < batch]
        assert changed <= batch and all(changed > b for b in earlier if b >= before["order_timestamp"][:10])
    # Every order whose final status is not 'completed' reaches it in some batch.
    final = {order_id: random.Random(f"order_status|{order_id}").choices(
        ["completed", "cancelled", "returned"], [0.88, 0.07, 0.05])[0] for order_id in first_seen}
    latest = {order_id: delivered_again.get(order_id, (None, row))[1]["order_status"] for order_id, row in first_seen.items()}
    pending = {order_id for order_id in first_seen if latest[order_id] != final[order_id]}
    for order_id in pending:
        # Still to come: its change is after the last batch.
        placed = datetime.fromisoformat(first_seen[order_id]["order_timestamp"])
        assert final[order_id] in ("returned", "cancelled") and placed + timedelta(days=20) >= datetime.fromisoformat(BATCHES[-1])


def test_each_impossible_lifecycle_has_a_dirty_row_recorded_in_the_truth_file(three_batches):
    for batch, (orders, truth) in three_batches.items():
        assert truth[0]["generator_version"] == "coherent_source_v6"
        fixtures = {row["reason"]: orders.loc[row["row_index"]] for row in truth
                    if row["kind"] == "quality_fixture" and row["table"] == "orders.csv"}
        early = fixtures["return_before_order"]
        assert early["order_status"] == "returned" and early["status_timestamp"] < early["order_timestamp"], batch
        above = fixtures["refund_above_net"]
        assert above["order_status"] == "returned" and above["refund_amount"] > above["cart_value"], batch
        stray = fixtures["refund_on_non_returned_order"]
        assert stray["order_status"] == "completed" and stray["refund_amount"] > 0, batch


LIFECYCLE_RULES = [
    ({"order_status": "returned", "status_timestamp": "2026-06-10T08:48:11", "refund_amount": "10.00"},
     "Invalid order record: status_timestamp is before order_timestamp"),
    ({"order_status": "returned", "status_timestamp": "2026-06-14T08:48:11", "refund_amount": "999999.00"},
     "Invalid order record: refund_amount is more than the order's net value"),
    ({"order_status": "completed", "refund_amount": "10.00"},
     "Invalid order record: refund_amount on an order that was not returned"),
    ({"order_status": "cancelled", "status_timestamp": "2026-06-11T09:48:11", "refund_amount": "10.00"},
     "Invalid order record: refund_amount on an order that was not returned"),
    ({"order_status": "returned", "status_timestamp": "2026-06-14T08:48:11", "refund_amount": "0.0"},
     "Invalid order record: a returned order has no refund_amount"),
    ({"refund_amount": "-1.00"}, "Invalid order record: refund_amount is missing or negative"),
    ({"refund_amount": None}, "Invalid order record: refund_amount is missing or negative"),
    ({"status_timestamp": None}, "Invalid order record: status_timestamp or updated_at is missing"),
    ({"updated_at": "not a time"}, "Invalid order record: status_timestamp or updated_at is missing"),
    ({"order_status": "returned", "status_timestamp": "2026-06-19T08:48:11", "refund_amount": "10.00"},
     "Invalid order record: status_timestamp is after the end of the batch day"),
]


@pytest.mark.parametrize("override, reason", LIFECYCLE_RULES)
def test_an_impossible_lifecycle_is_quarantined_with_its_reason(load_job, write_bronze, override, reason):
    silver = load_job(SILVER_JOB, BATCH_DATE="2026-06-18")
    write_bronze("orders", [{"order_id": "1"}, {"order_id": "2", **override}])
    valid, invalid = silver.clean_orders(silver.create_spark_session())
    assert [row["order_id"] for row in valid.collect()] == [1]
    assert [(row["order_id"], row["quarantine_reason"]) for row in invalid.collect()] == [(2, reason)]


def test_silver_keeps_the_latest_version_of_an_order(load_job, write_bronze):
    silver = load_job(SILVER_JOB, BATCH_DATE="2026-06-25")
    returned = {"order_id": "1", "order_status": "returned", "status_timestamp": "2026-06-20T10:00:00",
                "refund_amount": "40.00", "updated_at": "2026-06-20T10:00:00"}
    # The first delivery (completed), then the return in a later batch; order 2
    # is delivered twice with the same updated_at: the later batch wins.
    write_bronze("orders", [{"order_id": "1"}, {"order_id": "2", "payment_method": "COD"}], ingestion_date="2026-06-18")
    write_bronze("orders", [returned, {"order_id": "2", "payment_method": "Card"}], ingestion_date="2026-06-25", append=True)
    for _ in range(2):   # the same answer on every run
        valid, invalid = silver.clean_orders(silver.create_spark_session())
        rows = {row["order_id"]: row for row in valid.collect()}
        assert sorted(rows) == [1, 2] and invalid.count() == 0
        assert (rows[1]["order_status"], rows[1]["refund_amount"]) == ("returned", 40.0)
        assert rows[1]["status_timestamp"] == rows[1]["updated_at"] > rows[1]["order_timestamp"]
        assert rows[2]["payment_method"] == "Card"

    # Delivered in the other order, the answer is the same: updated_at decides.
    write_bronze("orders", [returned], ingestion_date="2026-06-18")
    write_bronze("orders", [{"order_id": "1"}], ingestion_date="2026-06-25", append=True)
    valid, _ = silver.clean_orders(silver.create_spark_session())
    assert [row["order_status"] for row in valid.collect() if row["order_id"] == 1] == ["returned"]


def test_an_order_refund_is_split_over_its_items_to_the_paisa(spark, load_job):
    pit = load_job(PIT_JOB)
    orders = spark.createDataFrame([(1, 100.00), (2, 0.0), (3, 33.33)], "order_id int, refund_amount double")
    items = spark.createDataFrame(
        [(10, 1, 100.0), (11, 1, 100.0), (12, 1, 100.0), (20, 2, 50.0), (30, 3, 10.0), (31, 3, 20.01), (32, 3, 3.32),
         (40, 4, 5.0)],
        "order_item_id int, order_id int, line_amount double")
    result = {row["order_item_id"]: row["refund_amount"] for row in pit.allocate_order_refunds(items, orders).collect()}
    assert result[20] == 0.0 and result[40] == 0.0, "no refund, or an order that is not in the facts"
    assert round(result[10] + result[11] + result[12], 2) == 100.00 and sorted(result[k] for k in (10, 11, 12)) == [33.33, 33.33, 33.34]
    assert round(result[30] + result[31] + result[32], 2) == 33.33
    assert all(0 <= result[k] <= line for k, line in ((30, 10.0), (31, 20.01), (32, 3.32)))
    assert all(round(value * 100, 6) == round(value * 100) for value in result.values()), "whole paise"


# --- 2 and 3: revenue with refunds, and the daily snapshots (PostgreSQL) -------------

# Staging timestamps are UTC; business dates are Asia/Kolkata dates.
#   order 3  placed 11 June, returned 14 June with a partial refund of 120 (of 300)
#   order 4  placed 13 June 04:30 IST, cancelled 14 June 01:30 IST
#   order 5  placed 13 June, returned 16 June in full (50)
LIFECYCLE_ROWS = """
INSERT INTO staging.stg_gold_fact_orders_scd2 VALUES
    (3, 7, 5, '2026-06-11 10:00', 300.00, 'UPI', '2026-06-18', 'returned', 54.00, 499.00, 853.00, '2026-06-14 12:00', 120.00),
    (4, 7, 5, '2026-06-12 23:00', 150.00, 'UPI', '2026-06-18', 'cancelled', 27.00, 499.00, 676.00, '2026-06-13 20:00', 0),
    (5, 7, NULL, '2026-06-13 10:00', 50.00, 'UPI', '2026-06-18', 'returned', 9.00, 499.00, 558.00, '2026-06-16 10:00', 50.00);
INSERT INTO staging.stg_gold_fact_order_items_scd2 VALUES
    (31, 3, 4, 2, 120.00, 100.00, 200.00, '2026-06-18', 80.00), (32, 3, 4, 1, 120.00, 100.00, 100.00, '2026-06-18', 40.00),
    (40, 4, 4, 1, 150.00, 150.00, 150.00, '2026-06-18', 0), (50, 5, 4, 1, 50.00, 50.00, 50.00, '2026-06-18', 50.00);
INSERT INTO staging.stg_gold_fact_campaign_spend_scd2 VALUES
    (3, 5, 'facebook', '2026-06-12', 80.00, 2000, 40, '2026-06-18', 4, 900.00, 'India', 'Kerala', 80.00);
INSERT INTO staging.stg_gold_fact_web_events VALUES
    ('e3', 's3', 7, 4, 5, '2026-06-11 09:55', 'purchase', 'facebook', 300.00, '2026-06-18'),
    ('e5', 's5', 7, 4, 5, '2026-06-12 22:55', 'purchase', 'facebook', 150.00, '2026-06-18'),
    ('e6', 's6', 7, 4, NULL, '2026-06-13 09:55', 'purchase', 'organic', 50.00, '2026-06-18');
"""


@pytest.fixture
def pg():
    require_sandbox_database()
    import psycopg
    with psycopg.connect(host="127.0.0.1", port=5546, user="project2", password="project2",
                         dbname="marketing_analytics", autocommit=True) as conn:
        reset_sandbox(conn)
        conn.execute("CREATE SCHEMA staging")
        conn.execute(STAGING_ROWS + LIFECYCLE_ROWS)
        yield conn


def rows(pg, sql):
    cursor = pg.execute(sql)
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def test_sales_are_recognised_on_the_order_date_and_refunds_on_the_return_date(pg, tmp_path):
    build(tmp_path)
    daily = {str(row["revenue_date"]): row for row in rows(pg, "SELECT * FROM marts.mart_revenue_daily")}

    def day(date, *columns):
        return tuple(float(daily[date][column]) for column in columns)

    measures = ("order_count", "gross_revenue", "discount_amount", "net_revenue", "refund_amount",
                "net_revenue_after_refunds", "returned_order_count", "cancelled_order_count")
    # 11 June: order 3 is a sale of 300 (gross 360 less a discount of 60) on the day it was placed.
    assert day("2026-06-11", *measures) == (1, 360, 60, 300, 0, 300, 0, 0)
    # 13 June: order 5 is a sale; order 4 (cancelled) is not.
    assert day("2026-06-13", *measures) == (1, 50, 0, 50, 0, 50, 0, 1)
    # 14 June: no sale; order 3 comes back and 120 is refunded.
    assert day("2026-06-14", *measures) == (0, 0, 0, 0, 120, -120, 1, 0)
    assert day("2026-06-16", *measures) == (0, 0, 0, 0, 50, -50, 1, 0)
    assert "2026-06-12" not in daily, "as before, a date has a row when something happened on it"
    total = {column: sum(float(row[column]) for row in daily.values()) for column in measures}
    assert (total["net_revenue"], total["refund_amount"], total["net_revenue_after_refunds"]) == (650, 170, 480)

    # The customer, campaign and product marts use the same definitions.
    customer = rows(pg, "SELECT * FROM marts.mart_customer_360 WHERE customer_id = '7'")[0]
    assert (customer["total_order_count"], float(customer["net_sales"]), float(customer["refund_amount"]),
            float(customer["lifetime_value"]), customer["returned_order_count"], customer["cancelled_order_count"]) == \
        (4, 650.0, 170.0, 480.0, 2, 1)
    campaigns = {row["campaign_id"]: row for row in rows(pg, "SELECT * FROM marts.mart_campaign_performance")}
    assert (float(campaigns["5"]["net_sales"]), float(campaigns["5"]["refund_amount"]),
            float(campaigns["5"]["attributed_revenue"])) == (500.0, 120.0, 380.0)
    assert sum(float(row["attributed_revenue"]) for row in campaigns.values()) == 480.0
    product = rows(pg, "SELECT * FROM marts.mart_product_sales WHERE product_id = '4'")[0]
    assert (float(product["net_sales"]), float(product["refund_amount"]), float(product["product_revenue"]),
            product["units_sold"], product["returned_units"], product["cancelled_units"]) == (650.0, 170.0, 480.0, 7, 4, 1)

    # The facts carry the lifecycle; the items of an order carry its refund.
    assert pg.execute("SELECT order_status, (status_timestamp AT TIME ZONE 'UTC')::text, refund_amount::float "
                      "FROM warehouse.fact_orders WHERE order_id = '3'").fetchone() == ("returned", "2026-06-14 12:00:00", 120.0)
    assert pg.execute("SELECT sum(refund_amount)::float FROM warehouse.fact_order_items WHERE order_id = '3'").fetchone()[0] == 120.0

    # Refunds in the marts equal refunds in the facts, exactly; every check passes.
    checks = dict(pg.execute("SELECT check_name, status FROM audit.mart_validation_report").fetchall())
    for name in ("daily_refunds_match_fact_orders", "customer_refunds_match_fact_orders",
                 "campaign_refunds_match_fact_orders", "product_refunds_match_fact_order_items"):
        assert checks[name] == "PASS", name
        source, target, tolerance = pg.execute("SELECT source_value::float, target_value::float, tolerance::float "
                                               "FROM audit.mart_validation_report WHERE check_name = %s", (name,)).fetchone()
        assert (source, target, tolerance) == (170.0, 170.0, 0.0), name
    assert set(checks.values()) == {"PASS"}
    reconciliation = dict(pg.execute("SELECT check_name, status FROM audit.reconciliation_report").fetchall())
    assert set(reconciliation.values()) == {"PASS"} and len(reconciliation) == 29
    assert {"Item refunds sum to their order refund", "Mart refunds equal warehouse fact_orders refunds"} <= set(reconciliation)

    # A refund that the marts lose fails the build's own check.
    pg.execute("UPDATE warehouse.fact_order_items SET refund_amount = 0 WHERE order_item_nk = '32'")
    assert pg.execute("SELECT (SELECT sum(refund_amount) FROM warehouse.fact_orders) "
                      "- (SELECT sum(refund_amount) FROM marts.mart_product_sales)").fetchone()[0] == 40


SNAPSHOTS = {
    "customer": ("mart_customer_daily_snapshot", "mart_customer_360", "customer_id"),
    "product": ("mart_product_daily_snapshot", "mart_product_sales", "product_id"),
    "campaign": ("mart_campaign_daily_snapshot", "mart_campaign_performance", "campaign_id"),
}

# The snapshot measures recomputed from the facts, filtered to one date: written
# here from the definitions, not from the marts' SQL. {day} is the business
# date; an order's status and refund count only once they have happened.
RECOMPUTED = {
    "customer": """
        WITH known AS (
            SELECT o.customer_id, o.net_revenue,
                   CASE WHEN warehouse.business_date(o.status_timestamp) <= DATE '{day}' THEN o.order_status ELSE 'completed' END AS status,
                   CASE WHEN warehouse.business_date(o.status_timestamp) <= DATE '{day}' THEN o.refund_amount ELSE 0 END AS refund
            FROM warehouse.fact_orders o
            WHERE warehouse.business_date(o.order_timestamp) <= DATE '{day}'
        )
        SELECT customer_id AS id,
               count(*) FILTER (WHERE status <> 'cancelled') AS total_order_count,
               coalesce(sum(net_revenue) FILTER (WHERE status <> 'cancelled'), 0) AS net_sales,
               sum(refund) AS refund_amount,
               coalesce(sum(net_revenue) FILTER (WHERE status <> 'cancelled'), 0) - sum(refund) AS lifetime_value,
               count(*) FILTER (WHERE status = 'cancelled') AS cancelled_order_count,
               count(*) FILTER (WHERE status = 'returned') AS returned_order_count
        FROM known GROUP BY customer_id""",
    "product": """
        WITH known AS (
            SELECT i.product_id, i.quantity, i.line_revenue,
                   CASE WHEN warehouse.business_date(o.status_timestamp) <= DATE '{day}' THEN o.order_status ELSE 'completed' END AS status,
                   CASE WHEN warehouse.business_date(o.status_timestamp) <= DATE '{day}' THEN i.refund_amount ELSE 0 END AS refund
            FROM warehouse.fact_order_items i JOIN warehouse.fact_orders o ON o.order_sk = i.order_sk
            WHERE warehouse.business_date(o.order_timestamp) <= DATE '{day}'
        )
        SELECT product_id AS id,
               coalesce(sum(quantity) FILTER (WHERE status <> 'cancelled'), 0) AS units_sold,
               coalesce(sum(line_revenue) FILTER (WHERE status <> 'cancelled'), 0) AS net_sales,
               sum(refund) AS refund_amount,
               coalesce(sum(line_revenue) FILTER (WHERE status <> 'cancelled'), 0) - sum(refund) AS product_revenue,
               coalesce(sum(quantity) FILTER (WHERE status = 'cancelled'), 0) AS cancelled_units,
               coalesce(sum(quantity) FILTER (WHERE status = 'returned'), 0) AS returned_units
        FROM known GROUP BY product_id""",
    "campaign": """
        WITH known AS (
            SELECT o.campaign_id, o.net_revenue,
                   CASE WHEN warehouse.business_date(o.status_timestamp) <= DATE '{day}' THEN o.order_status ELSE 'completed' END AS status,
                   CASE WHEN warehouse.business_date(o.status_timestamp) <= DATE '{day}' THEN o.refund_amount ELSE 0 END AS refund
            FROM warehouse.fact_orders o
            WHERE warehouse.business_date(o.order_timestamp) <= DATE '{day}'
        ), orders AS (
            SELECT campaign_id AS id,
                   count(*) FILTER (WHERE status <> 'cancelled') AS order_count,
                   coalesce(sum(net_revenue) FILTER (WHERE status <> 'cancelled'), 0) AS net_sales,
                   sum(refund) AS refund_amount,
                   coalesce(sum(net_revenue) FILTER (WHERE status <> 'cancelled'), 0) - sum(refund) AS attributed_revenue,
                   count(*) FILTER (WHERE status = 'cancelled') AS cancelled_order_count
            FROM known GROUP BY campaign_id
        ), spend AS (
            SELECT s.campaign_id AS id, sum(s.spend_amount) AS total_spend
            FROM warehouse.fact_campaign_spend s JOIN warehouse.dim_date d ON d.date_sk = s.spend_date_sk
            WHERE d.full_date <= DATE '{day}' GROUP BY s.campaign_id
        )
        SELECT coalesce(o.id, s.id) AS id, coalesce(o.order_count, 0) AS order_count, coalesce(o.net_sales, 0) AS net_sales,
               coalesce(o.refund_amount, 0) AS refund_amount,
               coalesce(o.attributed_revenue, 0) AS attributed_revenue,
               coalesce(o.cancelled_order_count, 0) AS cancelled_order_count, coalesce(s.total_spend, 0) AS total_spend
        FROM orders o FULL JOIN spend s ON s.id = o.id""",
}


ORDER_MEASURES = {
    "customer": ("total_order_count", "net_sales", "refund_amount", "lifetime_value", "cancelled_order_count", "returned_order_count"),
    "product": ("units_sold", "net_sales", "refund_amount", "product_revenue", "cancelled_units", "returned_units"),
    "campaign": ("order_count", "net_sales", "refund_amount", "attributed_revenue", "cancelled_order_count", "total_spend"),
}


def snapshot_differences(pg, days=None) -> list:
    """Snapshot rows whose measures differ from the facts filtered to that date."""
    wrong = []
    for entity, (snapshot, _, key) in SNAPSHOTS.items():
        dates = days or [str(row[0]) for row in pg.execute(f"SELECT DISTINCT snapshot_date FROM marts.{snapshot}").fetchall()]
        for day in dates:
            expected = {row["id"]: row for row in rows(pg, RECOMPUTED[entity].format(day=day))}
            for row in rows(pg, f"SELECT * FROM marts.{snapshot} WHERE snapshot_date = DATE '{day}'"):
                want = expected.pop(row[key], None)
                if want is None:
                    # Activity that is not an order (a web event): no order measures yet.
                    want = {column: 0 for column in ORDER_MEASURES[entity]}
                for column, value in want.items():
                    if column != "id" and float(row[column]) != float(value):
                        wrong.append((snapshot, day, row[key], column, float(row[column]), float(value)))
            wrong += [(snapshot, day, key_value, "missing from the snapshot") for key_value in expected]
    return wrong


def test_a_snapshot_row_holds_only_what_was_known_at_the_end_of_its_date(pg, tmp_path):
    build(tmp_path)

    def customer(day):
        row = rows(pg, f"SELECT * FROM marts.mart_customer_daily_snapshot WHERE customer_id = '7' AND snapshot_date = '{day}'")[0]
        return (row["total_order_count"], float(row["net_sales"]), float(row["refund_amount"]), float(row["lifetime_value"]),
                row["returned_order_count"], row["cancelled_order_count"], row["days_since_last_order"])

    # One row per date from the customer's first activity (10 May) to the last date in the data (16 June).
    assert pg.execute("SELECT min(snapshot_date)::text, max(snapshot_date)::text, count(*) FROM marts.mart_customer_daily_snapshot "
                      "WHERE customer_id = '7'").fetchone() == ("2026-05-10", "2026-06-16", 38)
    assert customer("2026-05-10") == (1, 200.0, 0.0, 200.0, 0, 0, 0)
    assert customer("2026-06-09") == (1, 200.0, 0.0, 200.0, 0, 0, 30)
    # 11 June: order 3 is a completed sale. Its return on the 14th has not happened.
    assert customer("2026-06-11") == (3, 600.0, 0.0, 600.0, 0, 0, 0)
    # 13 June: orders 4 and 5 placed; order 4 is cancelled only on the 14th (01:30 IST).
    assert customer("2026-06-13") == (5, 800.0, 0.0, 800.0, 0, 0, 0)
    assert customer("2026-06-14") == (4, 650.0, 120.0, 530.0, 1, 1, 1)
    assert customer("2026-06-16") == (4, 650.0, 170.0, 480.0, 2, 1, 3)

    product = {str(row["snapshot_date"]): row for row in rows(pg, "SELECT * FROM marts.mart_product_daily_snapshot WHERE product_id = '4'")}
    assert (product["2026-06-13"]["units_sold"], float(product["2026-06-13"]["refund_amount"]), product["2026-06-13"]["returned_units"]) == (8, 0.0, 0)
    assert (product["2026-06-14"]["units_sold"], float(product["2026-06-14"]["refund_amount"]), product["2026-06-14"]["returned_units"]) == (7, 120.0, 3)
    campaign = {str(row["snapshot_date"]): row for row in rows(pg, "SELECT * FROM marts.mart_campaign_daily_snapshot WHERE campaign_id = '5'")}
    assert min(campaign) == "2026-05-09", "the campaign's first activity is its first spend"
    assert (float(campaign["2026-06-11"]["total_spend"]), float(campaign["2026-06-12"]["total_spend"])) == (50.0, 130.0)
    assert float(campaign["2026-06-13"]["attributed_revenue"]) == 650.0
    assert float(campaign["2026-06-16"]["attributed_revenue"]) == 380.0

    # Recomputed from the facts filtered to each date, every snapshot row agrees.
    assert snapshot_differences(pg) == []

    # The last date's snapshot is the lifetime mart, column for column.
    for snapshot, mart, key in SNAPSHOTS.values():
        different = pg.execute(
            f"SELECT count(*) FROM marts.{snapshot} s JOIN marts.{mart} m USING ({key}) "
            f"WHERE s.snapshot_date = (SELECT max(snapshot_date) FROM marts.{snapshot}) "
            f"AND to_jsonb(s) - 'snapshot_date' - 'days_since_last_order' <> to_jsonb(m) - 'days_since_last_order'").fetchone()[0]
        last_rows, active = pg.execute(
            f"SELECT (SELECT count(*) FROM marts.{snapshot} WHERE snapshot_date = (SELECT max(snapshot_date) FROM marts.{snapshot})), "
            f"(SELECT count(DISTINCT {key}) FROM marts.{snapshot})").fetchone()
        assert (different, last_rows) == (0, active) and active > 0, snapshot
        columns = [row[0] for row in pg.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'marts' AND table_name = %s "
            "ORDER BY ordinal_position", (snapshot,)).fetchall()]
        mart_columns = [row[0] for row in pg.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'marts' AND table_name = %s "
            "ORDER BY ordinal_position", (mart,)).fetchall()]
        assert columns == ["snapshot_date", *mart_columns], "the same measures as the lifetime mart"
    checks = dict(pg.execute("SELECT check_name, status FROM audit.mart_validation_report").fetchall())
    for entity in SNAPSHOTS:
        assert checks[f"{entity}_snapshot_last_date_matches_lifetime_mart"] == "PASS"


LATER = """
-- After 13 June: a new order, a return of order 1 (placed in May), a later refund,
-- more spend, a web event, and a new version of the customer.
INSERT INTO staging.stg_gold_fact_orders_scd2 VALUES
    (6, 7, 5, '2026-06-17 10:00', 400.00, 'UPI', '2026-06-25', 'completed', 72.00, 0.00, 472.00, '2026-06-17 10:00', 0);
INSERT INTO staging.stg_gold_fact_order_items_scd2 VALUES (60, 6, 4, 4, 120.00, 100.00, 400.00, '2026-06-25', 0);
UPDATE staging.stg_gold_fact_orders_scd2 SET order_status = 'returned', status_timestamp = '2026-06-18 09:00', refund_amount = 200.00
    WHERE order_id = 1;
UPDATE staging.stg_gold_fact_order_items_scd2 SET refund_amount = 200.00 WHERE order_item_id = 10;
INSERT INTO staging.stg_gold_fact_campaign_spend_scd2 VALUES
    (4, 5, 'facebook', '2026-06-17', 60.00, 1000, 20, '2026-06-25', 1, 100.00, 'India', 'Kerala', 60.00);
INSERT INTO staging.stg_gold_fact_web_events VALUES
    ('e7', 's7', 7, 4, 5, '2026-06-17 09:55', 'purchase', 'facebook', 400.00, '2026-06-25');
UPDATE staging.stg_gold_dim_customers_scd2 SET effective_to = '2026-06-17', is_current = false WHERE is_current;
INSERT INTO staging.stg_gold_dim_customers_scd2 VALUES (7, 'Asha Rao', '2026-06-17', '9999-12-31', true, '2026-06-17 00:00:00');
"""


def test_nothing_after_its_date_reaches_a_snapshot_row(pg, tmp_path):
    build(tmp_path)

    def history(until):
        return {snapshot: pg.execute(f"SELECT to_jsonb(s)::text FROM marts.{snapshot} s WHERE snapshot_date <= DATE '{until}' "
                                     f"ORDER BY 1").fetchall() for snapshot, _, _ in SNAPSHOTS.values()}

    # What consumers saw up to 13 June, and up to 16 June.
    before_13, before_16 = history("2026-06-13"), history("2026-06-16")
    assert all(len(snapshot_rows) > 30 for snapshot_rows in before_13.values())
    lifetime_before = float(pg.execute("SELECT lifetime_value FROM marts.mart_customer_360 WHERE customer_id = '7'").fetchone()[0])

    pg.execute(LATER)
    build(tmp_path)

    # The lifetime mart moved (a new order of 400, a refund of 200 on an order placed in May).
    assert float(pg.execute("SELECT lifetime_value FROM marts.mart_customer_360 WHERE customer_id = '7'").fetchone()[0]) == lifetime_before + 200
    assert pg.execute("SELECT customer_name FROM marts.mart_customer_360 WHERE customer_id = '7'").fetchone()[0] == "Asha Rao"
    # Every snapshot row up to 16 June is what it was: no later order, status
    # change, refund, spend, event or dimension version reached it.
    assert history("2026-06-13") == before_13
    assert history("2026-06-16") == before_16
    assert pg.execute("SELECT DISTINCT customer_name FROM marts.mart_customer_daily_snapshot WHERE snapshot_date < '2026-06-17'").fetchall() == [("Asha",)]
    # The return of order 1 is in the snapshots from 18 June on, and only there.
    assert pg.execute("SELECT min(snapshot_date)::text FROM marts.mart_customer_daily_snapshot "
                      "WHERE returned_order_count = 3").fetchone()[0] == "2026-06-18"
    assert snapshot_differences(pg) == []

    # In SQL: no row holds more orders, refunds or spend than the facts dated up to it.
    leaks = pg.execute("""
        SELECT count(*) FROM marts.mart_customer_daily_snapshot s
        WHERE s.total_order_count + s.cancelled_order_count >
                  (SELECT count(*) FROM warehouse.fact_orders o
                   WHERE o.customer_id = s.customer_id AND warehouse.business_date(o.order_timestamp) <= s.snapshot_date)
           OR s.refund_amount <> (SELECT coalesce(sum(o.refund_amount), 0) FROM warehouse.fact_orders o
                                  WHERE o.customer_id = s.customer_id AND o.order_status = 'returned'
                                    AND warehouse.business_date(o.status_timestamp) <= s.snapshot_date)
           OR s.last_order_timestamp >= ((s.snapshot_date + 1)::timestamp AT TIME ZONE 'Asia/Kolkata')""").fetchone()[0]
    assert leaks == 0


def test_a_customer_with_two_scd_versions_who_engages_with_one_campaign_counts_once(pg, tmp_path):
    # Customer 7 has two versions (the second from 1 June 00:00 UTC, 05:30 IST) and visits campaign 5 under both:
    # e1 on 10 May (version 1), e3 on 11 June (version 2), and twice on the business date 1 June, at 04:30 IST
    # (version 1) and 06:30 IST (version 2). engaged_customer_count counted customer_sk, so it said two customers.
    pg.execute("""
        INSERT INTO staging.stg_gold_fact_web_events VALUES
            ('f1', 'sf1', 7, 4, 5, '2026-05-31 23:00', 'page_view', 'facebook', 0, '2026-06-18'),
            ('f2', 'sf2', 7, 4, 5, '2026-06-01 01:00', 'page_view', 'facebook', 0, '2026-06-18')""")
    build(tmp_path)
    versions = pg.execute("SELECT count(DISTINCT f.customer_sk), count(DISTINCT f.customer_id) FROM warehouse.fact_web_events f "
                          "WHERE f.campaign_id = '5'").fetchone()
    assert versions == (2, 1), "the events sit on two versions of one customer"

    assert pg.execute("SELECT engaged_customer_count FROM marts.mart_campaign_performance WHERE campaign_id = '5'").fetchone()[0] == 1
    assert pg.execute("SELECT engaged_customer_count, total_event_count FROM marts.mart_marketing_funnel "
                      "WHERE campaign_id = '5' AND event_date = '2026-06-01'").fetchone() == (1, 2)
    assert pg.execute("SELECT max(engaged_customer_count) FROM marts.mart_marketing_funnel").fetchone()[0] == 1
    # The daily campaign snapshot runs the lifetime mart's query: one customer on every date.
    assert pg.execute("SELECT DISTINCT engaged_customer_count FROM marts.mart_campaign_daily_snapshot "
                      "WHERE campaign_id = '5' AND snapshot_date >= '2026-05-10'").fetchall() == [(1,)]
    assert pg.execute("SELECT status FROM audit.mart_validation_report "
                      "WHERE check_name = 'campaign_snapshot_last_date_matches_lifetime_mart'").fetchone()[0] == "PASS"
