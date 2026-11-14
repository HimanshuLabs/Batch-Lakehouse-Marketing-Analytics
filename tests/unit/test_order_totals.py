"""Order totals equal the sum of their lines (p2-data-v2).

The generator used to give each order a cart_value copied from an unrelated
purchase event, so no order total matched its items. Gold quality checks and
the point-in-time job now check every complete order: its total must equal the
sum of its lines' line_amount within 0.01. An order that lost an item (Silver
quarantine or a point-in-time reject) is excluded and counted, never failed.
"""
from __future__ import annotations

import json

import pytest

GOLD_JOB = "spark-batch/gold_quality_checks.py"
PIT_JOB = "spark-batch/point_in_time_fact_joins.py"

# The same check lives in both jobs, each with its fact table's amount column.
CHECKS = [(GOLD_JOB, "order_amount"), (PIT_JOB, "total_amount")]


def frames(spark, amount_column, orders, items, bronze):
    """orders: (order_id, total); items: (order_item_id, order_id, line_amount); bronze: (order_item_id, order_id)."""
    return (
        spark.createDataFrame(orders, f"order_id int, {amount_column} double"),
        spark.createDataFrame(items, "order_item_id int, order_id int, line_amount double"),
        spark.createDataFrame([(str(i), str(o)) for i, o in bronze], "order_item_id string, order_id string"),
    )


CONSISTENT_ORDERS = [(1, 30.0), (2, 5.5)]
CONSISTENT_ITEMS = [(10, 1, 10.0), (11, 1, 20.0), (20, 2, 5.5)]
CONSISTENT_BRONZE = [(10, 1), (11, 1), (20, 2)]


@pytest.mark.parametrize("job, amount_column", CHECKS)
def test_consistent_orders_pass(spark, load_job, job, amount_column):
    check = load_job(job).order_total_check
    result = check(*frames(spark, amount_column, CONSISTENT_ORDERS, CONSISTENT_ITEMS, CONSISTENT_BRONZE),
                   amount_column)
    assert result["status"] == "PASS", result
    assert (result["checked_orders"], result["mismatched_orders"], result["excluded_incomplete_orders"],
            result["orders_without_bronze_items"]) == (2, 0, 0, 0)


@pytest.mark.parametrize("job, amount_column", CHECKS)
def test_one_tampered_order_fails(spark, load_job, job, amount_column):
    check = load_job(job).order_total_check
    tampered = [(1, 30.02), (2, 5.5)]  # 0.02 over the sum of its lines
    result = check(*frames(spark, amount_column, tampered, CONSISTENT_ITEMS, CONSISTENT_BRONZE), amount_column)
    assert result["status"] == "FAIL"
    assert (result["checked_orders"], result["mismatched_orders"]) == (2, 1)
    assert result["mismatched_order_examples"] == [1]


@pytest.mark.parametrize("job, amount_column", CHECKS)
def test_an_order_that_lost_an_item_is_excluded_not_failed(spark, load_job, job, amount_column):
    check = load_job(job).order_total_check
    # Order 1's real total is 45.0, but item 12 (15.0) was quarantined: it is
    # in Bronze and missing from the facts, so 45.0 vs 30.0 is not a mismatch.
    orders = [(1, 45.0), (2, 5.5)]
    bronze = CONSISTENT_BRONZE + [(12, 1)]
    result = check(*frames(spark, amount_column, orders, CONSISTENT_ITEMS, bronze), amount_column)
    assert result["status"] == "PASS", result
    assert (result["checked_orders"], result["mismatched_orders"], result["excluded_incomplete_orders"]) == (1, 0, 1)


@pytest.mark.parametrize("job, amount_column", CHECKS)
def test_an_order_with_no_bronze_items_fails(spark, load_job, job, amount_column):
    check = load_job(job).order_total_check
    orders = CONSISTENT_ORDERS + [(3, 0.0)]
    result = check(*frames(spark, amount_column, orders, CONSISTENT_ITEMS, CONSISTENT_BRONZE), amount_column)
    assert result["status"] == "FAIL"
    assert (result["orders_without_bronze_items"], result["mismatched_orders"]) == (1, 0)


def write_gold_inputs(spark, root, orders, items, bronze):
    order_frame, item_frame, bronze_frame = frames(spark, "order_amount", orders, items, bronze)
    order_frame.write.mode("overwrite").parquet(str(root / "data" / "gold" / "fact_orders"))
    item_frame.write.mode("overwrite").parquet(str(root / "data" / "gold" / "fact_order_items"))
    bronze_frame.write.mode("overwrite").parquet(str(root / "data" / "bronze" / "bronze_order_items"))


@pytest.mark.parametrize(
    "orders, status, failure",
    [(CONSISTENT_ORDERS, "PASSED", ""), ([(1, 30.0), (2, 5.75)], "FAILED", "1 order(s) whose total differs")],
)
def test_gold_quality_report_row(spark, load_job, project_root, orders, status, failure):
    gold = load_job(GOLD_JOB, BATCH_DATE="2026-06-18")
    write_gold_inputs(spark, project_root, orders, CONSISTENT_ITEMS, CONSISTENT_BRONZE)

    result, row = gold.validate_order_totals(spark)

    assert row["table_name"] == "order_totals_vs_lines"
    assert row["status"] == status
    assert row["failure_reason"].startswith(failure)
    assert result["checked_orders"] == 2
    # The report row has the same columns as every other Gold report row.
    assert set(row) == {"table_name", "status", "gold_row_count", "source_silver_row_count",
                        "missing_required_columns", "null_violations", "negative_violations",
                        "failure_reason", "batch_date"}
    json.dumps(result)  # written as the JSON report
