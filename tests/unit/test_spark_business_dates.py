"""Spark business dates are Asia/Kolkata days, like Gold (N-12b, p2-data-v2).

Gold derives order_date and event_date in SOURCE_TIMEZONE (Asia/Kolkata), but
the jobs that run in a UTC session derived their dates in UTC, so an event at
00:30 IST landed on the previous day:
  scd2_gold_marts           the SCD2 funnel's event_date, and the CLV mart's
                            customer_lifetime_days (datediff of two timestamps)
  point_in_time_fact_joins  order_year (the fact tables' partition year)
They now take the date of the timestamp in Asia/Kolkata. The session time zone
stays UTC and stored timestamps stay UTC.

Each case uses 2026-06-17 19:00:00 UTC, which is 2026-06-18 00:30 IST.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest
from pyspark.sql import functions as F

MARTS_JOB = "spark-batch/scd2_gold_marts.py"
PIT_JOB = "spark-batch/point_in_time_fact_joins.py"

AT_0030_IST = "2026-06-17 19:00:00"  # UTC


@pytest.fixture
def marts(load_job):
    module = load_job(MARTS_JOB)
    module.create_spark_session()  # the job's own session settings (UTC)
    return module


def test_the_funnel_event_date_is_the_ist_date(spark, marts):
    events = spark.createDataFrame([("e1", AT_0030_IST)], "event_id string, event_timestamp string") \
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    normalized = marts.normalize_web_events(events)
    row = normalized.select("event_date", F.col("event_timestamp").cast("long").alias("epoch")).collect()[0]

    assert row["event_date"] == dt.date(2026, 6, 18)
    # The timestamp itself is unchanged (epoch seconds: independent of any zone).
    assert row["epoch"] == int(dt.datetime(2026, 6, 17, 19, 0, tzinfo=dt.timezone.utc).timestamp())


def test_customer_lifetime_days_count_ist_days(spark, marts):
    # 17:30 IST on 06-17 and 00:30 IST on 06-18: one business day apart (zero in UTC).
    orders = spark.createDataFrame(
        [(1, "c-1", "2026-06-17 12:00:00", 10.0, "k-1", "completed", 0.0, 5),
         (2, "c-1", AT_0030_IST, 20.0, "k-1", "completed", 0.0, 5)],
        "order_id int, customer_sk string, order_date string, cart_value double, campaign_sk string, "
        "order_status string, refund_amount double, campaign_id int",
    ).withColumn("order_date", F.to_timestamp("order_date"))
    customers = spark.createDataFrame(
        [("c-1", 7, "Asha", "asha@example.org", "Pune", "MH", "India", "Student", "Gold")],
        "customer_sk string, customer_id int, customer_name string, email string, city string, state string, "
        "country string, customer_segment string, loyalty_tier string",
    )

    row = marts.build_customer_lifetime_value_mart(orders, customers).collect()[0]

    assert row["customer_lifetime_days"] == 1


def test_the_order_year_partition_is_the_ist_year(spark, load_job, project_root, no_stop):
    from test_point_in_time import write_project

    pit = load_job(PIT_JOB)
    pit.create_spark_session()
    # 2025-12-31 19:00 UTC is 2026-01-01 00:30 IST.
    write_project(spark, project_root, orders=[(1, 5, 4, "2025-12-31 19:00:00")], items=[(10, 1, 4)])
    pit.main()

    for table in ("fact_orders_scd2", "fact_order_items_scd2"):
        partitions = sorted(p.name for p in (project_root / "data" / "gold" / table).glob("order_year=*"))
        assert partitions == ["order_year=2026"], (table, partitions)
    report = json.loads((project_root / "logs" / "point_in_time_fact_join_report.json").read_text())
    assert report["overall_status"] == "PASS"
