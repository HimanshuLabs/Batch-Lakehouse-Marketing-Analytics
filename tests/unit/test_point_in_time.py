"""Point-in-time fact joins.

P2-02 (fixed in fa6f11a): a fact with no covering dimension version silently
took the current version's key. Every fact key now resolves explicitly to
in_range / no_key / key_not_in_dimension / no_version_for_time; only in_range
facts are valid, the rest are rejected with a reason, and no_version_for_time
(a corrupt dimension) fails the job.
P2-05 (fixed in fa6f11a): order items whose order was rejected were filtered
out silently; they are rejected with a reason, and the job checks that every
Silver row is a valid fact or a reject.
"""
from __future__ import annotations

import json

import pytest
from pyspark.sql import functions as F

PIT_JOB = "spark-batch/point_in_time_fact_joins.py"

V2_START = "2026-05-18 02:43:04"


def dimension(spark, key: str, sk: str, versions: list[tuple]):
    """versions: (key value, surrogate key, effective_from, effective_to, is_current)."""
    return spark.createDataFrame(
        versions, f"{key} int, {sk} string, effective_from string, effective_to string, is_current boolean"
    ).withColumn("effective_from", F.to_timestamp("effective_from")) \
     .withColumn("effective_to", F.to_timestamp("effective_to"))


def customers(spark):
    return dimension(spark, "customer_id", "customer_sk", [
        (5, "customer-5-v1", "1900-01-01 00:00:00", V2_START, False),
        (5, "customer-5-v2", V2_START, "9999-12-31 00:00:00", True),
    ])


def campaigns(spark):
    return dimension(spark, "campaign_id", "campaign_sk", [
        (4, "campaign-4-v1", "1900-01-01 00:00:00", "9999-12-31 00:00:00", True),
    ])


def products(spark):
    return dimension(spark, "product_id", "product_sk", [
        (4, "product-4-v1", "1900-01-01 00:00:00", "9999-12-31 00:00:00", True),
    ])


def orders_frame(spark, rows: list[tuple]):
    """rows: (order_id, customer_id, order time)."""
    return spark.createDataFrame(rows, "order_id int, customer_id int, order_date string") \
        .withColumn("order_date", F.to_timestamp("order_date"))


@pytest.fixture
def pit(spark, load_job):
    module = load_job(PIT_JOB)
    module.create_spark_session()
    return module


def join_customers(pit, spark, rows):
    return pit.point_in_time_join(
        fact_df=orders_frame(spark, rows), dim_df=customers(spark), fact_key="customer_id",
        dim_key="customer_id", fact_timestamp="order_date", surrogate_key="customer_sk",
    )


def test_a_fact_takes_only_the_version_that_covers_its_time(spark, pit):
    joined = join_customers(pit, spark, [
        (1, 5, "2026-01-01 00:00:00"),
        (2, 5, "2026-06-01 00:00:00"),
        (3, 5, "1899-12-31 12:00:00"),
    ])
    keys = {row["order_id"]: row["customer_sk"] for row in joined.collect()}
    assert keys[1] == "customer-5-v1"
    assert keys[2] == "customer-5-v2"
    assert keys[3] is None, "no version covers 1899; the current version must not be used instead"


def test_every_fact_key_resolves_exactly_once(spark, pit):
    joined = join_customers(pit, spark, [
        (1, 5, "2026-06-01 00:00:00"),
        (2, None, "2026-06-01 00:00:00"),
        (3, 999999, "2026-06-01 00:00:00"),
        (4, 5, "1899-12-31 12:00:00"),
    ])
    rows = {row["order_id"]: row.asDict() for row in joined.collect()}
    assert joined.count() == 4, "the join must not drop or duplicate facts"
    assert {k: v["customer_sk_resolution"] for k, v in rows.items()} == {
        1: "in_range", 2: "no_key", 3: "key_not_in_dimension", 4: "no_version_for_time",
    }


def test_rejects_name_every_failing_key(spark, pit):
    facts = join_customers(pit, spark, [
        (1, 5, "2026-06-01 00:00:00"),
        (2, 999999, "2026-06-01 00:00:00"),
        (3, 999999, "2026-06-01 00:00:00"),
    ]).withColumn("campaign_id", F.when(F.col("order_id") == 3, F.lit(None)).otherwise(F.lit(4)))
    facts = pit.point_in_time_join(
        fact_df=facts, dim_df=campaigns(spark), fact_key="campaign_id", dim_key="campaign_id",
        fact_timestamp="order_date", surrogate_key="campaign_sk",
    )
    valid, rejected = pit.split_valid_and_rejected_facts(facts, resolved_keys=[
        ("customer_sk", "customer_id", "order_date", "dim_customers_scd2"),
        ("campaign_sk", "campaign_id", "order_date", "dim_campaigns_scd2"),
    ])
    assert [row["order_id"] for row in valid.collect()] == [1]
    assert "customer_sk_resolution" not in valid.columns, "valid facts keep their original schema"
    reasons = {row["order_id"]: row["reject_reason"] for row in rejected.collect()}
    assert reasons == {
        2: "customer_id not in dim_customers_scd2",
        3: "customer_id not in dim_customers_scd2; campaign_id is NULL",
    }


# --- the job end to end, in a temporary project ----------------------------

def write_project(spark, root, orders: list[tuple], items: list[tuple]):
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"

    def silver_table(name, frame):
        frame.withColumn("processed_date", F.to_date(F.lit("2026-06-18"))) \
            .write.mode("overwrite").partitionBy("processed_date").parquet(str(silver / name))

    silver_table("silver_orders", spark.createDataFrame(
        [(o, c, k, t, "UPI", 1000.0, 0.1, "India", "Pune") for o, c, k, t in orders],
        "order_id int, user_id int, campaign_id int, order_timestamp string, payment_method string, "
        "cart_value double, fraud_score double, country string, city string",
    ).withColumn("order_timestamp", F.to_timestamp("order_timestamp")))
    silver_table("silver_order_items", spark.createDataFrame(
        [(i, o, p, "Speaker", "Fitness", 1, 100.0, 0.0, 100.0, 100.0) for i, o, p in items],
        "order_item_id int, order_id int, product_id int, product_name string, category string, quantity int, "
        "original_price double, discount_percent double, discounted_price double, line_amount double",
    ))
    silver_table("silver_ad_spend", spark.createDataFrame(
        [(1, 4, "facebook", "2026-06-17", 1000, 10, 50.0)],
        "spend_id int, campaign_id int, traffic_source string, spend_date string, impressions bigint, "
        "clicks bigint, spend_amount double",
    ).withColumn("spend_date", F.to_date("spend_date")))

    for name, frame in (("dim_customers_scd2", customers(spark)), ("dim_campaigns_scd2", campaigns(spark)),
                        ("dim_products_scd2", products(spark))):
        frame.write.mode("overwrite").parquet(str(gold / name))


def read_rejects(spark, root, name):
    return {row[0]: row.asDict() for row in spark.read.parquet(str(root / "data" / "quarantine" / name)).collect()}


def test_the_job_rejects_items_of_rejected_orders_and_conserves_rows(spark, pit, project_root, no_stop):
    write_project(spark, project_root,
                  orders=[(1, 5, 4, "2026-06-01 10:00:00"), (2, 999999, 4, "2026-06-01 10:00:00")],
                  items=[(10, 1, 4), (20, 2, 4)])
    pit.main()

    item_rejects = spark.read.parquet(str(project_root / "data" / "quarantine" / "fact_order_items_scd2_rejects"))
    reasons = {row["order_item_id"]: row["reject_reason"] for row in item_rejects.collect()}
    assert reasons == {20: "order not in fact_orders_scd2"}, "the item of a rejected order must be a reject"

    report = json.loads((project_root / "logs" / "point_in_time_fact_join_report.json").read_text())
    assert report["overall_status"] == "PASS"
    for fact in ("fact_orders_scd2", "fact_order_items_scd2", "fact_campaign_spend_scd2"):
        conservation = report["facts"][fact]["conservation"]
        assert conservation["status"] == "PASS" and conservation["unaccounted_rows"] == 0, (fact, conservation)
    assert report["facts"]["fact_orders_scd2"]["resolutions"]["customer_sk"]["key_not_in_dimension"] == 1


def test_the_job_fails_on_a_corrupt_dimension(spark, pit, project_root, no_stop):
    write_project(spark, project_root,
                  orders=[(1, 5, 4, "2026-06-01 10:00:00"), (2, 5, 4, "1899-12-31 12:00:00")],
                  items=[(10, 1, 4)])
    with pytest.raises(SystemExit):
        pit.main()

    rejects = spark.read.parquet(str(project_root / "data" / "quarantine" / "fact_orders_scd2_rejects"))
    rows = {row["order_id"]: row.asDict() for row in rejects.collect()}
    assert rows[2]["customer_sk_resolution"] == "no_version_for_time"
    assert "no dim_customers_scd2 version covers order_date" in rows[2]["reject_reason"]
