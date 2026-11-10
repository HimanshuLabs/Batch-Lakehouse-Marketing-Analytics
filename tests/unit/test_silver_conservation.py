"""No row is lost between Bronze and Silver, and Bronze keeps every batch.

P2-03 (fixed in 1831e6b): the Silver check computed duplicates as
bronze - silver - quarantine and only failed below zero, so a lost row looked
like a duplicate; a run with no invalid rows left last run's quarantine behind.
P2-01 (fixed in 9b514db): Bronze was written with a plain overwrite, so each
run deleted every earlier ingestion_date partition.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

QC_JOB = "spark-batch/silver_quality_checks.py"
SILVER_JOB = "spark-batch/silver_transformations.py"
BRONZE_JOB = "spark-batch/bronze_ingestion.py"
BATCH_DATE = "2026-06-18"


def write_orders(spark, path: Path, rows: list[tuple], partition: str) -> None:
    """rows: (order_id, ingestion_date, record_hash)."""
    frame = spark.createDataFrame(
        [(str(order_id), "352", "2026-06-11T08:48:11", "138901.11", day, record_hash)
         for order_id, day, record_hash in rows],
        "order_id string, user_id string, order_timestamp string, cart_value string, day string, record_hash string",
    ).withColumn("ingestion_date", F.to_date("day")).drop("day")
    if partition == "bronze":
        frame.write.mode("overwrite").partitionBy("ingestion_date").parquet(str(path))
        return
    typed = (
        frame
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("cart_value", F.col("cart_value").cast("double"))
        .withColumn("silver_record_hash", F.sha2(F.col("order_id").cast("string"), 256))
        .withColumn("processed_date", F.to_date(F.lit(BATCH_DATE)))
    )
    typed.write.mode("overwrite").partitionBy("processed_date").parquet(str(path))


@pytest.fixture
def check_orders(spark, load_job, project_root):
    """Write Bronze / Silver / quarantine orders and run the real validate_table on them."""

    def _check(bronze: list[tuple], silver: list[tuple], quarantine: list[tuple]):
        qc = load_job(QC_JOB, BATCH_DATE=BATCH_DATE)
        session = qc.create_spark_session()
        config = dict(qc.TABLES["orders"])
        config["bronze"] = project_root / "data" / "bronze" / "bronze_orders"
        config["silver"] = project_root / "data" / "silver" / "silver_orders"
        config["quarantine"] = project_root / "data" / "quarantine" / "quarantine_orders"
        write_orders(session, config["bronze"], bronze, "bronze")
        write_orders(session, config["silver"], silver, "silver")
        if quarantine:
            write_orders(session, config["quarantine"], quarantine, "quarantine")
        return qc.validate_table(session, "orders", config)

    return _check


def test_a_lost_row_fails_the_check(check_orders):
    bronze = [(1, "2026-06-18", "h1"), (2, "2026-06-18", "h2"), (3, "2026-06-18", "h3")]
    result = check_orders(bronze, silver=[(1, "2026-06-18", "h1")], quarantine=[(3, "2026-06-18", "h3")])
    assert result["status"] == "FAILED", "order 2 is in neither Silver nor quarantine"
    assert result["unaccounted_row_count"] == 1
    assert result["passed_row_count"] == 2


def test_a_duplicate_across_batches_is_dedupe_not_loss(check_orders):
    bronze = [(1, "2026-06-04", "h1"), (1, "2026-06-18", "h1"), (2, "2026-06-18", "h2")]
    result = check_orders(bronze, silver=[(1, "2026-06-18", "h1"), (2, "2026-06-18", "h2")], quarantine=[])
    assert result["status"] == "PASSED", result["failure_reason"]
    assert result["duplicate_dropped_count"] == 1
    assert result["passed_row_count"] == 3
    assert result["unaccounted_row_count"] == 0


def test_quarantine_that_is_not_from_bronze_fails_the_check(check_orders):
    bronze = [(1, "2026-06-18", "h1"), (2, "2026-06-18", "h2"), (3, "2026-06-18", "h3")]
    # Order 3 vanished, and a stale quarantine row from another run hides it in the totals.
    result = check_orders(bronze, silver=[(1, "2026-06-18", "h1"), (2, "2026-06-18", "h2")],
                          quarantine=[(9, "2026-06-01", "stale")])
    assert result["status"] == "FAILED"
    assert result["untraceable_quarantine_row_count"] == 1
    assert result["unaccounted_row_count"] == 1


def test_a_run_without_invalid_rows_rewrites_quarantine_empty(spark, load_job, project_root):
    silver = load_job(SILVER_JOB, BATCH_DATE=BATCH_DATE)
    session = silver.create_spark_session()
    quarantine = silver.QUARANTINE_BASE_DIR / "quarantine_orders"

    stale = session.createDataFrame([(7, "stale")], "order_id int, quarantine_reason string")
    stale.withColumn("processed_date", F.to_date(F.lit("2026-06-04"))) \
        .write.mode("overwrite").partitionBy("processed_date").parquet(str(quarantine))

    valid = silver.add_silver_metadata(session.createDataFrame([(1,)], "order_id int"), "silver_orders")
    invalid = silver.add_silver_metadata(
        session.createDataFrame([], "order_id int").withColumn("quarantine_reason", F.lit("x")),
        "quarantine_orders",
    )
    silver.run_table("orders", lambda _spark: (valid, invalid), session, [])

    partitions = sorted(p.name for p in quarantine.iterdir() if p.is_dir())
    assert partitions == [f"processed_date={BATCH_DATE}"], f"last run's quarantine survived: {partitions}"
    assert session.read.parquet(str(quarantine)).count() == 0


def test_bronze_keeps_every_batch(spark, load_job, project_root):
    bronze = load_job(BRONZE_JOB, BATCH_DATE=BATCH_DATE)
    session = bronze.create_spark_session()
    target = project_root / "data" / "bronze" / "bronze_orders"
    for day in ("2026-06-04", "2026-06-18"):
        frame = session.createDataFrame([(f"order-{day}",)], "order_id string") \
            .withColumn("ingestion_date", F.to_date(F.lit(day)))
        bronze.write_bronze_table(frame, target)
    days = sorted(str(r[0]) for r in session.read.parquet(str(target)).select("ingestion_date").distinct().collect())
    assert days == ["2026-06-04", "2026-06-18"], f"writing a batch deleted another batch: {days}"
