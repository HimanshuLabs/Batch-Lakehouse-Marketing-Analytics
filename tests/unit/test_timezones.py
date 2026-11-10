"""Session time zones are pinned, so results do not depend on the machine.

N-12 (fixed in 1bcab6c): Silver parsed the naive source times in the machine's
zone and Gold derived dates in it; on a UTC machine every stored time moved by
5:30. Silver and Gold now use SOURCE_TIMEZONE (default Asia/Kolkata), every
other job UTC. Run this file under TZ=UTC and TZ=Asia/Kolkata: it must pass both.
"""
from __future__ import annotations

import ast
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from conftest import CODE_ROOT

SOURCE_ZONE = "os.environ.get('SOURCE_TIMEZONE', 'Asia/Kolkata')"  # as ast.unparse prints it
EXPECTED_SESSION_ZONE = {
    "silver_transformations": SOURCE_ZONE,
    "gold_transformations": SOURCE_ZONE,
    "bronze_ingestion": "'UTC'",
    "bronze_quality_checks": "'UTC'",
    "silver_quality_checks": "'UTC'",
    "gold_quality_checks": "'UTC'",
    "scd2_dimensions": "'UTC'",
    "point_in_time_fact_joins": "'UTC'",
    "scd2_gold_marts": "'UTC'",
}

SOURCE_TEXT = "2026-02-14T14:34:07"  # naive local time as written by the source

# Set on the shared session before a job's create_spark_session() runs, so a
# job that does not set its own zone cannot pass by inheriting the test's UTC.
UNRELATED_ZONE = "America/New_York"


def session_zone_expression(job: str) -> str | None:
    tree = ast.parse((CODE_ROOT / "spark-batch" / f"{job}.py").read_text(encoding="utf-8"))
    for function in ast.walk(tree):
        if isinstance(function, ast.FunctionDef) and function.name == "create_spark_session":
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "config"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value == "spark.sql.session.timeZone"
                ):
                    return ast.unparse(node.args[1])
    return None


def epoch(text_utc: str) -> int:
    return int(datetime.fromisoformat(text_utc).replace(tzinfo=timezone.utc).timestamp())


@pytest.mark.parametrize("job", sorted(EXPECTED_SESSION_ZONE))
def test_every_job_pins_its_session_time_zone(job):
    assert session_zone_expression(job) == EXPECTED_SESSION_ZONE[job], \
        f"{job}.create_spark_session must set spark.sql.session.timeZone explicitly"


def test_silver_reads_source_times_in_the_source_zone(spark, load_job, monkeypatch):
    monkeypatch.delenv("SOURCE_TIMEZONE", raising=False)
    silver = load_job("spark-batch/silver_transformations.py", BATCH_DATE="2026-06-18")
    spark.conf.set("spark.sql.session.timeZone", UNRELATED_ZONE)
    session = silver.create_spark_session()
    frame = session.createDataFrame([(SOURCE_TEXT,)], "t string")
    stored = frame.select(silver.safe_timestamp("t").cast("long").alias("s")).first()["s"]
    assert stored == epoch("2026-02-14T09:04:07"), "14:34:07 in Asia/Kolkata is 09:04:07 UTC"


def test_source_timezone_can_be_overridden(spark, load_job):
    silver = load_job("spark-batch/silver_transformations.py", BATCH_DATE="2026-06-18", SOURCE_TIMEZONE="UTC")
    spark.conf.set("spark.sql.session.timeZone", UNRELATED_ZONE)
    session = silver.create_spark_session()
    frame = session.createDataFrame([(SOURCE_TEXT,)], "t string")
    stored = frame.select(silver.safe_timestamp("t").cast("long").alias("s")).first()["s"]
    assert stored == epoch(SOURCE_TEXT)


def test_gold_dates_are_source_zone_dates(spark, load_job, monkeypatch):
    monkeypatch.delenv("SOURCE_TIMEZONE", raising=False)
    gold = load_job("spark-batch/gold_transformations.py", BATCH_DATE="2026-06-18")
    spark.conf.set("spark.sql.session.timeZone", UNRELATED_ZONE)
    session = gold.create_spark_session()
    instant = session.createDataFrame([(epoch("2026-02-14T20:00:00"),)], "s long") \
        .select(F.col("s").cast("timestamp").alias("t"))
    day = instant.select(F.date_format(F.to_date("t"), "yyyy-MM-dd").alias("d")).first()["d"]
    assert day == "2026-02-15", "20:00 UTC is already the next day in Asia/Kolkata"
    # Gold derives the date in the source zone only; the instant itself is unchanged.
    assert instant.select(F.col("t").cast("long")).first()[0] == epoch("2026-02-14T20:00:00")
