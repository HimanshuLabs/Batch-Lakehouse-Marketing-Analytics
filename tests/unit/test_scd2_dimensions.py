"""SCD2 dimensions.

P2-07 (fixed in ee30b39): tracked columns that did not exist in Silver were
filled with NULL, so tier / segment / price / channel changes never opened a
version; a missing tracked column now raises.
P2-12 (fixed in 39b545a): dimensions were built from current Silver rows only,
so earlier versions were lost; they are built from the Silver history tables.
N-11 (fixed in a541e13): the first version opens at 1900-01-01 so earlier facts
match it, while keys stay stable and source_updated_at keeps the real change
time (N-10).
"""
from __future__ import annotations

import inspect

import pytest
from pyspark.sql import functions as F

SCD2_JOB = "spark-batch/scd2_dimensions.py"

V1_TIME = "2026-02-10 10:06:05"
V2_TIME = "2026-06-18 08:00:00"
V3_TIME = "2026-07-02 08:00:00"


def customer_history(spark, versions: list[tuple]):
    """versions: (user_id, updated_at, membership_tier, user_segment)."""
    rows = [
        (user_id, "Asha Rao", "asha@example.org", "Pune", "Maharashtra", "India", segment, tier, updated_at)
        for user_id, updated_at, tier, segment in versions
    ]
    return spark.createDataFrame(
        rows,
        "user_id int, user_name string, email string, home_city string, home_state string, country string, "
        "user_segment string, membership_tier string, updated_at string",
    ).withColumn("updated_at", F.to_timestamp("updated_at"))


def build(scd2, frame, dimension="customers"):
    """Call the real build_scd2_dimension with the job's own configuration."""
    config = scd2.DIMENSION_CONFIGS[dimension]
    parameters = inspect.signature(scd2.build_scd2_dimension).parameters
    kwargs = {name: config[name] for name in parameters if name in config}
    if "dimension_name" in parameters:
        kwargs["dimension_name"] = dimension
    return scd2.build_scd2_dimension(frame, **kwargs)


def versions_of(frame, key_value, key="customer_id"):
    """The key's versions in order, timestamps as UTC text; run-time columns dropped."""
    fmt = "yyyy-MM-dd HH:mm:ss"
    if "source_updated_at" not in frame.columns:
        frame = frame.withColumn("source_updated_at", F.lit(None).cast("timestamp"))
    rows = (
        frame.filter(F.col(key) == key_value)
        .drop("created_at", "updated_at")
        .withColumn("effective_from", F.date_format("effective_from", fmt))
        .withColumn("effective_to", F.date_format("effective_to", fmt))
        .withColumn("source_updated_at", F.date_format("source_updated_at", fmt))
        .orderBy("effective_from")
        .collect()
    )
    return [row.asDict() for row in rows]


@pytest.fixture
def scd2(spark, load_job):
    module = load_job(SCD2_JOB)
    module.create_spark_session()
    return module


def test_tracked_attributes_come_from_the_silver_columns(spark, scd2):
    history = customer_history(spark, [(7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Enterprise")])
    versions = versions_of(build(scd2, history), 7)
    assert len(versions) == 2, "a tier/segment change must open a new version"
    assert [v["loyalty_tier"] for v in versions] == ["Gold", "Platinum"]
    assert [v["customer_segment"] for v in versions] == ["Student", "Enterprise"]


def test_a_missing_tracked_column_fails_loudly(spark, scd2):
    history = customer_history(spark, [(7, V1_TIME, "Gold", "Student")]).drop("user_segment")
    with pytest.raises(ValueError, match="customer_segment"):
        build(scd2, history).collect()


def test_first_version_opens_at_1900_and_versions_chain(spark, scd2):
    history = customer_history(spark, [(7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Enterprise")])
    first, second = versions_of(build(scd2, history), 7)
    assert first["effective_from"] == "1900-01-01 00:00:00"
    assert first["effective_to"] == V2_TIME and first["is_current"] is False
    assert second["effective_from"] == V2_TIME
    assert second["effective_to"] == "9999-12-31 00:00:00" and second["is_current"] is True


def test_keys_are_stable_and_source_time_is_the_change_time(spark, scd2):
    two = [(7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Enterprise")]
    first_build = versions_of(build(scd2, customer_history(spark, two)), 7)
    rebuild = versions_of(build(scd2, customer_history(spark, two)), 7)
    with_third = versions_of(build(scd2, customer_history(spark, two + [(7, V3_TIME, "Silver", "Student")])), 7)

    assert [v["customer_sk"] for v in first_build] == [v["customer_sk"] for v in rebuild], "keys changed on a rerun"
    assert [v["customer_sk"] for v in with_third[:2]] == [v["customer_sk"] for v in first_build], \
        "a later version changed the keys of earlier versions"
    assert [v["source_updated_at"] for v in first_build] == [V1_TIME, V2_TIME], \
        "source_updated_at must be the record's change time, not the run time or 1900"


def test_dimension_is_built_from_silver_history(spark, scd2, project_root, no_stop):
    silver = project_root / "data" / "silver"

    def write(name, frame):
        frame.withColumn("processed_date", F.to_date(F.lit("2026-06-18"))) \
            .write.mode("overwrite").partitionBy("processed_date").parquet(str(silver / name))

    both = customer_history(spark, [(7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Enterprise")])
    write("silver_customers_history", both)
    write("silver_customers", both.filter(F.col("updated_at") == F.to_timestamp(F.lit(V2_TIME))))

    product = spark.createDataFrame(
        [(4, "Bluetooth Speaker", "Fitness", 13673.69, V1_TIME)],
        "product_id int, product_name string, category string, discounted_price double, updated_at string",
    ).withColumn("updated_at", F.to_timestamp("updated_at"))
    write("silver_products_history", product)
    write("silver_products", product)

    campaign = spark.createDataFrame(
        [(5, "Facebook Campaign 5", "facebook", "Enterprise", 232331.86, "completed", "2026-05-07", "2026-07-24", V1_TIME)],
        "campaign_id int, campaign_name string, traffic_source string, target_segment string, budget double, "
        "campaign_status string, start_date string, end_date string, updated_at string",
    ).withColumn("start_date", F.to_date("start_date")).withColumn("end_date", F.to_date("end_date")) \
     .withColumn("updated_at", F.to_timestamp("updated_at"))
    write("silver_campaigns_history", campaign)
    write("silver_campaigns", campaign)

    scd2.main()

    output = spark.read.parquet(str(project_root / "data" / "gold" / "dim_customers_scd2"))
    versions = versions_of(output, 7)
    assert [v["loyalty_tier"] for v in versions] == ["Gold", "Platinum"], \
        "the dimension must keep the version that is only in Silver history"
