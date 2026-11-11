"""SCD2 dimensions.

P2-07 (fixed in ee30b39): tracked columns that did not exist in Silver were
filled with NULL, so tier / segment / price / channel changes never opened a
version; a missing tracked column now raises.
P2-12 (fixed in 39b545a): dimensions were built from current Silver rows only,
so earlier versions were lost; they are built from the Silver history tables.
N-11 (fixed in a541e13): the first version opens at 1900-01-01 so earlier facts
match it, while keys stay stable and source_updated_at keeps the real change
time (N-10).
E1 (p2-data-v2): the dimensions dropped every source attribute that was not a
tracked column (customer gender, age, preferred_language, loyalty_points,
is_prime_user; product original_price, inventory_remaining; campaign
ab_test_group), so the warehouse showed them as NULL, 0 or false. They are now
carried on each version from the source row that opened it, never hashed: keys
and version counts do not change.
"""
from __future__ import annotations

import inspect

import pytest
from pyspark.sql import functions as F

SCD2_JOB = "spark-batch/scd2_dimensions.py"

V1_TIME = "2026-02-10 10:06:05"
V2_TIME = "2026-06-18 08:00:00"
V3_TIME = "2026-07-02 08:00:00"


CARRIED_DEFAULTS = ("Female", 31, "Tamil", 1200, True)


def customer_history(spark, versions: list[tuple]):
    """versions: (user_id, updated_at, membership_tier, user_segment[, gender, age,
    preferred_language, loyalty_points, is_prime_user])."""
    rows = [
        (version[0], "Asha Rao", "asha@example.org", "Pune", "Maharashtra", "India", version[3], version[2],
         version[1], *(version[4:] or CARRIED_DEFAULTS))
        for version in versions
    ]
    return spark.createDataFrame(
        rows,
        "user_id int, user_name string, email string, home_city string, home_state string, country string, "
        "user_segment string, membership_tier string, updated_at string, gender string, age int, "
        "preferred_language string, loyalty_points int, is_prime_user boolean",
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
        [(4, "Bluetooth Speaker", "Fitness", 15192.99, 13673.69, 1938, V1_TIME)],
        "product_id int, product_name string, category string, original_price double, discounted_price double, "
        "inventory_remaining int, updated_at string",
    ).withColumn("updated_at", F.to_timestamp("updated_at"))
    write("silver_products_history", product)
    write("silver_products", product)

    campaign = spark.createDataFrame(
        [(5, "Facebook Campaign 5", "facebook", "B", "Enterprise", 232331.86, "completed", "2026-05-07", "2026-07-24",
          V1_TIME)],
        "campaign_id int, campaign_name string, traffic_source string, ab_test_group string, target_segment string, "
        "budget double, campaign_status string, start_date string, end_date string, updated_at string",
    ).withColumn("start_date", F.to_date("start_date")).withColumn("end_date", F.to_date("end_date")) \
     .withColumn("updated_at", F.to_timestamp("updated_at"))
    write("silver_campaigns_history", campaign)
    write("silver_campaigns", campaign)

    scd2.main()

    output = spark.read.parquet(str(project_root / "data" / "gold" / "dim_customers_scd2"))
    versions = versions_of(output, 7)
    assert [v["loyalty_tier"] for v in versions] == ["Gold", "Platinum"], \
        "the dimension must keep the version that is only in Silver history"

    products = spark.read.parquet(str(project_root / "data" / "gold" / "dim_products_scd2")).collect()
    assert [(r["original_price"], r["inventory_remaining"]) for r in products] == [(15192.99, 1938)]
    campaigns = spark.read.parquet(str(project_root / "data" / "gold" / "dim_campaigns_scd2")).collect()
    assert [(r["campaign_status"], r["ab_test_group"]) for r in campaigns] == [("completed", "B")]


def test_each_dimension_carries_its_untracked_source_attributes(scd2):
    carried = {name: config.get("carried_columns") for name, config in scd2.DIMENSION_CONFIGS.items()}
    assert carried == {
        "customers": ["gender", "age", "preferred_language", "loyalty_points", "is_prime_user"],
        "products": ["original_price", "inventory_remaining"],
        "campaigns": ["ab_test_group"],
    }
    # campaign_status is a tracked (hashed) campaign attribute already.
    assert "campaign_status" in scd2.DIMENSION_CONFIGS["campaigns"]["tracked_columns"]


def test_carried_attributes_come_from_the_row_that_opened_each_version(spark, scd2):
    history = customer_history(spark, [
        (7, V1_TIME, "Gold", "Student", "Female", 31, "Tamil", 1200, False),
        # Only carried attributes change: no new version.
        (7, V2_TIME, "Gold", "Student", "Female", 32, "Hindi", 1500, True),
        # A tracked change opens version 2, carrying this row's values.
        (7, V3_TIME, "Platinum", "Student", "Female", 32, "Hindi", 1800, True),
    ])
    versions = versions_of(build(scd2, history), 7)

    assert len(versions) == 2, "a change in a carried attribute alone must not open a version"
    carried = [(v["gender"], v["age"], v["preferred_language"], v["loyalty_points"], v["is_prime_user"])
               for v in versions]
    assert carried == [("Female", 31, "Tamil", 1200, False), ("Female", 32, "Hindi", 1800, True)]


def test_carried_attributes_change_no_key(spark, scd2):
    tracked = [(7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Enterprise")]
    defaults = versions_of(build(scd2, customer_history(spark, tracked)), 7)
    other_values = versions_of(build(scd2, customer_history(spark, [
        version + ("Male", 58, "Bengali", 7, False) for version in tracked])), 7)

    assert [v["customer_sk"] for v in other_values] == [v["customer_sk"] for v in defaults]
    assert [v["record_hash"] for v in other_values] == [v["record_hash"] for v in defaults]
    assert [v["loyalty_points"] for v in other_values] == [7, 7]


def test_a_missing_carried_column_fails_loudly(spark, scd2):
    history = customer_history(spark, [(7, V1_TIME, "Gold", "Student")]).drop("loyalty_points")
    with pytest.raises(ValueError, match="loyalty_points"):
        build(scd2, history).collect()
