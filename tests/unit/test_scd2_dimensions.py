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
Final part (p2-data-v2): every attribute is SCD Type 0, 1 or 2
(scd2.SCD_ATTRIBUTES). Type 0 keeps the first value and counts ignored source
changes; Type 1 puts the latest value on every version; only Type 2 opens a
version. Name and email were Type 2 and are Type 1; the attributes that were
"carried" are Type 1, and signup_date (new) is Type 0.
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


def customer_history(spark, versions: list[tuple], **columns):
    """versions: (user_id, updated_at, membership_tier, user_segment[, gender, age,
    preferred_language, loyalty_points, is_prime_user]).

    columns: per-version values of email, signup_date or home_city, for example
    email=["a@example.org", "b@example.org"]."""
    rows = [
        (version[0], "Asha Rao", columns.get("email", ["asha@example.org"] * len(versions))[index],
         columns.get("home_city", ["Pune"] * len(versions))[index], "Maharashtra", "India", version[3], version[2],
         version[1], *(version[4:] or CARRIED_DEFAULTS),
         columns.get("signup_date", ["2024-03-01"] * len(versions))[index])
        for index, version in enumerate(versions)
    ]
    return spark.createDataFrame(
        rows,
        "user_id int, user_name string, email string, home_city string, home_state string, country string, "
        "user_segment string, membership_tier string, updated_at string, gender string, age int, "
        "preferred_language string, loyalty_points int, is_prime_user boolean, signup_date string",
    ).withColumn("updated_at", F.to_timestamp("updated_at")).withColumn("signup_date", F.to_date("signup_date"))


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

    # The second row changes only the status: an SCD2-tracked attribute, so it
    # opens a new version (the price, name and category are the same).
    product = spark.createDataFrame(
        [(4, "Bluetooth Speaker", "Fitness", "Stridex", "active", 15192.99, 13673.69, 1938, V1_TIME),
         (4, "Bluetooth Speaker", "Fitness", "Stridex", "discontinued", 15192.99, 13673.69, 0, V2_TIME)],
        "product_id int, product_name string, category string, brand string, product_status string, "
        "original_price double, discounted_price double, inventory_remaining int, updated_at string",
    ).withColumn("updated_at", F.to_timestamp("updated_at"))
    write("silver_products_history", product)
    write("silver_products", product)

    campaign = spark.createDataFrame(
        [(5, "Facebook Campaign 5", "facebook", "retention", "B", "Enterprise", 232331.86, "completed", "2026-05-07",
          "2026-07-24", V1_TIME)],
        "campaign_id int, campaign_name string, traffic_source string, campaign_type string, ab_test_group string, "
        "target_segment string, "
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

    products = spark.read.parquet(str(project_root / "data" / "gold" / "dim_products_scd2")) \
        .orderBy("effective_from").collect()
    assert [(r["brand"], r["status"], r["inventory_remaining"], r["is_current"]) for r in products] == [
        ("Stridex", "active", 0, False), ("Stridex", "discontinued", 0, True)], \
        "a status change must open a new product version; the stock level is Type 1: the latest on both"
    assert products[0]["product_sk"] != products[1]["product_sk"]
    campaigns = spark.read.parquet(str(project_root / "data" / "gold" / "dim_campaigns_scd2")).collect()
    assert [(r["campaign_status"], r["campaign_type"], r["ab_test_group"]) for r in campaigns] == [
        ("completed", "retention", "B")]


def test_every_attribute_has_one_scd_type_and_a_reason(scd2):
    types = {dimension: {column: kind for column, (kind, _) in attributes.items()}
             for dimension, attributes in scd2.SCD_ATTRIBUTES.items()}
    assert types == {
        "customers": {"customer_name": 1, "email": 1, "city": 2, "state": 2, "country": 2, "customer_segment": 2,
                      "loyalty_tier": 2, "gender": 1, "age": 1, "preferred_language": 1, "loyalty_points": 1,
                      "is_prime_user": 1, "signup_date": 0},
        "products": {"product_name": 1, "category": 0, "price": 2, "status": 2, "brand": 1, "original_price": 1,
                     "inventory_remaining": 1},
        "campaigns": {"campaign_name": 1, "channel": 0, "target_segment": 2, "budget": 2, "campaign_status": 2,
                      "start_date": 2, "end_date": 2, "campaign_type": 0, "ab_test_group": 1},
    }
    for dimension, attributes in scd2.SCD_ATTRIBUTES.items():
        assert all(len(reason) > 20 for _, reason in attributes.values()), "every type is justified"
        config = scd2.DIMENSION_CONFIGS[dimension]
        # The job's configuration is derived from the table: nothing is typed twice.
        assert config["tracked_columns"] == [c for c, kind in types[dimension].items() if kind == 2]
        assert config["type1_columns"] == [c for c, kind in types[dimension].items() if kind == 1]
        assert config["type0_columns"] == [c for c, kind in types[dimension].items() if kind == 0]
        assert config["output_columns"] == list(attributes) and config["untracked_optional"] == []
    assert scd2.DIMENSION_CONFIGS["products"]["column_map"]["status"] == "product_status"


def test_a_type_0_change_is_ignored_and_counted(spark, scd2):
    history = customer_history(spark, [
        (7, V1_TIME, "Gold", "Student"),
        # The source now states another signup date: ignored, no version.
        (7, V2_TIME, "Gold", "Student"),
        # A tier change (Type 2) opens version 2; the changed signup date is still ignored.
        (7, V3_TIME, "Platinum", "Student"),
        (8, V1_TIME, "Silver", "Student"),
    ], signup_date=["2024-03-01", "2024-03-15", "2024-03-15", "2023-01-10"])
    built = build(scd2, history)
    versions = versions_of(built, 7)
    assert len(versions) == 2, "a Type 0 change opens no version"
    assert [str(v["signup_date"]) for v in versions] == ["2024-03-01", "2024-03-01"], "the first value is kept forever"
    assert [str(v["signup_date"]) for v in versions_of(built, 8)] == ["2023-01-10"]

    config = scd2.DIMENSION_CONFIGS["customers"]
    ignored = scd2.type0_changes_ignored(history, "customer_id", config["type0_columns"], config["date_candidates"],
                                         config["column_map"])
    assert ignored == {"signup_date": 1}, "one customer's signup date changed in the source; it is counted"
    same = customer_history(spark, [(7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Student")])
    assert scd2.type0_changes_ignored(same, "customer_id", config["type0_columns"], config["date_candidates"],
                                      config["column_map"]) == {"signup_date": 0}

    # The same key and hash as without the ignored change: it left no trace.
    clean = versions_of(build(scd2, customer_history(spark, [
        (7, V1_TIME, "Gold", "Student"), (7, V3_TIME, "Platinum", "Student")])), 7)
    assert [v["customer_sk"] for v in versions] == [v["customer_sk"] for v in clean]


def test_a_type_1_change_updates_every_version_and_creates_none(spark, scd2):
    history = customer_history(spark, [
        (7, V1_TIME, "Gold", "Student", "Female", 31, "Tamil", 1200, False),
        # A tier change (Type 2): version 2.
        (7, V2_TIME, "Platinum", "Student", "Female", 31, "Tamil", 31000, False),
        # Only Type 1 attributes change: the email is corrected, a birthday passes, points are earned.
        (7, V3_TIME, "Platinum", "Student", "Female", 32, "Hindi", 31500, True),
    ], email=["asha@example.org", "asha@example.org", "asha.rao@example.net"])
    versions = versions_of(build(scd2, history), 7)

    assert len(versions) == 2, "a Type 1 change opens no version"
    assert [v["email"] for v in versions] == ["asha.rao@example.net"] * 2, "the latest value is on every version"
    assert [(v["age"], v["preferred_language"], v["loyalty_points"], v["is_prime_user"]) for v in versions] == \
        [(32, "Hindi", 31500, True)] * 2
    assert [v["loyalty_tier"] for v in versions] == ["Gold", "Platinum"], "the history attributes are untouched"
    # No key, hash or date moved because of it.
    before = versions_of(build(scd2, customer_history(spark, [
        (7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Platinum", "Student")])), 7)
    for column in ("customer_sk", "record_hash", "effective_from", "effective_to", "is_current"):
        assert [v[column] for v in versions] == [v[column] for v in before], column


def test_a_type_2_change_creates_exactly_one_version_with_the_right_dates(spark, scd2):
    one = versions_of(build(scd2, customer_history(spark, [(7, V1_TIME, "Gold", "Student")])), 7)
    assert [(v["effective_from"], v["effective_to"], v["is_current"]) for v in one] == [
        ("1900-01-01 00:00:00", "9999-12-31 00:00:00", True)]
    # The customer moves: city is Type 2.
    moved = versions_of(build(scd2, customer_history(spark, [
        (7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Gold", "Student")], home_city=["Pune", "Chennai"])), 7)
    assert len(moved) == len(one) + 1, "exactly one new version"
    assert [(v["city"], v["effective_from"], v["effective_to"], v["is_current"]) for v in moved] == [
        ("Pune", "1900-01-01 00:00:00", V2_TIME, False),
        ("Chennai", V2_TIME, "9999-12-31 00:00:00", True)]
    assert moved[0]["customer_sk"] == one[0]["customer_sk"], "the earlier version keeps its key"
    assert moved[1]["customer_sk"] != moved[0]["customer_sk"]
    assert [v["source_updated_at"] for v in moved] == [V1_TIME, V2_TIME]
    # The same row delivered again opens nothing.
    again = versions_of(build(scd2, customer_history(spark, [
        (7, V1_TIME, "Gold", "Student"), (7, V2_TIME, "Gold", "Student"), (7, V2_TIME, "Gold", "Student")],
        home_city=["Pune", "Chennai", "Chennai"])), 7)
    assert [v["customer_sk"] for v in again] == [v["customer_sk"] for v in moved]


def test_facts_still_take_the_version_of_their_time_after_changes_of_every_type(spark, scd2, load_job):
    pit = load_job("spark-batch/point_in_time_fact_joins.py")
    history = customer_history(spark, [
        (7, V1_TIME, "Gold", "Student"),
        (7, V2_TIME, "Platinum", "Student"),     # Type 2: version 2 from V2_TIME
        (7, V3_TIME, "Platinum", "Student"),     # Type 1 (email) and Type 0 (signup date) only
    ], email=["asha@example.org", "asha@example.org", "asha.rao@example.net"],
        signup_date=["2024-03-01", "2024-03-01", "2024-04-01"])
    dimension = build(scd2, history)
    first, second = versions_of(dimension, 7)
    orders = spark.createDataFrame(
        [(1, 7, "2026-01-05 09:00:00"), (2, 7, "2026-06-18 07:59:59"), (3, 7, V2_TIME), (4, 7, "2026-07-10 12:00:00")],
        "order_id int, customer_id int, order_date string").withColumn("order_date", F.to_timestamp("order_date"))
    joined = pit.point_in_time_join(
        fact_df=orders, dim_df=dimension, fact_key="customer_id", dim_key="customer_id",
        fact_timestamp="order_date", surrogate_key="customer_sk")
    keys = {row["order_id"]: (row["customer_sk"], row["customer_sk_resolution"]) for row in joined.collect()}
    assert keys == {1: (first["customer_sk"], "in_range"), 2: (first["customer_sk"], "in_range"),
                    3: (second["customer_sk"], "in_range"), 4: (second["customer_sk"], "in_range")}
    # Whichever version a fact takes, it reads the overwritten and the fixed values.
    assert {(v["email"], str(v["signup_date"])) for v in (first, second)} == {("asha.rao@example.net", "2024-03-01")}


def test_a_missing_type_1_or_type_0_column_fails_loudly(spark, scd2):
    for column in ("loyalty_points", "signup_date"):
        history = customer_history(spark, [(7, V1_TIME, "Gold", "Student")]).drop(column)
        with pytest.raises(ValueError, match=column):
            build(scd2, history).collect()
