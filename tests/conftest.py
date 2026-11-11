"""Shared fixtures for the Project 2 tests.

Jobs are loaded from a copy of their source file inside tmp_path, so each job's
PROJECT_ROOT (the parent of its folder) is a temporary project and nothing is
read from or written to the real data/ or logs/ folders.

P2_CODE_ROOT selects which checkout the job files are copied from (default:
this repository). Pointing it at an older checkout runs the same tests against
older code, which is how each test is shown to fail on the code before its fix.
"""
from __future__ import annotations

import importlib.util
import itertools
import os
import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CODE_ROOT = Path(os.environ.get("P2_CODE_ROOT", REPO_ROOT)).resolve()

_module_counter = itertools.count()

# The staging tables create_warehouse_tables.sql requires (a missing one is an
# error). Database tests that build the warehouse from a few hand-made rows run
# this first; a table the test already created is kept.
MINIMAL_STAGING_SQL = "".join(
    f"CREATE TABLE IF NOT EXISTS staging.{table} ({columns});\n" for table, columns in (
        ("stg_gold_dim_customers_scd2", "customer_id text, effective_from timestamp"),
        ("stg_gold_dim_products_scd2", "product_id text, effective_from timestamp"),
        ("stg_gold_dim_campaigns_scd2", "campaign_id text, effective_from timestamp"),
        ("stg_gold_fact_orders_scd2", "order_id text"),
        ("stg_gold_fact_order_items_scd2", "order_item_id text"),
        ("stg_gold_fact_campaign_spend_scd2", "spend_id text"),
        ("stg_gold_fact_web_events", "event_id text"),
    )
)


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole run: UTC, one shuffle partition, no UI."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("project2-unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _restore_session_settings(request):
    """Jobs set their own session settings through getOrCreate(); undo them after each test."""
    if "spark" not in request.fixturenames:
        yield
        return
    session = request.getfixturevalue("spark")
    keys = ["spark.sql.session.timeZone", "spark.sql.ansi.enabled", "spark.sql.shuffle.partitions"]
    saved = {key: session.conf.get(key) for key in keys}
    yield
    for key, value in saved.items():
        session.conf.set(key, value)


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    return root


@pytest.fixture
def load_job(project_root: Path, monkeypatch):
    """Load a job module from a copy of its file inside the temporary project.

    load_job("spark-batch/silver_transformations.py", BATCH_DATE="2026-06-18")
    Environment variables passed as keywords are set before the module runs, so
    module-level settings (BATCH_DATE, SOURCE_TIMEZONE) see them. root= loads
    the copy into another temporary project instead of the default one.
    """

    def _load(relative_path: str, root: Path | None = None, **env: str):
        source = CODE_ROOT / relative_path
        if not source.is_file():
            pytest.fail(f"{relative_path} does not exist in {CODE_ROOT}")
        target = (root or project_root) / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, target)
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        name = f"p2_job_{target.stem}_{next(_module_counter)}"
        spec = importlib.util.spec_from_file_location(name, target)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    return _load


@pytest.fixture
def no_stop(spark, monkeypatch):
    """Let a job's main() call spark.stop() without stopping the shared session."""
    monkeypatch.setattr(spark, "stop", lambda: None)
    return spark


# ---------------------------------------------------------------------------
# Tiny Bronze tables, shaped like the real ones: the six CSV sources are all
# strings, web events carry the types Spark infers from the JSON source.
# Each template is a real row that passes Silver today.
# ---------------------------------------------------------------------------

BRONZE_TEMPLATES: dict[str, dict] = {
    "customers": {
        "user_id": "4", "user_name": "Jagrati Padmanabhan", "email": "caleb78@example.org", "gender": "Male",
        "age": "28", "membership_tier": "Platinum", "loyalty_points": "22298", "preferred_language": "Telugu",
        "home_city": "Tiruchirappalli", "home_state": "Madhya Pradesh", "country": "India",
        "user_segment": "Working Professional", "is_prime_user": "True", "schema_version": "v1",
        "source": "crm_customer_master", "updated_at": "2026-02-10T10:06:05",
    },
    "products": {
        "product_id": "4", "product_name": "Bluetooth Speaker", "category": "Fitness", "original_price": "15192.99",
        "discount_percent": "10", "discounted_price": "13673.69", "inventory_remaining": "1938",
        "schema_version": "v1", "source": "product_catalog", "updated_at": "2026-02-23T07:26:51",
    },
    "campaigns": {
        "campaign_id": "5.0", "campaign_name": "Facebook Campaign 5", "traffic_source": "facebook",
        "ab_test_group": "control", "target_segment": "Enterprise", "budget": "232331.86",
        "campaign_status": "completed", "start_date": "2026-05-07", "end_date": "2026-07-24",
        "schema_version": "v1", "source": "marketing_campaign_master", "updated_at": "2026-05-13T19:38:00",
    },
    "orders": {
        "order_id": "3100027", "user_id": "352", "campaign_id": "26.0", "order_timestamp": "2026-06-11T08:48:11",
        "payment_method": "UPI", "cart_value": "138901.11", "fraud_score": "0.3686", "country": "India",
        "city": "Cuttack", "schema_version": "v1", "source": "orders_from_purchase_events",
    },
    "order_items": {
        "order_item_id": "3100012", "order_id": "3100227", "product_id": "4", "product_name": "Bluetooth Speaker",
        "category": "Fitness", "quantity": "5", "original_price": "15192.99", "discount_percent": "10.0",
        "discounted_price": "13673.69", "line_amount": "68368.45",
    },
    "ad_spend": {
        "spend_id": "3100003", "campaign_id": "999999", "traffic_source": "organic", "spend_date": "2026-06-17",
        "impressions": "406454", "clicks": "153713", "spend_amount": "88287.74", "schema_version": "v1",
        "source": "marketing_ad_platform",
    },
    "web_events": {
        "ab_test_group": "B", "age": 53, "api_latency_ms": 2413, "app_version": "5.7.1", "browser": "Edge",
        "campaign_id": 23, "cart_abandonment_probability": 0.4374, "cart_value": 94492.98, "category": "Books",
        "city": "Rampur", "click_position": 9, "country": "India", "device_type": "tablet",
        "discount_percent": 5.0, "discounted_price": 31497.66, "email": "jasmitwali@example.net",
        "engagement_score": 0.6282, "event_id": "5cfd8c65-b70b-4b9d-9e91-eb1f1d26110d",
        "event_time": "2026-06-15T08:57:02", "event_timestamp": "2026-06-15T08:57:02", "event_type": "purchase",
        "fraud_score": 0.8811, "gender": "Female", "home_city": "Khammam", "home_state": "Telangana",
        "hover_duration_ms": 22146, "inventory_remaining": 7844, "ip_address": "34.164.194.108",
        "is_prime_user": True, "items_viewed_in_session": 34, "loyalty_points": 29841, "membership_tier": "Platinum",
        "network_type": "4G", "operating_system": "iOS", "original_price": 33155.43, "page_load_time_ms": 8156,
        "payment_method": "COD", "preferred_language": "Telugu", "product_id": 5, "product_name": "Laptop Stand",
        "purchase_probability": 0.3992, "quantity": 3, "recommendation_algorithm": "ranking_model",
        "recommendation_clicked": False, "recommendation_rank": 17, "repeat_product_view_count": 7,
        "schema_version": "v1", "scroll_depth_percent": 40, "search_query": "enim", "session_duration_sec": 2375,
        "session_id": "8736cdad-2d0f-4d7f-9649-6fbbe32e8e21", "source": "project_1_behavioral_events",
        "time_on_page_sec": 783, "time_since_last_event_ms": 586084, "traffic_source": "facebook",
        "user_id": 106, "user_journey_stage": "awareness", "user_name": "Parth Iyer",
        "user_segment": "Working Professional",
    },
}

BRONZE_KEYS = {
    "customers": "user_id", "products": "product_id", "campaigns": "campaign_id", "orders": "order_id",
    "order_items": "order_item_id", "ad_spend": "spend_id", "web_events": "event_id",
}


def _bronze_type(table: str, column: str, value):
    from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType

    if table != "web_events":
        return StringType()
    template = BRONZE_TEMPLATES[table][column]
    if isinstance(template, bool):
        return BooleanType()
    if isinstance(template, int):
        return LongType()
    if isinstance(template, float):
        return DoubleType()
    return StringType()


@pytest.fixture
def write_bronze(spark, project_root: Path):
    """write_bronze("campaigns", [{"campaign_id": "1"}, {...}], ingestion_date="2026-06-18")

    Each row starts from the table's valid template; keys in the dict override
    it (None makes the column NULL). Rows get Bronze metadata and a unique
    record_hash. Returns the written rows' keys and record hashes.
    """
    import hashlib

    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

    def _write(table: str, rows: list[dict], ingestion_date: str = "2026-06-18", append: bool = False):
        template = BRONZE_TEMPLATES[table]
        columns = list(template)
        fields = [StructField(c, _bronze_type(table, c, template[c]), True) for c in columns]
        fields += [
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("batch_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("record_hash", StringType(), True),
            StructField("ingestion_date", DateType(), True),
        ]
        data = []
        for index, override in enumerate(rows):
            row = {**template, **override}
            record_hash = hashlib.sha256(f"{table}|{ingestion_date}|{index}|{sorted(row.items(), key=str)}".encode()).hexdigest()
            data.append(tuple(row[c] for c in columns) + (None, f"batch-{ingestion_date}", table, f"{table}.src", record_hash, None))
        frame = (
            spark.createDataFrame(data, StructType(fields))
            .withColumn("ingestion_timestamp", F.to_timestamp(F.lit(f"{ingestion_date} 06:00:00")))
            .withColumn("ingestion_date", F.to_date(F.lit(ingestion_date)))
        )
        target = project_root / "data" / "bronze" / f"bronze_{table}"
        (
            frame.write.mode("append" if append else "overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("ingestion_date")
            .parquet(str(target))
        )
        return [(row_values[columns.index(BRONZE_KEYS[table])], row_values[len(columns) + 4])
                for row_values in data]

    return _write
