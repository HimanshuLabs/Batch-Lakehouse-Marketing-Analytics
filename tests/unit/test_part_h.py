"""Part H1 (p2-data-v2): keys, lineage, batch dates, clean starts, the run lock, a reader role.

0  Dimension keys were a 32-bit hash (abs(hashtext) + 1e9): a collision was
   plausible within years. Every key is now the 63-bit hash of the fact keys.
1  Facts carried surrogate keys only, and nothing told a consumer when a row
   last changed except warehouse_loaded_at, which every rebuild resets. Facts
   now carry their natural IDs, a source_updated_at and a record_hash.
2  BATCH_DATE was the UTC day of the run, so a run at 00:30 IST processed
   "yesterday". It is the Asia/Kolkata day.
3  Silver needed 7,000 web events in total, which only the first batch has, so
   a start from any other date failed; an empty batch failed Bronze checks.
4  Point-in-time rows carried the last run's date; Bronze stored absolute file
   paths; two runs at once could overwrite each other.
5  Consumers connected as the owner. p2_reader is read-only.

The PostgreSQL cases need the disposable sandbox on localhost:5546 (P2_F_SANDBOX=1).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import types

import pytest
from pyspark.sql import functions as F

from conftest import CODE_ROOT, MINIMAL_STAGING_SQL, begin_build, build_step, require_sandbox_database, reset_sandbox

WAREHOUSE_SQL = CODE_ROOT / "sql" / "warehouse" / "create_warehouse_tables.sql"
BATCH_DATE = "2026-06-18"
FACTS = {
    "fact_orders": ["customer_id", "campaign_id"],
    "fact_order_items": ["customer_id", "product_id"],
    "fact_campaign_spend": ["campaign_id"],
    "fact_web_events": ["customer_id", "product_id", "campaign_id"],
    "fact_conversions": ["customer_id", "product_id", "campaign_id"],
}


def sql_code(path) -> str:
    return "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())


@pytest.fixture
def pg():
    require_sandbox_database()
    import psycopg
    with psycopg.connect(host="127.0.0.1", port=5546, user="project2", password="project2",
                         dbname="marketing_analytics", autocommit=True) as conn:
        reset_sandbox(conn)
        conn.execute(""
                     "CREATE SCHEMA staging; SELECT set_config('p2.run_id', 'part-h-test', false)")
        yield conn


STAGING_ROWS = """
CREATE TABLE staging.stg_gold_dim_customers_scd2 (customer_id int, customer_name text, effective_from timestamp,
    effective_to timestamp, is_current boolean, source_updated_at timestamp);
INSERT INTO staging.stg_gold_dim_customers_scd2 VALUES
    (7, 'Asha', '1900-01-01', '2026-06-01', false, '2026-02-10 10:06:05'),
    (7, 'Asha', '2026-06-01', '9999-12-31', true, '2026-06-01 00:00:00');
CREATE TABLE staging.stg_gold_dim_products_scd2 (product_id int, product_name text, category text,
    effective_from timestamp, effective_to timestamp, is_current boolean, source_updated_at timestamp,
    brand text, status text);
INSERT INTO staging.stg_gold_dim_products_scd2 VALUES
    (4, 'Speaker', 'Audio', '1900-01-01', '9999-12-31', true, '2026-02-01', 'Voltix', 'active');
CREATE TABLE staging.stg_gold_dim_campaigns_scd2 (campaign_id int, campaign_name text, channel text,
    effective_from timestamp, effective_to timestamp, is_current boolean, source_updated_at timestamp,
    campaign_type text);
INSERT INTO staging.stg_gold_dim_campaigns_scd2 VALUES
    (5, 'Summer', 'facebook', '1900-01-01', '9999-12-31', true, '2026-03-01', 'conversion');
CREATE TABLE staging.stg_gold_fact_orders_scd2 (order_id int, customer_id int, campaign_id int,
    order_timestamp timestamp, cart_value numeric, payment_method text, ingestion_date date,
    order_status text, tax_amount numeric, shipping_amount numeric, total_amount numeric,
    status_timestamp timestamp, refund_amount numeric);
INSERT INTO staging.stg_gold_fact_orders_scd2 VALUES
    (1, 7, 5, '2026-05-10 10:00', 200.00, 'UPI', '2026-05-18', 'completed', 36.00, 0.00, 236.00, '2026-05-10 10:00', 0),
    (2, 7, NULL, '2026-06-10 10:00', 100.00, 'COD', '2026-06-18', 'completed', 18.00, 499.00, 617.00, '2026-06-10 10:00', 0);
CREATE TABLE staging.stg_gold_fact_order_items_scd2 (order_item_id int, order_id int, product_id int, quantity int,
    original_price numeric, discounted_price numeric, line_amount numeric, ingestion_date date, refund_amount numeric);
INSERT INTO staging.stg_gold_fact_order_items_scd2 VALUES
    (10, 1, 4, 2, 120.00, 100.00, 200.00, '2026-05-18', 0), (20, 2, 4, 1, 120.00, 100.00, 100.00, '2026-06-18', 0);
CREATE TABLE staging.stg_gold_fact_campaign_spend_scd2 (spend_id int, campaign_id int, traffic_source text,
    spend_date date, spend_amount numeric, impressions bigint, clicks bigint, ingestion_date date,
    conversions bigint, attributed_revenue numeric, country text, state text, campaign_day_spend numeric);
INSERT INTO staging.stg_gold_fact_campaign_spend_scd2 VALUES
    (1, 5, 'facebook', '2026-05-09', 30.00, 600, 6, '2026-05-18', 2, 300.00, 'India', 'Kerala', 50.00),
    (2, 5, 'facebook', '2026-05-09', 20.00, 400, 4, '2026-05-18', 1, 150.00, 'India', 'Punjab', 50.00);
CREATE TABLE staging.stg_gold_fact_web_events (event_id text, session_id text, customer_id int, product_id int,
    campaign_id int, event_timestamp timestamp, event_type text, traffic_source text, cart_value numeric,
    ingestion_date date);
INSERT INTO staging.stg_gold_fact_web_events VALUES
    ('e1', 's1', 7, 4, 5, '2026-05-10 09:55', 'purchase', 'facebook', 200.00, '2026-05-18'),
    ('e2', 's2', 7, NULL, NULL, '2026-06-10 09:00', 'page_view', 'organic', 0, '2026-06-18');
"""


def build_warehouse(pg):
    pg.execute(begin_build() + build_step("create_warehouse_tables") + "COMMIT;")


def snapshot(pg) -> dict:
    out = {}
    for table, key in (("dim_customer", "customer_sk"), ("dim_product", "product_sk"), ("dim_campaign", "campaign_sk"),
                       ("dim_region", "region_sk"), ("dim_channel", "channel_sk")):
        out[table] = pg.execute(f"SELECT {key} FROM warehouse.{table} ORDER BY 1").fetchall()
    for table, ids in FACTS.items():
        out[table] = pg.execute(
            f"SELECT {', '.join(ids)}, source_updated_at, record_hash FROM warehouse.{table} ORDER BY record_hash"
        ).fetchall()
    return out


# --- 0: one 63-bit key hash -----------------------------------------------------

def test_no_32_bit_key_function_is_used():
    for path in (CODE_ROOT / "sql" / "warehouse").glob("*.sql"):
        assert "hashtext" not in sql_code(path), f"{path.name} still uses the 32-bit hashtext"
    code = re.sub(r"\s+", " ", sql_code(WAREHOUSE_SQL))
    body = re.search(r"FUNCTION warehouse\.deterministic_sk\(prefix TEXT, value TEXT\).*?\$\$(.*?)\$\$", code).group(1)
    assert body.strip() == (
        "SELECT warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown'));")
    fact_body = re.search(r"FUNCTION warehouse\.deterministic_fact_sk\(.*?\$\$(.*?)\$\$", code).group(1)
    assert "md5(prefix || '|' || natural_key), 1, 16))::BIT(64)::BIGINT & 9223372036854775807" in fact_body


def test_keys_are_63_bit_hashes_and_stable_across_rebuilds(pg):
    pg.execute(STAGING_ROWS)
    build_warehouse(pg)
    first = snapshot(pg)
    build_warehouse(pg)
    assert snapshot(pg) == first, "a rebuild changed a key, a source_updated_at or a record_hash"

    assert pg.execute("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                      "WHERE n.nspname = 'warehouse' AND p.prosrc ILIKE '%hashtext%'").fetchone()[0] == 0
    # Every dimension key is the 63-bit hash of its natural key (0 is the unknown member).
    assert pg.execute(
        "SELECT count(*) FROM warehouse.dim_customer WHERE customer_sk NOT IN (0, -1) AND customer_sk <> "
        "warehouse.deterministic_fact_sk('customer', customer_id || '|' || to_char(source_updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SS'))"
    ).fetchone()[0] == 0
    assert pg.execute(
        "SELECT (SELECT count(*) FROM warehouse.dim_region WHERE region_sk <> 0 AND region_sk <> "
        "warehouse.deterministic_fact_sk('region', region_nk)) + (SELECT count(*) FROM warehouse.dim_channel "
        "WHERE channel_sk <> 0 AND channel_sk <> warehouse.deterministic_fact_sk('channel', channel_nk))"
    ).fetchone()[0] == 0
    keys = [row[0] for table in ("dim_customer", "dim_product", "dim_campaign", "dim_channel") for row in first[table]]
    assert max(keys) > 2 ** 32, "63-bit keys: at least one of these is far above the old 32-bit range"


# --- 1: natural IDs, source_updated_at and record_hash on every fact ------------

def test_every_fact_table_has_natural_ids_and_lineage_columns():
    code = sql_code(WAREHOUSE_SQL)
    for table, ids in FACTS.items():
        ddl = re.search(rf"CREATE TABLE warehouse\.{table} \((.*?)\n\);", code, re.S).group(1)
        for column in ids:
            assert re.search(rf"\b{column} TEXT NOT NULL,", ddl), (table, column)
        assert "source_updated_at TIMESTAMPTZ," in ddl and "record_hash TEXT," in ddl, table
        assert f"ALTER TABLE warehouse.{table} ALTER COLUMN record_hash SET NOT NULL;" in code


def test_facts_carry_natural_ids_a_source_time_and_a_stable_hash(pg):
    pg.execute(STAGING_ROWS)
    build_warehouse(pg)
    orders = pg.execute("SELECT order_id, customer_id, campaign_id, source_updated_at::date::text, record_hash "
                        "FROM warehouse.fact_orders ORDER BY order_id").fetchall()
    assert [row[:4] for row in orders] == [("1", "7", "5", "2026-05-18"), ("2", "7", "UNKNOWN", "2026-06-18")]
    assert pg.execute("SELECT order_item_nk, customer_id, product_id FROM warehouse.fact_order_items "
                      "ORDER BY 1").fetchall() == [("10", "7", "4"), ("20", "7", "4")]
    assert pg.execute("SELECT event_id, customer_id, product_id, campaign_id, source_updated_at::date::text "
                      "FROM warehouse.fact_web_events ORDER BY 1").fetchall() == [
        ("e1", "7", "4", "5", "2026-05-18"), ("e2", "7", "UNKNOWN", "UNKNOWN", "2026-06-18")]
    assert pg.execute("SELECT customer_id, product_id, campaign_id, source_updated_at::date::text "
                      "FROM warehouse.fact_conversions").fetchall() == [("7", "4", "5", "2026-05-18")]
    assert pg.execute("SELECT campaign_id, source_updated_at::date::text FROM warehouse.fact_campaign_spend"
                      ).fetchall() == [("5", "2026-05-18")] * 2, "one row per region of the campaign-day"
    # The natural ID is the dimension member's ID wherever the key is a real member.
    assert pg.execute("SELECT count(*) FROM warehouse.fact_orders f JOIN warehouse.dim_customer d USING (customer_sk) "
                      "WHERE d.customer_id <> f.customer_id").fetchone()[0] == 0

    # A change in a business column changes the hash of that row only; a rebuild alone changes nothing.
    pg.execute("UPDATE staging.stg_gold_fact_orders_scd2 SET payment_method = 'Card' WHERE order_id = 2")
    build_warehouse(pg)
    after = pg.execute("SELECT order_id, record_hash FROM warehouse.fact_orders ORDER BY order_id").fetchall()
    assert after[0][1] == orders[0][4] and after[1][1] != orders[1][4]


# --- 2: BATCH_DATE is the Asia/Kolkata day ---------------------------------------

@pytest.mark.parametrize("field", ["logical_date", "run_after"])
@pytest.mark.parametrize("utc, expected", [
    ("2026-06-17T19:00:00", "2026-06-18"),   # 00:30 IST on the 18th
    ("2026-06-17T18:29:59", "2026-06-17"),   # 23:59:59 IST on the 17th
    ("2026-06-18T12:00:00", "2026-06-18"),
    ("2026-12-31T18:30:00", "2027-01-01"),   # midnight IST on New Year's Day
])
def test_batch_date_is_the_ist_day_of_the_run(load_job, monkeypatch, field, utc, expected):
    import dateutil.tz
    from jinja2.sandbox import SandboxedEnvironment
    from test_dag_structure import DAG_FILE, stub_airflow

    for name in ("PROJECT_ROOT", "PROJECT_PYTHON", "DBT_BIN"):
        monkeypatch.delenv(name, raising=False)
    stub_airflow(monkeypatch)
    template = load_job(DAG_FILE).DEFAULT_ENV["BATCH_DATE"]

    environment = SandboxedEnvironment()
    environment.filters["ds"] = lambda value: value.strftime("%Y-%m-%d")  # Airflow's ds filter
    moment = dt.datetime.fromisoformat(utc).replace(tzinfo=dt.timezone.utc)
    dag_run = types.SimpleNamespace(logical_date=moment if field == "logical_date" else None, run_after=moment)
    rendered = environment.from_string(template).render(dag_run=dag_run, macros=types.SimpleNamespace(dateutil=dateutil))
    assert rendered == expected


# --- 3: a start from any date, and an empty batch --------------------------------

def test_the_silver_volume_threshold_is_per_batch(spark, load_job, project_root):
    from test_silver_conservation import write_orders

    qc = load_job("spark-batch/silver_quality_checks.py", BATCH_DATE=BATCH_DATE)
    assert "minimum_silver_rows" not in qc.TABLES["web_events"]
    assert qc.TABLES["web_events"]["minimum_batch_pass_ratio"] == 0.875

    def check(batch_rows: int, quarantined: int):
        session = qc.create_spark_session()
        root = project_root / f"case-{batch_rows}-{quarantined}"
        config = {**qc.TABLES["orders"], "minimum_batch_pass_ratio": 0.875,
                  "bronze": root / "bronze_orders", "silver": root / "silver_orders",
                  "quarantine": root / "quarantine_orders"}
        batch = [(100 + i, BATCH_DATE, f"hash-{i}") for i in range(batch_rows)]
        write_orders(session, config["bronze"], batch, "bronze")
        write_orders(session, config["silver"], batch[quarantined:], "silver")
        if quarantined:
            write_orders(session, config["quarantine"], batch[:quarantined], "quarantine")
        return qc.validate_table(session, "orders", config)

    # A start from any batch date: 8 rows in all, far below any total-row floor.
    assert check(batch_rows=8, quarantined=1)["status"] == "PASSED"      # 7 of 8 pass
    failed = check(batch_rows=8, quarantined=2)                          # 6 of 8 pass
    assert failed["status"] == "FAILED"
    assert f"too few of batch {BATCH_DATE}'s rows passed Silver: passed=6 of 8" in failed["failure_reason"]


def test_an_empty_feed_passes_the_bronze_checks_only_when_the_batch_is_declared_empty(spark, load_job, project_root):
    qc = load_job("spark-batch/bronze_quality_checks.py", BATCH_DATE=BATCH_DATE)
    raw_dir = project_root / "data" / "raw" / f"batch_date={BATCH_DATE}"
    raw_dir.mkdir(parents=True)
    raw = raw_dir / "orders.csv"
    raw.write_text("order_id,user_id,cart_value\n", encoding="utf-8")       # header only
    events = raw_dir / "web_events.json"
    events.write_text("\n", encoding="utf-8")                                # no event
    assert qc.count_raw_file_lines(raw, has_header=True) == 0
    assert qc.count_raw_file_lines(events, has_header=False) == 0
    bronze = project_root / "data" / "bronze" / "bronze_orders"

    # Part J: an empty feed is a failure (a feed that stopped delivering) ...
    session = qc.create_spark_session()
    undeclared = qc.validate_table(session, "orders", raw, bronze, True)
    assert undeclared["status"] == "FAILED"
    assert undeclared["failure_reason"] == (
        f"the feed is empty and the batch is not declared empty (no _EMPTY_BATCH file in batch_date={BATCH_DATE})")

    # ... unless whoever delivers the batch declares it empty.
    (raw_dir / qc.EMPTY_BATCH_MARKER).write_text("", encoding="utf-8")
    result = qc.validate_table(session, "orders", raw, bronze, True)
    assert (result["status"], result["raw_row_count"], result["bronze_row_count"], result["failure_reason"]) == (
        "PASSED", 0, 0, "")

    # Rows in Bronze for a batch whose raw file is empty are still an error.
    session.createDataFrame([("1", BATCH_DATE)], "order_id string, day string") \
        .withColumn("ingestion_date", F.to_date("day")).drop("day") \
        .write.mode("overwrite").partitionBy("ingestion_date").parquet(str(bronze))
    assert qc.validate_table(session, "orders", raw, bronze, True)["status"] == "FAILED"


def test_a_feed_far_below_its_usual_volume_fails_the_bronze_checks(spark, load_job, project_root):
    """Part J. The rule: fewer rows than 0.5 x the median of the earlier batches, the initial load excluded."""
    qc = load_job("spark-batch/bronze_quality_checks.py", BATCH_DATE=BATCH_DATE)
    assert qc.MIN_VOLUME_RATIO == 0.5
    session = qc.create_spark_session()

    def check(case: str, earlier: dict[str, int], rows: int, declared_empty: bool = False):
        """earlier: {batch date: rows already in Bronze}; rows: this batch's feed."""
        raw_dir = project_root / case / "raw" / f"batch_date={BATCH_DATE}"
        raw_dir.mkdir(parents=True)
        raw = raw_dir / "orders.csv"
        raw.write_text("order_id\n" + "".join(f"{n}\n" for n in range(rows)), encoding="utf-8")
        if declared_empty:
            (raw_dir / qc.EMPTY_BATCH_MARKER).write_text("", encoding="utf-8")
        bronze = project_root / case / "bronze_orders"
        frames = [session.range(count).select(
            F.col("id").cast("string").alias("order_id"), F.current_timestamp().alias("ingestion_timestamp"),
            F.lit(f"batch-{day}").alias("batch_id"), F.lit("orders").alias("source_system"),
            F.lit("orders.csv").alias("source_file_name"), F.col("id").cast("string").alias("record_hash"),
            F.to_date(F.lit(day)).alias("ingestion_date"),
        ) for day, count in {**earlier, BATCH_DATE: rows}.items() if count]
        frame = frames[0]
        for other in frames[1:]:
            frame = frame.unionByName(other)
        frame.write.mode("overwrite").partitionBy("ingestion_date").parquet(str(bronze))
        return qc.validate_table(session, "orders", raw, bronze, True)

    usual = {"2026-05-18": 8000, "2026-06-01": 1000, "2026-06-04": 1100, "2026-06-11": 900}
    low = check("low", usual, rows=499)
    assert low["status"] == "FAILED" and low["usual_row_count"] == 1000
    assert low["failure_reason"] == (
        "volume far below usual: 499 rows, less than 0.5 x the median of the earlier batches (1000)")
    assert check("enough", usual, rows=500)["status"] == "PASSED", "exactly half the usual volume passes"

    # The first batch ever has no baseline; neither has the second, because the
    # first (the initial load: months of history in one file) is never a baseline.
    first = check("first", {}, rows=7)
    assert (first["status"], first["usual_row_count"]) == ("PASSED", 0)
    second = check("second", {"2026-05-18": 8000}, rows=7)
    assert (second["status"], second["usual_row_count"]) == ("PASSED", 0)
    third = check("third", {"2026-05-18": 8000, "2026-06-04": 1000}, rows=7)
    assert third["status"] == "FAILED" and "volume far below usual: 7 rows" in third["failure_reason"]
    # A batch dated before the others is judged against earlier batches only.
    assert check("backfill", {"2026-06-25": 1000, "2026-07-02": 1000}, rows=7)["status"] == "PASSED"
    # A batch that is declared empty may also be small.
    assert check("declared", usual, rows=7, declared_empty=True)["status"] == "PASSED"


# --- 4: batch stamps, relative paths, the run lock -------------------------------
# (point-in-time stamping: test_point_in_time.py::test_point_in_time_rows_are_stamped_with_their_own_batch_date)

def test_bronze_source_file_name_is_relative_to_the_project(spark, load_job, project_root):
    bronze = load_job("spark-batch/bronze_ingestion.py", BATCH_DATE=BATCH_DATE)
    raw = project_root / "data" / "raw" / f"batch_date={BATCH_DATE}" / "orders.csv"
    raw.parent.mkdir(parents=True)
    raw.write_text("order_id\n1\n", encoding="utf-8")

    frame = bronze.add_bronze_metadata(bronze.read_source(spark, "csv", raw), "orders", raw)
    assert [row["source_file_name"] for row in frame.collect()] == [f"data/raw/batch_date={BATCH_DATE}/orders.csv"]


def lock(project_root, run_id, *args, stale=None):
    env = {**os.environ, "PROJECT_ROOT": str(project_root), "P2_RUN_ID": run_id}
    if stale is not None:
        env["P2_RUN_LOCK_STALE_SECONDS"] = str(stale)
    return subprocess.run([sys.executable, str(CODE_ROOT / "scripts" / "pipeline_lock.py"), *args],
                          env=env, capture_output=True, text=True)


def test_the_run_lock_allows_one_pipeline_run_at_a_time(project_root):
    marker = project_root / "ran-by-b"
    touch = [sys.executable, "-c", f"open({str(marker)!r}, 'w').close()"]

    assert lock(project_root, "run-a", "generate_data", "--", "true").returncode == 0
    blocked = lock(project_root, "run-b", "generate_data", "--", *touch)
    assert blocked.returncode == 75 and "another pipeline run is in progress" in blocked.stderr
    assert not marker.exists(), "the second run's task must not execute"
    assert lock(project_root, "run-a", "bronze_ingestion", "--", "true").returncode == 0, "the owner continues"

    # Two tasks at the same moment: the second fails at once.
    running = subprocess.Popen(
        [sys.executable, str(CODE_ROOT / "scripts" / "pipeline_lock.py"), "silver", "--", "sleep", "3"],
        env={**os.environ, "PROJECT_ROOT": str(project_root), "P2_RUN_ID": "run-a"})
    for _ in range(50):
        state = (project_root / "logs" / "pipeline_run.lock").read_text()
        if '"task": "silver"' in state:
            break
        import time
        time.sleep(0.1)
    at_once = lock(project_root, "run-a", "gold", "--", "true")
    assert at_once.returncode == 75 and "another pipeline task is running now" in at_once.stderr
    assert running.wait() == 0

    # The owner's last task releases the lock; then another run may start.
    assert lock(project_root, "run-a", "--last", "power_bi_export", "--", "true").returncode == 0
    assert lock(project_root, "run-b", "generate_data", "--", *touch).returncode == 0 and marker.exists()

    # A failed task releases it too; the failed run's retry then finds the new owner.
    assert lock(project_root, "run-b", "bronze_ingestion", "--", "false").returncode == 1
    assert lock(project_root, "run-c", "generate_data", "--", "true").returncode == 0
    assert lock(project_root, "run-b", "bronze_ingestion", "--", "true").returncode == 75
    # A dead owner is replaced after the stale time, or released by hand.
    assert lock(project_root, "run-d", "generate_data", "--", "true", stale=0).returncode == 0
    assert lock(project_root, "run-e", "--release").returncode == 75
    assert lock(project_root, "run-d", "--release").returncode == 0
    assert json.loads((project_root / "logs" / "pipeline_run.lock").read_text())["state"] == "released"


def test_every_dag_task_runs_under_the_run_lock(load_job, monkeypatch):
    from test_dag_structure import DAG_FILE, stub_airflow

    for name in ("PROJECT_ROOT", "PROJECT_PYTHON", "DBT_BIN"):
        monkeypatch.delenv(name, raising=False)
    recorder = stub_airflow(monkeypatch)
    load_job(DAG_FILE)
    assert len(recorder.tasks) == 20
    for task, kwargs in recorder.tasks.items():
        flag = "--last " if task == "power_bi_export" else ""
        assert re.match(rf'"\$PROJECT_PYTHON" "\S+/scripts/pipeline_lock\.py" {flag}{task} -- bash -c ',
                        kwargs["bash_command"]), task


# Part I: every entry point that writes to the lake or the warehouse takes the
# lock itself when it is started without it. (run_dag.py runs the DAG's own
# commands, which the test above covers.)
ENTRY_POINTS = [
    ["scripts/run_airflow_task.sh", "no_such_task"],
    ["scripts/run_warehouse_sql.sh"],
    ["scripts/run_warehouse_bi_pipeline.sh"],
    ["scripts/run_warehouse_reconciliation_pipeline.sh"],
    ["tests/integration/rebuild_warehouse.sh", "rebuild-work"],
]


def entry_point(project_root, command, **env):
    # Never a real database: the container named here does not exist.
    environment = {key: value for key, value in os.environ.items() if key not in ("P2_LOCK_HELD", "P2_RUN_ID", "BATCH_DATE")}
    arguments = [str(project_root / argument) if argument == "rebuild-work" else argument for argument in command[1:]]
    return subprocess.run(["bash", str(CODE_ROOT / command[0]), *arguments], cwd=CODE_ROOT, capture_output=True, text=True,
                          env={**environment, "PROJECT_ROOT": str(project_root), "PGCONTAINER": "p2_no_such_container",
                               "RUN_MODE": "docker", "PROJECT_PYTHON": sys.executable, **env})


@pytest.mark.parametrize("command", ENTRY_POINTS, ids=[command[0].rsplit("/", 1)[-1] for command in ENTRY_POINTS])
def test_every_writing_entry_point_takes_the_run_lock(project_root, command):
    assert lock(project_root, "run-a", "generate_data", "--", "true").returncode == 0   # run-a owns the lock
    blocked = entry_point(project_root, command)
    assert blocked.returncode == 75, blocked.stdout + blocked.stderr
    assert "PIPELINE LOCKED: another pipeline run is in progress (run 'run-a'" in blocked.stderr
    assert not (project_root / "rebuild-work").exists() and "Running warehouse SQL" not in blocked.stdout
    assert json.loads((project_root / "logs" / "pipeline_run.lock").read_text())["run_id"] == "run-a"


def test_a_task_run_by_hand_is_a_one_task_run_and_a_pipeline_task_does_not_lock_twice(project_root):
    runner = ["scripts/run_airflow_task.sh", "no_such_task"]
    by_hand = entry_point(project_root, runner)
    assert by_hand.returncode == 1 and "Unknown task: no_such_task" in by_hand.stdout
    state = json.loads((project_root / "logs" / "pipeline_run.lock").read_text())
    assert state["run_id"].startswith("manual-") and state["task"] == "no_such_task" and state["state"] == "released"

    # Inside a pipeline task the run already holds the lock (P2_LOCK_HELD): the
    # script runs at once instead of failing on its own run's lock.
    inside = lock(project_root, "run-a", "generate_data", "--", "bash", str(CODE_ROOT / runner[0]), "no_such_task")
    assert inside.returncode == 1 and "Unknown task: no_such_task" in inside.stdout and "PIPELINE LOCKED" not in inside.stderr
    held = lock(project_root, "run-a", "generate_data", "--", sys.executable, "-c", "import os; print(os.environ['P2_LOCK_HELD'])")
    assert held.stdout.strip() == "run-a"


# Part I: an empty first-ever batch. Bronze writes nothing for an empty file, so
# with no earlier batch there is no Bronze table and Silver failed on it.

def empty_first_batch(project_root, events="\n"):
    raw = project_root / "data" / "raw" / f"batch_date={BATCH_DATE}"
    raw.mkdir(parents=True)
    for name in ("customers", "products", "campaigns", "orders", "order_items", "ad_spend"):
        (raw / f"{name}.csv").write_text("id,value\n", encoding="utf-8")       # header only
    (raw / "web_events.json").write_text(events, encoding="utf-8")
    # Declared empty by whoever delivers it; without this the Bronze checks fail the batch.
    (raw / "_EMPTY_BATCH").write_text("", encoding="utf-8")
    return raw


def test_an_empty_first_ever_batch_has_nothing_to_build_and_every_later_task_succeeds(project_root):
    raw = empty_first_batch(project_root)

    def task(name):
        return entry_point(project_root, ["scripts/run_airflow_task.sh", name], BATCH_DATE=BATCH_DATE)

    runner = (CODE_ROOT / "scripts" / "run_airflow_task.sh").read_text(encoding="utf-8")
    later = re.search(r'TASKS_AFTER_BRONZE="([^"]*)"', runner).group(1).replace("\\\n", " ").split()
    for name in later:
        result = task(name)
        assert result.returncode == 0, (name, result.stdout + result.stderr)
        assert f"NOTHING TO PROCESS: batch {BATCH_DATE} is empty and no earlier batch has been loaded." in result.stdout
    assert sorted(path.name for path in (project_root / "data").iterdir()) == ["raw"], "no lake table is created"

    # It is only for that case. (In this empty project a task that does run
    # fails on its missing job file, which shows it was not skipped.)
    for name in ("bronze_ingestion", "bronze_quality_checks"):
        assert "NOTHING TO PROCESS" not in task(name).stdout, "Bronze always ingests and checks the batch"
    # Part J: an empty batch that nobody declared empty is not skipped; the
    # Bronze checks fail it, so in a pipeline run no later task starts.
    (raw / "_EMPTY_BATCH").unlink()
    undeclared = task("silver_transformations")
    assert undeclared.returncode == 1 and "NOTHING TO PROCESS" not in undeclared.stdout
    (raw / "_EMPTY_BATCH").write_text("", encoding="utf-8")
    (raw / "orders.csv").write_text("id,value\n1,2\n", encoding="utf-8")
    with_rows = task("silver_transformations")
    assert with_rows.returncode == 1 and "NOTHING TO PROCESS" not in with_rows.stdout, "a batch with rows is processed"
    (raw / "orders.csv").write_text("id,value\n", encoding="utf-8")
    (raw / "web_events.json").write_text('{"event_id": "e1"}\n', encoding="utf-8")
    assert "NOTHING TO PROCESS" not in task("silver_transformations").stdout, "a batch with events is processed"
    (raw / "web_events.json").write_text("", encoding="utf-8")
    earlier = project_root / "data" / "bronze" / "bronze_orders" / "ingestion_date=2026-05-18"
    earlier.mkdir(parents=True)
    (earlier / "part-0.parquet").write_bytes(b"PAR1")
    after_data = task("silver_transformations")
    assert after_data.returncode == 1 and "NOTHING TO PROCESS" not in after_data.stdout, \
        "an empty batch after an earlier one is processed normally"


def test_the_nothing_to_process_rule_names_every_task_after_the_bronze_checks(load_job, monkeypatch):
    from test_dag_structure import DAG_FILE, stub_airflow

    for name in ("PROJECT_ROOT", "PROJECT_PYTHON", "DBT_BIN"):
        monkeypatch.delenv(name, raising=False)
    recorder = stub_airflow(monkeypatch)
    load_job(DAG_FILE)
    runner = (CODE_ROOT / "scripts" / "run_airflow_task.sh").read_text(encoding="utf-8")
    later = re.search(r'TASKS_AFTER_BRONZE="([^"]*)"', runner).group(1).replace("\\\n", " ").split()
    assert ["generate_data", "bronze_ingestion", "bronze_quality_checks", *later] == list(recorder.tasks)


# Part I: the database tests run automatically, and in CI they cannot be skipped.

def test_a_missing_sandbox_database_is_a_failure_when_the_database_tests_are_required(monkeypatch):
    import socket

    import conftest

    monkeypatch.delenv("P2_F_SANDBOX", raising=False)
    monkeypatch.delenv("P2_REQUIRE_DB", raising=False)
    with pytest.raises(pytest.skip.Exception, match="requires the disposable PostgreSQL sandbox on localhost:5546"):
        conftest.require_sandbox_database()

    # P2_REQUIRE_DB=1 (CI): no database is a failure, with or without P2_F_SANDBOX.
    with socket.socket() as free:
        free.bind(("127.0.0.1", 0))
        closed_port = free.getsockname()[1]
    monkeypatch.setattr(conftest, "SANDBOX_DATABASE", ("127.0.0.1", closed_port))
    for offered in (None, "1"):
        monkeypatch.setenv("P2_REQUIRE_DB", "1")
        monkeypatch.delenv("P2_F_SANDBOX", raising=False) if offered is None else monkeypatch.setenv("P2_F_SANDBOX", offered)
        with pytest.raises(pytest.fail.Exception, match="P2_REQUIRE_DB=1 but no PostgreSQL sandbox answers"):
            conftest.require_sandbox_database()

    # A database that answers: the test runs.
    with socket.socket() as listening:
        listening.bind(("127.0.0.1", 0))
        listening.listen(1)
        monkeypatch.setattr(conftest, "SANDBOX_DATABASE", listening.getsockname())
        assert conftest.require_sandbox_database() is None


def test_every_database_test_uses_the_one_gate_and_ci_runs_them_in_both_time_zones():
    tests = CODE_ROOT / "tests"
    own_gates = [path.name for path in tests.rglob("*.py")
                 if path.name != "conftest.py" and re.search(r"environ\.get\(.P2_F_SANDBOX.\)", path.read_text(encoding="utf-8"))]
    assert own_gates == [], "a private skip rule would not fail in CI"
    gated = [path.name for path in (tests / "unit").glob("test_*.py") if "require_sandbox_database()" in path.read_text(encoding="utf-8")]
    assert sorted(gated) == ["test_generator_sessions_budgets.py", "test_order_lifecycle.py", "test_part_f.py",
                             "test_part_g.py", "test_part_h.py", "test_part_h2.py"]

    workflow = (CODE_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    unit_job = workflow[workflow.index("\n  unit:\n"):workflow.index("\n  integration:\n")]
    assert re.search(r"services:\n\s+sandbox:\n\s+image: postgres:16\.14\n", unit_job)
    assert "- 5546:5432" in unit_job and "--name p2-part-f-sandbox" in unit_job and "--health-cmd" in unit_job
    assert re.search(r'\n    env:\n      P2_F_SANDBOX: "1"\n      P2_REQUIRE_DB: "1"\n', unit_job), "for every step of the job"
    runs = re.findall(r"TZ: (\S+)\n\s+run: \|\n\s+python -m pytest tests/unit -p no:cacheprovider -rs\n", unit_job)
    assert runs == ["UTC", "Asia/Kolkata"]


# --- 5: the read-only role ---------------------------------------------------------

def test_the_reader_role_is_defined_read_only():
    schemas = (CODE_ROOT / "sql" / "warehouse" / "create_schemas.sql").read_text(encoding="utf-8")
    assert "CREATE ROLE p2_reader NOLOGIN;" in schemas
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA warehouse, marts, audit TO p2_reader;" in schemas
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse, marts, audit GRANT SELECT ON TABLES TO p2_reader;" in schemas
    assert not re.search(r"GRANT[^;]*staging[^;]*TO p2_reader", schemas)
    dbt = (CODE_ROOT / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert 'select: ["p2_reader"]' in dbt and "grant usage on schema {{ schema }} to p2_reader" in dbt
    readme = (CODE_ROOT / "README.md").read_text(encoding="utf-8")
    assert "p2_reader" in readme and "P2_READER_PASSWORD" in readme


def test_the_dbt_counts_in_the_docs_are_the_projects_and_explain_the_grant_hook():
    """Part I: README said PASS=21 / PASS=65 beside '21 models and 74 tests'."""
    dbt = CODE_ROOT / "dbt"
    models = len(list((dbt / "models").rglob("*.sql")))

    def list_items(path, key_pattern) -> int:
        """Items of every YAML list under a key matching key_pattern (read by indentation; no YAML library in CI)."""
        count, key_indent, item_indent = 0, None, None
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip())
            if key_indent is not None and indent <= key_indent and not (indent == key_indent and line.lstrip().startswith("- ")):
                key_indent = item_indent = None
            if key_indent is not None and line.lstrip().startswith("- "):
                item_indent = indent if item_indent is None else item_indent
                count += indent == item_indent
            if re.fullmatch(rf"\s*(- )?({key_pattern}):", line.rstrip()):
                key_indent, item_indent = indent + (2 if line.lstrip().startswith("- ") else 0), None
        return count

    in_yaml = sum(list_items(path, "tests|data_tests") for path in (dbt / "models").rglob("*.yml"))
    singular = len(list((dbt / "tests").glob("*.sql")))
    tests = in_yaml + singular
    hooks = list_items(dbt / "dbt_project.yml", "on-run-end")
    assert (models, tests, hooks) == (24, 82, 1)

    readme = (CODE_ROOT / "README.md").read_text(encoding="utf-8")
    state = (CODE_ROOT / "docs" / "warehouse" / "airflow_working_state.md").read_text(encoding="utf-8")
    assert f"{models} dbt models and {tests} dbt tests" in readme and f"{models} models and {tests} tests" in readme
    # dbt counts the on-run-end hook as a result of each command.
    assert f"# {models} models: PASS={models + hooks}" in readme and f"# {tests} tests:  PASS={tests + hooks}" in readme
    assert f"({in_yaml} in `schema.yml`, {singular} in `dbt/tests/`)" in readme
    assert "`on-run-end` hook" in readme and "grants the read-only role `p2_reader` USAGE on the dbt schemas" in readme
    assert f"{models} dbt models and {tests} dbt tests" in state
    assert f"`PASS={models + hooks}` for `dbt run` and `PASS={tests + hooks}` for `dbt test`" in state and "`on-run-end` hook" in state
    # No other current PASS= figure anywhere in README or docs.
    stated = set()
    for path in [CODE_ROOT / "README.md", *(CODE_ROOT / "docs").rglob("*.md")]:
        stated |= set(re.findall(r"PASS=(\d+)", path.read_text(encoding="utf-8")))
    assert stated == {str(models + hooks), str(tests + hooks)}


def test_the_reader_role_can_read_and_cannot_write(pg, tmp_path):
    import psycopg
    result = subprocess.run(
        ["bash", str(CODE_ROOT / "scripts" / "run_warehouse_sql.sh"), str(CODE_ROOT / "sql" / "warehouse" / "create_schemas.sql")],
        cwd=tmp_path, env={**os.environ, "PGCONTAINER": "p2-part-f-sandbox", "RUN_MODE": "docker",
                           "P2_READER_PASSWORD": "reader-test-password"}, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "reader-test-password" not in result.stdout + result.stderr
    pg.execute(MINIMAL_STAGING_SQL)
    build_warehouse(pg)   # tables created after the grants: default privileges apply

    with psycopg.connect(host="127.0.0.1", port=5546, user="p2_reader", password="reader-test-password",
                         dbname="marketing_analytics", autocommit=True) as reader:
        # The unknown member and the invalid-reference member.
        assert reader.execute("SELECT customer_sk FROM warehouse.dim_customer ORDER BY 1").fetchall() == [(-1,), (0,)]
        assert reader.execute("SELECT count(*) FROM warehouse.fact_web_events").fetchone()[0] == 0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM staging.stg_gold_fact_orders_scd2")
        with pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)):
            reader.execute("DELETE FROM warehouse.dim_customer")
        with pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)):
            reader.execute("CREATE TABLE warehouse.mine (x int)")
