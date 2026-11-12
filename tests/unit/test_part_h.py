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

from conftest import CODE_ROOT, MINIMAL_STAGING_SQL

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
    if os.environ.get("P2_F_SANDBOX") != "1":
        pytest.skip("requires the disposable PostgreSQL sandbox on localhost:5546")
    import psycopg
    with psycopg.connect(host="127.0.0.1", port=5546, user="project2", password="project2",
                         dbname="marketing_analytics", autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS marts CASCADE; DROP SCHEMA IF EXISTS warehouse CASCADE; "
                     "DROP SCHEMA IF EXISTS audit CASCADE; DROP SCHEMA IF EXISTS staging CASCADE; "
                     "CREATE SCHEMA staging; SELECT set_config('p2.run_id', 'part-h-test', false)")
        yield conn


STAGING_ROWS = """
CREATE TABLE staging.stg_gold_dim_customers_scd2 (customer_id int, customer_name text, effective_from timestamp,
    effective_to timestamp, is_current boolean, source_updated_at timestamp);
INSERT INTO staging.stg_gold_dim_customers_scd2 VALUES
    (7, 'Asha', '1900-01-01', '2026-06-01', false, '2026-02-10 10:06:05'),
    (7, 'Asha', '2026-06-01', '9999-12-31', true, '2026-06-01 00:00:00');
CREATE TABLE staging.stg_gold_dim_products_scd2 (product_id int, product_name text, category text,
    effective_from timestamp, effective_to timestamp, is_current boolean, source_updated_at timestamp);
INSERT INTO staging.stg_gold_dim_products_scd2 VALUES (4, 'Speaker', 'Audio', '1900-01-01', '9999-12-31', true, '2026-02-01');
CREATE TABLE staging.stg_gold_dim_campaigns_scd2 (campaign_id int, campaign_name text, channel text,
    effective_from timestamp, effective_to timestamp, is_current boolean, source_updated_at timestamp);
INSERT INTO staging.stg_gold_dim_campaigns_scd2 VALUES (5, 'Summer', 'facebook', '1900-01-01', '9999-12-31', true, '2026-03-01');
CREATE TABLE staging.stg_gold_fact_orders_scd2 (order_id int, customer_id int, campaign_id int,
    order_timestamp timestamp, cart_value numeric, payment_method text, ingestion_date date);
INSERT INTO staging.stg_gold_fact_orders_scd2 VALUES
    (1, 7, 5, '2026-05-10 10:00', 200.00, 'UPI', '2026-05-18'), (2, 7, NULL, '2026-06-10 10:00', 100.00, 'COD', '2026-06-18');
CREATE TABLE staging.stg_gold_fact_order_items_scd2 (order_item_id int, order_id int, product_id int, quantity int,
    original_price numeric, discounted_price numeric, line_amount numeric, ingestion_date date);
INSERT INTO staging.stg_gold_fact_order_items_scd2 VALUES
    (10, 1, 4, 2, 120.00, 100.00, 200.00, '2026-05-18'), (20, 2, 4, 1, 120.00, 100.00, 100.00, '2026-06-18');
CREATE TABLE staging.stg_gold_fact_campaign_spend_scd2 (spend_id int, campaign_id int, traffic_source text,
    spend_date date, spend_amount numeric, impressions bigint, clicks bigint, ingestion_date date);
INSERT INTO staging.stg_gold_fact_campaign_spend_scd2 VALUES (1, 5, 'facebook', '2026-05-09', 50.00, 1000, 10, '2026-05-18');
CREATE TABLE staging.stg_gold_fact_web_events (event_id text, session_id text, customer_id int, product_id int,
    campaign_id int, event_timestamp timestamp, event_type text, traffic_source text, cart_value numeric,
    ingestion_date date);
INSERT INTO staging.stg_gold_fact_web_events VALUES
    ('e1', 's1', 7, 4, 5, '2026-05-10 09:55', 'purchase', 'facebook', 200.00, '2026-05-18'),
    ('e2', 's2', 7, NULL, NULL, '2026-06-10 09:00', 'page_view', 'organic', 0, '2026-06-18');
"""


def build_warehouse(pg):
    pg.execute(WAREHOUSE_SQL.read_text(encoding="utf-8"))


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
        "SELECT count(*) FROM warehouse.dim_customer WHERE customer_sk <> 0 AND customer_sk <> "
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
        assert "source_updated_at TIMESTAMP," in ddl and "record_hash TEXT," in ddl, table
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
                      ).fetchall() == [("5", "2026-05-18")]
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


def test_an_empty_batch_passes_the_bronze_checks(spark, load_job, project_root):
    qc = load_job("spark-batch/bronze_quality_checks.py", BATCH_DATE=BATCH_DATE)
    raw = project_root / "orders.csv"
    raw.write_text("order_id,user_id,cart_value\n", encoding="utf-8")       # header only
    events = project_root / "web_events.json"
    events.write_text("\n", encoding="utf-8")                                # no event
    assert qc.count_raw_file_lines(raw, has_header=True) == 0
    assert qc.count_raw_file_lines(events, has_header=False) == 0

    session = qc.create_spark_session()
    result = qc.validate_table(session, "orders", raw, project_root / "data" / "bronze" / "bronze_orders", True)
    assert (result["status"], result["raw_row_count"], result["bronze_row_count"], result["failure_reason"]) == (
        "PASSED", 0, 0, "")

    # Rows in Bronze for a batch whose raw file is empty are still an error.
    bronze = project_root / "data" / "bronze" / "bronze_orders"
    session.createDataFrame([("1", BATCH_DATE)], "order_id string, day string") \
        .withColumn("ingestion_date", F.to_date("day")).drop("day") \
        .write.mode("overwrite").partitionBy("ingestion_date").parquet(str(bronze))
    assert qc.validate_table(session, "orders", raw, bronze, True)["status"] == "FAILED"


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
    assert len(recorder.tasks) == 23
    for task, kwargs in recorder.tasks.items():
        flag = "--last " if task == "power_bi_export" else ""
        assert re.match(rf'"\$PROJECT_PYTHON" "\S+/scripts/pipeline_lock\.py" {flag}{task} -- bash -c ',
                        kwargs["bash_command"]), task


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
        assert reader.execute("SELECT count(*) FROM warehouse.dim_customer").fetchone()[0] == 1
        assert reader.execute("SELECT count(*) FROM audit.fact_web_events_rejects").fetchone()[0] == 0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM staging.stg_gold_fact_orders_scd2")
        with pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)):
            reader.execute("DELETE FROM warehouse.dim_customer")
        with pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)):
            reader.execute("CREATE TABLE warehouse.mine (x int)")
