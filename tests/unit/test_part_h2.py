"""Part H2 (p2-data-v2): a rebuild nobody notices, and timestamps that are instants.

1  A rebuild dropped the dbt schemas first, then dropped and recreated every
   warehouse table and mart, each step in its own task: for minutes a reader
   got "relation does not exist", and when dbt or the export failed the dbt
   views stayed gone. The warehouse is now built as a new generation of its
   schemas in ONE transaction (sql/warehouse/begin_generation.sql): readers
   see the old warehouse until the commit and the new one after it, the staging
   loader fills staging_next, and views outside the generation (the dbt views)
   keep reading the retired one until they are replaced.
   A build that fails a check (reconciliation, SCD2 validation, mart
   validation) is never published: the gate runs before the commit, the build
   is rolled back, and the failing checks go to the task log and to
   logs/warehouse/failed_build_<timestamp>.json.
2  Warehouse and mart timestamps were TIMESTAMP (naive UTC): a consumer in
   another time zone read them as local time. They are TIMESTAMPTZ; business
   dates stay Asia/Kolkata dates.

The PostgreSQL cases need the disposable sandbox on localhost:5546 (P2_F_SANDBOX=1).
"""
from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import re
import stat
import subprocess
import sys
import threading

import pytest

from conftest import CODE_ROOT, begin_build, build_step, require_sandbox_database, reset_sandbox
from test_part_h import STAGING_ROWS

WAREHOUSE_DIR = CODE_ROOT / "sql" / "warehouse"
RUNNER = CODE_ROOT / "scripts" / "run_warehouse_sql.sh"
BUILD_FILES = ["begin_generation", "create_warehouse_tables", "scd2_validation_queries", "create_marts",
               "reconciliation_report"]
SANDBOX = "p2-part-f-sandbox"
READER_PASSWORD = "reader-test-password"
GENERATION_SCHEMAS = "^(staging|warehouse|marts|audit)(_next|_retired_[0-9]+)?$|^dbt_"


def sql_code(path) -> str:
    return "\n".join(line.split("--", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())


INJECTED_FAILURE = CODE_ROOT / "tests" / "sql" / "inject_reconciliation_fail.sql"


def run_sql_files(tmp_path, *names, **env):
    """The runner on warehouse SQL files, by name, or on any other file, by path."""
    return subprocess.run(
        ["bash", str(RUNNER), *[str(name if os.path.isabs(str(name)) else WAREHOUSE_DIR / f"{name}.sql") for name in names]],
        cwd=tmp_path, capture_output=True, text=True,
        env={**os.environ, "PGCONTAINER": SANDBOX, "RUN_MODE": "docker", "P2_RUN_ID": "part-h2-test",
             "P2_READER_PASSWORD": READER_PASSWORD, **env})


def build(tmp_path):
    result = run_sql_files(tmp_path, *BUILD_FILES)
    assert result.returncode == 0, (result.stdout + result.stderr)[-3000:]
    return result


@pytest.fixture
def pg(tmp_path):
    require_sandbox_database()
    import psycopg
    with psycopg.connect(host="127.0.0.1", port=5546, user="project2", password="project2",
                         dbname="marketing_analytics", autocommit=True) as conn:
        reset_sandbox(conn)
        conn.execute("SELECT set_config('p2.run_id', 'part-h2-test', false)")
        result = run_sql_files(tmp_path, "create_schemas")
        assert result.returncode == 0, result.stdout + result.stderr
        yield conn


@contextlib.contextmanager
def reading(relations, expected):
    """p2_reader, over TCP, counts the rows of the relations until the block ends."""
    import psycopg

    stop = threading.Event()
    seen = {"rounds": 0, "problems": []}

    def read():
        with psycopg.connect(host="127.0.0.1", port=5546, user="p2_reader", password=READER_PASSWORD,
                             dbname="marketing_analytics", autocommit=True) as reader:
            while not stop.is_set():
                for relation in relations:
                    try:
                        count = reader.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
                        if count != expected[relation]:
                            seen["problems"].append(f"{relation}: {count} rows, expected {expected[relation]}")
                    except psycopg.Error as exc:
                        seen["problems"].append(f"{relation}: {exc}")
                seen["rounds"] += 1

    thread = threading.Thread(target=read)
    thread.start()
    try:
        yield seen
    finally:
        stop.set()
        thread.join()


def schemas(pg, pattern: str) -> list[str]:
    return [row[0] for row in pg.execute("SELECT nspname FROM pg_namespace WHERE nspname ~ %s ORDER BY 1", (pattern,))]


# --- 1: the build is one transaction over a new generation ----------------------

def test_the_dag_builds_the_warehouse_in_one_task_and_one_transaction(load_job, monkeypatch):
    from test_dag_structure import DAG_FILE, stub_airflow

    for name in ("PROJECT_ROOT", "PROJECT_PYTHON", "DBT_BIN"):
        monkeypatch.delenv(name, raising=False)
    recorder = stub_airflow(monkeypatch)
    load_job(DAG_FILE)
    assert not {"build_warehouse_tables", "run_scd2_validation", "build_reporting_marts",
                "run_warehouse_reconciliation"} & set(recorder.tasks), "the build is split over several tasks again"
    assert "scripts/run_airflow_task.sh build_warehouse" in recorder.tasks["build_warehouse"]["bash_command"]
    runner = re.sub(r"\\\n\s*", "", (CODE_ROOT / "scripts" / "run_airflow_task.sh").read_text(encoding="utf-8"))
    files = " ".join(f"sql/warehouse/{name}.sql" for name in BUILD_FILES)
    task = re.search(r"\n  build_warehouse\)\n(.*?)\n    ;;", runner, re.S).group(1)
    assert f"bash scripts/run_warehouse_sql.sh {files}" in task, "the task must run all five files in one call"


def test_several_sql_files_run_as_one_transaction(tmp_path):
    """The runner's input for psql, captured by a stand-in psql."""
    captured = tmp_path / "captured.sql"
    fake = tmp_path / "bin" / "psql"
    fake.parent.mkdir()
    fake.write_text('#!/usr/bin/env bash\nwhile [ $# -gt 0 ]; do\n  if [ "$1" = "--file" ]; then cat "$2" > "$CAPTURED"; fi\n'
                    "  shift\ndone\n", encoding="utf-8")
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    result = subprocess.run(
        ["bash", str(RUNNER), *[str(WAREHOUSE_DIR / f"{name}.sql") for name in BUILD_FILES]],
        cwd=tmp_path, capture_output=True, text=True,
        env={**os.environ, "RUN_MODE": "host", "PATH": f"{fake.parent}:{os.environ['PATH']}", "CAPTURED": str(captured)})
    assert result.returncode == 0, result.stdout + result.stderr
    lines = captured.read_text(encoding="utf-8").splitlines()

    begins = [number for number, line in enumerate(lines) if line.strip() == "BEGIN;"]
    commits = [number for number, line in enumerate(lines) if line.strip() == "COMMIT;"]
    assert begins == [1], "one BEGIN, straight after the run id"
    assert len(commits) == 1, "one COMMIT: nothing is visible before every file has succeeded"
    position = {name: next(n for n, line in enumerate(lines) if line == f"-- ---- {WAREHOUSE_DIR / (name + '.sql')}")
                for name in BUILD_FILES}
    assert sorted(position, key=position.get) == BUILD_FILES
    gate = next(n for n, line in enumerate(lines) if "Quality gate failed" in line)
    assert position["reconciliation_report"] < gate < commits[0], \
        "the gate runs before the commit: a build that fails a check is never published"


@pytest.mark.parametrize("name", BUILD_FILES[1:])
def test_each_build_file_has_one_top_level_begin_and_commit(name):
    # The runner leaves out exactly these lines; a second one would end the
    # build transaction early.
    lines = (WAREHOUSE_DIR / f"{name}.sql").read_text(encoding="utf-8").splitlines()
    assert [line.strip() for line in lines].count("BEGIN;") == 1
    assert [line.strip() for line in lines].count("COMMIT;") == 1
    assert lines.count("BEGIN;") == 1 and lines.count("COMMIT;") == 1, "they must start in the first column"


def test_nothing_a_reader_uses_is_dropped_before_the_build():
    code = sql_code(WAREHOUSE_DIR / "create_schemas.sql")
    assert not re.search(r"\bDROP\b", code), "create_schemas.sql must not drop anything (it dropped the dbt schemas)"
    generation = sql_code(WAREHOUSE_DIR / "begin_generation.sql")
    assert "ALTER SCHEMA %I RENAME TO %I" in generation and "ALTER SCHEMA staging_next RENAME TO staging" in generation
    assert not re.search(r"\b(BEGIN|COMMIT);", generation), "the build supplies the transaction"
    loader = (CODE_ROOT / "scripts" / "load_gold_to_postgres_staging.py").read_text(encoding="utf-8")
    assert "schema=load_schema," in loader and 'LOAD_SCHEMA_SUFFIX = "_next"' in loader


def test_a_reader_never_notices_a_rebuild_and_outside_views_survive(pg, tmp_path):
    import psycopg

    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS)
    build(tmp_path)
    # A view outside the generation, as dbt makes them.
    pg.execute("CREATE SCHEMA dbt_marts; GRANT USAGE ON SCHEMA dbt_marts TO p2_reader; "
               "CREATE VIEW dbt_marts.mart_revenue_daily AS SELECT * FROM marts.mart_revenue_daily; "
               "GRANT SELECT ON dbt_marts.mart_revenue_daily TO p2_reader")
    first_generation = pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0]

    relations = ["warehouse.fact_orders", "warehouse.dim_customer", "audit.reconciliation_report",
                 "dbt_marts.mart_revenue_daily"]
    relations += [f"marts.{name}" for (name,) in pg.execute(
        "SELECT table_name FROM information_schema.views WHERE table_schema = 'marts' ORDER BY 1").fetchall()]
    expected = {relation: pg.execute(f"SELECT count(*) FROM {relation}").fetchone()[0] for relation in relations}
    relations = [relation for relation in relations if expected[relation] > 0]
    assert len(relations) >= 6, expected

    with reading(relations, expected) as seen:
        build(tmp_path)
        build(tmp_path)
        # A build that fails changes nothing: the transaction is rolled back.
        pg.execute("ALTER TABLE staging.stg_gold_fact_web_events RENAME TO hidden")
        live = pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0]
        failed = run_sql_files(tmp_path, *BUILD_FILES)
        pg.execute("ALTER TABLE staging.hidden RENAME TO stg_gold_fact_web_events")
        assert failed.returncode != 0
        assert pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0] == live
    assert seen["rounds"] > 5, seen
    assert seen["problems"] == [], seen["problems"][:5]
    assert pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0] != first_generation

    # The outside view still answers, from the generation it was created on,
    # which is therefore kept; the one retired after it is kept until the next build.
    assert pg.execute("SELECT count(*) FROM dbt_marts.mart_revenue_daily").fetchone()[0] == expected["dbt_marts.mart_revenue_daily"]
    source = pg.execute("SELECT DISTINCT n.nspname FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid "
                        "JOIN pg_class c ON c.oid = d.refobjid JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE r.ev_class = 'dbt_marts.mart_revenue_daily'::regclass AND c.relname = 'mart_revenue_daily' "
                        "AND n.nspname <> 'dbt_marts'").fetchall()
    assert len(source) == 1 and re.fullmatch(r"marts_retired_\d+", source[0][0]), source
    assert len(schemas(pg, r"^warehouse_retired_\d+$")) == 2

    # Once the view is replaced (dbt run does this), the next build drops what nothing reads.
    pg.execute("CREATE OR REPLACE VIEW dbt_marts.mart_revenue_daily AS SELECT * FROM marts.mart_revenue_daily")
    build(tmp_path)
    assert len(schemas(pg, r"^warehouse_retired_\d+$")) == 1
    assert len(schemas(pg, r"_retired_\d+$")) == 3, "warehouse, marts and audit of the generation just retired"
    with psycopg.connect(host="127.0.0.1", port=5546, user="p2_reader", password=READER_PASSWORD,
                         dbname="marketing_analytics", autocommit=True) as reader:
        assert reader.execute("SELECT count(*) FROM marts.mart_revenue_daily").fetchone()[0] > 0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(f"SELECT count(*) FROM {schemas(pg, r'^warehouse_retired_')[0]}.fact_orders")


def test_a_build_that_fails_a_check_is_never_published(pg, tmp_path):
    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS)
    build(tmp_path)

    relations = ["warehouse.fact_orders", "warehouse.fact_order_items", "warehouse.dim_customer",
                 "audit.reconciliation_report", "marts.mart_revenue_daily", "marts.mart_customer_360"]

    def state():
        out = {"schemas": schemas(pg, GENERATION_SCHEMAS),
               "generation": pg.execute("SELECT to_regnamespace('warehouse')::oid, to_regnamespace('marts')::oid, "
                                        "to_regnamespace('audit')::oid").fetchone()}
        for relation in relations:
            out[relation] = pg.execute(
                f"SELECT count(*), md5(coalesce(string_agg(t::TEXT, ',' ORDER BY t::TEXT), '')) FROM {relation} t").fetchone()
        return out

    before = state()
    expected = {relation: before[relation][0] for relation in relations}
    assert all(expected.values()), expected

    # The same build, with one loaded order removed before the reconciliation.
    files = [*BUILD_FILES[:-1], INJECTED_FAILURE, BUILD_FILES[-1]]
    with reading(relations, expected) as seen:
        failed = run_sql_files(tmp_path, *files, P2_RUN_ID="part-h2-failing-build")
        while seen["rounds"] < 20:   # and it keeps reading after the failure
            pass
    output = failed.stdout + failed.stderr

    assert failed.returncode != 0, "the task must fail"
    assert "Quality gate failed" in output and "Warehouse SQL completed successfully" not in output
    assert state() == before, "the failed build changed what is published"
    assert seen["problems"] == [], seen["problems"][:5]

    reports = sorted((tmp_path / "logs" / "warehouse").glob("failed_build_*.json"))
    assert len(reports) == 1, output[-2000:]
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["run_id"] == "part-h2-failing-build" and "rolled back" in report["outcome"]
    failing = {check["check_name"]: check for check in report["failing_checks"]}
    assert report["failing_check_count"] == len(failing) >= 1
    orders = failing["Gold order count equals warehouse fact_orders count"]
    assert orders["report"] == "audit.reconciliation_report" and orders["status"] == "FAIL"
    assert (float(orders["expected"]), float(orders["actual"])) == (2.0, 1.0) and orders["details"]
    assert f"FAILED CHECK audit.reconciliation_report | {orders['check_name']} | expected" in output
    assert str(reports[0].relative_to(tmp_path)) in output, "the task log says where the report is"

    # The same files without the injection build and publish normally.
    build(tmp_path)
    assert state()["generation"] != before["generation"]


def test_the_generation_file_refuses_to_run_outside_the_build_transaction(pg, tmp_path):
    live = pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0]
    result = run_sql_files(tmp_path, "begin_generation")
    assert result.returncode != 0 and "can only be used in transaction blocks" in result.stdout + result.stderr
    assert pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0] == live
    assert schemas(pg, r"_retired_\d+$") == []


def test_the_staging_loader_fills_staging_next_and_the_build_publishes_it(pg, tmp_path):
    import pandas as pd

    # The live staging table, with a view on it as dbt creates them.
    pg.execute("CREATE TABLE staging.stg_gold_fact_orders_scd2 (order_id int); "
               "INSERT INTO staging.stg_gold_fact_orders_scd2 VALUES (1); "
               "CREATE SCHEMA dbt_staging; "
               "CREATE VIEW dbt_staging.stg_orders AS SELECT * FROM staging.stg_gold_fact_orders_scd2")
    gold = tmp_path / "data" / "gold" / "fact_orders_scd2"
    gold.mkdir(parents=True)
    pd.DataFrame({"order_id": [1, 2, 3]}).to_parquet(gold / "part-0.parquet")

    environment = {key: value for key, value in os.environ.items() if key not in ("DATABASE_URL", "PGSCHEMA")}
    result = subprocess.run(
        [sys.executable, str(CODE_ROOT / "scripts" / "load_gold_to_postgres_staging.py"), "--project-root", str(tmp_path)],
        cwd=tmp_path, capture_output=True, text=True,
        env={**environment, "PGHOST": "127.0.0.1", "PGPORT": "5546", "PGDATABASE": "marketing_analytics",
             "PGUSER": "project2", "PGPASSWORD": "project2"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert pg.execute("SELECT count(*) FROM staging_next.stg_gold_fact_orders_scd2").fetchone()[0] == 3
    assert pg.execute("SELECT count(*) FROM staging.stg_gold_fact_orders_scd2").fetchone()[0] == 1, "live staging untouched"

    # The generation step publishes it; the view keeps reading the table it was made on.
    pg.execute("BEGIN;\n" + (WAREHOUSE_DIR / "begin_generation.sql").read_text(encoding="utf-8") + "\nCOMMIT;")
    assert pg.execute("SELECT count(*) FROM staging.stg_gold_fact_orders_scd2").fetchone()[0] == 3
    assert pg.execute("SELECT count(*) FROM dbt_staging.stg_orders").fetchone()[0] == 1
    assert schemas(pg, "^staging_next$") == [] and len(schemas(pg, r"^staging_retired_\d+$")) == 1
    # The loader's history moved to the new audit schema.
    assert pg.execute("SELECT count(*) FROM audit.gold_to_staging_load_audit WHERE status = 'LOADED'").fetchone()[0] == 1
    assert pg.execute("SELECT count(*) FROM audit.v_gold_to_staging_latest_counts").fetchone()[0] >= 1


# Part I: the build files run only inside the build transaction ------------------

@pytest.mark.parametrize("name", BUILD_FILES[1:])
def test_each_build_file_checks_for_the_build_transaction_before_anything_else(name):
    code = sql_code(WAREHOUSE_DIR / f"{name}.sql")
    guard = code.index("IF current_setting('p2.build_txid', true) IS DISTINCT FROM txid_current()::TEXT THEN")
    first_change = re.search(r"^(DROP|CREATE|INSERT|UPDATE|DELETE|ALTER|TRUNCATE)\b", code, re.M).start()
    assert code.index("\nBEGIN;") < guard < first_change, "the check must be the first statement of the transaction"
    assert f"RAISE EXCEPTION '{name}.sql runs only inside the warehouse build transaction" in code
    assert "PERFORM set_config('p2.build_txid', txid_current()::TEXT, true);" in sql_code(WAREHOUSE_DIR / "begin_generation.sql")


def published(pg):
    """What a reader can see: the live schemas, every table in them and its row count."""
    tables = pg.execute(
        "SELECT c.oid, n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('warehouse', 'marts', 'audit') AND c.relkind IN ('r', 'v') ORDER BY 2").fetchall()
    return {"generation": pg.execute("SELECT to_regnamespace('warehouse')::oid, to_regnamespace('marts')::oid, "
                                     "to_regnamespace('audit')::oid").fetchone(),
            "tables": tables,
            "rows": {name: pg.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for _, name in tables}}


@pytest.mark.parametrize("name", BUILD_FILES[1:])
def test_a_build_file_refuses_to_run_outside_the_build_transaction(pg, tmp_path, name):
    import psycopg

    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS)
    build(tmp_path)
    before = published(pg)
    assert before["rows"]["warehouse.fact_orders"] == 2

    # Through the runner, on its own (it used to drop and rebuild the live tables).
    alone = run_sql_files(tmp_path, name)
    assert alone.returncode != 0, alone.stdout[-1500:]
    assert f"{name}.sql runs only inside the warehouse build transaction started by sql/warehouse/begin_generation.sql" \
        in alone.stdout + alone.stderr
    assert published(pg) == before, "the file changed the live warehouse"

    # Pasted into a session, even one that is inside a transaction of its own.
    text = (WAREHOUSE_DIR / f"{name}.sql").read_text(encoding="utf-8")
    for prefix in ("", "BEGIN; SELECT set_config('p2.build_txid', '1', true);"):
        with pytest.raises(psycopg.errors.RaiseException, match="runs only inside the warehouse build transaction"):
            pg.execute(prefix + text)
        pg.execute("ROLLBACK")
    assert published(pg) == before


# Part I: a consumer's view on the warehouse can never go silently stale ----------

def test_the_pipeline_schema_list_of_the_view_check_matches_dbt():
    generation = (WAREHOUSE_DIR / "begin_generation.sql").read_text(encoding="utf-8")
    pattern = re.search(r"pipeline_schema CONSTANT TEXT := '([^']*)';", generation).group(1)
    project = (CODE_ROOT / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    profile = (CODE_ROOT / "dbt" / "profiles.yml.example").read_text(encoding="utf-8")
    dbt_schemas = re.findall(r"\+schema: (\w+)", project) + re.findall(r"^\s+schema: (\w+)", profile, re.M)
    assert sorted(dbt_schemas) == ["dbt", "dbt_audit", "dbt_marts", "dbt_staging", "dbt_warehouse"]
    for schema in [*dbt_schemas, "staging", "warehouse", "marts", "audit", "staging_next", "marts_retired_812", "pg_temp_3"]:
        assert re.search(pattern, schema), schema
    for schema in ("public", "consumer", "dbt_other", "reporting", "warehouse_copy"):
        assert not re.search(pattern, schema), schema


def test_a_build_refuses_to_leave_a_consumer_view_on_the_old_warehouse(pg, tmp_path):
    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS)
    build(tmp_path)
    # The pipeline's own views (dbt) are replaced by dbt run: they do not stop a build.
    pg.execute("CREATE SCHEMA dbt_marts; CREATE VIEW dbt_marts.mart_revenue_daily AS SELECT * FROM marts.mart_revenue_daily")
    build(tmp_path)

    # Views that somebody else made on the warehouse: nothing would ever replace them.
    pg.execute("DROP SCHEMA IF EXISTS consumer CASCADE; CREATE SCHEMA consumer; "
               "CREATE VIEW consumer.my_revenue AS SELECT revenue_date, net_revenue FROM marts.mart_revenue_daily; "
               "CREATE MATERIALIZED VIEW consumer.order_days AS "
               "SELECT order_id, warehouse.business_date(order_timestamp) AS day FROM warehouse.fact_orders; "
               "CREATE VIEW public.p2_test_on_a_dbt_view AS SELECT * FROM dbt_marts.mart_revenue_daily")
    try:
        before = published(pg)
        retired = schemas(pg, r"_retired_\d+$")
        refused = run_sql_files(tmp_path, *BUILD_FILES)
        output = refused.stdout + refused.stderr
        assert refused.returncode != 0, "the build left consumer views on the retired warehouse: silently stale"
        assert "Warehouse build refused: 2 view(s) outside the pipeline's schemas read the warehouse" in output
        assert "consumer.my_revenue (reads marts.mart_revenue_daily)" in output
        assert "consumer.order_days (reads warehouse.business_date(), warehouse.fact_orders)" in output
        assert "p2_test_on_a_dbt_view" not in output, "a view on a dbt view is replaced with it, by dbt"
        assert "Drop these views, run the build, then create them again" in output
        assert published(pg) == before and schemas(pg, r"_retired_\d+$") == retired, "a refused build changes nothing"
        assert pg.execute("SELECT count(*) FROM consumer.my_revenue").fetchone()[0] == before["rows"]["marts.mart_revenue_daily"]

        # Without them the same build runs.
        pg.execute("DROP SCHEMA consumer CASCADE")
        build(tmp_path)
        assert published(pg)["generation"] != before["generation"]
    finally:
        pg.execute("DROP SCHEMA IF EXISTS consumer CASCADE; DROP VIEW IF EXISTS public.p2_test_on_a_dbt_view")


# Part J: a fact is never dropped for a bad dimension reference -------------------

BAD_REFERENCES = """
INSERT INTO staging.stg_gold_fact_orders_scd2 VALUES
    (3, 99, 77, '2026-06-11 10:00', 300.00, 'UPI', '2026-06-18', 'completed', 54.00, 499.00, 853.00, '2026-06-11 10:00', 0),
    (4, 7, 5, '2026-06-12 10:00', 150.00, 'UPI', '2026-06-18', 'cancelled', 27.00, 499.00, 676.00, '2026-06-12 10:30', 0),
    (5, 7, 5, '2026-06-13 10:00', 50.00, 'UPI', '2026-06-18', 'returned', 9.00, 499.00, 558.00, '2026-06-15 10:00', 50.00);
INSERT INTO staging.stg_gold_fact_order_items_scd2 VALUES
    (30, 3, 44, 3, 120.00, 100.00, 300.00, '2026-06-18', 0), (40, 4, 4, 1, 150.00, 150.00, 150.00, '2026-06-18', 0),
    (50, 5, 4, 1, 50.00, 50.00, 50.00, '2026-06-18', 50.00);
INSERT INTO staging.stg_gold_fact_campaign_spend_scd2 VALUES
    (3, 77, 'google', '2026-06-10', 80.00, 2000, 40, '2026-06-18', 4, 900.00, 'India', 'Kerala', 80.00);
INSERT INTO staging.stg_gold_fact_web_events VALUES
    ('e3', 's3', 99, 44, 77, '2026-06-11 09:55', 'purchase', 'google', 300.00, '2026-06-18'),
    ('e5', 's5', 7, 4, 5, '2026-06-13 09:55', 'purchase', 'facebook', 50.00, '2026-06-18');
"""
NOW_VALID = """
INSERT INTO staging.stg_gold_dim_customers_scd2 VALUES (99, 'Ravi', '1900-01-01', '9999-12-31', true, '2026-06-20');
INSERT INTO staging.stg_gold_dim_products_scd2 VALUES
    (44, 'Lamp', 'Home', '1900-01-01', '9999-12-31', true, '2026-06-20', 'Casaro', 'active');
INSERT INTO staging.stg_gold_dim_campaigns_scd2 VALUES
    (77, 'Monsoon', 'google', '1900-01-01', '9999-12-31', true, '2026-06-20', 'retention');
"""


def test_a_fact_with_a_bad_reference_is_kept_and_resolves_once_the_dimension_row_is_valid(pg, tmp_path):
    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS + BAD_REFERENCES)   # customer 99, product 44 and campaign 77 are in no dimension
    build(tmp_path)

    def keys():
        return {
            "order": pg.execute("SELECT customer_sk, campaign_sk, customer_id, campaign_id FROM warehouse.fact_orders "
                                "WHERE order_id = '3'").fetchone(),
            "item": pg.execute("SELECT product_sk, product_id FROM warehouse.fact_order_items "
                               "WHERE order_item_nk = '30'").fetchone(),
            "spend": pg.execute("SELECT campaign_sk, campaign_id FROM warehouse.fact_campaign_spend "
                                "WHERE campaign_spend_nk = '3'").fetchone(),
            "event": pg.execute("SELECT customer_sk, product_sk, campaign_sk, customer_id, product_id, campaign_id "
                                "FROM warehouse.fact_web_events WHERE event_id = 'e3'").fetchone(),
            "conversion": pg.execute("SELECT customer_sk, product_sk, campaign_sk FROM warehouse.fact_conversions "
                                     "WHERE event_id = 'e3'").fetchone(),
        }

    # Every fact is loaded, on the invalid-reference member (-1), with its natural ID.
    assert keys() == {"order": (-1, -1, "99", "77"), "item": (-1, "44"), "spend": (-1, "77"),
                      "event": (-1, -1, -1, "99", "44", "77"), "conversion": (-1, -1, -1)}
    assert pg.execute("SELECT (SELECT count(*) FROM warehouse.fact_orders), (SELECT count(*) FROM warehouse.fact_order_items), "
                      "(SELECT count(*) FROM warehouse.fact_campaign_spend), (SELECT count(*) FROM warehouse.fact_web_events)"
                      ).fetchone() == (5, 5, 3, 4), "no fact is rejected or dropped"
    assert pg.execute("SELECT to_regclass('audit.fact_web_events_rejects')").fetchone()[0] is None
    # A missing key is the unknown member (0), not the invalid one: order 2 is organic.
    assert pg.execute("SELECT campaign_sk, campaign_id FROM warehouse.fact_orders WHERE order_id = '2'").fetchone() == (0, "UNKNOWN")
    members = pg.execute("SELECT customer_sk, customer_id, customer_name FROM warehouse.dim_customer "
                         "WHERE customer_sk <= 0 ORDER BY 1").fetchall()
    assert members == [(-1, "INVALID", "Invalid reference"), (0, "UNKNOWN", "Unknown")]

    # The counts per fact and dimension are in the reconciliation (all PASS: the build was published).
    counts = dict(pg.execute("SELECT metric_name, target_count::int FROM audit.v_latest_reconciliation_report "
                             "WHERE check_category = 'Invalid references' AND status = 'PASS'").fetchall())
    assert counts == {
        "invalid_customer_references_in_fact_orders": 1, "invalid_campaign_references_in_fact_orders": 1,
        "invalid_product_references_in_fact_order_items": 1, "invalid_campaign_references_in_fact_campaign_spend": 1,
        "invalid_customer_references_in_fact_web_events": 1, "invalid_product_references_in_fact_web_events": 1,
        "invalid_campaign_references_in_fact_web_events": 1}
    # D: the SCD2 version counts of Gold and the warehouse are compared, without the two special members.
    versions = dict(pg.execute("SELECT metric_name, (source_count::int, target_count::int)::text "
                               "FROM audit.v_latest_reconciliation_report WHERE metric_name LIKE '%_dimension_versions'").fetchall())
    assert versions == {"customer_dimension_versions": "(2,2)", "product_dimension_versions": "(1,1)",
                        "campaign_dimension_versions": "(1,1)"}

    # A sale is an order that was not cancelled (K1): the returned order 5 is a
    # sale of 50 with a refund of 50. The revenue of the order with the bad
    # references is not lost: it is in the daily mart.
    daily = pg.execute("SELECT sum(order_count), sum(net_revenue), sum(tax_amount), sum(shipping_amount), sum(total_amount), "
                       "sum(cancelled_order_count), sum(cancelled_amount), sum(returned_order_count), sum(returned_amount) "
                       "FROM marts.mart_revenue_daily").fetchone()
    assert [float(value) for value in daily] == [4, 650.0, 117.0, 1497.0, 2264.0, 1, 150.0, 1, 50.0]
    customer = pg.execute("SELECT total_order_count, lifetime_value, cancelled_order_count, cancelled_amount, "
                          "returned_order_count, returned_amount, total_units_purchased FROM marts.mart_customer_360 "
                          "WHERE customer_id = '7'").fetchone()
    assert [float(value) for value in customer] == [3, 300.0, 1, 150.0, 1, 50.0, 4]
    campaign = pg.execute("SELECT order_count, attributed_revenue, cancelled_order_count, returned_order_count, "
                          "ad_platform_conversions, platform_attributed_revenue, campaign_type "
                          "FROM marts.mart_campaign_performance WHERE campaign_id = '5'").fetchone()
    assert campaign[:6] == (2, 200, 1, 1, 3, 450) and campaign[6] == "conversion"
    product = pg.execute("SELECT units_sold, product_revenue, cancelled_units, returned_units, returned_amount, brand, "
                         "product_status FROM marts.mart_product_sales WHERE product_id = '4'").fetchone()
    assert product == (4, 300, 1, 1, 50, "Voltix", "active")
    # J3: every mart has a flagged row for the unknown and for the invalid-reference
    # member, so it ties out to the facts: the 300 of order 3 is on the invalid row.
    assert pg.execute("SELECT customer_id, member_type, lifetime_value, total_units_purchased FROM marts.mart_customer_360 "
                      "ORDER BY 1").fetchall() == [("7", "known", 300, 4), ("INVALID", "invalid_reference", 300, 3),
                                                   ("UNKNOWN", "unknown_member", 0, 0)]
    assert pg.execute("SELECT campaign_id, member_type, attributed_revenue, total_spend FROM marts.mart_campaign_performance "
                      "ORDER BY 1").fetchall() == [("5", "known", 200, 50), ("INVALID", "invalid_reference", 300, 80),
                                                   ("UNKNOWN", "unknown_member", 100, 0)]
    assert pg.execute("SELECT product_id, member_type, product_revenue, units_sold FROM marts.mart_product_sales "
                      "ORDER BY 1").fetchall() == [("4", "known", 300, 4), ("INVALID", "invalid_reference", 300, 3),
                                                   ("UNKNOWN", "unknown_member", 0, 0)]
    ties = dict(pg.execute("SELECT check_name, status FROM audit.mart_validation_report").fetchall())
    assert {ties[name] for name in ("customer_lifetime_value_matches_fact_orders", "campaign_revenue_matches_fact_orders",
                                    "customer_units_match_fact_order_items", "product_units_match_fact_order_items",
                                    "product_revenue_matches_fact_order_items", "campaign_spend_matches_fact_campaign_spend",
                                    "campaign_platform_results_match_fact_campaign_spend")} == {"PASS"}
    # The regional rows of campaign 5 sum to its campaign-day total (30 + 20 = 50).
    assert pg.execute("SELECT status FROM audit.v_latest_reconciliation_report "
                      "WHERE check_name = 'Regional ad spend rows sum to the campaign-day total'").fetchone()[0] == "PASS"
    assert pg.execute("SELECT r.state, f.spend_amount FROM warehouse.fact_campaign_spend f JOIN warehouse.dim_region r "
                      "USING (region_sk) WHERE f.campaign_id = '5' ORDER BY 1").fetchall() == [("Kerala", 30), ("Punjab", 20)]

    # The dimension rows arrive valid in a later batch: the same facts resolve to the real members.
    pg.execute(NOW_VALID)
    build(tmp_path)
    resolved = keys()
    assert all(key > 0 for key in resolved["order"][:2] + resolved["item"][:1] + resolved["spend"][:1]
               + resolved["event"][:3] + resolved["conversion"]), resolved
    assert pg.execute("SELECT c.customer_name, k.campaign_name, k.campaign_type FROM warehouse.fact_orders f "
                      "JOIN warehouse.dim_customer c USING (customer_sk) JOIN warehouse.dim_campaign k USING (campaign_sk) "
                      "WHERE f.order_id = '3'").fetchone() == ("Ravi", "Monsoon", "retention")
    assert pg.execute("SELECT sum(target_count)::int FROM audit.v_latest_reconciliation_report "
                      "WHERE check_category = 'Invalid references'").fetchone()[0] == 0
    assert pg.execute("SELECT attributed_revenue, ad_platform_conversions FROM marts.mart_campaign_performance "
                      "WHERE campaign_id = '77'").fetchone() == (300, 4)


def test_an_order_that_breaks_the_total_identity_fails_the_build(pg, tmp_path):
    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS)
    build(tmp_path)
    live = pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0]
    pg.execute("UPDATE staging.stg_gold_fact_orders_scd2 SET total_amount = total_amount + 1 WHERE order_id = 1")
    failed = run_sql_files(tmp_path, *BUILD_FILES)
    output = failed.stdout + failed.stderr
    assert failed.returncode != 0
    assert "FAILED CHECK audit.reconciliation_report | Order total equals net revenue plus tax plus shipping for every order" in output
    assert pg.execute("SELECT to_regnamespace('warehouse')::oid").fetchone()[0] == live, "the build was not published"


# --- 2: timestamps are TIMESTAMPTZ; business dates stay IST dates ----------------

@pytest.mark.parametrize("name", ["create_warehouse_tables", "create_marts", "scd2_validation_queries", "reconciliation_report"])
def test_no_timestamp_without_time_zone_in_the_warehouse_sql(name):
    code = sql_code(WAREHOUSE_DIR / f"{name}.sql")
    # The only plain TIMESTAMP left turns a date into midnight UTC, explicitly.
    leftovers = [match.group(0) for match in re.finditer(r"[^\n]*\bTIMESTAMP\b(?!TZ)[^\n]*", code)
                 if "::TIMESTAMP AT TIME ZONE 'UTC'" not in match.group(0)]
    assert leftovers == []


def test_exports_keep_the_utc_wall_time(load_job):
    export = load_job("scripts/export_power_bi_marts.py")
    india = dt.timezone(dt.timedelta(hours=5, minutes=30))
    assert str(export.csv_value(dt.datetime(2026, 6, 14, 17, 35, 53, tzinfo=india))) == "2026-06-14 12:05:53"
    assert str(export.csv_value(dt.datetime(2026, 6, 14, 12, 5, 53, tzinfo=dt.timezone.utc))) == "2026-06-14 12:05:53"
    assert export.csv_value("text") == "text" and export.csv_value(None) is None


def test_timestamps_are_instants_and_do_not_depend_on_the_session_time_zone(pg, tmp_path):
    pg.execute("DROP SCHEMA staging CASCADE; CREATE SCHEMA staging")
    pg.execute(STAGING_ROWS)
    # An order at 00:30 IST on 11 May (19:00 UTC on the 10th).
    pg.execute("UPDATE staging.stg_gold_fact_orders_scd2 SET order_timestamp = '2026-05-10 19:00', status_timestamp = '2026-05-10 19:00' WHERE order_id = 1")
    build(tmp_path)

    naive = pg.execute("SELECT table_schema, table_name, column_name FROM information_schema.columns "
                       "WHERE table_schema IN ('warehouse', 'marts', 'audit') "
                       "AND data_type = 'timestamp without time zone'").fetchall()
    assert naive == []

    facts = ("SELECT order_id, order_timestamp, order_date_sk, source_updated_at, record_hash, customer_sk "
             "FROM warehouse.fact_orders ORDER BY order_id")
    in_utc = pg.execute(facts).fetchall()
    utc = dt.timezone.utc
    assert in_utc[0][1] == dt.datetime(2026, 5, 10, 19, 0, tzinfo=utc)
    assert in_utc[0][2] == 20260511, "the business date is the Asia/Kolkata date"
    assert in_utc[0][3] == dt.datetime(2026, 5, 18, tzinfo=utc), "source_updated_at: midnight UTC of the batch date"
    marts_first = pg.execute("SELECT first_order_timestamp FROM marts.mart_customer_360 WHERE customer_id = '7'").fetchone()[0]
    assert marts_first == dt.datetime(2026, 5, 10, 19, 0, tzinfo=utc)

    # The same build and the same reads in a session that lives in India.
    pg.execute("SET timezone = 'Asia/Kolkata'")
    try:
        assert pg.execute(facts).fetchall() == in_utc
        assert pg.execute("SELECT warehouse.business_date('2026-06-17 19:00+00'), "
                          "warehouse.business_date('2026-06-17 18:29:59+00')").fetchone() == (dt.date(2026, 6, 18), dt.date(2026, 6, 17))
        pg.execute(begin_build() + build_step("create_warehouse_tables") + "COMMIT;")
        assert pg.execute(facts).fetchall() == in_utc, "a build in an IST session gave other times, dates, hashes or keys"
    finally:
        pg.execute("SET timezone = 'UTC'")
