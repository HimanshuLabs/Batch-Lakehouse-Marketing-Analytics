"""Part F regressions. PostgreSQL cases require the dedicated disposable sandbox."""
import json
import os
import sys

import pytest
from conftest import CODE_ROOT, MINIMAL_STAGING_SQL


@pytest.mark.parametrize('price,reason', [
    (None, 'original_price is NULL'),
    ('0', 'original_price must be positive'),
    ('-1', 'original_price must be positive'),
    ('100', 'discounted_price exceeds original_price'),
    ('Infinity', 'original_price is NULL'),
])
def test_invalid_original_prices(spark, load_job, write_bronze, price, reason):
    silver = load_job('spark-batch/silver_transformations.py', BATCH_DATE='2026-06-18')
    write_bronze('order_items', [{'order_item_id': '1', 'original_price': price}, {'order_item_id': '2'}])
    good, bad = silver.clean_order_items(silver.create_spark_session())
    accepted, rejected = good.collect(), bad.collect()
    assert [r.order_item_id for r in accepted] == [2]
    assert [(r.order_item_id, r.quarantine_reason) for r in rejected] == [
        (1, 'Invalid order item record: ' + reason)]
    assert len(accepted) + len(rejected) == 2


@pytest.mark.parametrize('status', ['FAIL', None, 'UNKNOWN'])
def test_export_rejects_quality_before_writing(load_job, monkeypatch, tmp_path, status):
    export = load_job('sql/export_power_bi_dataset.py', BATCH_DATE='2026-06-18')
    rows = [{'metric_group': 'Warehouse Reconciliation', 'metric_name': 'injected',
             'metric_value': 1, 'status': status}]
    monkeypatch.setattr(export, 'read_warehouse_checks', lambda: ([], [], {}))
    monkeypatch.setattr(export, 'read_silver_report', lambda _: [])
    monkeypatch.setattr(export, 'read_pit_report', lambda: {})
    monkeypatch.setattr(export, 'build_data_quality_summary', lambda *a: rows)
    monkeypatch.setattr(export, 'EXPORTS', {'sample': 'SELECT 1 AS value'})
    target, report = tmp_path / 'exports', tmp_path / 'report.json'
    monkeypatch.setattr(sys, 'argv', ['export', '--export-dir', str(target), '--report-path', str(report)])
    with pytest.raises((ValueError, SystemExit), match='non-PASS|failed'):
        export.main()
    assert not list(target.glob('*.csv'))
    result = json.loads(report.read_text())
    assert result['overall_status'] == 'FAIL'
    assert result['data_quality_summary'] == rows


@pytest.fixture
def pg():
    if os.environ.get('P2_F_SANDBOX') != '1':
        pytest.skip('requires dedicated Part F PostgreSQL sandbox on localhost:5546')
    import psycopg
    # Deliberately no configurable/default real-warehouse connection.
    with psycopg.connect(host='127.0.0.1', port=5546, user='project2', password='project2',
                          dbname='marketing_analytics', autocommit=True) as conn:
        conn.execute('DROP SCHEMA IF EXISTS marts CASCADE; DROP SCHEMA IF EXISTS warehouse CASCADE; '
                     'DROP SCHEMA IF EXISTS audit CASCADE; DROP SCHEMA IF EXISTS staging CASCADE; '
                     "CREATE SCHEMA staging; SELECT set_config('p2.run_id', 'part-f-test', false)")
        yield conn


def warehouse_sql():
    return MINIMAL_STAGING_SQL + (CODE_ROOT / 'sql/warehouse/create_warehouse_tables.sql').read_text()


def marts_sql():
    return (CODE_ROOT / 'sql/warehouse/create_marts.sql').read_text()


@pytest.mark.parametrize('dimension', ['customer', 'product', 'campaign'])
@pytest.mark.parametrize('same_natural', [False, True])
def test_forced_scd_collision(pg, dimension, same_natural):
    import psycopg
    pg.execute(f'CREATE TABLE staging.stg_gold_dim_{dimension}s_scd2 '
               f'({dimension}_id text, effective_from timestamp, source_updated_at timestamp)')
    pg.execute(f"INSERT INTO staging.stg_gold_dim_{dimension}s_scd2 VALUES "
               f"('a', '2026-01-01', '2026-01-01'), "
               f"('{ 'a' if same_natural else 'b'}', '2026-02-01', '2026-02-01')")
    sql = warehouse_sql().replace(
        "SELECT warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown'));",
        f"SELECT CASE WHEN prefix = '{dimension}' THEN 1000000001::BIGINT ELSE "
        "warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown')) END;")
    with pytest.raises(psycopg.errors.RaiseException, match='Surrogate-key collision'):
        pg.execute(sql)


@pytest.mark.parametrize('dimension', ['customer', 'product', 'campaign'])
def test_duplicate_dimension_rows_are_not_collisions(pg, dimension):
    pg.execute(f'CREATE TABLE staging.stg_gold_dim_{dimension}s_scd2 ({dimension}_id text, effective_from timestamp)')
    pg.execute(f"INSERT INTO staging.stg_gold_dim_{dimension}s_scd2 VALUES ('a', '2026-01-01'), ('a', '2026-01-01')")
    pg.execute(warehouse_sql())
    assert pg.execute(f'SELECT count(*) FROM warehouse.dim_{dimension} WHERE {dimension}_sk <> 0').fetchone()[0] == 1


def test_revenue_check_detects_unknown_date_loss(pg):
    pg.execute(warehouse_sql())
    pg.execute("INSERT INTO warehouse.fact_orders(order_sk,order_id,customer_sk,campaign_sk,order_date_sk,"
               "region_sk,channel_sk,net_revenue,customer_id,campaign_id,record_hash) VALUES (1,'one',0,0,0,0,0,25,'UNKNOWN','UNKNOWN','h1')")
    pg.execute(marts_sql())
    assert pg.execute("SELECT source_value,target_value,status FROM audit.mart_validation_report "
                      "WHERE check_name='daily_revenue_matches_fact_orders'").fetchone() == (25, 0, 'FAIL')


def test_recency_uses_latest_business_order_date(pg):
    pg.execute(warehouse_sql())
    pg.execute("INSERT INTO warehouse.dim_customer(customer_sk,customer_id,effective_from,effective_to,is_current) "
               "VALUES (1,'one','2026-01-01','9999-12-31',true),(2,'two','2026-01-01','9999-12-31',true),"
               "(3,'no orders','2026-01-01','9999-12-31',true)")
    pg.execute("INSERT INTO warehouse.fact_orders(order_sk,order_id,customer_sk,campaign_sk,order_date_sk,"
               "region_sk,channel_sk,order_timestamp,customer_id,campaign_id,record_hash) VALUES "
               "(1,'one',1,0,0,0,0,'2026-06-17 18:29:59','one','UNKNOWN','h1'),"
               "(2,'two',2,0,0,0,0,'2026-06-17 18:30:00','two','UNKNOWN','h2')")
    pg.execute(marts_sql())
    for today in ('2026-06-18', '2030-01-01'):
        pg.execute(f"CREATE OR REPLACE FUNCTION warehouse.business_today() RETURNS DATE LANGUAGE SQL STABLE "
                   f"AS $$ SELECT DATE '{today}' $$")
        assert pg.execute('SELECT customer_id,days_since_last_order FROM marts.mart_customer_360 ORDER BY customer_id').fetchall() == [
            ('no orders', None), ('one', 1), ('two', 0)]


@pytest.mark.parametrize('table,column', [('reconciliation_report','reconciliation_run_id'),
    ('scd2_validation_results','validation_run_id'), ('mart_validation_report','validation_run_id')])
@pytest.mark.parametrize('stale', [False, True])
def test_gate_rejects_failures_and_stale_success(pg, table, column, stale):
    import psycopg
    pg.execute('CREATE SCHEMA audit')
    for name, run_col in [('reconciliation_report','reconciliation_run_id'),
                          ('scd2_validation_results','validation_run_id'),('mart_validation_report','validation_run_id')]:
        pg.execute(f'CREATE TABLE audit.{name} ({run_col} text, status text)')
        pg.execute(f"INSERT INTO audit.{name} VALUES ('part-f-test','PASS'), ('previous-success','PASS')")
    if stale:
        pg.execute(f"DELETE FROM audit.{table} WHERE {column}='part-f-test'")
    else:
        pg.execute(f"UPDATE audit.{table} SET status='FAIL' WHERE {column}='part-f-test'")
    sql = (CODE_ROOT / 'sql/warehouse/reconciliation_report.sql').read_text()
    gate = sql.partition('-- Persist diagnostic FAIL rows')[2]
    gate = gate[gate.find('DO $$'):] if gate else 'SELECT 1'
    with pytest.raises(psycopg.errors.RaiseException, match='Quality gate failed'):
        pg.execute(gate)


@pytest.mark.parametrize('dimension,source,column', [
    ('region', 'customers', 'home_city'), ('channel', 'campaigns', 'traffic_source'),
])
def test_ordinary_dimension_collision_fails_on_primary_key(pg, dimension, source, column):
    import psycopg
    pg.execute(f'CREATE TABLE staging.stg_gold_dim_{source} ({column} text)')
    pg.execute(f"INSERT INTO staging.stg_gold_dim_{source} VALUES ('a'), ('b')")
    sql = warehouse_sql().replace(
        "SELECT warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown'));",
        f"SELECT CASE WHEN prefix = '{dimension}' THEN 1000000001::BIGINT ELSE "
        "warehouse.deterministic_fact_sk(coalesce(prefix, 'unknown'), coalesce(value, 'unknown')) END;")
    with pytest.raises(psycopg.errors.UniqueViolation, match=f'dim_{dimension}_pkey'):
        pg.execute(sql)


def test_reconciliation_task_failure_persists_and_reaches_summary(pg, load_job, tmp_path):
    import subprocess
    pg.execute(warehouse_sql())
    pg.execute("CREATE TABLE audit.scd2_validation_results (validation_run_id text,status text); "
               "INSERT INTO audit.scd2_validation_results VALUES ('part-f-test','PASS'); "
               "CREATE TABLE IF NOT EXISTS staging.stg_gold_fact_web_events (event_id text)")
    pg.execute(marts_sql())
    # A staging order that never reached the warehouse is a genuine failed
    # source-vs-target check (missing staging tables now fail the build itself).
    pg.execute("INSERT INTO staging.stg_gold_fact_orders_scd2 (order_id) VALUES ('never-loaded')")
    result = subprocess.run(['bash', str(CODE_ROOT / 'scripts/run_warehouse_sql.sh'),
                             str(CODE_ROOT / 'sql/warehouse/reconciliation_report.sql')],
                            cwd=tmp_path, env={**os.environ, 'PGCONTAINER': 'p2-part-f-sandbox',
                            'P2_RUN_ID': 'part-f-test', 'RUN_MODE': 'docker'}, capture_output=True, text=True)
    assert result.returncode != 0, result.stdout
    assert 'Quality gate failed' in result.stdout
    assert 'Warehouse SQL completed successfully' not in result.stdout
    from psycopg.rows import dict_row
    with pg.cursor(row_factory=dict_row) as cur:
        cur.execute('SELECT * FROM audit.reconciliation_report')
        reconciliation = cur.fetchall()
    assert any(row['status'] == 'FAIL' for row in reconciliation)
    export = load_job('sql/export_power_bi_dataset.py')
    rows = export.build_data_quality_summary(
        reconciliation, [{'check_name': 'scd', 'failed_row_count': 0, 'status': 'PASS'}],
        [{'table_name': 'items', 'status': 'PASSED', 'quarantine_row_count': 0, 'unaccounted_row_count': 0}],
        {'facts': {'fact_orders_scd2': {
            'conservation': {'status': 'PASS', 'rejected_rows': 0, 'unaccounted_rows': 0},
            'fact_resolutions': {'campaign_sk': {'unknown_member': 0}}}},
         'order_totals': {'status': 'PASS', 'checked_orders': 0, 'mismatched_orders': 0,
                          'excluded_incomplete_orders': 0, 'orders_without_bronze_items': 0}},
        {'rejected_rows': 0, 'customer': 0, 'product': 0, 'campaign': 0})
    summary = {row['metric_name']: row['status'] for row in rows}
    for check in reconciliation:
        assert summary[check['check_name']] == check['status']
