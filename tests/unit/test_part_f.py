"""Part F regressions. PostgreSQL cases require the dedicated disposable sandbox."""
import json
import os
import sys

import pytest
from conftest import CODE_ROOT, MINIMAL_STAGING_SQL, begin_build, build_step, require_sandbox_database, reset_sandbox


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
    monkeypatch.setattr(export, 'read_warehouse_checks', lambda: ([], []))
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
    require_sandbox_database()
    import psycopg
    # Deliberately no configurable/default real-warehouse connection.
    with psycopg.connect(host='127.0.0.1', port=5546, user='project2', password='project2',
                          dbname='marketing_analytics', autocommit=True) as conn:
        reset_sandbox(conn)
        conn.execute(''
                     "CREATE SCHEMA staging; SELECT set_config('p2.run_id', 'part-f-test', false)")
        yield conn


def warehouse_sql():
    # The build files run only inside the build transaction, which stays open
    # for the test's own statements and is rolled back when the test ends.
    return begin_build() + MINIMAL_STAGING_SQL + build_step('create_warehouse_tables')


def marts_sql():
    return build_step('create_marts')


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
    assert pg.execute(f'SELECT count(*) FROM warehouse.dim_{dimension} WHERE {dimension}_sk NOT IN (0, -1)').fetchone()[0] == 1


def test_revenue_check_detects_unknown_date_loss(pg):
    pg.execute(warehouse_sql())
    pg.execute("INSERT INTO warehouse.fact_orders(order_sk,order_id,customer_sk,campaign_sk,order_date_sk,"
               "region_sk,channel_sk,net_revenue,customer_id,campaign_id,record_hash,order_status,tax_amount,shipping_amount,"
               "total_amount,status_timestamp,refund_amount) VALUES (1,'one',0,0,0,0,0,25,'UNKNOWN','UNKNOWN','h1','completed',0,0,25,'2026-06-17 10:00',0)")
    pg.execute(marts_sql())
    assert pg.execute("SELECT source_value,target_value,status FROM audit.mart_validation_report "
                      "WHERE check_name='daily_revenue_matches_fact_orders'").fetchone() == (25, 0, 'FAIL')


def test_recency_uses_latest_business_order_date(pg):
    pg.execute(warehouse_sql())
    pg.execute("INSERT INTO warehouse.dim_customer(customer_sk,customer_id,effective_from,effective_to,is_current) "
               "VALUES (1,'one','2026-01-01','9999-12-31',true),(2,'two','2026-01-01','9999-12-31',true),"
               "(3,'no orders','2026-01-01','9999-12-31',true)")
    pg.execute("INSERT INTO warehouse.fact_orders(order_sk,order_id,customer_sk,campaign_sk,order_date_sk,"
               "region_sk,channel_sk,order_timestamp,customer_id,campaign_id,record_hash,order_status,tax_amount,"
               "shipping_amount,total_amount,status_timestamp,refund_amount) VALUES "
               "(1,'one',1,0,0,0,0,'2026-06-17 18:29:59','one','UNKNOWN','h1','completed',0,0,0,'2026-06-17 18:29:59',0),"
               "(2,'two',2,0,0,0,0,'2026-06-17 18:30:00','two','UNKNOWN','h2','completed',0,0,0,'2026-06-17 18:30:00',0)")
    pg.execute(marts_sql())
    for today in ('2026-06-18', '2030-01-01'):
        pg.execute(f"CREATE OR REPLACE FUNCTION warehouse.business_today() RETURNS DATE LANGUAGE SQL STABLE "
                   f"AS $$ SELECT DATE '{today}' $$")
        assert pg.execute("SELECT customer_id,days_since_last_order FROM marts.mart_customer_360 "
                          "WHERE member_type = 'known' ORDER BY customer_id").fetchall() == [
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
    gate = sql.partition('-- Quality gate, before the commit')[2]
    gate = gate[gate.find('DO $$'):gate.find('COMMIT;')] if gate else 'SELECT 1'
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


def test_reconciliation_task_failure_is_rolled_back_reported_and_reaches_summary(pg, load_job, tmp_path):
    # Until H2 a failed reconciliation was committed (and so published) before
    # the task failed. It is now rolled back; the failing checks are in the
    # task log and in logs/warehouse/failed_build_<timestamp>.json.
    import json
    import subprocess
    from test_part_h import STAGING_ROWS
    # The whole build, with one loaded order removed before the reconciliation
    # (tests/sql/inject_reconciliation_fail.sql): a genuine failed
    # source-vs-target check. The build files run only as one build.
    pg.execute(STAGING_ROWS)
    warehouse = CODE_ROOT / 'sql/warehouse'
    files = [warehouse / 'begin_generation.sql', warehouse / 'create_warehouse_tables.sql',
             warehouse / 'scd2_validation_queries.sql', warehouse / 'create_marts.sql',
             CODE_ROOT / 'tests/sql/inject_reconciliation_fail.sql', warehouse / 'reconciliation_report.sql']
    result = subprocess.run(['bash', str(CODE_ROOT / 'scripts/run_warehouse_sql.sh'), *map(str, files)],
                            cwd=tmp_path, env={**os.environ, 'PGCONTAINER': 'p2-part-f-sandbox',
                            'P2_RUN_ID': 'part-f-test', 'RUN_MODE': 'docker'}, capture_output=True, text=True)
    assert result.returncode != 0, result.stdout
    assert 'Quality gate failed' in result.stdout
    assert 'Warehouse SQL completed successfully' not in result.stdout
    assert pg.execute("SELECT to_regclass('audit.reconciliation_report')").fetchone()[0] is None, \
        'the failed reconciliation was committed'
    reports = list((tmp_path / 'logs' / 'warehouse').glob('failed_build_*.json'))
    assert len(reports) == 1, result.stdout[-2000:]
    report = json.loads(reports[0].read_text(encoding='utf-8'))
    assert report['run_id'] == 'part-f-test' and report['failing_check_count'] == len(report['failing_checks']) >= 1
    # (The gate lists every report's failures; this test built only the reconciliation.)
    failing = [check for check in report['failing_checks'] if check['report'] == 'audit.reconciliation_report']
    assert failing
    for check in failing:
        assert check['status'] == 'FAIL'
        assert check['expected'] != check['actual'] and check['details']
        assert f"FAILED CHECK audit.reconciliation_report | {check['check_name']} |" in result.stdout
    # The export's summary shows a failed check as FAIL (it takes report rows).
    reconciliation = [{'check_name': check['check_name'], 'status': check['status'],
                       'source_count': check['expected'], 'target_count': check['actual'],
                       'count_difference': check['difference'], 'source_amount': None, 'target_amount': None,
                       'amount_difference': None, 'details': check['details']} for check in failing]
    export = load_job('sql/export_power_bi_dataset.py')
    # The passing checks are gone with the rollback; the summary requires
    # these (every web event loaded, and the seven invalid-reference counts).
    from test_power_bi_export import invalid_reference_checks
    failed_names = {check['check_name'] for check in reconciliation}
    required = [{'check_name': export.WEB_EVENT_CONSERVATION_CHECK, 'status': 'PASS', 'target_count': 0,
                 'count_difference': 0, 'amount_difference': None}] + invalid_reference_checks()
    reconciliation += [check for check in required if check['check_name'] not in failed_names]
    rows = export.build_data_quality_summary(
        reconciliation, [{'check_name': 'scd', 'failed_row_count': 0, 'status': 'PASS'}],
        [{'table_name': 'items', 'status': 'PASSED', 'quarantine_row_count': 0, 'unaccounted_row_count': 0}],
        {'facts': {'fact_orders_scd2': {
            'conservation': {'status': 'PASS', 'rejected_rows': 0, 'unaccounted_rows': 0},
            'fact_resolutions': {'campaign_sk': {'unknown_member': 0, 'invalid_reference': 0}}}},
         'order_totals': {'status': 'PASS', 'checked_orders': 0, 'mismatched_orders': 0,
                          'excluded_incomplete_orders': 0, 'orders_without_bronze_items': 0}})
    summary = {row['metric_name']: row['status'] for row in rows}
    for check in failing:
        assert summary[check['check_name']] == check['status'] == 'FAIL'
