"""Session and campaign-period regressions; all generated inputs are temporary."""
from datetime import date
import random

import pandas as pd
import pytest

JOB = 'data-generator/generate_data.py'


@pytest.fixture
def batch(load_job):
    module = load_job(JOB, BATCH_DATE='2026-05-18')
    return module, module.build_batch()


def events(outputs):
    return pd.DataFrame([e for e in outputs['web_events.json'] if e['event_id'] is not None])


def test_sessions_are_reused_and_have_one_customer_and_context(batch):
    _, output = batch
    frame = events(output)
    groups = frame.groupby('session_id')
    assert groups.size().max() > 1, 'one new UUID per event is not a visit model'
    for column in ['user_id', 'campaign_id', 'traffic_source', 'device_type', 'ip_address']:
        assert groups[column].nunique(dropna=False).max() == 1
    assert groups['event_type'].apply(lambda s: (s == 'purchase').sum()).max() == 1


def test_session_counters_are_causal_and_timestamps_are_ordered(batch):
    _, output = batch
    frame = events(output)
    assert frame.event_timestamp.is_monotonic_increasing
    for _, group in frame.groupby('session_id'):
        times = pd.to_datetime(group.event_timestamp)
        gaps = times.diff().dt.total_seconds().fillna(0).astype(int)
        assert group.time_since_last_event_ms.tolist() == (gaps * 1000).tolist()
        assert group.session_duration_sec.tolist() == (times - times.iloc[0]).dt.total_seconds().tolist()
        assert (gaps.iloc[1:] > 0).all() and (gaps < 1800).all()
        seen = {}
        for row in group.itertuples():
            if row.event_type == 'product_view':
                seen[row.product_id] = seen.get(row.product_id, 0) + 1
            assert row.items_viewed_in_session == len(seen)
            assert row.repeat_product_view_count == max(seen.get(row.product_id, 0) - 1, 0)
    visits = frame.groupby(['user_id', 'session_id']).event_timestamp.agg(['min', 'max']).reset_index()
    for _, customer in visits.groupby('user_id'):
        customer = customer.sort_values('min')
        starts, ends = pd.to_datetime(customer['min']), pd.to_datetime(customer['max'])
        assert (starts.iloc[1:].reset_index(drop=True) - ends.iloc[:-1].reset_index(drop=True)).dt.total_seconds().gt(1800).all()


def test_purchase_events_are_not_resampled_into_multiple_clean_orders(batch):
    _, output = batch
    # Without the dirty fixtures: rows 0, 2, 3, 4 and the appended ones (top of the ID block).
    clean = output['orders.csv'].drop(index=[0, 2, 3, 4])
    clean = clean[clean['order_id'] % 100_000 < 99_900]
    assert not clean.duplicated(['user_id', 'order_timestamp', 'campaign_id']).any()


def test_spend_is_unique_per_campaign_day_within_campaign_period(batch):
    _, output = batch
    spend = output['ad_spend.csv']
    spend = spend[spend['spend_id'] % 100_000 < 99_900]   # without the appended dirty fixtures
    campaigns = output['campaigns.csv'].drop_duplicates('campaign_id').set_index('campaign_id')
    # J3: the feed's grain is campaign, day and region.
    assert not spend.duplicated(['campaign_id', 'spend_date', 'state']).any()
    assert spend.duplicated(['campaign_id', 'spend_date']).any(), 'a campaign-day has several regional rows'
    for row in spend.itertuples():
        campaign = campaigns.loc[row.campaign_id]
        assert campaign.start_date <= row.spend_date <= campaign.end_date


def campaign(budget=1000.):
    return pd.DataFrame([{'campaign_id': 7, 'traffic_source': 'google', 'budget': budget,
                          'start_date': '2026-06-09', 'end_date': '2026-06-18',
                          'updated_at': '2026-06-09T00:00:00'}])


def test_spend_uses_budget_units_and_period_instead_of_independent_amounts(load_job):
    m = load_job(JOB, BATCH_DATE='2026-06-18')
    a = m.generate_ad_spend(campaign(), random.Random(9), 14, 100)
    b = m.generate_ad_spend(campaign(2000.), random.Random(9), 14, 100)
    assert a.groupby('spend_date').ngroups == 10, 'one campaign-day for each of the ten eligible days'
    # The budget is compared with the spend summed over the regions of a day.
    a, b = (frame.groupby('spend_date', as_index=False).agg(spend_amount=('spend_amount', 'sum'),
            total=('campaign_day_spend', 'max')) for frame in (a, b))
    assert (abs(a.spend_amount - a.total) < .005).all(), 'the regional rows sum to the campaign-day total'
    assert (abs(b.spend_amount - 2*a.spend_amount) <= .011).all()
    assert (a.spend_date <= '2026-06-18').all()


def test_daily_spend_cannot_use_a_later_budget_revision(load_job):
    m = load_job(JOB, BATCH_DATE='2026-06-18')
    base = campaign()
    revision = campaign(2000.).assign(updated_at='2026-06-18T08:00:00')
    a = m.generate_ad_spend(base, random.Random(3), 14, 100)
    b = m.generate_ad_spend(pd.concat([base, revision]), random.Random(3), 14, 100)
    pd.testing.assert_frame_equal(a, b)


def test_simulation_truth_is_separate_and_quality_fixtures_remain(batch):
    m, output = batch
    assert 'generation_truth.json' in output
    truth = output['generation_truth.json']
    assert truth[0]['currency'] == 'INR'
    assert truth[0]['labels_are_model_inputs'] is False
    assignments = [r['delivery_surge'] for r in truth if r['kind'] == 'delivery_scenario']
    assert {True, False} <= set(assignments)
    assert not any('delivery_surge' in e for e in output['web_events.json'])
    assert 'delivery_surge' not in output['ad_spend.csv'].columns
    spend = output['ad_spend.csv']
    assert spend[spend['spend_id'] % 100_000 == 99_999]['spend_amount'].tolist() == [-100]
    assert output['orders.csv'].loc[2, 'cart_value'] == -500
    assert output['order_items.csv'].loc[2, 'line_amount'] == 1
    assert output['web_events.json'][-1]['event_timestamp'] == 'bad-timestamp'


def test_delivery_scenarios_have_overlapping_spend_not_a_perfect_threshold(load_job):
    m = load_job(JOB, BATCH_DATE='2026-06-18')
    rows = campaign(10000.).assign(start_date='2025-01-01', updated_at='2025-01-01T00:00:00')
    spend = m.generate_ad_spend(rows, random.Random(4), 400, 400).iloc[4:]
    surge = spend.apply(lambda r: m.delivery_surge(int(r.campaign_id), date.fromisoformat(r.spend_date)), axis=1)
    assert surge.any() and (~surge).any()
    assert spend.loc[surge, 'spend_amount'].min() < spend.loc[~surge, 'spend_amount'].max()


def test_campaign_mart_counts_budget_once_across_days_and_versions():
    """Protect the existing correct aggregation while changing source delivery."""
    import os
    import psycopg
    from conftest import MINIMAL_STAGING_SQL, begin_build, build_step, require_sandbox_database, reset_sandbox
    require_sandbox_database()
    with psycopg.connect(host='127.0.0.1', port=5546, user='project2', password='project2',
                         dbname='marketing_analytics', autocommit=True) as conn:
        reset_sandbox(conn)
        conn.execute('CREATE SCHEMA staging; '
                     "SELECT set_config('p2.run_id', 'budget-regression', false)")
        conn.execute(begin_build() + MINIMAL_STAGING_SQL + build_step('create_warehouse_tables'))
        conn.execute("INSERT INTO warehouse.dim_campaign(campaign_sk,campaign_id,budget,effective_from,effective_to,is_current) "
                     "VALUES (1,'campaign',1000,'2026-01-01','2026-02-01',false),"
                     "(2,'campaign',1200,'2026-02-01','9999-12-31',true)")
        conn.execute("INSERT INTO warehouse.fact_campaign_spend(campaign_spend_sk,campaign_spend_nk,campaign_sk,spend_date_sk,region_sk,channel_sk,spend_amount,campaign_id,record_hash,conversions,attributed_revenue) "
                     "VALUES (1,'day1',1,0,0,0,700,'campaign','h1',4,900),(2,'day2',2,0,0,0,800,'campaign','h2',6,1100)")
        conn.execute(build_step('create_marts'))
        row = conn.execute("SELECT campaign_budget,total_spend,budget_remaining FROM marts.mart_campaign_performance "
                           "WHERE campaign_id='campaign'").fetchone()
        assert tuple(map(float, row)) == (1200., 1500., -300.)
