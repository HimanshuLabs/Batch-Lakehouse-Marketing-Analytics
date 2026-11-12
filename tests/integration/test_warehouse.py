"""Assertions on a finished end-to-end run (tests/integration/run_integration.sh).

Reads the lake and job reports under P2_IT_PROJECT_ROOT (default: this
checkout) and queries the warehouse through `docker exec` in the container
named P2_IT_PGCONTAINER (default: project2_postgres). Read-only.

    python -m pytest tests/integration -m integration
    python tests/integration/test_warehouse.py --write-expected   # regenerate the golden counts
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(os.environ.get("P2_IT_PROJECT_ROOT", Path(__file__).resolve().parents[2])).resolve()
PGCONTAINER = os.environ.get("P2_IT_PGCONTAINER", "project2_postgres")
BATCH_DATES = os.environ.get("P2_IT_BATCH_DATES", "2026-05-18,2026-06-18").split(",")
EXPECTED_COUNTS = Path(__file__).with_name("expected_counts.json")

TABLES = ["customers", "products", "campaigns", "orders", "order_items", "ad_spend", "web_events"]
DIMENSIONS = ["customers", "products", "campaigns"]
GOLD_TABLES = [
    "dim_customers", "dim_products", "dim_campaigns", "fact_orders", "fact_order_items", "fact_ad_spend",
    "fact_web_events", "dim_customers_scd2", "dim_products_scd2", "dim_campaigns_scd2",
    "fact_orders_scd2", "fact_order_items_scd2", "fact_campaign_spend_scd2",
]
PIT_REJECTS = ["fact_orders_scd2_rejects", "fact_order_items_scd2_rejects", "fact_campaign_spend_scd2_rejects"]
WAREHOUSE_TABLES = [
    "warehouse.dim_customer", "warehouse.dim_product", "warehouse.dim_campaign", "warehouse.dim_date",
    "warehouse.dim_region", "warehouse.dim_channel", "warehouse.fact_orders", "warehouse.fact_order_items",
    "warehouse.fact_campaign_spend", "warehouse.fact_web_events", "warehouse.fact_conversions",
    "marts.mart_customer_360", "marts.mart_product_sales", "marts.mart_campaign_performance",
    "marts.mart_revenue_daily", "marts.mart_marketing_funnel",
]
JOB_REPORTS = [
    "scd2_quality_report.json", "point_in_time_fact_join_report.json", "scd2_gold_mart_report.json",
    "scd2_duckdb_analytics_report.json", "power_bi_export_report.json",
]


def sql(query: str) -> list[list[str]]:
    result = subprocess.run(
        ["docker", "exec", "-i", PGCONTAINER, "psql", "-U", "project2", "-d", "marketing_analytics",
         "-v", "ON_ERROR_STOP=1", "-At", "-F", "\t", "-c", query],
        check=True, capture_output=True, text=True,
    )
    return [line.split("\t") for line in result.stdout.splitlines() if line]


def parquet_count(path: Path) -> int:
    import duckdb

    if not any(path.rglob("*.parquet")):
        return 0
    return duckdb.sql(
        f"select count(*) from read_parquet('{path}/**/*.parquet', hive_partitioning=false, union_by_name=true)"
    ).fetchone()[0]


def collect_counts() -> dict[str, dict[str, int]]:
    data = PROJECT_ROOT / "data"
    lake: dict[str, int] = {}
    for table in TABLES:
        lake[f"bronze_{table}"] = parquet_count(data / "bronze" / f"bronze_{table}")
        lake[f"silver_{table}"] = parquet_count(data / "silver" / f"silver_{table}")
        if table in DIMENSIONS:
            lake[f"silver_{table}_history"] = parquet_count(data / "silver" / f"silver_{table}_history")
        lake[f"quarantine_{table}"] = parquet_count(data / "quarantine" / f"quarantine_{table}")
    for table in GOLD_TABLES:
        lake[f"gold_{table}"] = parquet_count(data / "gold" / table)
    for table in PIT_REJECTS:
        lake[f"quarantine_{table}"] = parquet_count(data / "quarantine" / table)
    warehouse = {table: int(sql(f"select count(*) from {table}")[0][0]) for table in WAREHOUSE_TABLES}
    return {"lake": lake, "warehouse": warehouse}


def read_report(directory: Path) -> list[dict]:
    files = sorted(directory.glob("*.csv"))
    assert files, f"missing report: {directory}"
    with files[0].open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
@pytest.mark.parametrize("batch_date", BATCH_DATES)
def test_quality_reports_pass(layer, batch_date):
    rows = read_report(PROJECT_ROOT / "logs" / f"{layer}_quality_report_{batch_date}")
    failed = {row["table_name"]: row.get("failure_reason") for row in rows if row["status"] != "PASSED"}
    assert rows and not failed, failed


@pytest.mark.parametrize("report", JOB_REPORTS)
def test_job_reports_pass(report):
    content = json.loads((PROJECT_ROOT / "logs" / report).read_text(encoding="utf-8"))
    assert content["overall_status"] == "PASS", content.get("error")


def test_point_in_time_facts_conserve_every_silver_row():
    report = json.loads((PROJECT_ROOT / "logs" / "point_in_time_fact_join_report.json").read_text(encoding="utf-8"))
    for fact, details in report["facts"].items():
        conservation = details["conservation"]
        assert conservation["status"] == "PASS" and conservation["unaccounted_rows"] == 0, (fact, conservation)
        corrupt = sum(c.get("no_version_for_time", 0) for k, c in details["resolutions"].items() if k != "order")
        assert corrupt == 0, (fact, details["resolutions"])


def test_latest_scd2_validation_all_pass():
    rows = sql(
        "select check_name, status, failed_row_count from audit.scd2_validation_results "
        "where validation_run_id = (select validation_run_id from audit.scd2_validation_results "
        "order by checked_at desc limit 1)"
    )
    assert rows, "no SCD2 validation run recorded"
    assert [row for row in rows if row[1] != "PASS"] == []


def test_reconciliation_all_pass():
    rows = sql("select check_name, status from audit.v_latest_reconciliation_report")
    # 15 checks from before Part J, 7 invalid-reference counts, 3 SCD2 version
    # counts, the order-total identity and the regional ad-spend sum.
    assert len(rows) == 27, len(rows)
    assert [row for row in rows if row[1] != "PASS"] == []


def test_no_fact_key_without_a_covering_version():
    assert sql("select count(*) from audit.scd2_fact_key_without_covering_version") == [["0"]]


def test_region_and_channel_keys_are_hashes_of_their_natural_keys():
    bad = sql(
        "select 'region', region_nk from warehouse.dim_region "
        "where region_sk <> case when region_nk = 'unknown|unknown|unknown' then 0 "
        "else warehouse.deterministic_sk('region', region_nk) end "
        "union all select 'channel', channel_nk from warehouse.dim_channel "
        "where channel_sk <> case when channel_nk = 'unknown' then 0 "
        "else warehouse.deterministic_sk('channel', channel_nk) end"
    )
    assert bad == []


def pit_report() -> dict:
    return json.loads((PROJECT_ROOT / "logs" / "point_in_time_fact_join_report.json").read_text(encoding="utf-8"))


def test_organic_orders_are_on_the_unknown_campaign_not_rejected():
    import duckdb

    rejects = PROJECT_ROOT / "data" / "quarantine" / "fact_orders_scd2_rejects"
    null_campaign_rejects = duckdb.sql(
        f"select count(*) from read_parquet('{rejects}/**/*.parquet', hive_partitioning=false, union_by_name=true) "
        "where reject_reason like '%campaign_id is NULL%'"
    ).fetchone()[0] if any(rejects.rglob("*.parquet")) else 0
    assert null_campaign_rejects == 0

    # Organic orders are facts whatever their other keys are: an organic order
    # of a quarantined customer is kept too (on the invalid-reference customer).
    organic = pit_report()["facts"]["fact_orders_scd2"]["fact_resolutions"]["campaign_sk"]["unknown_member"]
    assert organic > 0, "the sample data has organic orders; none reached the facts"
    # The warehouse keeps them on the unknown campaign member (campaign_sk 0).
    warehouse_unknown = int(sql(
        "select count(*) from warehouse.fact_orders fo "
        "join staging.stg_gold_fact_orders_scd2 s on s.order_id::TEXT = fo.order_id "
        "where s.campaign_id is null and fo.campaign_sk = 0"
    )[0][0])
    assert warehouse_unknown == organic


def test_unknown_members_use_one_label():
    # The dbt singular test, run against the warehouse tables and marts.
    query = (PROJECT_ROOT / "dbt" / "tests" / "assert_unknown_members_use_one_label.sql").read_text(encoding="utf-8")
    for model, relation in (("dim_customer", "warehouse.dim_customer"), ("dim_product", "warehouse.dim_product"),
                            ("dim_campaign", "warehouse.dim_campaign"),
                            ("mart_marketing_funnel", "marts.mart_marketing_funnel")):
        query = query.replace("{{ ref('%s') }}" % model, relation)
    assert "{{" not in query
    assert sql(query) == []

    spellings = sql(
        "select distinct v from ("
        " select customer_id as v from warehouse.dim_customer where customer_sk = 0"
        " union all select customer_name from warehouse.dim_customer where customer_sk = 0"
        " union all select product_id from warehouse.dim_product where product_sk = 0"
        " union all select product_name from warehouse.dim_product where product_sk = 0"
        " union all select campaign_id from warehouse.dim_campaign where campaign_sk = 0"
        " union all select campaign_name from warehouse.dim_campaign where campaign_sk = 0"
        " union all select channel_name from warehouse.dim_channel where channel_sk = 0"
        " union all select city from warehouse.dim_region where region_sk = 0"
        " union all select campaign_name from marts.mart_marketing_funnel where campaign_id = 'UNKNOWN'"
        ") labels"
    )
    # Compared as a set: ORDER BY follows the database collation.
    assert {row[0] for row in spellings} == {"UNKNOWN", "Unknown"}


def test_business_dates_are_ist_dates_like_gold():
    # Gold derives order_date in Asia/Kolkata; the warehouse date key must agree
    # for every order, including those between 00:00 and 05:30 IST, whose UTC
    # date is the day before.
    mismatched = sql(
        "select fo.order_id, fo.order_date_sk, g.order_date from warehouse.fact_orders fo "
        "join staging.stg_gold_fact_orders g on g.order_id::TEXT = fo.order_id "
        "where fo.order_date_sk <> warehouse.date_sk(g.order_date::DATE)"
    )
    assert mismatched == []
    early = int(sql(
        "select count(*) from warehouse.fact_orders "
        "where (order_timestamp at time zone 'Asia/Kolkata')::TIME < TIME '05:30'"
    )[0][0])
    assert early > 0, "no order between 00:00 and 05:30 IST to check"
    # Items, web events and conversions use the same business date.
    assert sql(
        "select count(*) from warehouse.fact_order_items foi join warehouse.fact_orders fo using (order_sk) "
        "where foi.order_date_sk <> fo.order_date_sk"
    ) == [["0"]]
    assert sql(
        "select count(*) from warehouse.fact_web_events "
        "where event_date_sk <> warehouse.date_sk("
        "(event_timestamp at time zone 'Asia/Kolkata')::DATE)"
    ) == [["0"]]
    assert sql(
        "select count(*) from warehouse.fact_conversions c join warehouse.fact_web_events e using (event_sk) "
        "where c.conversion_date_sk <> e.event_date_sk"
    ) == [["0"]]


def test_no_phantom_column_and_no_invented_member():
    # campaign_type was 'Unknown' in every row until the source delivered it
    # (Part J): it is the campaign's objective now, never a placeholder.
    assert sql("select distinct campaign_type from warehouse.dim_campaign where campaign_sk not in (0, -1) "
               "order by 1") == [["awareness"], ["consideration"], ["conversion"], ["retention"]]
    # Every SCD2 dimension member is a Gold SCD2 key, the unknown member or the
    # invalid-reference member.
    for dimension, key, staging in (
        ("dim_customer", "customer_id", "stg_gold_dim_customers_scd2"),
        ("dim_product", "product_id", "stg_gold_dim_products_scd2"),
        ("dim_campaign", "campaign_id", "stg_gold_dim_campaigns_scd2"),
    ):
        invented = sql(
            f"select {key} from warehouse.{dimension} d where {key} not in ('UNKNOWN', 'INVALID') "
            f"and not exists (select 1 from staging.{staging} s where s.{key}::BIGINT::TEXT = d.{key})"
        )
        assert invented == [], (dimension, invented)
    assert sql(
        "select count(*) from warehouse.dim_campaign where campaign_name ilike 'inferred%' "
        "union all select count(*) from warehouse.dim_customer where customer_name ilike 'inferred%' "
        "union all select count(*) from warehouse.dim_product where product_name ilike 'inferred%'"
    ) == [["0"], ["0"], ["0"]]


# Part J: a fact is never dropped because its dimension reference is bad.
# (fact table, its natural key column, dimension, Silver table, Silver key column)
REFERENCES = [
    ("fact_orders", "order_id", "customer", "silver_orders", "user_id"),
    ("fact_orders", "order_id", "campaign", "silver_orders", "campaign_id"),
    ("fact_order_items", "order_item_nk", "product", "silver_order_items", "product_id"),
    ("fact_campaign_spend", "campaign_spend_nk", "campaign", "silver_ad_spend", "campaign_id"),
    ("fact_web_events", "event_id", "customer", "silver_web_events", "user_id"),
    ("fact_web_events", "event_id", "product", "silver_web_events", "product_id"),
    ("fact_web_events", "event_id", "campaign", "silver_web_events", "campaign_id"),
]


def test_no_fact_is_dropped_for_a_bad_dimension_reference():
    data = PROJECT_ROOT / "data"
    # Every Silver order, ad-spend row and web event is a warehouse fact. An
    # item is rejected only for its own defect: its order is not in Silver.
    assert int(sql("select count(*) from warehouse.fact_orders")[0][0]) == parquet_count(data / "silver" / "silver_orders")
    assert int(sql("select count(*) from warehouse.fact_campaign_spend")[0][0]) == parquet_count(data / "silver" / "silver_ad_spend")
    assert int(sql("select count(*) from warehouse.fact_web_events")[0][0]) == parquet_count(data / "silver" / "silver_web_events")
    assert not sql("select 1 from information_schema.tables where table_name = 'fact_web_events_rejects'")
    report = pit_report()["facts"]
    assert report["fact_orders_scd2"]["conservation"]["rejected_rows"] == 0
    assert report["fact_campaign_spend_scd2"]["conservation"]["rejected_rows"] == 0
    import duckdb

    item_rejects = data / "quarantine" / "fact_order_items_scd2_rejects"
    reasons = duckdb.sql(f"select reject_reason, count(*) from read_parquet('{item_rejects}/**/*.parquet', "
                         "hive_partitioning=false, union_by_name=true) group by 1").fetchall()
    assert [reason for reason, _ in reasons] == ["order not in fact_orders_scd2"], reasons
    assert int(sql("select count(*) from warehouse.fact_order_items")[0][0]) + reasons[0][1] \
        == parquet_count(data / "silver" / "silver_order_items")

    checks = {row[0]: row for row in sql(
        "select check_name, status, source_count::BIGINT, target_count::BIGINT from audit.v_latest_reconciliation_report")}
    with (data / "bi_exports" / "data_quality_summary.csv").open(encoding="utf-8", newline="") as f:
        summary = {row["metric_name"]: row for row in csv.DictReader(f)}
    for fact, natural_key, dimension, silver, silver_key in REFERENCES:
        # On the invalid-reference member (-1): the facts whose key is present
        # and whose dimension row is not valid. They keep their natural ID.
        invalid = sql(f"select {dimension}_id from warehouse.{fact} where {dimension}_sk = -1")
        assert invalid, f"the sample data has {fact} rows for quarantined {dimension}s"
        ids = {row[0] for row in invalid}
        assert "INVALID" not in ids and "UNKNOWN" not in ids, "the fact keeps its own natural ID"
        assert sql(f"select count(*) from warehouse.dim_{dimension} where {dimension}_id = any(%s)"
                   .replace("%s", "array[" + ",".join(f"'{i}'" for i in sorted(ids)) + "]")) == [["0"]]
        # A missing key is the unknown member (0), never the invalid one.
        assert sql(f"select count(*) from warehouse.{fact} where {dimension}_id = 'UNKNOWN' and {dimension}_sk <> 0") == [["0"]]
        # The count is in the reconciliation and in data_quality_summary.csv.
        name = f"Invalid {dimension} references in {fact} are on the invalid-reference member"
        assert checks[name][1:] == ["PASS", str(len(invalid)), str(len(invalid))], checks[name]
        row = summary[f"Warehouse rows on the invalid-reference member, {dimension}: {fact}"]
        assert (int(row["metric_value"]), row["status"], row["metric_group"]) == (len(invalid), "PASS", "Invalid References")
    for fact, keys in (("fact_orders_scd2", ("customer_sk", "campaign_sk")), ("fact_order_items_scd2", ("product_sk",)),
                       ("fact_campaign_spend_scd2", ("campaign_sk",))):
        for key in keys:
            gold = report[fact]["fact_resolutions"][key]["invalid_reference"]
            assert int(summary[f"Gold rows on the invalid-reference member, {key}: {fact}"]["metric_value"]) == gold > 0
    # Gold and the warehouse agree on which orders have an invalid customer.
    assert report["fact_orders_scd2"]["fact_resolutions"]["customer_sk"]["invalid_reference"] == \
        int(sql("select count(*) from warehouse.fact_orders where customer_sk = -1")[0][0])

    assert checks["Staging web event row count equals warehouse fact_web_events row count"][1] == "PASS"
    # A web event with no campaign is on the unknown campaign (0), as before.
    assert int(sql(
        "select count(*) from warehouse.fact_web_events f join staging.stg_gold_fact_web_events s "
        "on s.event_id::TEXT = f.event_id where s.campaign_id is null and f.campaign_sk = 0"
    )[0][0]) > 0
    # Conversions are built from the kept events.
    assert sql("select count(*) from warehouse.fact_conversions c join warehouse.fact_web_events f using (event_id) "
               "where c.customer_sk <> f.customer_sk") == [["0"]]


def test_a_fact_resolves_to_the_real_member_once_its_dimension_row_is_valid():
    # The rule that makes that happen, on the built warehouse: the member is -1
    # exactly when the fact's natural ID is in no version of the dimension, so
    # a build after the dimension row turns valid gives the real key.
    for fact, _, dimension, _, _ in REFERENCES:
        assert sql(
            f"select count(*) from warehouse.{fact} f where (f.{dimension}_sk = -1) <> "
            f"(not exists (select 1 from warehouse.dim_{dimension} d where d.{dimension}_id = f.{dimension}_id))"
        ) == [["0"]], (fact, dimension)
        assert sql(
            f"select count(*) from warehouse.{fact} f join warehouse.dim_{dimension} d using ({dimension}_sk) "
            f"where f.{dimension}_sk > 0 and d.{dimension}_id <> f.{dimension}_id"
        ) == [["0"]], (fact, dimension)


def test_scd2_version_counts_match_gold():
    checks = {row[0]: row for row in sql(
        "select check_name, status, source_count::BIGINT, target_count::BIGINT from audit.v_latest_reconciliation_report")}
    data = PROJECT_ROOT / "data" / "gold"
    for dimension, gold in (("customer", "dim_customers_scd2"), ("product", "dim_products_scd2"),
                            ("campaign", "dim_campaigns_scd2")):
        versions = parquet_count(data / gold)
        row = checks[f"Gold {dimension} SCD2 version count equals warehouse dim_{dimension} version count"]
        assert row[1:] == ["PASS", str(versions), str(versions)], row
        assert int(sql(f"select count(*) from warehouse.dim_{dimension}")[0][0]) == versions + 2, \
            "plus the unknown and the invalid-reference member"


# Part J: real source attributes.

def test_order_status_is_real_and_revenue_counts_completed_orders_only():
    statuses = {row[0]: int(row[1]) for row in sql("select order_status, count(*) from warehouse.fact_orders group by 1")}
    assert set(statuses) == {"completed", "cancelled", "returned"}, statuses
    total = sum(statuses.values())
    assert 0.80 < statuses["completed"] / total < 0.95 and statuses["cancelled"] > statuses["returned"] > 0, statuses
    # The warehouse has the status Silver has, order by order.
    assert sql("select count(*) from warehouse.fact_orders f join staging.stg_gold_fact_orders_scd2 s "
               "on s.order_id::TEXT = f.order_id where s.order_status <> f.order_status") == [["0"]]

    completed, not_sold = sql(
        "select coalesce(sum(net_revenue) filter (where order_status = 'completed'), 0), "
        "coalesce(sum(net_revenue) filter (where order_status <> 'completed'), 0) from warehouse.fact_orders")[0]
    assert float(not_sold) > 0
    daily = sql("select sum(net_revenue), sum(cancelled_amount + returned_amount), sum(order_count), "
                "sum(cancelled_order_count), sum(returned_order_count) from marts.mart_revenue_daily")[0]
    assert (daily[0], daily[1]) == (completed, not_sold), "daily revenue is that of completed orders"
    assert [int(v) for v in daily[2:]] == [statuses["completed"], statuses["cancelled"], statuses["returned"]]
    # Lifetime value, campaign revenue and product revenue: completed orders only.
    assert sql(
        "select count(*) from marts.mart_customer_360 m join (select customer_id, "
        "coalesce(sum(net_revenue) filter (where order_status = 'completed'), 0) as value, "
        "count(*) filter (where order_status = 'completed') as orders, "
        "count(*) filter (where order_status = 'cancelled') as cancelled, "
        "count(*) filter (where order_status = 'returned') as returned from warehouse.fact_orders "
        "where customer_sk > 0 group by 1) f using (customer_id) "
        "where m.lifetime_value <> round(f.value, 2) or m.total_order_count <> f.orders "
        "or m.cancelled_order_count <> f.cancelled or m.returned_order_count <> f.returned") == [["0"]]
    assert sql(
        "select (select sum(attributed_revenue) from marts.mart_campaign_performance where member_type = 'known') = "
        "(select sum(net_revenue) from warehouse.fact_orders where order_status = 'completed' and campaign_sk > 0), "
        "(select sum(product_revenue) from marts.mart_product_sales where member_type = 'known') = "
        "(select sum(i.line_revenue) from warehouse.fact_order_items i join warehouse.fact_orders o using (order_sk) "
        " where o.order_status = 'completed' and i.product_sk > 0), "
        "(select sum(units_sold) from marts.mart_product_sales where member_type = 'known') = "
        "(select sum(i.quantity) from warehouse.fact_order_items i join warehouse.fact_orders o using (order_sk) "
        " where o.order_status = 'completed' and i.product_sk > 0)") == [["t", "t", "t"]]
    # Route B: the Gold marts agree with the warehouse on completed revenue.
    import duckdb

    gold = PROJECT_ROOT / "data" / "gold"
    clv = duckdb.sql(f"select sum(total_revenue), sum(cancelled_amount + returned_amount) "
                     f"from read_parquet('{gold}/mart_customer_lifetime_value_scd2/*.parquet') "
                     "where member_type = 'known'").fetchone()
    valid_customer = sql("select sum(net_revenue) filter (where order_status = 'completed'), "
                         "sum(net_revenue) filter (where order_status <> 'completed') "
                         "from warehouse.fact_orders where customer_sk > 0")[0]
    assert [round(v, 2) for v in clv] == [float(v) for v in valid_customer]


def test_tax_shipping_and_order_total_are_real_and_add_up():
    assert sql("select count(*) from warehouse.fact_orders "
               "where total_amount <> net_revenue + tax_amount + shipping_amount") == [["0"]]
    orders, taxed, shipped, free = (int(v) for v in sql(
        "select count(*), count(*) filter (where tax_amount = round(net_revenue * 0.18, 2)), "
        "count(*) filter (where shipping_amount > 0), count(*) filter (where shipping_amount = 0) "
        "from warehouse.fact_orders")[0])
    assert taxed == orders, "tax is 18% of the net value"
    # J3: with realistic order values the shipping rule applies to a meaningful share.
    assert 0.20 < shipped / orders < 0.70 and free > 0, (shipped, orders)
    assert sql("select count(*) from warehouse.fact_orders "
               "where (net_revenue >= 4999) <> (shipping_amount = 0) or shipping_amount not in (0, 99)") == [["0"]]
    assert sql("select sum(total_amount) = sum(net_revenue + tax_amount + shipping_amount) "
               "from marts.mart_revenue_daily") == [["t"]]
    checks = dict(sql("select check_name, status from audit.v_latest_reconciliation_report"))
    assert checks["Order total equals net revenue plus tax plus shipping for every order"] == "PASS"


def test_ad_platform_results_are_real_and_beside_the_warehouse_figures():
    rows, with_conversions, impossible = (int(v) for v in sql(
        "select count(*), count(*) filter (where conversions > 0), "
        "count(*) filter (where conversions > clicks or clicks > impressions "
        "or (conversions = 0) <> (attributed_revenue = 0)) from warehouse.fact_campaign_spend")[0])
    assert with_conversions > 0.2 * rows and impossible == 0
    assert sql("select count(*) from warehouse.fact_campaign_spend f join staging.stg_gold_fact_campaign_spend_scd2 s "
               "on s.spend_id::TEXT = f.campaign_spend_nk "
               "where s.conversions <> f.conversions or s.attributed_revenue::NUMERIC(18, 2) <> f.attributed_revenue") == [["0"]]
    assert sql(
        "select (select sum(ad_platform_conversions) from marts.mart_campaign_performance) = "
        "(select sum(conversions) from warehouse.fact_campaign_spend), "
        "(select sum(platform_attributed_revenue) from marts.mart_campaign_performance) = "
        "(select sum(attributed_revenue) from warehouse.fact_campaign_spend), "
        "(select count(*) from marts.mart_campaign_performance "
        " where platform_attributed_revenue <> attributed_revenue) > 0") == [["t", "t", "t"]], \
        "the platform's figures are shown beside the warehouse-measured ones, not instead of them"


def test_product_brand_status_and_campaign_type_are_real():
    brands, statuses = sql("select count(distinct brand), count(distinct product_status) "
                           "from warehouse.dim_product where product_sk > 0")[0]
    assert int(brands) > 10 and int(statuses) == 2
    # A status change alone opens a new SCD2 version (same price before and after).
    status_versions = sql(
        "select count(*) from warehouse.dim_product a join warehouse.dim_product b "
        "on a.product_id = b.product_id and a.effective_to = b.effective_from "
        "where a.product_status <> b.product_status and a.current_price = b.current_price")
    assert int(status_versions[0][0]) >= 1, "the 2026-06-18 batch changes one product's status only"
    assert sql("select count(*) from marts.mart_product_sales m join warehouse.dim_product d using (product_id) "
               "where d.is_current and (m.brand <> d.brand or m.product_status <> d.product_status)") == [["0"]]
    assert int(sql("select count(distinct campaign_type) from marts.mart_campaign_performance "
                   "where member_type = 'known'")[0][0]) == 4


def test_every_new_dirty_row_is_quarantined_with_its_reason():
    import duckdb

    def reasons(table):
        path = PROJECT_ROOT / "data" / "quarantine" / f"quarantine_{table}"
        return dict(duckdb.sql(f"select quarantine_reason, count(*) from read_parquet('{path}/**/*.parquet', "
                               "hive_partitioning=false, union_by_name=true) group by 1").fetchall())

    batches = len(BATCH_DATES)
    products, campaigns, orders, spend = reasons("products"), reasons("campaigns"), reasons("orders"), reasons("ad_spend")
    assert products["Invalid product record: product_status must be active or discontinued"] == batches
    assert products["Invalid product record: brand is missing"] == batches
    assert campaigns["Invalid campaign record: campaign_type must be awareness, consideration, conversion or retention"] == batches
    assert orders["Invalid order record: order_status must be completed, cancelled or returned"] == batches
    assert orders["Invalid order record: tax_amount is missing or negative"] == batches
    assert orders["Invalid order record: shipping_amount is missing or negative"] == batches
    assert orders["Invalid order record: total_amount is not cart_value + tax_amount + shipping_amount"] == batches
    assert spend["Invalid ad spend record: conversions are missing, negative or more than the clicks"] == batches
    assert spend["Invalid ad spend record: attributed_revenue is missing or negative"] == batches
    assert spend["Invalid ad spend record: region (country, state) is missing"] == batches
    # negative spend, clicks above impressions and a missing date; the row for a
    # campaign nobody delivered is valid and kept on the invalid-reference member.
    assert spend["Invalid ad spend record: bad id/date/impressions/clicks/spend"] == 3 * batches
    assert int(sql("select count(*) from warehouse.fact_campaign_spend where campaign_id = '999999' "
                   "and campaign_sk = -1")[0][0]) == batches


# Carried SCD2 attributes: (dimension, column, Gold SCD2 staging table).
CARRIED_ATTRIBUTES = [
    ("dim_customer", "gender", "stg_gold_dim_customers_scd2"),
    ("dim_customer", "age", "stg_gold_dim_customers_scd2"),
    ("dim_customer", "preferred_language", "stg_gold_dim_customers_scd2"),
    ("dim_customer", "loyalty_points", "stg_gold_dim_customers_scd2"),
    ("dim_customer", "is_prime_user", "stg_gold_dim_customers_scd2"),
    ("dim_product", "original_price", "stg_gold_dim_products_scd2"),
    ("dim_product", "inventory_remaining", "stg_gold_dim_products_scd2"),
    ("dim_campaign", "campaign_status", "stg_gold_dim_campaigns_scd2"),
    ("dim_campaign", "ab_test_group", "stg_gold_dim_campaigns_scd2"),
    ("dim_campaign", "campaign_type", "stg_gold_dim_campaigns_scd2"),
    ("dim_product", "brand", "stg_gold_dim_products_scd2"),
    ("dim_product", "product_status", "stg_gold_dim_products_scd2"),
]


@pytest.mark.parametrize("dimension,column,staging", CARRIED_ATTRIBUTES)
def test_dimension_attributes_carry_the_source_values(dimension, column, staging):
    key = dimension.replace("dim_", "") + "_sk"
    warehouse_rows, warehouse_values, distinct = (int(v) for v in sql(
        f"select count(*), count({column}), count(distinct {column}) from warehouse.{dimension} "
        f"where {key} not in (0, -1)")[0])
    # Gold calls a product's status "status"; the warehouse, product_status.
    staging_column = {"product_status": "status"}.get(column, column)
    staging_rows, staging_values = (int(v) for v in sql(
        f"select count(*), count({staging_column}) from staging.{staging}")[0])
    # Every Gold SCD2 version is in the warehouse with its value; none is
    # replaced by a default, none is lost.
    assert (warehouse_rows, warehouse_values) == (staging_rows, staging_values)
    assert warehouse_values > 0 and distinct > 1, (dimension, column, warehouse_values, distinct)


def test_orders_take_the_channel_of_their_campaign():
    by_campaign = dict(sql(
        "select case when campaign_sk = 0 then 'organic' when campaign_sk = -1 then 'invalid' else 'campaign' end, "
        "count(*) filter (where channel_sk = 0) from warehouse.fact_orders group by 1"
    ))
    # An order on a real campaign has that campaign's channel. An organic order
    # and an order whose campaign has no valid dimension row have none (0).
    assert by_campaign == {"organic": by_campaign["organic"], "invalid": by_campaign["invalid"], "campaign": "0"}
    assert int(by_campaign["invalid"]) == int(sql(
        "select count(*) from warehouse.fact_orders where campaign_sk = -1")[0][0]) > 0
    assert int(by_campaign["organic"]) == int(sql(
        "select count(*) from warehouse.fact_orders where campaign_sk = 0")[0][0]) > 0
    # The channel is the order's campaign version's channel; items share it.
    assert sql(
        "select count(*) from warehouse.fact_orders fo join warehouse.dim_campaign dc using (campaign_sk) "
        "join warehouse.dim_channel ch using (channel_sk) "
        "where fo.campaign_sk <> -1 and ch.channel_nk <> warehouse.channel_nk(dc.channel)"
    ) == [["0"]]
    assert sql(
        "select count(*) from warehouse.fact_order_items foi join warehouse.fact_orders fo using (order_sk) "
        "where foi.channel_sk <> fo.channel_sk"
    ) == [["0"]]


@pytest.mark.parametrize("column", ["search_query", "time_on_page_sec", "scroll_depth_percent",
                                    "schema_version", "source_system"])
def test_web_event_engagement_and_lineage_fields_are_loaded(column):
    loaded = int(sql(f"select count({column}) from warehouse.fact_web_events")[0][0])
    expected = int(sql(
        f"select count(s.{column}) from staging.stg_gold_fact_web_events s "
        f"join warehouse.fact_web_events f on f.event_id = s.event_id::TEXT"
    )[0][0])
    assert loaded == expected > 0


def test_order_gross_and_discount_are_the_sums_of_their_items():
    mismatched, total, gross_mismatched = sql(
        "select count(*) filter (where fo.discount_amount <> coalesce(i.discount, 0)), sum(fo.discount_amount), "
        "count(*) filter (where fo.gross_amount <> coalesce(i.gross, 0)) "
        "from warehouse.fact_orders fo left join (select order_sk, sum(discount_amount) as discount, "
        "sum(gross_amount) as gross from warehouse.fact_order_items group by order_sk) i using (order_sk)"
    )[0]
    assert mismatched == "0" and float(total) > 0
    assert gross_mismatched == "0"
    # gross - discount = net for every complete order (its loaded lines add up to
    # its net revenue); the incomplete ones are exactly those the point-in-time
    # order totals check excludes.
    complete_mismatched, excluded = (int(v) for v in sql(
        "select count(*) filter (where i.lines = fo.net_revenue "
        "and fo.gross_amount - fo.discount_amount <> fo.net_revenue), "
        "count(*) filter (where i.lines is distinct from fo.net_revenue) "
        "from warehouse.fact_orders fo left join (select order_sk, sum(line_revenue) as lines "
        "from warehouse.fact_order_items group by order_sk) i using (order_sk)"
    )[0])
    assert complete_mismatched == 0
    assert excluded == pit_report()["order_totals"]["excluded_incomplete_orders"]
    checks = dict(sql("select check_name, status from audit.v_latest_reconciliation_report"))
    assert checks["Order gross minus discount equals net revenue for every complete order"] == "PASS"
    # Net revenue (the discounted order total) still equals Gold's.
    checks = dict(sql("select check_name, status from audit.v_latest_reconciliation_report"))
    assert checks["Gold revenue equals warehouse fact_orders net revenue"] == "PASS"
    # Each item's discount is (original_price - discounted_price) x quantity from Gold.
    assert sql(
        "select count(*) from warehouse.fact_order_items f join staging.stg_gold_fact_order_items_scd2 s "
        "on s.order_item_id::TEXT = f.order_item_nk "
        "where f.discount_amount is distinct from ((s.original_price - s.discounted_price) * s.quantity)::NUMERIC(18, 2) "
        "or f.gross_amount is distinct from (s.original_price * s.quantity)::NUMERIC(18, 2)"
    ) == [["0"]]


# Warehouse and mart columns that are allowed to be one constant value, each
# with the reason: a value the source really delivers that happens not to vary
# in the sample. A column the source has no data for is not listed: it does not
# exist (Part I removed them). Every other column must vary, and a column listed
# here that varies again is stale and fails too.
CONSTANT_COLUMNS_ALLOWED = {
    ("*", "warehouse_loaded_at"): "set once per build",
    ("warehouse.fact_conversions", "conversion_type"): "the sample's only conversion event is 'purchase'",
    ("warehouse.fact_web_events", "schema_version"): "every source row is schema v1",
    ("warehouse.fact_web_events", "source_system"): "one source system: web_events",
    ("marts.mart_marketing_funnel", "year_number"): "every sample event is in 2026",
    ("marts.mart_revenue_daily", "year_number"): "every sample order is in 2026",
}


def column_profile() -> dict[tuple[str, str], str]:
    """{(relation, column): why it is degenerate} for warehouse and mart columns."""
    columns: dict[str, list[str]] = {}
    for schema, table, column in sql(
        "select table_schema, table_name, column_name from information_schema.columns "
        "where table_schema in ('warehouse', 'marts') order by table_schema, table_name, ordinal_position"
    ):
        columns.setdefault(f"{schema}.{table}", []).append(column)
    degenerate: dict[tuple[str, str], str] = {}
    for relation, names in columns.items():
        parts = ["count(*)"]
        for name in names:
            parts += [f'count("{name}")', f'count(distinct "{name}"::TEXT)',
                      f"""count(*) filter (where "{name}"::TEXT ~* '^(unknown|inferred)')"""]
        row = [int(v) for v in sql(f"select {', '.join(parts)} from {relation}")[0]]
        if row[0] == 0:
            degenerate[(relation, "*")] = "no rows"
            continue
        for index, name in enumerate(names):
            values, distinct, unknown = row[1 + 3 * index: 4 + 3 * index]
            if values == 0:
                degenerate[(relation, name)] = "entirely NULL"
            elif unknown == values:
                degenerate[(relation, name)] = "entirely Unknown/Inferred"
            elif distinct == 1:
                degenerate[(relation, name)] = "one constant value"
    return degenerate


def test_no_warehouse_or_mart_column_is_empty_unknown_or_constant():
    degenerate = column_profile()

    def allowed(key):
        return key in CONSTANT_COLUMNS_ALLOWED or ("*", key[1]) in CONSTANT_COLUMNS_ALLOWED

    assert {key: why for key, why in degenerate.items() if not allowed(key)} == {}
    stale = [key for key in CONSTANT_COLUMNS_ALLOWED if key[0] != "*" and key not in degenerate]
    assert stale == [], "these columns vary now; remove them from CONSTANT_COLUMNS_ALLOWED"


def test_every_key_is_a_63_bit_hash():
    # No key function is 32-bit, and dimension keys are far outside that range.
    assert sql("select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace "
               "where n.nspname = 'warehouse' and p.prosrc ilike '%hashtext%'") == [["0"]]
    for dimension in ("customer", "product", "campaign", "region"):
        above = int(sql(f"select count(*) from warehouse.dim_{dimension} where {dimension}_sk > 4294967296")[0][0])
        total = int(sql(f"select count(*) from warehouse.dim_{dimension} where {dimension}_sk not in (0, -1)")[0][0])
        assert above == total > 0, (dimension, above, total)


FACT_NATURAL_IDS = {
    "fact_orders": {"customer": "customer_id", "campaign": "campaign_id"},
    "fact_order_items": {"customer": "customer_id", "product": "product_id"},
    "fact_campaign_spend": {"campaign": "campaign_id"},
    "fact_web_events": {"customer": "customer_id", "product": "product_id", "campaign": "campaign_id"},
    "fact_conversions": {"customer": "customer_id", "product": "product_id", "campaign": "campaign_id"},
}


@pytest.mark.parametrize("fact", sorted(FACT_NATURAL_IDS))
def test_facts_carry_natural_ids_source_time_and_record_hash(fact):
    rows = int(sql(f"select count(*) from warehouse.{fact}")[0][0])
    for dimension, column in FACT_NATURAL_IDS[fact].items():
        # The natural ID beside each key is that dimension member's ID
        # ('UNKNOWN' exactly where the key is the unknown member). On the
        # invalid-reference member (-1) it is the fact's own source ID.
        assert sql(
            f"select count(*) from warehouse.{fact} f join warehouse.dim_{dimension} d using ({dimension}_sk) "
            f"where f.{dimension}_sk <> -1 and d.{column} <> f.{column}"
        ) == [["0"]], (fact, column)
        assert sql(f"select count(*) from warehouse.{fact} where {dimension}_sk = -1 "
                   f"and {column} in ('INVALID', 'UNKNOWN')") == [["0"]], (fact, column)
    hashes, stamped, batches = sql(
        f"select count(distinct record_hash), count(source_updated_at), "
        f"string_agg(distinct source_updated_at::date::text, ',' order by source_updated_at::date::text) "
        f"from warehouse.{fact}")[0]
    assert int(hashes) == rows and int(stamped) == rows
    # source_updated_at is the batch that delivered the row, never the load time.
    assert set(batches.split(",")) <= set(BATCH_DATES), batches


def test_the_reader_role_reads_but_cannot_write_or_see_staging():
    def as_reader(query):
        return subprocess.run(
            ["docker", "exec", "-i", PGCONTAINER, "psql", "-U", "project2", "-d", "marketing_analytics",
             "-v", "ON_ERROR_STOP=1", "-At", "-c", f"set role p2_reader; {query}"],
            capture_output=True, text=True)

    for relation in ("warehouse.fact_orders", "marts.mart_revenue_daily", "audit.v_latest_reconciliation_report"):
        result = as_reader(f"select count(*) from {relation}")
        assert result.returncode == 0 and int(result.stdout.split()[-1]) > 0, (relation, result.stderr)
    for forbidden in ("select count(*) from staging.stg_gold_fact_orders_scd2",
                      "delete from warehouse.fact_orders", "create table marts.mine (x int)"):
        result = as_reader(forbidden)
        assert result.returncode != 0 and "permission denied" in result.stderr, (forbidden, result.stderr)


def test_every_warehouse_mart_and_audit_timestamp_is_an_instant():
    # H2: TIMESTAMPTZ everywhere a consumer reads; business dates stay IST dates.
    assert sql(
        "select table_schema || '.' || table_name || '.' || column_name from information_schema.columns "
        "where table_schema in ('warehouse', 'marts', 'audit') and data_type = 'timestamp without time zone'"
    ) == []
    typed = dict(sql(
        "select table_schema || '.' || table_name || '.' || column_name, data_type from information_schema.columns "
        "where (table_schema, table_name, column_name) in (('warehouse', 'fact_orders', 'order_timestamp'), "
        "('warehouse', 'fact_web_events', 'event_timestamp'), ('warehouse', 'dim_customer', 'effective_from'), "
        "('warehouse', 'fact_orders', 'source_updated_at'), ('marts', 'mart_customer_360', 'last_order_timestamp'), "
        "('marts', 'mart_product_sales', 'first_sold_timestamp'), ('warehouse', 'fact_orders', 'order_date_sk'))"
    ))
    assert typed.pop("warehouse.fact_orders.order_date_sk") == "integer"
    assert len(typed) == 6 and set(typed.values()) == {"timestamp with time zone"}, typed
    # Readers get UTC whatever their client's zone: it is the database's setting.
    assert sql("show timezone") == [["UTC"]]
    # source_updated_at is midnight UTC of the batch date.
    assert sql("select distinct (source_updated_at at time zone 'UTC')::time::text from warehouse.fact_orders") == [["00:00:00"]]


def test_no_schema_of_an_unpublished_or_stale_generation_is_left_behind():
    # staging_next is published by the build; at most the generation retired by
    # the last build (and one that dbt views still read) remains.
    names = [row[0] for row in sql("select nspname from pg_namespace where nspname ~ '_next$|_retired_' order by 1")]
    assert "staging_next" not in names
    assert len({name.rsplit("_", 1)[1] for name in names}) <= 2, names


def test_point_in_time_rows_carry_their_own_batch_date():
    import duckdb

    facts = PROJECT_ROOT / "data" / "gold" / "fact_orders_scd2"
    mismatched, stamps = duckdb.sql(
        f"select count(*) filter (where processed_date <> ingestion_date), "
        f"string_agg(distinct processed_date::varchar, ',' order by processed_date::varchar) "
        f"from read_parquet('{facts}/**/*.parquet', hive_partitioning=false, union_by_name=true)"
    ).fetchone()
    assert mismatched == 0 and stamps == ",".join(BATCH_DATES)
    bronze = PROJECT_ROOT / "data" / "bronze" / "bronze_orders"
    paths = duckdb.sql(
        f"select distinct source_file_name from read_parquet('{bronze}/**/*.parquet', hive_partitioning=false)"
    ).fetchall()
    assert sorted(p[0] for p in paths) == [f"data/raw/batch_date={d}/orders.csv" for d in BATCH_DATES]


def test_golden_counts():
    expected = json.loads(EXPECTED_COUNTS.read_text(encoding="utf-8"))
    actual = collect_counts()
    differences = {
        f"{group}.{name}": (expected[group].get(name), actual[group].get(name))
        for group in ("lake", "warehouse")
        for name in sorted(set(expected[group]) | set(actual[group]))
        if expected[group].get(name) != actual[group].get(name)
    }
    assert differences == {}, f"(expected, actual): {differences}"


if __name__ == "__main__":
    if sys.argv[1:] != ["--write-expected"]:
        raise SystemExit("usage: python tests/integration/test_warehouse.py --write-expected")
    counts = {"batch_dates": BATCH_DATES, **collect_counts()}
    EXPECTED_COUNTS.write_text(json.dumps(counts, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {EXPECTED_COUNTS}")


# --- Part J3 ------------------------------------------------------------------------

def test_ad_spend_is_per_campaign_day_and_region_and_sums_to_the_campaign_day():
    rows, grains, regions, unknown_region = (int(v) for v in sql(
        "select count(*), count(distinct (campaign_id, spend_date, region_sk)), count(distinct region_sk), "
        "count(*) filter (where region_sk = 0) from warehouse.fact_campaign_spend")[0])
    assert rows == grains and unknown_region == 0 and regions >= 10
    # Each campaign targets a few states (2 to 5), the same ones every day.
    targeted = sql("select min(n), max(n) from (select campaign_id, count(distinct region_sk) n "
                   "from warehouse.fact_campaign_spend where campaign_sk > 0 group by 1) c")[0]
    assert (int(targeted[0]), int(targeted[1])) >= (2, 2) and int(targeted[1]) <= 5
    assert sql("select count(*) from warehouse.fact_campaign_spend f join warehouse.dim_region r using (region_sk) "
               "where r.state = 'Unknown' or r.city <> 'Unknown' or r.country <> 'India'") == [["0"]]
    # Regional rows sum to the platform's campaign-day total (carried in Gold and staging).
    assert sql(
        "select count(*) from (select f.campaign_id, f.spend_date from warehouse.fact_campaign_spend f "
        "join staging.stg_gold_fact_campaign_spend_scd2 s on s.spend_id::TEXT = f.campaign_spend_nk "
        "group by 1, 2 having sum(f.spend_amount) <> max(s.campaign_day_spend)::NUMERIC(18, 2)) d") == [["0"]]
    checks = dict(sql("select check_name, status from audit.v_latest_reconciliation_report"))
    assert checks["Regional ad spend rows sum to the campaign-day total"] == "PASS"
    # A campaign's budget is compared with its spend summed over regions and days.
    assert sql(
        "select count(*) from marts.mart_campaign_performance m join (select campaign_id, sum(spend_amount) spend "
        "from warehouse.fact_campaign_spend group by 1) f using (campaign_id) "
        "where m.total_spend <> round(f.spend, 2) or m.budget_remaining <> round(m.campaign_budget - f.spend, 2)") == [["0"]]


def test_spend_and_roas_by_channel_and_by_region_are_answerable():
    import duckdb

    # From the warehouse: by channel (spend and the orders of the campaign's channel).
    by_channel = sql(
        "select ch.channel_name, round(sum(f.spend_amount), 2), round(sum(f.attributed_revenue) / sum(f.spend_amount), 2) "
        "from warehouse.fact_campaign_spend f join warehouse.dim_channel ch using (channel_sk) group by 1 order by 1")
    assert len(by_channel) >= 5 and all(0.5 < float(roas) < 9 for _, _, roas in by_channel), by_channel
    # By region: spend and platform ROAS per state, and warehouse ROAS from the
    # completed orders of the customers who live in that state.
    by_region = sql(
        "with spend as (select r.state, sum(f.spend_amount) spend, sum(f.attributed_revenue) platform_revenue "
        "  from warehouse.fact_campaign_spend f join warehouse.dim_region r using (region_sk) group by 1), "
        "revenue as (select r.state, sum(o.net_revenue) revenue from warehouse.fact_orders o "
        "  join warehouse.dim_region r using (region_sk) where o.order_status = 'completed' group by 1) "
        "select s.state, round(s.spend, 2), round(s.platform_revenue / s.spend, 2), round(coalesce(v.revenue, 0) / s.spend, 2) "
        "from spend s left join revenue v using (state) order by 1")
    assert len(by_region) >= 10
    assert all(0.5 < float(platform) < 9 for _, _, platform, _ in by_region), by_region
    assert sum(float(warehouse) > 0 for *_, warehouse in by_region) >= 10, "orders come from the targeted states"
    # An order's and a web event's region has a real state now (it was 'Unknown' for every one).
    assert sql("select (select count(*) from warehouse.fact_orders f join warehouse.dim_region r using (region_sk) "
               "where r.state = 'Unknown' and r.city <> 'Unknown'), (select count(*) from warehouse.fact_web_events f "
               "join warehouse.dim_region r using (region_sk) where r.state = 'Unknown' and r.city <> 'Unknown')") == [["0", "0"]]
    # From the exports (route B writes these Gold tables with SELECT *): the same two answers.
    gold = PROJECT_ROOT / "data" / "gold"
    exported = duckdb.sql(
        f"select state, channel, round(sum(spend_amount), 2), round(sum(attributed_revenue) / sum(spend_amount), 2) "
        f"from read_parquet('{gold}/fact_campaign_spend_scd2/**/*.parquet', hive_partitioning=false) group by 1, 2").fetchall()
    assert len({state for state, *_ in exported}) == len(by_region) and len({channel for _, channel, *_ in exported}) >= 5
    states = duckdb.sql(f"select count(distinct state) from read_parquet('{gold}/fact_orders_scd2/**/*.parquet', "
                        "hive_partitioning=false) where order_status = 'completed'").fetchone()[0]
    assert states >= 10, "revenue by state is in the orders export"
    # The two queries of the dashboard pack run and return a row per channel and per state.
    pack = (PROJECT_ROOT / "sql" / "warehouse" / "dashboard_query_pack.sql").read_text(encoding="utf-8")
    channel_query = pack[pack.index("Ad spend and return on ad spend by channel"):pack.index("Ad spend and return on ad spend by region")]
    region_query = pack[pack.index("Ad spend and return on ad spend by region"):]
    for query, expected in ((channel_query, len(by_channel)), (region_query, len(by_region))):
        statement = query[query.index("WITH spend AS"):query.rindex(";")]
        assert len(sql(statement)) == expected


def test_the_ad_feed_and_the_order_values_are_realistic():
    clicks, impressions, spend, conversions, platform_revenue = (float(v) for v in sql(
        "select sum(clicks), sum(impressions), sum(spend_amount), sum(conversions), sum(attributed_revenue) "
        "from warehouse.fact_campaign_spend")[0])
    assert 0.005 < clicks / impressions < 0.06, "click-through rate"
    assert 5 < spend / clicks < 50, "cost per click, INR"
    assert 0.002 < conversions / clicks < 0.05, "conversion rate"
    platform_roas = platform_revenue / spend
    completed, orders = (float(v) for v in sql(
        "select sum(net_revenue), count(*) from warehouse.fact_orders where order_status = 'completed'")[0])
    warehouse_roas = completed / spend
    assert 1 < platform_roas < 9 and 1 < warehouse_roas < 9, (platform_roas, warehouse_roas)
    assert 0.33 < platform_roas / warehouse_roas < 3, "the two are comparable"
    # Per channel, in the mart, both stay in single digits.
    assert sql("select count(*) from (select channel, sum(platform_attributed_revenue) / sum(total_spend) p "
               "from marts.mart_campaign_performance where member_type = 'known' and total_spend > 0 group by 1) c "
               "where p not between 0.5 and 9") == [["0"]]
    assert 2000 < completed / orders < 20000, "average order value, INR"
    prices = sql("select category, min(original_price), max(original_price) from warehouse.dim_product "
                 "where product_sk > 0 group by 1 order by 1")
    assert {row[0] for row in prices} == {"Beauty", "Books", "Electronics", "Fashion", "Fitness", "Home"}
    assert all(100 <= float(low) and float(high) <= 15000 for _, low, high in prices), prices
    assert sql("select max(quantity) <= 3, avg(quantity) < 2 from warehouse.fact_order_items") == [["t", "t"]]


def test_every_mart_has_flagged_member_rows_and_ties_out_to_the_facts():
    for mart, key in (("mart_customer_360", "customer_id"), ("mart_campaign_performance", "campaign_id"),
                      ("mart_product_sales", "product_id")):
        members = dict(sql(f"select {key}, member_type from marts.{mart} where member_type <> 'known'"))
        assert members == {"UNKNOWN": "unknown_member", "INVALID": "invalid_reference"}, (mart, members)
        assert sql(f"select count(*) from marts.{mart} where member_type = 'known' "
                   f"and {key} in ('UNKNOWN', 'INVALID')") == [["0"]]
    assert sql("select distinct member_type from marts.mart_marketing_funnel where campaign_id in ('UNKNOWN', 'INVALID') "
               "order by 1") == [["invalid_reference"], ["unknown_member"]]
    sold = "from warehouse.fact_orders where order_status = 'completed'"
    sold_items = ("from warehouse.fact_order_items i join warehouse.fact_orders o using (order_sk) "
                  "where o.order_status = 'completed'")
    totals = sql(
        f"select (select sum(net_revenue) {sold}), (select sum(i.line_revenue) {sold_items}), "
        f"(select sum(i.quantity) {sold_items}), (select sum(spend_amount) from warehouse.fact_campaign_spend)")[0]
    marts = sql(
        "select (select sum(lifetime_value) from marts.mart_customer_360), "
        "(select sum(attributed_revenue) from marts.mart_campaign_performance), "
        "(select sum(net_revenue) from marts.mart_revenue_daily), "
        "(select sum(product_revenue) from marts.mart_product_sales), "
        "(select sum(units_sold) from marts.mart_product_sales), "
        "(select sum(total_units_purchased) from marts.mart_customer_360), "
        "(select sum(total_spend) from marts.mart_campaign_performance)")[0]
    revenue, item_revenue, units, spend = totals
    assert marts == [revenue, revenue, revenue, item_revenue, units, units, spend], (totals, marts)
    checks = dict(sql("select check_name, status from audit.mart_validation_report"))
    for name in ("customer_lifetime_value_matches_fact_orders", "customer_units_match_fact_order_items",
                 "campaign_revenue_matches_fact_orders", "product_units_match_fact_order_items",
                 "product_revenue_matches_fact_order_items", "campaign_spend_matches_fact_campaign_spend",
                 "campaign_platform_results_match_fact_campaign_spend", "daily_revenue_matches_fact_orders"):
        assert checks[name] == "PASS", name
    # Route B: the Gold marts tie out to the Gold facts in the same way.
    report = json.loads((PROJECT_ROOT / "logs" / "scd2_gold_mart_report.json").read_text(encoding="utf-8"))
    metrics = report["validation"]["metrics"] if "validation" in report else report["marts"]["validation"]["metrics"]
    assert metrics["campaign_mart_revenue"] == pytest.approx(metrics["completed_order_revenue"])
    assert metrics["clv_mart_revenue"] == pytest.approx(metrics["completed_order_revenue"])
    assert metrics["campaign_mart_spend"] == pytest.approx(metrics["fact_campaign_spend"])


def test_total_amount_means_the_charged_amount_in_every_layer():
    import duckdb

    data = PROJECT_ROOT / "data"
    for layer, path in (("silver", data / "silver" / "silver_orders"), ("gold", data / "gold" / "fact_orders"),
                        ("gold scd2", data / "gold" / "fact_orders_scd2")):
        net = "order_amount" if layer == "gold" else "cart_value"
        rows, wrong = duckdb.sql(
            f"select count(*), count(*) filter (where round(total_amount, 2) <> round({net} + tax_amount + shipping_amount, 2)) "
            f"from read_parquet('{path}/**/*.parquet', hive_partitioning=false, union_by_name=true)").fetchone()
        assert rows > 0 and wrong == 0, layer
    # The warehouse carries the same value under the same name, order by order.
    assert sql("select count(*) from warehouse.fact_orders f join staging.stg_gold_fact_orders_scd2 s "
               "on s.order_id::TEXT = f.order_id where f.total_amount <> s.total_amount::NUMERIC(18, 2) "
               "or f.net_revenue <> s.cart_value::NUMERIC(18, 2)") == [["0"]]
    assert sql("select sum(total_amount) = (select sum(total_amount) from warehouse.fact_orders "
               "where order_status = 'completed') from marts.mart_revenue_daily") == [["t"]]
    # Revenue in the Gold marts is the net value (cart_value), never the charged amount.
    clv = duckdb.sql(f"select sum(total_revenue) from read_parquet('{data}/gold/mart_customer_lifetime_value_scd2/*.parquet')").fetchone()[0]
    assert round(clv, 2) == float(sql("select sum(net_revenue) from warehouse.fact_orders where order_status = 'completed'")[0][0])


# What Project 4 and Project 5 read from this warehouse, recorded from their
# repositories (P4: src/anomaly_detection/source_extract.py and
# configs/source_extract.yaml; P5: contracts/project2_project3_sources.yaml and
# src/project5_ai/sources/). Adding a column to a relation is free. Removing,
# renaming or retyping a column listed here, emptying the relation or breaking
# its key fails this test: change the consumer first, then this contract.
CONSUMER_CONTRACT = {
    "marts.mart_customer_360": {
        "consumer": "P4 transform_customer_360 (SELECT *; entity_id = customer_id)",
        "grain": "one row per customer, plus the unknown and the invalid-reference member (member_type)",
        "key": ["customer_id"],
        "columns": {
            "customer_id": "text", "last_order_timestamp": "timestamp with time zone", "total_order_count": "bigint",
            "gross_revenue": "numeric", "total_discount_amount": "numeric", "lifetime_value": "numeric",
            "average_order_value": "numeric", "total_units_purchased": "bigint",
            "product_categories_purchased": "bigint", "distinct_products_purchased": "bigint",
            "days_since_last_order": "integer", "web_event_count": "bigint", "session_count": "bigint",
            "purchase_intent_event_count": "bigint", "customer_conversion_rate_pct": "numeric",
            "avg_engagement_score": "numeric", "avg_purchase_probability": "numeric",
            "avg_cart_abandonment_probability": "numeric", "repeat_purchase_signal": "boolean",
        },
    },
    "marts.mart_campaign_performance": {
        "consumer": "P4 transform_campaign_performance (SELECT *; entity_id = campaign_id)",
        "grain": "one row per campaign, plus the unknown and the invalid-reference member (member_type)",
        "key": ["campaign_id"],
        "columns": {
            "campaign_id": "text", "campaign_end_date": "date", "campaign_budget": "numeric", "total_spend": "numeric",
            "budget_remaining": "numeric", "impressions": "bigint", "clicks": "bigint",
            "ad_platform_conversions": "bigint", "web_event_count": "bigint", "session_count": "bigint",
            "purchase_event_count": "bigint", "order_count": "bigint", "attributed_revenue": "numeric",
            "average_order_value": "numeric", "roas": "numeric", "click_through_rate_pct": "numeric",
            "cost_per_click": "numeric", "cost_per_order": "numeric",
            "session_to_order_conversion_rate_pct": "numeric", "avg_engagement_score": "numeric",
            "avg_purchase_probability": "numeric",
        },
    },
    "marts.mart_marketing_funnel": {
        "consumer": "P4 transform_marketing_funnel (SELECT *; entity_id = campaign_id + event_date + channel_name)",
        "grain": "one row per event date, channel and campaign (the unknown and invalid campaigns included)",
        "key": ["event_date", "channel_name", "campaign_id"],
        "columns": {
            "campaign_id": "text", "event_date": "date", "channel_name": "text", "total_event_count": "bigint",
            "session_count": "bigint", "product_view_event_count": "bigint", "cart_event_count": "bigint",
            "purchase_event_count": "bigint", "funnel_conversion_rate_pct": "numeric",
            "avg_engagement_score": "numeric", "avg_purchase_probability": "numeric",
            "avg_cart_abandonment_probability": "numeric",
        },
    },
    "marts.mart_product_sales": {
        "consumer": "P4 transform_product_sales (SELECT *; entity_id = product_id)",
        "grain": "one row per product, plus the unknown and the invalid-reference member (member_type)",
        "key": ["product_id"],
        "columns": {
            "product_id": "text", "last_sold_timestamp": "timestamp with time zone", "original_price": "numeric",
            "current_price": "numeric", "inventory_remaining": "integer", "order_count": "bigint",
            "units_sold": "bigint", "product_revenue": "numeric", "average_selling_price": "numeric",
            "product_web_event_count": "bigint", "product_session_count": "bigint",
            "product_view_event_count": "bigint", "cart_event_count": "bigint", "purchase_event_count": "bigint",
            "product_conversion_rate_pct": "numeric", "avg_product_engagement_score": "numeric",
            "category_revenue_rank": "bigint", "category_revenue_share_pct": "numeric",
        },
    },
    "warehouse.fact_web_events": {
        "consumer": "P4 transform_web_events (SELECT *; grouped by customer_sk)",
        "grain": "one row per web event",
        "key": ["event_id"],
        "columns": {
            "event_id": "text", "session_id": "text", "customer_sk": "bigint", "event_type": "text",
            "event_timestamp": "timestamp with time zone", "time_on_page_sec": "integer",
            "scroll_depth_percent": "numeric", "engagement_score": "numeric", "purchase_probability": "numeric",
            "cart_abandonment_probability": "numeric", "event_value": "numeric", "api_latency_ms": "integer",
            "page_load_time_ms": "integer", "fraud_score": "numeric",
        },
    },
}
# P5 reads no relation yet: it verifies that the database has these schemas
# before it activates a source (its adapter says it does not extract rows yet).
CONSUMER_SCHEMAS = {"P5 contracts/project2_project3_sources.yaml": ["staging", "warehouse", "marts", "audit"]}


def contract_violations(contract: dict, columns: dict[str, dict[str, str]], row_counts: dict[str, int],
                        duplicate_keys: dict[str, int]) -> list[str]:
    """Every way the database breaks the contract. columns: {relation: {column: data type}}."""
    problems = []
    for relation, expected in contract.items():
        actual = columns.get(relation)
        if actual is None:
            problems.append(f"{relation}: the relation is gone")
            continue
        for column, data_type in expected["columns"].items():
            if column not in actual:
                problems.append(f"{relation}.{column}: removed or renamed")
            elif actual[column] != data_type:
                problems.append(f"{relation}.{column}: type {actual[column]}, the consumer reads {data_type}")
        if row_counts.get(relation, 0) == 0:
            problems.append(f"{relation}: no rows (P4 fails on an empty source)")
        if duplicate_keys.get(relation, 0):
            problems.append(f"{relation}: {duplicate_keys[relation]} duplicate or NULL key(s) {expected['key']}")
    return problems


def test_the_consumer_contract_of_p4_and_p5_holds():
    columns: dict[str, dict[str, str]] = {}
    for schema, table, column, data_type in sql(
        "select table_schema, table_name, column_name, data_type from information_schema.columns "
        "where table_schema in ('warehouse', 'marts')"
    ):
        columns.setdefault(f"{schema}.{table}", {})[column] = data_type
    row_counts, duplicate_keys = {}, {}
    for relation, expected in CONSUMER_CONTRACT.items():
        if relation not in columns:
            continue
        key = ", ".join(expected["key"])
        row_counts[relation] = int(sql(f"select count(*) from {relation}")[0][0])
        duplicate_keys[relation] = int(sql(
            f"select (select count(*) from (select {key} from {relation} group by {key} having count(*) > 1) d) "
            f"+ (select count(*) from {relation} where ({key}) is null)")[0][0])
    assert contract_violations(CONSUMER_CONTRACT, columns, row_counts, duplicate_keys) == []
    schemas = {row[0] for row in sql("select nspname from pg_namespace")}
    for consumer, expected in CONSUMER_SCHEMAS.items():
        assert set(expected) <= schemas, consumer
    # The read-only role the consumers are told to use can read every contract relation.
    assert sql("select count(*) from (values " + ", ".join(f"('{r}')" for r in CONSUMER_CONTRACT) + ") v(r) "
               "where not has_table_privilege('p2_reader', v.r, 'SELECT')") == [["0"]]


def test_customer_value_segments_are_thirds_of_the_buying_customers():
    segments = {row[0]: (int(row[1]), float(row[2] or 0), float(row[3] or 0)) for row in sql(
        "select customer_value_segment, count(*), min(lifetime_value), max(lifetime_value) "
        "from marts.mart_customer_360 where member_type = 'known' group by 1")}
    assert set(segments) == {"High Value", "Medium Value", "Low Value", "No Purchase Yet"}
    high, medium, low = (segments[name] for name in ("High Value", "Medium Value", "Low Value"))
    assert max(high[0], medium[0], low[0]) - min(high[0], medium[0], low[0]) <= 1, "thirds"
    assert high[1] >= medium[2] and medium[1] >= low[2] and low[1] > 0, "ordered by lifetime value"
    assert segments["No Purchase Yet"][2] == 0
