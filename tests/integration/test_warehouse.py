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
    assert len(rows) >= 14
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

    # Organic orders that became facts: one rejected for another key (an
    # unknown customer) is in "resolutions" but never reaches the warehouse.
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
    # campaign_type existed in no source: it is gone from the warehouse and marts.
    assert sql(
        "select table_schema || '.' || table_name from information_schema.columns "
        "where table_schema in ('warehouse', 'marts') and column_name = 'campaign_type'"
    ) == []
    # Every SCD2 dimension member is a Gold SCD2 key or the unknown member.
    for dimension, key, staging in (
        ("dim_customer", "customer_id", "stg_gold_dim_customers_scd2"),
        ("dim_product", "product_id", "stg_gold_dim_products_scd2"),
        ("dim_campaign", "campaign_id", "stg_gold_dim_campaigns_scd2"),
    ):
        invented = sql(
            f"select {key} from warehouse.{dimension} d where {key} <> 'UNKNOWN' "
            f"and not exists (select 1 from staging.{staging} s where s.{key}::BIGINT::TEXT = d.{key})"
        )
        assert invented == [], (dimension, invented)
    assert sql(
        "select count(*) from warehouse.dim_campaign where campaign_name ilike 'inferred%' "
        "union all select count(*) from warehouse.dim_customer where customer_name ilike 'inferred%' "
        "union all select count(*) from warehouse.dim_product where product_name ilike 'inferred%'"
    ) == [["0"], ["0"], ["0"]]


def test_web_events_with_a_key_missing_from_its_dimension_are_rejected_and_counted():
    staging, facts, rejects = (int(sql(f"select count(*) from {t}")[0][0]) for t in (
        "staging.stg_gold_fact_web_events", "warehouse.fact_web_events", "audit.fact_web_events_rejects"))
    assert staging == facts + rejects
    assert rejects > 0, "the sample data has web events for quarantined customers, products and campaigns"

    reasons = {row[0] for row in sql("select distinct unnest(string_to_array(reject_reason, '; ')) "
                                     "from audit.fact_web_events_rejects")}
    assert reasons <= {f"{key}_id not in dim_{key}" for key in ("customer", "product", "campaign")} | {
        f"no dim_{key} version covers event_timestamp" for key in ("customer", "product", "campaign")}, reasons
    # Each named key really is absent from its dimension, and no rejected event
    # reached the facts or the conversions.
    assert sql(
        "select count(*) from audit.fact_web_events_rejects r where "
        "(r.reject_reason like '%customer_id not in dim_customer%' "
        " and exists (select 1 from warehouse.dim_customer d where d.customer_id = r.customer_id)) "
        "or (r.reject_reason like '%product_id not in dim_product%' "
        " and exists (select 1 from warehouse.dim_product d where d.product_id = r.product_id)) "
        "or (r.reject_reason like '%campaign_id not in dim_campaign%' "
        " and exists (select 1 from warehouse.dim_campaign d where d.campaign_id = r.campaign_id)) "
        "or r.customer_id = 'UNKNOWN' and r.product_id = 'UNKNOWN' and r.campaign_id = 'UNKNOWN'"
    ) == [["0"]]
    assert sql(
        "select (select count(*) from warehouse.fact_web_events f join audit.fact_web_events_rejects r using (event_id))"
        " + (select count(*) from warehouse.fact_conversions c join audit.fact_web_events_rejects r using (event_id))"
    ) == [["0"]]
    # A web event with no campaign is a fact on the unknown campaign, not a reject.
    assert int(sql(
        "select count(*) from warehouse.fact_web_events f join staging.stg_gold_fact_web_events s "
        "on s.event_id::TEXT = f.event_id where s.campaign_id is null and f.campaign_sk = 0"
    )[0][0]) > 0
    checks = dict(sql("select check_name, status from audit.v_latest_reconciliation_report"))
    assert checks["Staging web event row count equals warehouse fact_web_events plus rejected web events"] == "PASS"
    assert checks["Rejected web events equal staging events with a source key missing from its dimension"] == "PASS"

    with (PROJECT_ROOT / "data" / "bi_exports" / "data_quality_summary.csv").open(encoding="utf-8", newline="") as f:
        summary = {row["metric_name"]: row for row in csv.DictReader(f)}
    assert int(summary["Rejected rows: fact_web_events"]["metric_value"]) == rejects
    assert summary["Rejected rows: fact_web_events"]["status"] == "PASS"


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
]


@pytest.mark.parametrize("dimension,column,staging", CARRIED_ATTRIBUTES)
def test_dimension_attributes_carry_the_source_values(dimension, column, staging):
    key = dimension.replace("dim_", "") + "_sk"
    warehouse_rows, warehouse_values, distinct = (int(v) for v in sql(
        f"select count(*), count({column}), count(distinct {column}) from warehouse.{dimension} where {key} <> 0")[0])
    staging_rows, staging_values = (int(v) for v in sql(
        f"select count(*), count({column}) from staging.{staging}")[0])
    # Every Gold SCD2 version is in the warehouse with its value; none is
    # replaced by a default, none is lost.
    assert (warehouse_rows, warehouse_values) == (staging_rows, staging_values)
    assert warehouse_values > 0 and distinct > 1, (dimension, column, warehouse_values, distinct)


def test_orders_take_the_channel_of_their_campaign():
    by_campaign = dict(sql(
        "select case when campaign_sk = 0 then 'organic' else 'campaign' end, "
        "count(*) filter (where channel_sk = 0) from warehouse.fact_orders group by 1"
    ))
    assert by_campaign == {"organic": by_campaign["organic"], "campaign": "0"}
    assert int(by_campaign["organic"]) == int(sql(
        "select count(*) from warehouse.fact_orders where campaign_sk = 0")[0][0]) > 0
    # The channel is the order's campaign version's channel; items share it.
    assert sql(
        "select count(*) from warehouse.fact_orders fo join warehouse.dim_campaign dc using (campaign_sk) "
        "join warehouse.dim_channel ch using (channel_sk) where ch.channel_nk <> warehouse.channel_nk(dc.channel)"
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


# Warehouse and mart columns that are allowed to be entirely NULL, entirely
# Unknown or one constant value, each with the reason. Every other column must
# vary. A column listed here that varies again is stale and fails too.
CONSTANT_COLUMNS_ALLOWED = {
    ("*", "warehouse_loaded_at"): "set once per build",
    ("warehouse.dim_product", "brand"): "no source has a product brand",
    ("marts.mart_product_sales", "brand"): "no source has a product brand",
    ("warehouse.fact_orders", "order_status"): "orders have no status in the source; every order is 'completed'",
    ("warehouse.fact_orders", "tax_amount"): "not in the source orders",
    ("warehouse.fact_orders", "shipping_amount"): "not in the source orders",
    ("marts.mart_revenue_daily", "tax_amount"): "not in the source orders",
    ("marts.mart_revenue_daily", "shipping_amount"): "not in the source orders",
    ("warehouse.fact_campaign_spend", "region_sk"): "ad spend has no geography: the unknown region",
    ("warehouse.fact_campaign_spend", "conversions"): "the ad platform feed has no conversions",
    ("warehouse.fact_campaign_spend", "attributed_revenue"): "the ad platform feed has no revenue",
    ("marts.mart_campaign_performance", "ad_platform_conversions"): "the ad platform feed has no conversions",
    ("marts.mart_campaign_performance", "platform_attributed_revenue"): "the ad platform feed has no revenue",
    ("warehouse.fact_conversions", "conversion_type"): "the sample's only conversion event is 'purchase'",
    ("warehouse.fact_web_events", "schema_version"): "every source row is schema v1",
    ("warehouse.fact_web_events", "source_system"): "one source system: web_events",
    ("marts.mart_customer_360", "country"): "every sample customer is in India",
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
        total = int(sql(f"select count(*) from warehouse.dim_{dimension} where {dimension}_sk <> 0")[0][0])
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
        # ('UNKNOWN' exactly where the key is the unknown member).
        assert sql(
            f"select count(*) from warehouse.{fact} f join warehouse.dim_{dimension} d using ({dimension}_sk) "
            f"where d.{column} <> f.{column}"
        ) == [["0"]], (fact, column)
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
