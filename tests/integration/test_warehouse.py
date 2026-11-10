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
