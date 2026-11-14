"""Export the Power BI datasets (the DAG's power_bi_export task).

Writes, into data/bi_exports/ by default:
  - the Gold SCD2 marts and facts as CSV (DuckDB over the Gold parquet), and
  - data_quality_summary.csv, built only from checks the pipeline has verified:
      warehouse reconciliation   audit.v_latest_reconciliation_report
      invalid references         the reconciliation's count per fact and dimension (a source key
                                 with no covering dimension version)
      SCD2 validation            the latest run in audit.scd2_validation_results
      Silver quarantine          logs/silver_quality_report_<BATCH_DATE>/
      point-in-time facts        logs/point_in_time_fact_join_report.json
                                 (row conservation, organic orders on the
                                 unknown campaign, order totals vs lines)
    The warehouse is read in a read-only session. A missing or empty input
    fails the task; no number is ever written by hand.

Five of these files are uploaded to S3 for Power BI Online by
terraform/aws-bi-exports.

Usage:
    python sql/export_power_bi_dataset.py [--export-dir DIR] [--report-path FILE]

Postgres settings: P2_POSTGRES_HOST / _PORT / _DB / _USER / _PASSWORD, with the
same defaults as sql/publish_gold_to_postgres.py.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "data" / "bi_exports"
DEFAULT_REPORT_PATH = LOG_DIR / "power_bi_export_report.json"
DASHBOARD_CONTRACT = PROJECT_ROOT / "scripts" / "power_bi_dashboard_contract.json"

BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()

DATA_QUALITY_SUMMARY = "data_quality_summary"
SUMMARY_COLUMNS = ["metric_group", "metric_name", "metric_value", "status"]

EXPORTS = {
    "campaign_performance": f"SELECT * FROM read_parquet('{GOLD_DIR}/mart_campaign_performance_scd2/*.parquet')",
    "customer_lifetime_value": f"SELECT * FROM read_parquet('{GOLD_DIR}/mart_customer_lifetime_value_scd2/*.parquet')",
    "product_performance": f"SELECT * FROM read_parquet('{GOLD_DIR}/mart_product_performance_scd2/*.parquet')",
    "marketing_funnel": f"SELECT * FROM read_parquet('{GOLD_DIR}/mart_marketing_funnel_scd2/*.parquet')",
    "fact_orders": f"SELECT * FROM read_parquet('{GOLD_DIR}/fact_orders_scd2/**/*.parquet')",
    "fact_order_items": f"SELECT * FROM read_parquet('{GOLD_DIR}/fact_order_items_scd2/**/*.parquet')",
    "fact_campaign_spend": f"SELECT * FROM read_parquet('{GOLD_DIR}/fact_campaign_spend_scd2/**/*.parquet')",
}


def published_column_order(file_name: str, columns: list[str], route: str) -> list[str]:
    """The export's columns in the order the dashboards read them.

    Power BI reads a CSV by column position. The columns a file was published
    with (scripts/power_bi_dashboard_contract.json) come first, in that order;
    every newer column follows, in the mart's own order. A published column
    that the mart no longer has is an error: the dashboard would break.
    """
    contract = json.loads(DASHBOARD_CONTRACT.read_text(encoding="utf-8"))
    published = contract[route]["files"][file_name]
    missing = [column for column in published if column not in columns]
    if missing:
        raise ValueError(f"{file_name}: the dashboard reads column(s) {', '.join(missing)}, which the export no longer has")
    return published + [column for column in columns if column not in published]


# ---------------------------------------------------------------------------
# data_quality_summary.csv
# ---------------------------------------------------------------------------

def _status(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


WEB_EVENT_CONSERVATION_CHECK = "Staging web event row count equals warehouse fact_web_events row count"
# The reconciliation's checks that count the facts on the invalid-reference
# member: "Invalid <dimension> references in <fact> are on the invalid-reference member".
INVALID_REFERENCE_CHECK = re.compile(r"^Invalid (\w+) references in (\w+) are on the invalid-reference member$")
INVALID_REFERENCE_PAIRS = 7   # orders x2, order items, campaign spend, web events x3


def build_data_quality_summary(
    reconciliation: list[dict],
    scd2_validation: list[dict],
    silver_report: list[dict],
    pit_report: dict,
) -> list[dict]:
    """Rows for data_quality_summary.csv from the verified sources.

    reconciliation     rows of audit.v_latest_reconciliation_report
    scd2_validation    rows of the latest audit.scd2_validation_results run
    silver_report      rows of logs/silver_quality_report_<BATCH_DATE>/*.csv
    pit_report         logs/point_in_time_fact_join_report.json

    A fact whose customer, product or campaign has no valid dimension row is
    kept on the invalid-reference member; the reconciliation counts them per
    fact and dimension (its row carries target_count), and so does the
    point-in-time report (fact_resolutions, invalid_reference).
    """
    if not reconciliation:
        raise ValueError("no warehouse reconciliation run found")
    if not scd2_validation:
        raise ValueError("no SCD2 validation run found")
    if not silver_report:
        raise ValueError("the Silver quality report is empty")
    if not pit_report.get("facts"):
        raise ValueError("the point-in-time fact report has no facts")
    stale = [fact for fact, details in pit_report["facts"].items() if "conservation" not in details]
    if stale:
        raise ValueError(
            f"the point-in-time fact report has no conservation counts for {stale}: "
            "it predates the conservation check; rerun point_in_time_fact_joins"
        )
    # Counted over the orders that became facts: an organic order rejected for
    # another key (an unknown customer) is not an order on the unknown campaign.
    campaign_resolutions = (
        pit_report["facts"].get("fact_orders_scd2", {}).get("fact_resolutions", {}).get("campaign_sk", {})
    )
    if "unknown_member" not in campaign_resolutions:
        raise ValueError(
            "the point-in-time fact report has no fact_resolutions unknown_member count for fact_orders_scd2 "
            "campaign_sk: it predates counting organic orders among the facts; rerun point_in_time_fact_joins"
        )
    web_event_conservation = [r for r in reconciliation if r["check_name"] == WEB_EVENT_CONSERVATION_CHECK]
    if not web_event_conservation:
        raise ValueError(
            f"the reconciliation run has no check {WEB_EVENT_CONSERVATION_CHECK!r}: "
            "it predates loading every web event; rebuild the warehouse"
        )
    invalid_references = [r for r in reconciliation if INVALID_REFERENCE_CHECK.match(r["check_name"])]
    if len(invalid_references) != INVALID_REFERENCE_PAIRS or any(r.get("target_count") is None for r in invalid_references):
        raise ValueError(
            f"the reconciliation run has {len(invalid_references)} invalid-reference counts, expected "
            f"{INVALID_REFERENCE_PAIRS}: it predates the invalid-reference member; rebuild the warehouse"
        )
    for fact, details in pit_report["facts"].items():
        for key, counts in details.get("fact_resolutions", {}).items():
            if "invalid_reference" not in counts:
                raise ValueError(
                    f"the point-in-time fact report has no invalid_reference count for {fact} {key}: "
                    "it predates the invalid-reference member; rerun point_in_time_fact_joins"
                )
    order_totals = pit_report.get("order_totals") or {}
    if "checked_orders" not in order_totals:
        raise ValueError(
            "the point-in-time fact report has no order totals check: "
            f"{order_totals.get('error') or 'it predates the check'}; rerun point_in_time_fact_joins"
        )

    rows: list[dict] = []

    for check in sorted(reconciliation, key=lambda r: r["check_name"]):
        if INVALID_REFERENCE_CHECK.match(check["check_name"]):
            continue   # shown below, with the count itself
        difference = check.get("count_difference")
        if difference is None:
            difference = check.get("amount_difference")
        rows.append({
            "metric_group": "Warehouse Reconciliation",
            "metric_name": check["check_name"],
            "metric_value": 0 if difference is None else difference,
            "status": check["status"],
        })

    for check in sorted(scd2_validation, key=lambda r: r["check_name"]):
        rows.append({
            "metric_group": "SCD2 Validation",
            "metric_name": check["check_name"],
            "metric_value": check["failed_row_count"],
            "status": check["status"],
        })

    for table in sorted(silver_report, key=lambda r: r["table_name"]):
        status = _status(table["status"] == "PASSED")
        rows.append({
            "metric_group": "Silver Quarantine",
            "metric_name": f"Quarantined rows: {table['table_name']}",
            "metric_value": int(table["quarantine_row_count"]),
            "status": status,
        })
        rows.append({
            "metric_group": "Silver Quarantine",
            "metric_name": f"Unaccounted rows: {table['table_name']}",
            "metric_value": int(table["unaccounted_row_count"]),
            "status": status,
        })

    for fact, details in sorted(pit_report["facts"].items()):
        conservation = details["conservation"]
        status = _status(conservation["status"] == "PASS")
        rows.append({
            "metric_group": "Point-in-Time Facts",
            "metric_name": f"Rejected rows: {fact}",
            "metric_value": conservation["rejected_rows"],
            "status": status,
        })
        rows.append({
            "metric_group": "Point-in-Time Facts",
            "metric_name": f"Unaccounted rows: {fact}",
            "metric_value": conservation["unaccounted_rows"],
            "status": status,
        })

    # Organic orders are valid facts on the unknown campaign member; a count, not a failure.
    rows.append({
        "metric_group": "Point-in-Time Facts",
        "metric_name": "Orders with no campaign (unknown campaign member): orders",
        "metric_value": campaign_resolutions["unknown_member"],
        "status": "PASS",
    })

    status = _status(order_totals["status"] == "PASS")
    for metric_name, key in (
        ("Order totals checked: orders", "checked_orders"),
        ("Order totals not equal to sum of lines: orders", "mismatched_orders"),
        ("Order totals excluded, incomplete: orders", "excluded_incomplete_orders"),
        ("Order totals with no Bronze items: orders", "orders_without_bronze_items"),
    ):
        rows.append({
            "metric_group": "Point-in-Time Facts",
            "metric_name": metric_name,
            "metric_value": order_totals[key],
            "status": status,
        })

    # Facts kept on the invalid-reference member: their customer, product or
    # campaign key is present but has no valid dimension row. A count, not a
    # failure; the status is that of the check that the count is exact.
    for fact, details in sorted(pit_report["facts"].items()):
        for key, counts in sorted(details.get("fact_resolutions", {}).items()):
            rows.append({
                "metric_group": "Invalid References",
                "metric_name": f"Gold rows on the invalid-reference member, {key}: {fact}",
                "metric_value": counts["invalid_reference"],
                "status": _status(details["conservation"]["status"] == "PASS"),
            })
    for check in sorted(invalid_references, key=lambda r: r["check_name"]):
        dimension, fact = INVALID_REFERENCE_CHECK.match(check["check_name"]).groups()
        rows.append({
            "metric_group": "Invalid References",
            "metric_name": f"Warehouse rows on the invalid-reference member, {dimension}: {fact}",
            "metric_value": int(check["target_count"]),
            "status": check["status"],
        })

    return rows


def read_warehouse_checks() -> tuple[list[dict], list[dict]]:
    """The latest reconciliation (with mart validation) and SCD2 validation results, read-only."""
    import psycopg
    from psycopg.rows import dict_row

    connection = psycopg.connect(
        host=os.getenv("P2_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("P2_POSTGRES_PORT", "5434")),
        dbname=os.getenv("P2_POSTGRES_DB", "marketing_analytics"),
        user=os.getenv("P2_POSTGRES_USER", "project2"),
        password=os.getenv("P2_POSTGRES_PASSWORD", "project2"),
        options="-c default_transaction_read_only=on -c timezone=UTC",
        connect_timeout=10,
        row_factory=dict_row,
    )
    with connection:
        run_id = os.environ.get("P2_RUN_ID")
        reports = []
        for table, run_column, columns in (
            ("reconciliation_report", "reconciliation_run_id",
             "check_name, status, count_difference, amount_difference, target_count"),
            ("mart_validation_report", "validation_run_id",
             "check_name, status, NULL AS count_difference, difference AS amount_difference, NULL AS target_count"),
            ("scd2_validation_results", "validation_run_id",
             "check_name, status, failed_row_count"),
        ):
            # A DAG export must use that DAG run, never an older successful audit.
            if run_id:
                checks = connection.execute(
                    f"SELECT {columns} FROM audit.{table} WHERE {run_column} = %s", (run_id,),
                ).fetchall()
            else:
                checks = connection.execute(
                    f"SELECT {columns} FROM audit.{table} WHERE {run_column} = "
                    f"(SELECT {run_column} FROM audit.{table} ORDER BY checked_at DESC LIMIT 1)"
                ).fetchall()
            if not checks:
                raise ValueError(f"No {table} checks for run {run_id or 'latest'}")
            reports.append(checks)
        reconciliation = reports[0] + reports[1]
        scd2_validation = reports[2]
    return [dict(r) for r in reconciliation], [dict(r) for r in scd2_validation]


def read_silver_report(batch_date: str) -> list[dict]:
    report_dir = LOG_DIR / f"silver_quality_report_{batch_date}"
    files = sorted(report_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"missing Silver quality report: {report_dir}")
    with files[0].open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_pit_report() -> dict:
    return json.loads((LOG_DIR / "point_in_time_fact_join_report.json").read_text(encoding="utf-8"))


def write_summary(rows: list[dict], path: Path) -> None:
    columns = published_column_order(f"{DATA_QUALITY_SUMMARY}.csv", SUMMARY_COLUMNS, "route_b")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# The task
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Export the Power BI datasets.")
    parser.add_argument("--export-dir", type=Path, default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    args = parser.parse_args()

    export_dir = args.export_dir.resolve()
    export_dir.mkdir(parents=True, exist_ok=True)
    args.report_path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        "job_name": "export_power_bi_dataset",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_date": BATCH_DATE,
        "exports": {},
    }

    overall_status = "PASS"
    con = None

    try:
        # Inside try, so a failed connect still writes a FAIL report.
        reconciliation, scd2_validation = read_warehouse_checks()
        rows = build_data_quality_summary(
            reconciliation, scd2_validation, read_silver_report(BATCH_DATE), read_pit_report(),
        )
        failures = [row for row in rows if row.get("status") != "PASS"]
        if failures:
            report["data_quality_summary"] = rows
            raise ValueError(f"Data quality summary contains {len(failures)} non-PASS checks")
        con = duckdb.connect(database=":memory:")
        for name, query in EXPORTS.items():
            output_path = export_dir / f"{name}.csv"
            print(f"Exporting {name} -> {output_path}")

            try:
                # The dashboards read by position: published columns first, new ones after.
                columns = [column[0] for column in con.execute(f"DESCRIBE {query}").fetchall()]
                ordered = ", ".join(f'"{column}"' for column in published_column_order(f"{name}.csv", columns, "route_b"))
                # COPY returns the number of rows it wrote, so each query runs once.
                row_count = con.execute(
                    f"""
                    COPY (SELECT {ordered} FROM ({query}))
                    TO '{output_path}'
                    WITH (HEADER, DELIMITER ',')
                    """
                ).fetchone()[0]

                report["exports"][name] = {
                    "status": "PASS",
                    "output_path": str(output_path),
                    "row_count": row_count,
                }

                print(f"{name}: rows={row_count}")

            except Exception as exc:
                overall_status = "FAIL"
                report["exports"][name] = {
                    "status": "FAIL",
                    "error": str(exc),
                }
                print(f"{name}: FAILED - {exc}")

        summary_path = export_dir / f"{DATA_QUALITY_SUMMARY}.csv"
        print(f"Exporting {DATA_QUALITY_SUMMARY} -> {summary_path}")
        try:
            write_summary(rows, summary_path)
            report["exports"][DATA_QUALITY_SUMMARY] = {
                "status": "PASS",
                "output_path": str(summary_path),
                "row_count": len(rows),
            }
            print(f"{DATA_QUALITY_SUMMARY}: rows={len(rows)}")
        except Exception as exc:
            overall_status = "FAIL"
            report["exports"][DATA_QUALITY_SUMMARY] = {"status": "FAIL", "error": str(exc)}
            print(f"{DATA_QUALITY_SUMMARY}: FAILED - {exc}")

    except Exception as exc:
        overall_status = "FAIL"
        report["error"] = str(exc)
        raise

    finally:
        if con is not None:
            con.close()

        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["overall_status"] = overall_status

        args.report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"Report written to: {args.report_path}")
        print(f"Overall status: {overall_status}")

    if overall_status != "PASS":
        raise SystemExit("Power BI export failed.")


if __name__ == "__main__":
    main()
