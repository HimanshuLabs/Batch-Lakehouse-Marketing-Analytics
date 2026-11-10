"""Export the Power BI datasets (the DAG's power_bi_export task).

Writes, into data/bi_exports/ by default:
  - the Gold SCD2 marts and facts as CSV (DuckDB over the Gold parquet), and
  - data_quality_summary.csv, built only from checks the pipeline has verified:
      warehouse reconciliation   audit.v_latest_reconciliation_report
      SCD2 validation            the latest run in audit.scd2_validation_results
      Silver quarantine          logs/silver_quality_report_<BATCH_DATE>/
      point-in-time facts        logs/point_in_time_fact_join_report.json
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
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "data" / "bi_exports"
DEFAULT_REPORT_PATH = LOG_DIR / "power_bi_export_report.json"

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


# ---------------------------------------------------------------------------
# data_quality_summary.csv
# ---------------------------------------------------------------------------

def _status(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


def build_data_quality_summary(
    reconciliation: list[dict],
    scd2_validation: list[dict],
    silver_report: list[dict],
    pit_report: dict,
) -> list[dict]:
    """Rows for data_quality_summary.csv from the four verified sources.

    reconciliation   rows of audit.v_latest_reconciliation_report
    scd2_validation  rows of the latest audit.scd2_validation_results run
    silver_report    rows of logs/silver_quality_report_<BATCH_DATE>/*.csv
    pit_report       logs/point_in_time_fact_join_report.json
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

    rows: list[dict] = []

    for check in sorted(reconciliation, key=lambda r: r["check_name"]):
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

    return rows


def read_warehouse_checks() -> tuple[list[dict], list[dict]]:
    """The latest reconciliation and SCD2 validation results, read-only."""
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
        reconciliation = connection.execute(
            "SELECT check_name, status, count_difference, amount_difference "
            "FROM audit.v_latest_reconciliation_report"
        ).fetchall()
        scd2_validation = connection.execute(
            "SELECT check_name, status, failed_row_count FROM audit.scd2_validation_results "
            "WHERE validation_run_id = (SELECT validation_run_id FROM audit.scd2_validation_results "
            "ORDER BY checked_at DESC LIMIT 1)"
        ).fetchall()
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
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS, lineterminator="\n")
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
    con = duckdb.connect(database=":memory:")

    try:
        for name, query in EXPORTS.items():
            output_path = export_dir / f"{name}.csv"
            print(f"Exporting {name} -> {output_path}")

            try:
                con.execute(
                    f"""
                    COPY ({query})
                    TO '{output_path}'
                    WITH (HEADER, DELIMITER ',')
                    """
                )

                row_count = con.execute(query).fetchdf().shape[0]

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
            reconciliation, scd2_validation = read_warehouse_checks()
            rows = build_data_quality_summary(
                reconciliation, scd2_validation, read_silver_report(BATCH_DATE), read_pit_report()
            )
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
