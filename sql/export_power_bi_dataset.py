from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPORT_DIR = PROJECT_ROOT / "data" / "bi_exports"
LOG_DIR = PROJECT_ROOT / "logs"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


EXPORTS = {
    "campaign_performance": """
        SELECT *
        FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet')
    """,
    "customer_lifetime_value": """
        SELECT *
        FROM read_parquet('data/gold/mart_customer_lifetime_value_scd2/*.parquet')
    """,
    "product_performance": """
        SELECT *
        FROM read_parquet('data/gold/mart_product_performance_scd2/*.parquet')
    """,
    "marketing_funnel": """
        SELECT *
        FROM read_parquet('data/gold/mart_marketing_funnel_scd2/*.parquet')
    """,
    "fact_orders": """
        SELECT *
        FROM read_parquet('data/gold/fact_orders_scd2/**/*.parquet')
    """,
    "fact_order_items": """
        SELECT *
        FROM read_parquet('data/gold/fact_order_items_scd2/**/*.parquet')
    """,
    "fact_campaign_spend": """
        SELECT *
        FROM read_parquet('data/gold/fact_campaign_spend_scd2/**/*.parquet')
    """,
}


def main() -> None:
    report = {
        "job_name": "export_power_bi_dataset",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "exports": {},
    }

    overall_status = "PASS"
    con = duckdb.connect(database=":memory:")

    try:
        for name, query in EXPORTS.items():
            output_path = EXPORT_DIR / f"{name}.csv"
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

    finally:
        con.close()

        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["overall_status"] = overall_status

        report_path = LOG_DIR / "power_bi_export_report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"Report written to: {report_path}")
        print(f"Overall status: {overall_status}")

    if overall_status != "PASS":
        raise SystemExit("Power BI export failed.")


if __name__ == "__main__":
    main()
