#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/bi_exports docs sql

# Avoid duplicate .gitignore lines
grep -qxF "data/bi_exports/" .gitignore 2>/dev/null || cat >> .gitignore <<'EOF'

# Power BI generated exports
data/bi_exports/
EOF

cat > sql/export_power_bi_dataset.py <<'PY'
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
PY

cat > sql/power_bi_dashboard_queries.sql <<'SQL'
-- Power BI Dashboard Query Pack
-- Source: SCD2-aware Gold marts and facts.

-- 1. Executive KPI Summary
SELECT
    SUM(total_revenue) AS total_revenue,
    SUM(total_spend) AS total_spend,
    CASE WHEN SUM(total_spend) = 0 THEN 0 ELSE SUM(total_revenue) / SUM(total_spend) END AS roas,
    SUM(orders_count) AS total_orders,
    SUM(customers_count) AS total_customers
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet');

-- 2. Campaign Performance
SELECT
    campaign_id,
    campaign_name,
    channel,
    target_segment,
    campaign_status,
    total_spend,
    total_revenue,
    roas,
    cost_per_click,
    cost_per_acquisition,
    orders_count,
    customers_count
FROM read_parquet('data/gold/mart_campaign_performance_scd2/*.parquet')
ORDER BY roas DESC;

-- 3. Customer Value
SELECT
    customer_id,
    customer_name,
    city,
    state,
    country,
    customer_segment,
    loyalty_tier,
    total_orders,
    total_revenue,
    avg_order_value,
    customer_lifetime_days
FROM read_parquet('data/gold/mart_customer_lifetime_value_scd2/*.parquet')
ORDER BY total_revenue DESC;

-- 4. Product Performance
SELECT
    product_id,
    product_name,
    category,
    brand,
    status,
    units_sold,
    total_revenue,
    avg_selling_price,
    revenue_per_unit,
    order_count
FROM read_parquet('data/gold/mart_product_performance_scd2/*.parquet')
ORDER BY total_revenue DESC;

-- 5. Funnel Analytics
SELECT
    campaign_id,
    campaign_name,
    channel,
    target_segment,
    sessions_count,
    users_count,
    page_views,
    product_views,
    add_to_cart,
    checkout,
    purchases,
    view_to_cart_rate,
    cart_to_purchase_rate,
    overall_conversion_rate
FROM read_parquet('data/gold/mart_marketing_funnel_scd2/*.parquet')
ORDER BY purchases DESC;

-- 6. Data Quality Summary
SELECT
    'orders_rejects' AS reject_type,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_orders_scd2_rejects/**/*.parquet')

UNION ALL

SELECT
    'order_items_rejects' AS reject_type,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_order_items_scd2_rejects/**/*.parquet')

UNION ALL

SELECT
    'campaign_spend_rejects' AS reject_type,
    COUNT(*) AS rejected_rows
FROM read_parquet('data/quarantine/fact_campaign_spend_scd2_rejects/**/*.parquet');
SQL

cat > docs/power_bi_dashboard_spec.md <<'MD'
# Power BI Dashboard Specification

## Dashboard Name

Batch Lakehouse Marketing Analytics Dashboard

## Purpose

This dashboard visualizes a production-style batch lakehouse pipeline for marketing analytics. It uses SCD2-aware Gold facts and marts, including point-in-time joins and quarantine handling for invalid fact-dimension relationships.

## Data Sources

Power BI CSV exports are generated from:

- campaign_performance.csv
- customer_lifetime_value.csv
- product_performance.csv
- marketing_funnel.csv
- fact_orders.csv
- fact_order_items.csv
- fact_campaign_spend.csv

Export location:

```text
data/bi_exports/
MD
MD
exit
clear
