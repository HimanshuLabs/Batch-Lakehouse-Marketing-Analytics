#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/bi_exports docs sql

# Avoid duplicate .gitignore lines
grep -qxF "data/bi_exports/" .gitignore 2>/dev/null || cat >> .gitignore <<'EOF'

# Power BI generated exports
data/bi_exports/
EOF

# sql/export_power_bi_dataset.py is tracked in the repository (it also builds
# data_quality_summary.csv from verified checks); it is not regenerated here.
test -s sql/export_power_bi_dataset.py || {
  echo "Missing sql/export_power_bi_dataset.py: restore it from the repository."
  exit 1
}

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

The DAG's `power_bi_export` task (`sql/export_power_bi_dataset.py`) writes these CSV exports:

| File | Built from | Uploaded to S3 for Power BI Online |
|---|---|---|
| `campaign_performance.csv` | Gold `mart_campaign_performance_scd2` | yes |
| `customer_lifetime_value.csv` | Gold `mart_customer_lifetime_value_scd2` | yes |
| `product_performance.csv` | Gold `mart_product_performance_scd2` | yes |
| `marketing_funnel.csv` | Gold `mart_marketing_funnel_scd2` | yes |
| `data_quality_summary.csv` | verified pipeline checks (see below) | yes |
| `fact_orders.csv` | Gold `fact_orders_scd2` | no |
| `fact_order_items.csv` | Gold `fact_order_items_scd2` | no |
| `fact_campaign_spend.csv` | Gold `fact_campaign_spend_scd2` | no |

Export location:

```text
data/bi_exports/
```

Power BI Online reads the five uploaded files through public HTTPS S3 URLs managed by `terraform/aws-bi-exports` (read-only, exactly those five objects). Only synthetic data is ever uploaded.

## Data Quality Summary

`data_quality_summary.csv` has the columns `metric_group`, `metric_name`, `metric_value` and `status` (`PASS` or `FAIL`). Every row comes from a check the pipeline has already verified; the export task fails rather than write the file when an input is missing.

| metric_group | Source | metric_value |
|---|---|---|
| Warehouse Reconciliation | `audit.v_latest_reconciliation_report` | count or amount difference (0 when the check reconciles) |
| SCD2 Validation | latest run in `audit.scd2_validation_results` | failed row count |
| Silver Quarantine | `logs/silver_quality_report_<BATCH_DATE>/` | quarantined and unaccounted rows per table |
| Point-in-Time Facts | `logs/point_in_time_fact_join_report.json` | rejected and unaccounted rows per fact |
MD
