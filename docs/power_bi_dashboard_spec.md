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
| Invalid References | `audit.v_latest_reconciliation_report` and `logs/point_in_time_fact_join_report.json` | facts kept on the invalid-reference member (a customer, product or campaign key with no valid dimension row), per fact and dimension, in the warehouse and in Gold |
