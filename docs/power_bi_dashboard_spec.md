# Power BI Dashboard Specification

## Dashboard Name

Batch Lakehouse Marketing Analytics Dashboard

## Purpose

This dashboard visualizes a production-style batch lakehouse pipeline for marketing analytics. It uses SCD2-aware Gold facts and marts, including point-in-time joins and quarantine handling for invalid fact-dimension relationships.

## Data Sources

Power BI CSV exports are generated from:

- `campaign_performance.csv`
- `customer_lifetime_value.csv`
- `product_performance.csv`
- `marketing_funnel.csv`
- `fact_orders.csv`
- `fact_order_items.csv`
- `fact_campaign_spend.csv`

Export location:

```text
data/bi_exports/
