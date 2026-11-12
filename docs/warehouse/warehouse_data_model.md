# Warehouse Data Model

## Purpose

The warehouse data model turns trusted Gold/SCD2 lakehouse outputs into a relational star schema for reporting, reconciliation, and BI consumption.

## PostgreSQL Schemas

| Schema | Purpose |
|---|---|
| staging | Loaded Project 2 Gold/SCD2 outputs |
| warehouse | Star schema facts and dimensions |
| marts | BI-ready reporting views |
| audit | Reconciliation and validation results |

## Dimensions

| Table | Grain | Type |
|---|---|---|
| warehouse.dim_customer | One row per customer historical version | SCD Type 2 |
| warehouse.dim_product | One row per product historical version | SCD Type 2 |
| warehouse.dim_campaign | One row per campaign historical version | SCD Type 2 |
| warehouse.dim_date | One row per calendar date | Static dimension |
| warehouse.dim_region | One row per country/state/city | Conformed dimension |
| warehouse.dim_channel | One row per channel/source pair | Conformed dimension |

## Facts

| Table | Grain |
|---|---|
| warehouse.fact_orders | One row per order |
| warehouse.fact_order_items | One row per order line item |
| warehouse.fact_campaign_spend | One row per campaign/date spend record |
| warehouse.fact_web_events | One row per behavioral event |
| warehouse.fact_conversions | One row per conversion event |

## Marts

| Mart | Business Question |
|---|---|
| marts.mart_customer_360 | Who are the most valuable customers? |
| marts.mart_campaign_performance | Which campaigns generate the best ROAS? |
| marts.mart_product_sales | Which products and categories drive revenue? |
| marts.mart_revenue_daily | How is revenue trending over time? |
| marts.mart_marketing_funnel | Where do users move or drop in the funnel? |
| marts.mart_customer_daily_snapshot | What did mart_customer_360 say at the end of each business date? |
| marts.mart_product_daily_snapshot | What did mart_product_sales say at the end of each business date? |
| marts.mart_campaign_daily_snapshot | What did mart_campaign_performance say at the end of each business date? |

The three daily snapshots are tables with the key (entity ID, `snapshot_date`). A row uses only the facts, order statuses, refunds and dimension versions known at the end of its date, so features taken at a cutoff date cannot contain anything that happened later.

## Key Design Rule

Facts store surrogate keys, not only natural keys.

This keeps reports historically correct when customer, product, or campaign attributes change over time.
