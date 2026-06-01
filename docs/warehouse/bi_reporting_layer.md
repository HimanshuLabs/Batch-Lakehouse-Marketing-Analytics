# BI Reporting Layer

## Purpose

The BI reporting layer exposes trusted warehouse marts and dashboard SQL to business users.

BI tools must consume the warehouse and marts schemas only. They should not read Raw, Bronze, or Silver data directly.

## Reporting Assets

| Asset | Purpose |
|---|---|
| marts.mart_customer_360 | Customer value and lifecycle reporting |
| marts.mart_campaign_performance | Campaign spend, revenue, and ROAS reporting |
| marts.mart_product_sales | Product and category sales reporting |
| marts.mart_revenue_daily | Daily executive revenue reporting |
| marts.mart_marketing_funnel | Funnel movement and engagement reporting |
| sql/warehouse/dashboard_query_pack.sql | Reusable SQL pack for dashboards and interviews |

## Recommended Demo Flow

```text
Gold/SCD2 lakehouse output
        ↓
PostgreSQL staging
        ↓
warehouse star schema
        ↓
marts views
        ↓
dashboard query pack / BI tool
