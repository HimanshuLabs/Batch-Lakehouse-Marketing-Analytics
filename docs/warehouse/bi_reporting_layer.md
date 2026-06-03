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

---

# Dashboard Query Pack

SQL file:

~~~text
sql/warehouse/dashboard_query_pack.sql
~~~

## Query list

| # | Query | Source |
|---|---|---|
| 1 | Daily Revenue Trend | `marts.mart_revenue_daily` |
| 2 | Revenue by State | `marts.mart_customer_360` |
| 3 | Top Customers by Lifetime Value | `marts.mart_customer_360` |
| 4 | Campaign ROAS | `marts.mart_campaign_performance` |
| 5 | Product Category Performance | `marts.mart_product_sales` |
| 6 | Customer Segment Performance | `marts.mart_customer_360` |
| 7 | Marketing Funnel Conversion | `marts.mart_marketing_funnel` |
| 8 | Repeat Purchase Rate | `marts.mart_customer_360` |
| 9 | Average Order Value | `marts.mart_revenue_daily` |
| 10 | Revenue Reconciliation | `audit.reconciliation_report` |

## Validation

~~~bash
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/dashboard_query_pack.sql \
  2>&1 | tee logs/warehouse/dashboard_query_pack_run.log

grep -n "ERROR:" logs/warehouse/dashboard_query_pack_run.log || echo "No SQL error found"
~~~

Expected result:

~~~text
No SQL error found
~~~
