# Warehouse Reconciliation

## Purpose

Reconciliation proves that PostgreSQL warehouse outputs match trusted Project 2 Gold/SCD2 outputs.

The goal is simple: dashboards should not become beautiful lies.

## Checks

| Check | Source | Target |
|---|---|---|
| Order count | staging.gold_fact_orders_scd2 | warehouse.fact_orders |
| Revenue total | staging.gold_fact_orders_scd2 | warehouse.fact_orders |
| Campaign spend | staging.gold_fact_campaign_spend_scd2 | warehouse.fact_campaign_spend |
| Null customer keys | expected zero | warehouse.fact_orders |
| Duplicate order ids | expected zero | warehouse.fact_orders |
| Web event rows | staging.stg_gold_fact_web_events | warehouse.fact_web_events (every event is loaded) |
| Invalid references, per fact and dimension (7 checks) | fact rows whose customer, product or campaign ID is in no dimension version | fact rows on the invalid-reference member (key -1) |
| SCD2 version count, per dimension (3 checks) | staging.stg_gold_dim_*_scd2 rows | warehouse.dim_* rows without the unknown (0) and invalid-reference (-1) members |
| Order total equals net revenue plus tax plus shipping | expected zero mismatched orders | warehouse.fact_orders |
| Regional ad spend rows sum to the campaign-day total | staging campaign_day_spend | warehouse.fact_campaign_spend summed over regions |
| Mart tie-out (mart validation) | completed-order revenue, units, spend and ad-platform results in the facts | each mart, including its unknown and invalid-reference rows |
| Gross minus discount equals net revenue | expected zero mismatched complete orders | warehouse.fact_orders |

## Output Table

```text
audit.reconciliation_report
