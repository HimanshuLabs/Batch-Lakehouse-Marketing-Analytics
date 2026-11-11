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
| Web event rows | staging.stg_gold_fact_web_events | warehouse.fact_web_events + audit.fact_web_events_rejects |
| Rejected web events | staging events with a key missing from its dimension | audit.fact_web_events_rejects |
| Gross minus discount equals net revenue | expected zero mismatched complete orders | warehouse.fact_orders |

## Output Table

```text
audit.reconciliation_report
