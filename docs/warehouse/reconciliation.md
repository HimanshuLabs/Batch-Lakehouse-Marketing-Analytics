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

## Output Table

```text
audit.reconciliation_report
