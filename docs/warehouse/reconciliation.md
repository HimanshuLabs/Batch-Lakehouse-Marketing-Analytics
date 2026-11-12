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
| Mart tie-out (mart validation) | revenue (orders that were not cancelled, less refunds), units, spend and ad-platform results in the facts | each mart, including its unknown and invalid-reference rows |
| Item refunds sum to their order refund | expected zero orders whose items' `refund_amount` does not sum to the order's | warehouse.fact_order_items |
| Mart refunds equal warehouse fact_orders refunds | sum of `refund_amount` in warehouse.fact_orders | `refund_amount` in the daily, customer, campaign and product marts |
| Refunds per mart (mart validation, 4 checks) | refunds in the facts | refunds in each mart, tolerance 0 |
| Last snapshot date equals the lifetime mart (mart validation, 3 checks) | mart_customer_360, mart_product_sales, mart_campaign_performance | the last `snapshot_date` of each daily snapshot, column for column, with no gap in any entity's dates |
| Gross minus discount equals net revenue | expected zero mismatched complete orders | warehouse.fact_orders |

## Output Table

```text
audit.reconciliation_report
