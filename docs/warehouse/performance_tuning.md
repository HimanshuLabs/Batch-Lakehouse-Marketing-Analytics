# Warehouse Performance Tuning

## Purpose

The warehouse layer supports dashboard and BI reporting workloads.

Performance tuning focuses on the query paths that business users hit repeatedly: revenue trends, customer segmentation, campaign ROAS, product performance, and funnel movement.

## Index Strategy

Indexes are added for:

- SCD2 current-row lookups
- natural key and effective date range checks
- fact table date joins
- fact table foreign key joins
- campaign spend analysis
- web event timestamp filtering

## SQL Files

| File | Purpose |
|---|---|
| sql/warehouse/create_indexes.sql | Creates warehouse indexes for reporting workloads |
| sql/warehouse/performance_explain_analyze.sql | Contains EXPLAIN ANALYZE examples for key reporting queries |

## Validation Command

```bash
psql "$WAREHOUSE_DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/warehouse/performance_explain_analyze.sql
