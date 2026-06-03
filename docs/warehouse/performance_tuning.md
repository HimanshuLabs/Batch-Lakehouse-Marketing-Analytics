# PostgreSQL Warehouse Query Performance Tuning

This document explains the practical PostgreSQL query performance tuning added for the warehouse/reporting layer.

The goal is not fake benchmark theater. The goal is to show reproducible local tuning using:

- warehouse indexes
- `EXPLAIN ANALYZE`
- real dashboard-style query patterns
- honest interpretation of PostgreSQL query plans

## Files

| File | Purpose |
|---|---|
| `sql/warehouse/create_indexes.sql` | Creates or documents required warehouse indexes |
| `sql/warehouse/performance_explain_analyze.sql` | Runs EXPLAIN ANALYZE examples for reporting queries |
| `logs/warehouse/create_indexes_final.log` | Local proof that index setup ran successfully |
| `logs/warehouse/performance_explain_final.log` | Local proof that performance demos ran successfully |


## Local Dataset Size

The local PostgreSQL warehouse uses a small reproducible sample dataset.

Observed local row counts:

| Table | Row Count |
|---|---:|
| `warehouse.dim_campaign` | 40 |
| `warehouse.dim_customer` | 501 |
| `warehouse.dim_date` | 123 |
| `warehouse.dim_product` | 149 |
| `warehouse.fact_campaign_spend` | 951 |
| `warehouse.fact_order_items` | 4,584 |
| `warehouse.fact_orders` | 1,869 |

Because these tables are small, PostgreSQL may choose sequential scans even when useful indexes exist. That is normal optimizer behavior. A sequential scan over a tiny table can be cheaper than walking an index and then fetching heap pages.

So this project does not claim fake speedups for every query.

Instead, it proves:

1. required reporting indexes exist,
2. dashboard queries are index-eligible,
3. selective drilldown queries use indexes,
4. broad aggregate queries may still use sequential scans locally,
5. the design is ready for larger warehouse-scale data.


## Index Strategy

The warehouse indexes target repeated BI/reporting access paths: date filtering, fact-to-dimension joins, product drilldowns, campaign ROAS analysis, and SCD2 current-row lookups.

| Reporting Need | Index / Coverage | Why It Matters |
|---|---|---|
| Revenue by date | `idx_fact_orders_date` on `warehouse.fact_orders(order_date_sk)` | Speeds date-key filtering and joins to `warehouse.dim_date` |
| Customer revenue / customer 360 | `idx_fact_orders_customer` on `warehouse.fact_orders(customer_sk)` | Supports fact-to-customer joins |
| Product sales drilldown | `idx_fact_order_items_product` on `warehouse.fact_order_items(product_sk)` | Supports product performance queries |
| Campaign ROAS | `idx_fact_campaign_spend_campaign_date` on `warehouse.fact_campaign_spend(campaign_sk, spend_date_sk)` | Supports campaign-level and campaign-date spend analysis |
| Current customer lookup | `idx_dim_customer_natural_current` on `warehouse.dim_customer(customer_id, is_current)` | Supports SCD2 current customer lookup |
| Current product lookup | `idx_dim_product_natural_current` on `warehouse.dim_product(product_id, is_current)` | Supports SCD2 current product lookup |
| Current campaign lookup | `idx_dim_campaign_natural_current` on `warehouse.dim_campaign(campaign_id, is_current)` | Supports SCD2 current campaign lookup |

The original requirement mentioned `warehouse.fact_orders(order_date)`. In this warehouse model, the fact table stores the date surrogate key as `order_date_sk`, while the actual calendar date lives in `warehouse.dim_date.full_date`. Therefore the practical implementation indexes `warehouse.fact_orders(order_date_sk)`.


## EXPLAIN ANALYZE Findings

The performance demo uses three reporting patterns.

### Demo 1: Customer Revenue by Month and Membership Tier

This query joins:

- `warehouse.fact_orders`
- `warehouse.dim_date`
- `warehouse.dim_customer`

It groups revenue by year, month, and membership tier.

Observed local behavior:

- PostgreSQL used sequential scans on the small local tables.
- This is acceptable because:
  - `warehouse.fact_orders` has only 1,869 rows,
  - `warehouse.dim_date` has only 123 rows,
  - `warehouse.dim_customer` has only 501 rows.

The query is still index-eligible through:

- `warehouse.fact_orders(order_date_sk)`
- `warehouse.fact_orders(customer_sk)`
- `warehouse.dim_date(date_sk)`
- `warehouse.dim_customer(customer_sk)`

The local plan is honest: broad aggregations over tiny tables often scan the table.

### Demo 2: Product Revenue Lookup

This query chooses a product that actually appears in `warehouse.fact_order_items`, then drills into product-level sales.

Observed local proof:

- `Index Only Scan using idx_fact_order_items_product`
- `Index Scan using idx_fact_order_items_product`
- `rows=2975`

This proves the product drilldown path is using the product foreign-key index.

### Demo 3: Campaign Spend and ROAS Lookup

This query chooses an active campaign from `warehouse.fact_campaign_spend`, then calculates spend, attributed revenue, and ROAS.

Observed local proof:

- `Index Scan using idx_fact_campaign_spend_campaign_date`
- `rows=739`

This proves the campaign spend lookup uses the composite index on `(campaign_sk, spend_date_sk)`.

## Validation Commands

Run the index setup:

```bash
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/create_indexes.sql \
  2>&1 | tee logs/warehouse/create_indexes_final.log
```

Check for SQL errors:

```bash
grep -n "ERROR:" logs/warehouse/create_indexes_final.log || echo "No SQL error found"
```

Run the EXPLAIN ANALYZE demo:

```bash
env -u PGPORT bash scripts/run_warehouse_sql.sh sql/warehouse/performance_explain_analyze.sql \
  2>&1 | tee logs/warehouse/performance_explain_final.log
```

Check for SQL errors:

```bash
grep -n "ERROR:" logs/warehouse/performance_explain_final.log || echo "No SQL error found"
```

Confirm index usage and coverage:

```bash
grep -nE "Index Only Scan using idx_fact_order_items_product|Index Scan using idx_fact_order_items_product|Index Scan using idx_fact_campaign_spend_campaign_date|Final verification|idx_fact_orders_date|idx_fact_orders_customer|idx_fact_order_items_product|idx_fact_campaign_spend_campaign_date|idx_dim_customer_natural_current|idx_dim_product_natural_current|idx_dim_campaign_natural_current" \
  logs/warehouse/performance_explain_final.log
```

Expected proof:

- no SQL errors
- all 7 required index rows appear in final verification
- product drilldown uses `idx_fact_order_items_product`
- campaign spend lookup uses `idx_fact_campaign_spend_campaign_date`
