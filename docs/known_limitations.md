# Known Limitations in the Code

These are known weaknesses in the pipeline code. They are not fixed yet, and none of them affects the numbers the pipeline produces today. Each entry gives the file, what can go wrong, and why that is acceptable for now.

The design-level limitations are listed in the README under "Design Decisions and Known Limitations": public S3 read, organic orders, UTC business dates, unpinned dev dependencies, and screenshots.

## Warehouse keys

**Fact row keys depend on load order.**
`sql/warehouse/create_warehouse_tables.sql`

The fact tables number their rows with auto-incrementing identity keys: `order_sk`, `order_item_sk`, `campaign_spend_sk`, `event_sk` and `conversion_sk`. Rebuilding the warehouse in a different order can give the same rows different numbers.

*Why it is acceptable for now:* no mart, export, or dbt model reads these keys. `order_sk` is used only as the last tie-breaker when ordering one customer's orders in `mart_revenue_daily`. The dimension keys that facts join on are deterministic, so joins and reports do not change.
