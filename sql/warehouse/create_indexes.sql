-- Warehouse indexing strategy for dashboard/reporting workloads.
-- These indexes target SCD2 current-row lookups, point-in-time joins, and common BI aggregations.

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customer_current
ON warehouse.dim_customer(customer_id)
WHERE is_current = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_product_current
ON warehouse.dim_product(product_id)
WHERE is_current = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_campaign_current
ON warehouse.dim_campaign(campaign_id)
WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_customer_natural_dates
ON warehouse.dim_customer(customer_id, effective_start_date, effective_end_date);

CREATE INDEX IF NOT EXISTS idx_dim_product_natural_dates
ON warehouse.dim_product(product_id, effective_start_date, effective_end_date);

CREATE INDEX IF NOT EXISTS idx_dim_campaign_natural_dates
ON warehouse.dim_campaign(campaign_id, effective_start_date, effective_end_date);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_date_sk
ON warehouse.fact_orders(order_date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_sk
ON warehouse.fact_orders(customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_campaign_sk
ON warehouse.fact_orders(campaign_sk);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id
ON warehouse.fact_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_sk
ON warehouse.fact_order_items(product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_campaign_spend_campaign_date
ON warehouse.fact_campaign_spend(campaign_sk, spend_date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_web_events_timestamp
ON warehouse.fact_web_events(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_fact_web_events_customer_product_campaign
ON warehouse.fact_web_events(customer_sk, product_sk, campaign_sk);
