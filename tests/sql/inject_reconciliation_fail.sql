-- Test only: makes the build fail its reconciliation. Run between
-- create_marts.sql and reconciliation_report.sql, inside the build transaction,
-- it removes one loaded order, so the warehouse no longer equals staging.
DELETE FROM warehouse.fact_order_items
WHERE order_id = (SELECT min(order_id) FROM warehouse.fact_orders);
DELETE FROM warehouse.fact_orders
WHERE order_id = (SELECT min(order_id) FROM warehouse.fact_orders);
