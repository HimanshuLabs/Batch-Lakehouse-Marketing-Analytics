-- Key-free fingerprint of the warehouse: one line per table with its row count
-- and an md5 of its content, where region_sk / channel_sk are replaced by their
-- natural keys and run-time columns are left out. Two builds of the same data
-- must give identical output, whatever surrogate key values they assign.
-- Used by tests/integration/run_integration.sh and rebuild_warehouse.sh.

SELECT 'dim_region' AS fingerprint, count(*),
       md5(string_agg(region_nk || ':' || country || ':' || state || ':' || city, ',' ORDER BY region_nk))
FROM warehouse.dim_region
UNION ALL
SELECT 'dim_channel', count(*),
       md5(string_agg(channel_nk || ':' || channel_name || ':' || coalesce(channel_group, ''), ',' ORDER BY channel_nk))
FROM warehouse.dim_channel
UNION ALL
SELECT 'fact_orders', count(*),
       md5(string_agg(f.order_id || ':' || f.customer_sk || ':' || f.campaign_sk || ':' || f.order_date_sk
                      || ':' || r.region_nk || ':' || c.channel_nk || ':' || coalesce((f.order_timestamp AT TIME ZONE 'UTC')::TEXT, '')
                      || ':' || f.item_count || ':' || f.gross_amount || ':' || f.discount_amount || ':' || f.net_revenue
                      || ':' || coalesce(f.payment_method, '') || ':' || coalesce(f.source_record_hash, '')
                      || ':' || f.order_status || ':' || f.tax_amount || ':' || f.shipping_amount || ':' || f.total_amount,
                      ',' ORDER BY f.order_id))
FROM warehouse.fact_orders f
JOIN warehouse.dim_region r USING (region_sk)
JOIN warehouse.dim_channel c USING (channel_sk)
UNION ALL
SELECT 'fact_order_items without region/channel', count(*),
       md5(string_agg(f.order_item_nk || ':' || coalesce(f.order_id, '') || ':' || f.customer_sk || ':' || f.product_sk
                      || ':' || f.order_date_sk || ':' || coalesce((f.order_timestamp AT TIME ZONE 'UTC')::TEXT, '') || ':' || f.quantity
                      || ':' || f.unit_price || ':' || f.discount_percent || ':' || f.line_revenue,
                      ',' ORDER BY f.order_item_nk))
FROM warehouse.fact_order_items f
UNION ALL
SELECT 'fact_order_items with region/channel', count(*),
       md5(string_agg(f.order_item_nk || ':' || r.region_nk || ':' || c.channel_nk, ',' ORDER BY f.order_item_nk))
FROM warehouse.fact_order_items f
JOIN warehouse.dim_region r USING (region_sk)
JOIN warehouse.dim_channel c USING (channel_sk)
UNION ALL
SELECT 'fact_campaign_spend', count(*),
       md5(string_agg(f.campaign_spend_nk || ':' || f.campaign_sk || ':' || f.spend_date_sk
                      || ':' || c.channel_nk || ':' || f.spend_amount || ':' || f.impressions || ':' || f.clicks
                      || ':' || f.conversions || ':' || f.attributed_revenue,
                      ',' ORDER BY f.campaign_spend_nk))
FROM warehouse.fact_campaign_spend f
JOIN warehouse.dim_channel c USING (channel_sk)
UNION ALL
SELECT 'fact_web_events', count(*),
       md5(string_agg(f.event_id || ':' || f.customer_sk || ':' || f.product_sk || ':' || f.campaign_sk
                      || ':' || f.event_date_sk || ':' || r.region_nk || ':' || c.channel_nk
                      || ':' || coalesce((f.event_timestamp AT TIME ZONE 'UTC')::TEXT, '') || ':' || f.event_type || ':' || f.event_value,
                      ',' ORDER BY f.event_id))
FROM warehouse.fact_web_events f
JOIN warehouse.dim_region r USING (region_sk)
JOIN warehouse.dim_channel c USING (channel_sk)
UNION ALL
SELECT 'fact_conversions', count(*),
       md5(string_agg(f.conversion_nk || ':' || f.customer_sk || ':' || f.product_sk || ':' || f.campaign_sk
                      || ':' || f.conversion_date_sk || ':' || r.region_nk || ':' || c.channel_nk
                      || ':' || f.conversion_value,
                      ',' ORDER BY f.conversion_nk))
FROM warehouse.fact_conversions f
JOIN warehouse.dim_region r USING (region_sk)
JOIN warehouse.dim_channel c USING (channel_sk)
UNION ALL
SELECT 'facts on the invalid-reference member', count(*),
       md5(string_agg(fact || ':' || natural_id, ',' ORDER BY fact, natural_id))
FROM (
    SELECT 'orders.customer' AS fact, order_id || '>' || customer_id AS natural_id FROM warehouse.fact_orders WHERE customer_sk = -1
    UNION ALL SELECT 'orders.campaign', order_id || '>' || campaign_id FROM warehouse.fact_orders WHERE campaign_sk = -1
    UNION ALL SELECT 'items.product', order_item_nk || '>' || product_id FROM warehouse.fact_order_items WHERE product_sk = -1
    UNION ALL SELECT 'spend.campaign', campaign_spend_nk || '>' || campaign_id FROM warehouse.fact_campaign_spend WHERE campaign_sk = -1
    UNION ALL SELECT 'web.customer', event_id || '>' || customer_id FROM warehouse.fact_web_events WHERE customer_sk = -1
    UNION ALL SELECT 'web.product', event_id || '>' || product_id FROM warehouse.fact_web_events WHERE product_sk = -1
    UNION ALL SELECT 'web.campaign', event_id || '>' || campaign_id FROM warehouse.fact_web_events WHERE campaign_sk = -1
) invalid_references
UNION ALL
SELECT 'dim_customer / dim_product / dim_campaign', (SELECT count(*) FROM warehouse.dim_customer)
       + (SELECT count(*) FROM warehouse.dim_product) + (SELECT count(*) FROM warehouse.dim_campaign),
       md5((SELECT string_agg(customer_sk || ':' || customer_id || ':' || effective_from || ':' || coalesce(customer_segment, ''), ',' ORDER BY customer_sk) FROM warehouse.dim_customer)
           || (SELECT string_agg(product_sk || ':' || product_id || ':' || effective_from || ':' || coalesce(category, ''), ',' ORDER BY product_sk) FROM warehouse.dim_product)
           || (SELECT string_agg(campaign_sk || ':' || campaign_id || ':' || effective_from || ':' || coalesce(channel, ''), ',' ORDER BY campaign_sk) FROM warehouse.dim_campaign))
ORDER BY 1;
