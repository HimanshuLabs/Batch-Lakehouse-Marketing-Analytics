-- Fingerprint of every reporting mart: row count and md5 of the sorted rows.
-- days_since_last_order depends on today's date, so it is left out.

SELECT 'mart_customer_360', count(*), md5(string_agg(t::TEXT, ',' ORDER BY t::TEXT))
FROM (SELECT to_jsonb(x) - 'days_since_last_order' AS t FROM marts.mart_customer_360 x) j
UNION ALL
SELECT 'mart_product_sales', count(*), md5(string_agg(x::TEXT, ',' ORDER BY x::TEXT)) FROM marts.mart_product_sales x
UNION ALL
SELECT 'mart_campaign_performance', count(*), md5(string_agg(x::TEXT, ',' ORDER BY x::TEXT)) FROM marts.mart_campaign_performance x
UNION ALL
SELECT 'mart_revenue_daily', count(*), md5(string_agg(x::TEXT, ',' ORDER BY x::TEXT)) FROM marts.mart_revenue_daily x
UNION ALL
SELECT 'mart_marketing_funnel', count(*), md5(string_agg(x::TEXT, ',' ORDER BY x::TEXT)) FROM marts.mart_marketing_funnel x
UNION ALL
SELECT 'mart_customer_daily_snapshot', count(*), md5(string_agg(t::TEXT, ',' ORDER BY t::TEXT))
FROM (SELECT to_jsonb(x) - 'days_since_last_order' AS t FROM marts.mart_customer_daily_snapshot x) j
UNION ALL
SELECT 'mart_product_daily_snapshot', count(*), md5(string_agg(x::TEXT, ',' ORDER BY x::TEXT)) FROM marts.mart_product_daily_snapshot x
UNION ALL
SELECT 'mart_campaign_daily_snapshot', count(*), md5(string_agg(x::TEXT, ',' ORDER BY x::TEXT)) FROM marts.mart_campaign_daily_snapshot x
ORDER BY 1;
