-- SCD Type 2 (history): a change in a history attribute closes the current version
-- and opens a new one, so each dimension has exactly one current version per natural key.
-- Returns the offending keys; dbt test fails if any row comes back.

select 'dim_customer' as dimension, customer_id as natural_key, count(*) as current_rows
from {{ ref('dim_customer') }}
where is_current
group by customer_id
having count(*) <> 1

union all

select 'dim_product', product_id, count(*)
from {{ ref('dim_product') }}
where is_current
group by product_id
having count(*) <> 1

union all

select 'dim_campaign', campaign_id, count(*)
from {{ ref('dim_campaign') }}
where is_current
group by campaign_id
having count(*) <> 1
