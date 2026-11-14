-- SCD Type 0 (fixed): an entity has one value of each fixed attribute on all its
-- versions, the first one the source delivered. A later source change is ignored
-- (and counted in logs/scd2_quality_report.json), so it can never appear here.
-- Returns the offending entities; the test fails if any row comes back.

select 'dim_customer' as dimension, 'signup_date' as attribute, customer_id as natural_key,
       count(distinct coalesce(signup_date::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(signup_date::text, '<null>')) > 1

union all

select 'dim_product' as dimension, 'category' as attribute, product_id as natural_key,
       count(distinct coalesce(category::text, '<null>')) as distinct_values
from {{ ref('dim_product') }}
group by product_id
having count(distinct coalesce(category::text, '<null>')) > 1

union all

select 'dim_campaign' as dimension, 'channel' as attribute, campaign_id as natural_key,
       count(distinct coalesce(channel::text, '<null>')) as distinct_values
from {{ ref('dim_campaign') }}
group by campaign_id
having count(distinct coalesce(channel::text, '<null>')) > 1

union all

select 'dim_campaign' as dimension, 'campaign_type' as attribute, campaign_id as natural_key,
       count(distinct coalesce(campaign_type::text, '<null>')) as distinct_values
from {{ ref('dim_campaign') }}
group by campaign_id
having count(distinct coalesce(campaign_type::text, '<null>')) > 1
