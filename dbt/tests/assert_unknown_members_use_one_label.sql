-- Unknown members follow one convention: IDs and keys 'UNKNOWN', display names
-- and attribute labels 'Unknown'. Returns every unknown-member row (and every
-- funnel row on the unknown campaign) that spells it any other way.

select 'dim_customer' as relation, customer_id as id, customer_name as label
from {{ ref('dim_customer') }}
where customer_sk = 0
  and (customer_id is distinct from 'UNKNOWN' or customer_name is distinct from 'Unknown' or membership_tier is distinct from 'Unknown'
       or customer_segment is distinct from 'Unknown' or home_city is distinct from 'Unknown' or home_state is distinct from 'Unknown'
       or country is distinct from 'Unknown')

union all

select 'dim_product', product_id, product_name
from {{ ref('dim_product') }}
where product_sk = 0
  and (product_id is distinct from 'UNKNOWN' or product_name is distinct from 'Unknown' or category is distinct from 'Unknown'
       or brand is distinct from 'Unknown' or product_status is distinct from 'Unknown')

union all

select 'dim_campaign', campaign_id, campaign_name
from {{ ref('dim_campaign') }}
where campaign_sk = 0
  and (campaign_id is distinct from 'UNKNOWN' or campaign_name is distinct from 'Unknown' or channel is distinct from 'Unknown'
       or traffic_source is distinct from 'Unknown' or target_segment is distinct from 'Unknown' or campaign_status is distinct from 'Unknown'
       or ab_test_group is distinct from 'Unknown' or campaign_type is distinct from 'Unknown')

union all

select 'mart_marketing_funnel', campaign_id, campaign_name
from {{ ref('mart_marketing_funnel') }}
where campaign_id = 'UNKNOWN'
  and (campaign_name is distinct from 'Unknown' or traffic_source is distinct from 'Unknown' or target_segment is distinct from 'Unknown')

-- The invalid-reference members (key -1): a fact whose key is present but has
-- no valid dimension row. ID 'INVALID', label 'Invalid reference'; never the
-- unknown member's spelling, so the two cannot be confused.

union all

select 'dim_customer', customer_id, customer_name
from {{ ref('dim_customer') }}
where customer_sk = -1
  and (customer_id is distinct from 'INVALID' or customer_name is distinct from 'Invalid reference')

union all

select 'dim_product', product_id, product_name
from {{ ref('dim_product') }}
where product_sk = -1
  and (product_id is distinct from 'INVALID' or product_name is distinct from 'Invalid reference' or category is distinct from 'Invalid reference')

union all

select 'dim_campaign', campaign_id, campaign_name
from {{ ref('dim_campaign') }}
where campaign_sk = -1
  and (campaign_id is distinct from 'INVALID' or campaign_name is distinct from 'Invalid reference' or channel is distinct from 'Invalid reference')
