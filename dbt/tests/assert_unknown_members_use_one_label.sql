-- Unknown members follow one convention: IDs and keys 'UNKNOWN', display names
-- and attribute labels 'Unknown'. Returns every unknown-member row (and every
-- funnel row on the unknown campaign) that spells it any other way.

select 'dim_customer' as relation, customer_id as id, customer_name as label
from {{ ref('dim_customer') }}
where customer_sk = 0
  and (customer_id <> 'UNKNOWN' or customer_name <> 'Unknown' or membership_tier <> 'Unknown'
       or customer_segment <> 'Unknown' or home_city <> 'Unknown' or home_state <> 'Unknown'
       or country <> 'Unknown')

union all

select 'dim_product', product_id, product_name
from {{ ref('dim_product') }}
where product_sk = 0
  and (product_id <> 'UNKNOWN' or product_name <> 'Unknown' or category <> 'Unknown'
       or brand <> 'Unknown' or product_status <> 'Unknown')

union all

select 'dim_campaign', campaign_id, campaign_name
from {{ ref('dim_campaign') }}
where campaign_sk = 0
  and (campaign_id <> 'UNKNOWN' or campaign_name <> 'Unknown' or channel <> 'Unknown'
       or traffic_source <> 'Unknown' or target_segment <> 'Unknown' or campaign_status <> 'Unknown'
       or ab_test_group <> 'Unknown' or campaign_type <> 'Unknown')

union all

select 'mart_marketing_funnel', campaign_id, campaign_name
from {{ ref('mart_marketing_funnel') }}
where campaign_id = 'UNKNOWN'
  and (campaign_name <> 'Unknown' or traffic_source <> 'Unknown' or target_segment <> 'Unknown')

-- The invalid-reference members (key -1): a fact whose key is present but has
-- no valid dimension row. ID 'INVALID', label 'Invalid reference'; never the
-- unknown member's spelling, so the two cannot be confused.

union all

select 'dim_customer', customer_id, customer_name
from {{ ref('dim_customer') }}
where customer_sk = -1
  and (customer_id <> 'INVALID' or customer_name <> 'Invalid reference')

union all

select 'dim_product', product_id, product_name
from {{ ref('dim_product') }}
where product_sk = -1
  and (product_id <> 'INVALID' or product_name <> 'Invalid reference' or category <> 'Invalid reference')

union all

select 'dim_campaign', campaign_id, campaign_name
from {{ ref('dim_campaign') }}
where campaign_sk = -1
  and (campaign_id <> 'INVALID' or campaign_name <> 'Invalid reference' or channel <> 'Invalid reference')
