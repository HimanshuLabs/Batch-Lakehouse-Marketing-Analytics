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
  and (product_id <> 'UNKNOWN' or product_name <> 'Unknown' or category <> 'Unknown')

union all

select 'dim_campaign', campaign_id, campaign_name
from {{ ref('dim_campaign') }}
where campaign_sk = 0
  and (campaign_id <> 'UNKNOWN' or campaign_name <> 'Unknown' or campaign_type <> 'Unknown'
       or channel <> 'Unknown' or traffic_source <> 'Unknown' or target_segment <> 'Unknown')

union all

select 'mart_marketing_funnel', campaign_id, campaign_name
from {{ ref('mart_marketing_funnel') }}
where campaign_id = 'UNKNOWN'
  and (campaign_name <> 'Unknown' or campaign_type <> 'Unknown'
       or traffic_source <> 'Unknown' or target_segment <> 'Unknown')
