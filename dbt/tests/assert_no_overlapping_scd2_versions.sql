-- No two versions of the same natural key overlap in time, so a fact time
-- matches at most one version. Returns the overlapping pairs.

select 'dim_customer' as dimension, a.customer_id as natural_key,
       a.customer_sk as version_a, b.customer_sk as version_b
from {{ ref('dim_customer') }} a
join {{ ref('dim_customer') }} b
  on a.customer_id = b.customer_id
 and a.customer_sk < b.customer_sk
 and a.effective_from < b.effective_to
 and b.effective_from < a.effective_to

union all

select 'dim_product', a.product_id, a.product_sk, b.product_sk
from {{ ref('dim_product') }} a
join {{ ref('dim_product') }} b
  on a.product_id = b.product_id
 and a.product_sk < b.product_sk
 and a.effective_from < b.effective_to
 and b.effective_from < a.effective_to

union all

select 'dim_campaign', a.campaign_id, a.campaign_sk, b.campaign_sk
from {{ ref('dim_campaign') }} a
join {{ ref('dim_campaign') }} b
  on a.campaign_id = b.campaign_id
 and a.campaign_sk < b.campaign_sk
 and a.effective_from < b.effective_to
 and b.effective_from < a.effective_to
