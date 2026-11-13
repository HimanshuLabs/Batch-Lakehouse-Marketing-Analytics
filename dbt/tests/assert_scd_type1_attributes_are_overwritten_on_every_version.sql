-- SCD Type 1 (overwrite): the latest value of each overwritten attribute is on
-- every version of the entity: a change opens no version and keeps no history.
-- Returns the entities that show two values; the test fails if any row comes back.

select 'dim_customer' as dimension, 'customer_name' as attribute, customer_id as natural_key,
       count(distinct coalesce(customer_name::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(customer_name::text, '<null>')) > 1

union all

select 'dim_customer' as dimension, 'email' as attribute, customer_id as natural_key,
       count(distinct coalesce(email::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(email::text, '<null>')) > 1

union all

select 'dim_customer' as dimension, 'gender' as attribute, customer_id as natural_key,
       count(distinct coalesce(gender::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(gender::text, '<null>')) > 1

union all

select 'dim_customer' as dimension, 'age' as attribute, customer_id as natural_key,
       count(distinct coalesce(age::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(age::text, '<null>')) > 1

union all

select 'dim_customer' as dimension, 'preferred_language' as attribute, customer_id as natural_key,
       count(distinct coalesce(preferred_language::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(preferred_language::text, '<null>')) > 1

union all

select 'dim_customer' as dimension, 'loyalty_points' as attribute, customer_id as natural_key,
       count(distinct coalesce(loyalty_points::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(loyalty_points::text, '<null>')) > 1

union all

select 'dim_customer' as dimension, 'is_prime_user' as attribute, customer_id as natural_key,
       count(distinct coalesce(is_prime_user::text, '<null>')) as distinct_values
from {{ ref('dim_customer') }}
group by customer_id
having count(distinct coalesce(is_prime_user::text, '<null>')) > 1

union all

select 'dim_product' as dimension, 'product_name' as attribute, product_id as natural_key,
       count(distinct coalesce(product_name::text, '<null>')) as distinct_values
from {{ ref('dim_product') }}
group by product_id
having count(distinct coalesce(product_name::text, '<null>')) > 1

union all

select 'dim_product' as dimension, 'brand' as attribute, product_id as natural_key,
       count(distinct coalesce(brand::text, '<null>')) as distinct_values
from {{ ref('dim_product') }}
group by product_id
having count(distinct coalesce(brand::text, '<null>')) > 1

union all

select 'dim_product' as dimension, 'original_price' as attribute, product_id as natural_key,
       count(distinct coalesce(original_price::text, '<null>')) as distinct_values
from {{ ref('dim_product') }}
group by product_id
having count(distinct coalesce(original_price::text, '<null>')) > 1

union all

select 'dim_product' as dimension, 'inventory_remaining' as attribute, product_id as natural_key,
       count(distinct coalesce(inventory_remaining::text, '<null>')) as distinct_values
from {{ ref('dim_product') }}
group by product_id
having count(distinct coalesce(inventory_remaining::text, '<null>')) > 1

union all

select 'dim_campaign' as dimension, 'campaign_name' as attribute, campaign_id as natural_key,
       count(distinct coalesce(campaign_name::text, '<null>')) as distinct_values
from {{ ref('dim_campaign') }}
group by campaign_id
having count(distinct coalesce(campaign_name::text, '<null>')) > 1

union all

select 'dim_campaign' as dimension, 'ab_test_group' as attribute, campaign_id as natural_key,
       count(distinct coalesce(ab_test_group::text, '<null>')) as distinct_values
from {{ ref('dim_campaign') }}
group by campaign_id
having count(distinct coalesce(ab_test_group::text, '<null>')) > 1
