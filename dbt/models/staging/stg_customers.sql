{{ config(materialized='view') }}

select *
from {{ source('warehouse_staging', 'stg_gold_dim_customers_scd2') }}
