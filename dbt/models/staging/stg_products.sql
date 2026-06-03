{{ config(materialized='view') }}

select *
from {{ source('warehouse_staging', 'stg_gold_dim_products_scd2') }}
