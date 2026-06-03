{{ config(materialized='view') }}

select *
from {{ source('warehouse_core', 'dim_product') }}
