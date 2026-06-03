{{ config(materialized='view') }}

select *
from {{ source('warehouse_core', 'fact_orders') }}
