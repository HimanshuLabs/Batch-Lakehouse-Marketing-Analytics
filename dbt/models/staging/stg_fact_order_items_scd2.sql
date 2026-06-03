{{ config(materialized='view') }}

select *
from {{ source('warehouse_staging', 'stg_gold_fact_order_items_scd2') }}
