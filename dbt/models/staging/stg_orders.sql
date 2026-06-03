{{ config(materialized='view') }}

select *
from {{ source('warehouse_staging', 'stg_gold_fact_orders_scd2') }}
