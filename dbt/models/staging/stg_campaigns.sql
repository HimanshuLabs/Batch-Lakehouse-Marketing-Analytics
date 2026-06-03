{{ config(materialized='view') }}

select *
from {{ source('warehouse_staging', 'stg_gold_dim_campaigns_scd2') }}
