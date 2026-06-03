{{ config(materialized='view') }}

select *
from {{ source('warehouse_staging', 'stg_gold_fact_campaign_spend_scd2') }}
