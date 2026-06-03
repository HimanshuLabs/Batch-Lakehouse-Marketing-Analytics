{{ config(materialized='view') }}

select *
from {{ source('reporting_marts', 'mart_campaign_performance') }}
