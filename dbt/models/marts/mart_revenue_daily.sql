{{ config(materialized='view') }}

select *
from {{ source('reporting_marts', 'mart_revenue_daily') }}
