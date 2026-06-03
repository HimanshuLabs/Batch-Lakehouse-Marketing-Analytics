{{ config(materialized='view') }}

select *
from {{ source('reporting_marts', 'mart_customer_360') }}
