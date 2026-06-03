{{ config(materialized='view') }}

select *
from {{ source('reporting_marts', 'mart_product_sales') }}
