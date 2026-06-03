{{ config(materialized='view') }}

select *
from {{ source('warehouse_audit', 'reconciliation_report') }}
