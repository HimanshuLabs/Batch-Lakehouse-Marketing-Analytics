{{ config(materialized='view') }}

{% set src = source('warehouse_core', 'fact_campaign_spend') %}

{% if execute %}
  {% set columns = adapter.get_columns_in_relation(src) %}
  {% set column_names = columns | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set column_names = [] %}
{% endif %}

select
    src.*

    {% if 'spend_id' not in column_names %}
      , md5(row_to_json(src)::text) as spend_id
    {% endif %}

from {{ src }} as src
