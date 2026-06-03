{{ config(materialized='view') }}

{% set src = source('warehouse_core', 'dim_date') %}

{% if execute %}
  {% set columns = adapter.get_columns_in_relation(src) %}
  {% set column_names = columns | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set column_names = [] %}
{% endif %}

select
    src.*

    {% if 'date_key' not in column_names %}
      {% if 'date_day' in column_names %}
        , src.date_day as date_key
      {% elif 'calendar_date' in column_names %}
        , src.calendar_date as date_key
      {% elif 'full_date' in column_names %}
        , src.full_date as date_key
      {% elif 'date' in column_names %}
        , src.date as date_key
      {% else %}
        , md5(row_to_json(src)::text) as date_key
      {% endif %}
    {% endif %}

from {{ src }} as src
