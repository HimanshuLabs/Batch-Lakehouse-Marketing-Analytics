WITH fact_revenue AS (
    SELECT ROUND(SUM(total_amount), 2) AS amount
    FROM {{ ref('stg_fact_orders_scd2') }}
),

mart_revenue AS (
    SELECT ROUND(SUM(total_revenue), 2) AS amount
    FROM {{ ref('stg_mart_campaign_performance_scd2') }}
)

SELECT *
FROM fact_revenue, mart_revenue
WHERE fact_revenue.amount <> mart_revenue.amount
