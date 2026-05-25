WITH fact_spend AS (
    SELECT ROUND(SUM(spend_amount), 2) AS amount
    FROM {{ ref('stg_fact_campaign_spend_scd2') }}
),

mart_spend AS (
    SELECT ROUND(SUM(total_spend), 2) AS amount
    FROM {{ ref('stg_mart_campaign_performance_scd2') }}
)

SELECT *
FROM fact_spend, mart_spend
WHERE fact_spend.amount <> mart_spend.amount
