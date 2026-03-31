WITH base AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    hour_of_day,
    amount_bucket,
    transaction_type,
    COUNT(*) AS transaction_count,
    ROUND(AVG(Amount), 2) AS avg_amount,
    SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
FROM base
GROUP BY hour_of_day, amount_bucket, transaction_type
ORDER BY hour_of_day