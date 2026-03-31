WITH source AS (
    SELECT * FROM {{ source('credit_card_pipeline', 'creditcard') }}
),
	
cleaned AS (
    SELECT
        Time,
        Amount,
        Class,
        CASE WHEN Class = 1 THEN 'Fraud' ELSE 'Legit' END AS transaction_type,
        CASE
            WHEN Amount < 10 THEN 'Micro'
            WHEN Amount BETWEEN 10 AND 100 THEN 'Small'
            WHEN Amount BETWEEN 100 AND 1000 THEN 'Medium'
            ELSE 'Large'
        END AS amount_bucket,
        FLOOR(Time / 3600) AS hour_of_day
    FROM source
)

SELECT * FROM cleaned