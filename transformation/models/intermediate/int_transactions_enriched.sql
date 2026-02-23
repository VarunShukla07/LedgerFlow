-- Intermediate: Enrich transactions with merchant data

{{ config(
    materialized='view',
    schema='intermediate'
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

merchants AS (
    SELECT * FROM {{ ref('stg_merchants') }}
),

enriched AS (
    SELECT
        t.*,
        m.merchant_id,
        
        -- Extract date/time components
        EXTRACT(HOUR FROM t.transaction_timestamp) AS transaction_hour,
        EXTRACT(DOW FROM t.transaction_timestamp) AS day_of_week,
        DATE(t.transaction_timestamp) AS transaction_date,
        
        -- Create time buckets
        CASE
            WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 6 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM t.transaction_timestamp) BETWEEN 18 AND 23 THEN 'evening'
            ELSE 'night'
        END AS time_of_day
        
    FROM transactions t
    LEFT JOIN merchants m
        ON t.merchant_name = m.merchant_name
        AND t.merchant_category = m.merchant_category
)

SELECT * FROM enriched