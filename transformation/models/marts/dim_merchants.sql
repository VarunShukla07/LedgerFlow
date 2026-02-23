-- Mart: Merchant dimension with stats

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH merchant_stats AS (
    SELECT
        merchant_id,
        merchant_name,
        merchant_category,
        
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_transaction_amount,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_count
        
    FROM {{ ref('int_transactions_enriched') }}
    GROUP BY merchant_id, merchant_name, merchant_category
)

SELECT
    merchant_id,
    merchant_name,
    merchant_category,
    total_transactions,
    total_revenue,
    avg_transaction_amount,
    fraud_count,
    successful_count,
    
    ROUND(successful_count::NUMERIC / NULLIF(total_transactions, 0) * 100, 2) AS success_rate,
    ROUND(fraud_count::NUMERIC / NULLIF(total_transactions, 0) * 100, 2) AS fraud_rate

FROM merchant_stats