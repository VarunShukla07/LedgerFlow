-- Mart: Customer dimension

{{ config(
    materialized='table',
    schema='marts'
) }}

WITH customer_stats AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        
        -- Aggregates
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_spent,
        AVG(amount) AS avg_transaction_amount,
        MAX(amount) AS max_transaction_amount,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_count,
        
        MIN(transaction_timestamp) AS first_transaction_date,
        MAX(transaction_timestamp) AS last_transaction_date
        
    FROM {{ ref('int_transactions_enriched') }}
    GROUP BY customer_id, customer_name, customer_email
)

SELECT
    customer_id,
    customer_name,
    customer_email,
    total_transactions,
    total_spent,
    avg_transaction_amount,
    max_transaction_amount,
    fraud_count,
    successful_count,
    first_transaction_date,
    last_transaction_date,
    
    -- Derived metrics
    CASE 
        WHEN fraud_count > 0 THEN TRUE 
        ELSE FALSE 
    END AS has_fraud_history,
    
    ROUND(successful_count::NUMERIC / NULLIF(total_transactions, 0) * 100, 2) AS success_rate

FROM customer_stats