-- Mart: Daily revenue summary

{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT
    transaction_date,
    
    -- Volume metrics
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT merchant_id) AS unique_merchants,
    
    -- Revenue metrics
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction_amount,
    MIN(amount) AS min_transaction_amount,
    MAX(amount) AS max_transaction_amount,
    
    -- Success metrics
    SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_transactions,
    SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS failed_transactions,
    ROUND(SUM(CASE WHEN is_successful THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS success_rate,
    
    -- Fraud metrics
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) AS fraud_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS fraud_rate,
    
    -- Payment method breakdown
    SUM(CASE WHEN payment_method = 'UPI' THEN 1 ELSE 0 END) AS upi_count,
    SUM(CASE WHEN payment_method = 'Card' THEN 1 ELSE 0 END) AS card_count,
    SUM(CASE WHEN payment_method = 'NetBanking' THEN 1 ELSE 0 END) AS netbanking_count,
    SUM(CASE WHEN payment_method = 'Wallet' THEN 1 ELSE 0 END) AS wallet_count

FROM {{ ref('int_transactions_enriched') }}
GROUP BY transaction_date
ORDER BY transaction_date