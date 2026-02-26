-- Mart: Transaction fact table

{{ 
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    ) 
}}

SELECT
    transaction_id,
    customer_id,
    merchant_id,
    
    -- Measures
    amount,
    is_fraud,
    is_successful,
    
    -- Dimensions
    payment_method,
    transaction_size,
    time_of_day,
    city,
    state,
    country,
    
    -- Dates
    transaction_timestamp,
    transaction_date,
    transaction_hour,
    day_of_week,
    
    -- Metadata
    ingestion_timestamp

FROM {{ ref('int_transactions_enriched') }}

{% if is_incremental() %}
    WHERE transaction_date >= current_date - interval '3 day'
{% endif %}