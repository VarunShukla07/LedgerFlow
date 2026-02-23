-- Staging layer: Clean and standardize raw transactions

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_transactions') }}
),

cleaned AS (
    SELECT
        -- Primary keys
        transaction_id,
        customer_id,
        
        -- Customer info
        customer_name,
        customer_email,
        
        -- Merchant info
        merchant_name,
        merchant_category,
        
        -- Transaction details
        CAST(amount AS DECIMAL(10,2)) AS amount,
        payment_method,
        status,
        failure_reason,
        
        -- Location
        city,
        state,
        country,
        currency,
        
        -- Flags and metadata
        device_type,
        ip_address,
        CAST(is_fraud AS INTEGER) AS is_fraud,
        
        -- Timestamps
        CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,
        ingestion_timestamp,
        
        -- Derived fields
        CASE 
            WHEN status = 'SUCCESS' THEN TRUE 
            ELSE FALSE 
        END AS is_successful,
        
        CASE
            WHEN amount < 100 THEN 'small'
            WHEN amount BETWEEN 100 AND 1000 THEN 'medium'
            ELSE 'large'
        END AS transaction_size,
        
        -- Partition columns
        year AS partition_year,
        month AS partition_month,
        day AS partition_day
        
    FROM source
    WHERE is_valid = TRUE  -- Only valid records
)

SELECT * FROM cleaned