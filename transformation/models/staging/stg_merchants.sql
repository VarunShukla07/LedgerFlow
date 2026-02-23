-- Staging: Extract unique merchants

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH merchants AS (
    SELECT DISTINCT
        merchant_name,
        merchant_category
    FROM {{ source('raw', 'raw_transactions') }}
    WHERE merchant_name IS NOT NULL
),

with_id AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['merchant_name', 'merchant_category']) }} AS merchant_id,
        merchant_name,
        merchant_category
    FROM merchants
)

SELECT * FROM with_id