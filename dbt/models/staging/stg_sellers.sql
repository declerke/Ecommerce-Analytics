WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_sellers') }}
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM source
WHERE seller_id IS NOT NULL
