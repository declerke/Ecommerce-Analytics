WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_order_items') }}
)
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at,
    CAST(price AS DOUBLE)                  AS price,
    CAST(freight_value AS DOUBLE)          AS freight_value,
    CAST(price AS DOUBLE) + CAST(freight_value AS DOUBLE) AS total_item_value
FROM source
WHERE order_id IS NOT NULL
