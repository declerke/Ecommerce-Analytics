WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
)
SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP)     AS order_purchase_at,
    CAST(order_approved_at AS TIMESTAMP)            AS order_approved_at,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_at,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_at,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at
FROM source
WHERE order_id IS NOT NULL
