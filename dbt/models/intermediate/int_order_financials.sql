WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
categories AS (
    SELECT * FROM {{ ref('stg_product_categories') }}
),
sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),
payments AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2)        AS total_payment_value,
        MAX(payment_installments)            AS max_installments,
        COUNT(DISTINCT payment_type)         AS payment_types_count
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
)
SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    s.seller_state,
    s.seller_city,
    p.product_category_name,
    COALESCE(c.product_category_name_english, p.product_category_name) AS product_category_english,
    i.price,
    i.freight_value,
    i.total_item_value,
    pay.total_payment_value,
    pay.max_installments,
    pay.payment_types_count
FROM items i
LEFT JOIN products  p   ON i.product_id = p.product_id
LEFT JOIN categories c  ON p.product_category_name = c.product_category_name
LEFT JOIN sellers    s   ON i.seller_id  = s.seller_id
LEFT JOIN payments   pay ON i.order_id   = pay.order_id
