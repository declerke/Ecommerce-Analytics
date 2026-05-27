WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
reviews AS (
    SELECT
        order_id,
        ROUND(AVG(review_score), 2) AS avg_review_score
    FROM {{ ref('stg_order_reviews') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    o.order_status,
    o.order_purchase_at,
    o.order_approved_at,
    o.delivered_carrier_at,
    o.delivered_customer_at,
    o.estimated_delivery_at,
    r.avg_review_score,

    CASE
        WHEN o.delivered_customer_at IS NOT NULL AND o.order_purchase_at IS NOT NULL
        THEN DATEDIFF('day', o.order_purchase_at, o.delivered_customer_at)
        ELSE NULL
    END AS actual_delivery_days,

    CASE
        WHEN o.estimated_delivery_at IS NOT NULL AND o.order_purchase_at IS NOT NULL
        THEN DATEDIFF('day', o.order_purchase_at, o.estimated_delivery_at)
        ELSE NULL
    END AS estimated_delivery_days,

    CASE
        WHEN o.delivered_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
        THEN o.delivered_customer_at <= o.estimated_delivery_at
        ELSE NULL
    END AS is_on_time,

    CASE
        WHEN o.delivered_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
        THEN DATEDIFF('day', o.estimated_delivery_at, o.delivered_customer_at)
        ELSE NULL
    END AS days_late

FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN reviews r   ON o.order_id    = r.order_id
