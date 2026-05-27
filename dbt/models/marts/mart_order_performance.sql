WITH enriched AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
)
SELECT
    order_id,
    customer_unique_id,
    customer_state,
    order_status,
    order_purchase_at,
    DATE_TRUNC('month', order_purchase_at) AS order_month,
    actual_delivery_days,
    estimated_delivery_days,
    is_on_time,
    days_late,
    avg_review_score,

    CASE
        WHEN order_status = 'delivered' AND is_on_time = true  THEN 'On Time'
        WHEN order_status = 'delivered' AND is_on_time = false THEN 'Late'
        WHEN order_status IN ('canceled', 'unavailable')       THEN 'Cancelled'
        WHEN order_status IN ('shipped', 'processing', 'invoiced', 'approved', 'created')
                                                               THEN 'In Progress'
        ELSE 'Unknown'
    END AS delivery_status

FROM enriched
