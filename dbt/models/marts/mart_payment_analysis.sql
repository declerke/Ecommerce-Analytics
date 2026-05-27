WITH payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
)
SELECT
    payment_type,
    payment_installments,
    COUNT(DISTINCT order_id)      AS order_count,
    ROUND(SUM(payment_value), 2)  AS total_value,
    ROUND(AVG(payment_value), 2)  AS avg_value
FROM payments
GROUP BY payment_type, payment_installments
ORDER BY payment_type, payment_installments
