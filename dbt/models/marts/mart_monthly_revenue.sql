WITH enriched AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
),
financials AS (
    SELECT
        order_id,
        SUM(price) AS order_revenue
    FROM {{ ref('int_order_financials') }}
    GROUP BY order_id
)
SELECT
    DATE_TRUNC('month', e.order_purchase_at)      AS order_month,
    COUNT(DISTINCT e.order_id)                    AS total_orders,
    COUNT(DISTINCT e.customer_unique_id)          AS unique_customers,
    ROUND(COALESCE(SUM(f.order_revenue), 0), 2)   AS total_revenue,
    ROUND(AVG(f.order_revenue), 2)                AS avg_order_value,
    ROUND(AVG(e.actual_delivery_days), 1)         AS avg_delivery_days,
    ROUND(AVG(e.avg_review_score), 2)             AS avg_review_score,
    ROUND(
        SUM(CASE WHEN e.is_on_time = true  THEN 1.0 ELSE 0.0 END) /
        NULLIF(SUM(CASE WHEN e.is_on_time IS NOT NULL THEN 1.0 ELSE 0.0 END), 0) * 100,
    2)                                            AS on_time_pct
FROM enriched e
LEFT JOIN financials f ON e.order_id = f.order_id
WHERE e.order_purchase_at IS NOT NULL
GROUP BY DATE_TRUNC('month', e.order_purchase_at)
HAVING ROUND(COALESCE(SUM(f.order_revenue), 0), 2) > 0
ORDER BY order_month
