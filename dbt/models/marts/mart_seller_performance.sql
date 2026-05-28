WITH financials AS (
    SELECT * FROM {{ ref('int_order_financials') }}
),
enriched AS (
    SELECT order_id, avg_review_score, is_on_time
    FROM {{ ref('int_orders_enriched') }}
),
order_level AS (
    SELECT
        f.seller_id,
        f.seller_state,
        f.seller_city,
        f.order_id,
        SUM(f.price)           AS order_revenue,
        SUM(f.freight_value)   AS order_freight,
        COUNT(f.order_item_id) AS items_in_order,
        MAX(e.avg_review_score) AS avg_review_score,
        MAX(e.is_on_time)       AS is_on_time
    FROM financials f
    LEFT JOIN enriched e ON f.order_id = e.order_id
    GROUP BY f.seller_id, f.seller_state, f.seller_city, f.order_id
)
SELECT
    seller_id,
    seller_state,
    seller_city,
    COUNT(DISTINCT order_id)                                                 AS total_orders,
    SUM(items_in_order)                                                      AS total_items_sold,
    ROUND(SUM(order_revenue), 2)                                             AS total_revenue,
    ROUND(AVG(order_revenue), 2)                                             AS avg_order_revenue,
    ROUND(SUM(order_freight), 2)                                             AS total_freight,
    ROUND(AVG(avg_review_score), 2)                                          AS avg_review_score,
    ROUND(
        SUM(CASE WHEN is_on_time = true  THEN 1.0 ELSE 0.0 END) /
        NULLIF(SUM(CASE WHEN is_on_time IS NOT NULL THEN 1.0 ELSE 0.0 END), 0) * 100,
    2)                                                                       AS on_time_delivery_pct
FROM order_level
GROUP BY seller_id, seller_state, seller_city
