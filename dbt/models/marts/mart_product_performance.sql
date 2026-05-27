WITH financials AS (
    SELECT * FROM {{ ref('int_order_financials') }}
)
SELECT
    product_category_english                  AS category,
    COUNT(DISTINCT order_id)                  AS total_orders,
    COUNT(DISTINCT product_id)                AS unique_products,
    COUNT(order_item_id)                      AS total_items_sold,
    ROUND(SUM(price), 2)                      AS total_revenue,
    ROUND(AVG(price), 2)                      AS avg_item_price,
    ROUND(SUM(freight_value), 2)              AS total_freight,
    ROUND(AVG(freight_value), 2)              AS avg_freight,
    ROUND(
        SUM(freight_value) / NULLIF(SUM(price), 0) * 100,
    2)                                        AS freight_to_revenue_pct
FROM financials
WHERE product_category_english IS NOT NULL
GROUP BY product_category_english
ORDER BY total_revenue DESC
