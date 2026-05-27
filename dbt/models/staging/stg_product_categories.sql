WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_product_category_translation') }}
)
SELECT
    product_category_name,
    product_category_name_english
FROM source
WHERE product_category_name IS NOT NULL
  AND product_category_name_english IS NOT NULL
