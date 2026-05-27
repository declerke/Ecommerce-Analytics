WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_order_reviews') }}
)
SELECT
    review_id,
    order_id,
    CAST(review_score AS INTEGER)                  AS review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS TIMESTAMP)        AS review_created_at,
    CAST(review_answer_timestamp AS TIMESTAMP)     AS review_answered_at
FROM source
WHERE review_id IS NOT NULL
  AND order_id IS NOT NULL
