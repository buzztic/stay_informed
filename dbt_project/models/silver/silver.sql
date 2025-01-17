{{ config(materialized='table') }}

WITH ranked_data AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY title ORDER BY inserted_at DESC) AS row_num
    FROM
        {{ source('google_storage', 'raw') }}
)

SELECT
    *
FROM
    ranked_data
WHERE
    row_num = 1
