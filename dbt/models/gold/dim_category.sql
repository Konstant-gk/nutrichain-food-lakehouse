-- dbt/models/gold/dim_category.sql
-- ----------------------------------
-- Purpose: Category dimension — one row per unique food category.

WITH source AS (
    SELECT DISTINCT
        COALESCE(
            NULLIF(TRIM(CAST(primary_category AS STRING)), ''),
            'Unknown'
        ) AS category_name
    FROM {{ source('silver', 'silver_openfood_products') }}
),

final AS (
    SELECT
        SHA2(category_name, 256)    AS category_key,
        category_name,
        CURRENT_TIMESTAMP()         AS gold_built_at
    FROM source
)

SELECT * FROM final