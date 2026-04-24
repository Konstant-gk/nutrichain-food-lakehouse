-- dbt/models/gold/dim_category.sql
-- ----------------------------------
-- Purpose: Category dimension — one row per unique food category.

WITH source AS (
    SELECT DISTINCT primary_category AS category_name
    FROM {{ ref('silver_openfood_products') }}
    WHERE primary_category IS NOT NULL
      AND LENGTH(TRIM(primary_category)) > 0
),

final AS (
    SELECT
        SHA2(category_name, 256)    AS category_key,
        category_name,
        CURRENT_TIMESTAMP()         AS gold_built_at
    FROM source
)

SELECT * FROM final