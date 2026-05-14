-- dbt/models/gold/dim_category.sql
-- ----------------------------------
-- Purpose: Category dimension — one row per unique food category.

WITH source AS (
    SELECT DISTINCT COALESCE(NULLIF(TRIM(primary_category), ''), 'Unknown') AS category_name
    FROM {{ ref('silver_openfood_products') }}
),

final AS (
    SELECT
        SHA2(category_name, 256)    AS category_key,
        category_name,
        CURRENT_TIMESTAMP()         AS gold_built_at
    FROM source
)

SELECT * FROM final