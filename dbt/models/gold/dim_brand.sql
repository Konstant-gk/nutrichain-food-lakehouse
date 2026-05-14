-- dbt/models/gold/dim_brand.sql
-- --------------------------------
-- Purpose: Brand dimension — one row per unique brand name.
-- Grain: one brand name = one row.

WITH source AS (
    SELECT DISTINCT
        COALESCE(
            NULLIF(TRIM(CAST(primary_brand AS STRING)), ''),
            'Unknown'
        ) AS brand_name
    FROM {{ ref('silver_openfood_products') }}
),

final AS (
    SELECT
        SHA2(brand_name, 256)   AS brand_key,   -- surrogate key
        brand_name,                              -- natural key / display name
        CURRENT_TIMESTAMP()     AS gold_built_at
    FROM source
)

SELECT * FROM final