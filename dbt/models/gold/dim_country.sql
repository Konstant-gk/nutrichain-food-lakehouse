-- dbt/models/gold/dim_country.sql
-- ----------------------------------
-- Purpose: Country dimension — one row per unique country.

WITH source AS (
    SELECT DISTINCT primary_country AS country_name
    FROM {{ ref('silver_openfood_products') }}
    WHERE primary_country IS NOT NULL
      AND LENGTH(TRIM(primary_country)) > 0
),

final AS (
    SELECT
        SHA2(country_name, 256) AS country_key,
        country_name,
        CURRENT_TIMESTAMP()     AS gold_built_at
    FROM source
)

SELECT * FROM final