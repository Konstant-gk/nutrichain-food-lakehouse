-- dbt/models/gold/dim_country.sql
-- ----------------------------------
-- Purpose: Country dimension — one row per ISO code.
-- country_key hashes country_iso_code (not display name) so one label cannot
-- appear under multiple ISO codes and collide on the same surrogate key.
-- Gold dim: one row per country_iso_code (key is SHA2(iso), not display name).


WITH per_iso AS (
    SELECT
        country_iso_code,
        primary_country AS country_name
    FROM {{ source('silver', 'silver_openfood_products') }}
    WHERE country_iso_code IS NOT NULL
      AND LENGTH(TRIM(country_iso_code)) > 0
      AND primary_country IS NOT NULL
      AND LENGTH(TRIM(primary_country)) > 0
),

rolled AS (
    SELECT
        country_iso_code,
        MAX(country_name) AS country_name
    FROM per_iso
    GROUP BY country_iso_code
),

final AS (
    SELECT
        SHA2(country_iso_code, 256) AS country_key,
        country_iso_code,
        country_name,
        CURRENT_TIMESTAMP()         AS gold_built_at
    FROM rolled
)

SELECT * FROM final