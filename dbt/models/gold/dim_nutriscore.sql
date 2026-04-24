-- dbt/models/gold/dim_nutriscore.sql
-- ------------------------------------
-- Purpose: Static Nutri-Score grade lookup.
-- 5 rows only. A through E with descriptions and official score bands.

-- We use a VALUES clause because this is static reference data.
-- It never comes from the API — it's the official Nutri-Score standard.

WITH nutriscore_grades AS (
    SELECT *
    FROM (VALUES
        ('a', -100,  -1, 'Excellent nutritional quality',       '🟢'),
        ('b',    0,   2, 'Good nutritional quality',             '🟡'),
        ('c',    3,  10, 'Average nutritional quality',          '🟠'),
        ('d',   11,  18, 'Poor nutritional quality',             '🔴'),
        ('e',   19, 999, 'Very poor nutritional quality',        '⛔'),
        ('unknown', NULL, NULL, 'Score not available',           '⬜')
    ) AS t(grade, score_min, score_max, description, color_indicator)
)

SELECT
    SHA2(grade, 256)    AS nutriscore_key,
    grade,
    score_min,
    score_max,
    description,
    color_indicator,
    CURRENT_TIMESTAMP() AS gold_built_at

FROM nutriscore_grades