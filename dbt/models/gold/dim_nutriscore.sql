-- dbt/models/gold/dim_nutriscore.sql
-- ------------------------------------
-- Purpose: Static Nutri-Score grade lookup (official A–E bands + Unknown).
-- Grain: one row per grade. Data lives in seeds/nutriscore_grade_lookup.csv;
-- Gold dim: Nutri-Score grade labels from seed nutriscore_grade_lookup (run dbt seed first).

WITH seed AS (
    SELECT * FROM {{ ref('nutriscore_grade_lookup') }}
),

normalized AS (
    SELECT
        CASE
            WHEN UPPER(TRIM(CAST(grade AS STRING))) IN ('A', 'B', 'C', 'D', 'E')
                THEN UPPER(TRIM(CAST(grade AS STRING)))
            ELSE 'Unknown'
        END AS grade,
        score_min,
        score_max,
        description,
        color_indicator
    FROM seed
)

SELECT
    SHA2(grade, 256)    AS nutriscore_key,
    grade,
    score_min,
    score_max,
    description,
    color_indicator,
    CURRENT_TIMESTAMP() AS gold_built_at
FROM normalized
