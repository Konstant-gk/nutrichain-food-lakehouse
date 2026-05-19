-- dbt/models/gold/dim_nutriscore.sql
-- ------------------------------------
-- Purpose: Static Nutri-Score grade lookup (official A–E bands + Unknown).
-- Grain: one row per grade. Data lives in seeds/nutriscore_grade_lookup.csv;
-- ref() gives dbt a proper DAG edge so tests compile under path:models/gold.

SELECT
    SHA2(grade, 256)    AS nutriscore_key,
    grade,
    score_min,
    score_max,
    description,
    color_indicator,
    CURRENT_TIMESTAMP() AS gold_built_at
FROM {{ ref('nutriscore_grade_lookup') }}
