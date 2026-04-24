-- dbt/models/silver/silver_openfood_products.sql
-- -----------------------------------------------
-- Purpose: Expose the PySpark-built Silver Delta table as a dbt source.
-- This model creates a view that Gold models can reference using ref().
--
-- Why ref() instead of hardcoding the table name?
-- ref('silver_openfood_products') tells dbt that this Gold model DEPENDS
-- on the Silver model. dbt uses this to build a dependency graph and
-- always runs Silver before Gold automatically.

SELECT
    -- Identity
    barcode,
    product_name,
    primary_brand,
    primary_category,
    primary_country,
    brands_raw,
    categories_raw,
    countries_raw,
    packaging,
    allergens,

    -- Cleaned nutrition values (all per 100g, all in standard units)
    energy_kcal_per_100g,
    proteins_100g,
    fat_100g,
    carbohydrates_100g,
    sugars_100g,
    salt_100g,
    sodium_corrected_100g,
    fiber_100g,

    -- Derived columns from Silver cleaning
    protein_density_score,
    ingredient_count,
    nova_group,
    nova_group_label,

    -- Tier classifications (EU thresholds)
    sugar_tier,
    fat_tier,
    salt_tier,

    -- Nutri-Score
    nutriscore_score_raw,
    nutriscore_grade_reported,
    nutriscore_grade_recalculated,
    nutriscore_grade_mismatch,

    -- Lineage
    ingest_run_id,
    ingested_at,
    silver_processed_at,
    last_modified_unix,
    row_hash

FROM {{ source('silver', 'silver_openfood_products') }}

-- Only include rows where barcode exists (our primary key)
WHERE barcode IS NOT NULL