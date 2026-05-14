-- dbt/models/silver/silver_openfood_products.sql
-- -----------------------------------------------
-- PySpark (`silver_transform.py`) is the ONLY writer to the Delta table
-- `source('silver', 'silver_openfood_products')`. dbt does not rebuild Silver.
--
-- This model is **ephemeral** (`dbt_project.yml` → `silver:`): compiled as an inlined
-- CTE inside Gold models — no second physical Silver table.
--
-- Alternative pattern some teams use: Gold calls source() directly and deletes
-- this file. We keep one ephemeral "bridge" so all Gold models depend on one node.

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

WHERE barcode IS NOT NULL
