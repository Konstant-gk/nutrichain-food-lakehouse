-- dbt/models/gold/fact_product_nutrition.sql
-- -------------------------------------------
-- Purpose  : Central fact table — one row per product per snapshot date.
-- Grain    : barcode + snapshot_date
-- Measures : nutrition values, scores, rankings, category benchmarks
-- FK links : product_key, brand_key, category_key, country_key, nutriscore_key

WITH silver AS (
    SELECT * FROM {{ ref('silver_openfood_products') }}
),

-- Step 1: Add foreign keys that match the dimension tables.
-- OFF often has missing brands/categories; SHA2(NULL) is NULL and breaks not_null tests
-- and star-schema joins. Coalesce to a sentinel before hashing (dims include same sentinel).
-- CAST(... AS STRING) avoids edge cases where OFF columns are non-string in Delta; qualify
-- with silver.* so computed keys never shadow an upstream column name.
with_keys AS (
    SELECT
        silver.*,
        SHA2(silver.barcode, 256) AS product_key,
        SHA2(
            COALESCE(
                NULLIF(TRIM(CAST(silver.primary_brand AS STRING)), ''),
                'Unknown'
            ),
            256
        ) AS brand_key,
        SHA2(
            COALESCE(
                NULLIF(TRIM(CAST(silver.primary_category AS STRING)), ''),
                'Unknown'
            ),
            256
        ) AS category_key,
        SHA2(silver.primary_country, 256) AS country_key,
        SHA2(
            COALESCE(LOWER(silver.nutriscore_grade_reported), 'unknown'),
            256
        ) AS nutriscore_key,
        CURRENT_DATE() AS snapshot_date
    FROM silver
),

-- Step 2: Calculate category-level benchmark averages
-- These window functions compute averages across ALL products in the same category
-- so each product row can see how it compares to its peers
with_category_benchmarks AS (
    SELECT
        *,

        -- Average nutrition per category (the benchmark every product is compared to)
        ROUND(AVG(energy_kcal_per_100g)  OVER (PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')), 2)
            AS category_avg_kcal_100g,

        ROUND(AVG(sugars_100g)  OVER (PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')), 2)
            AS category_avg_sugar_100g,

        ROUND(AVG(fat_100g)     OVER (PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')), 2)
            AS category_avg_fat_100g,

        ROUND(AVG(salt_100g)    OVER (PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')), 2)
            AS category_avg_salt_100g,

        ROUND(AVG(proteins_100g) OVER (PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')), 2)
            AS category_avg_protein_100g,

        -- Healthiness rank within category
        -- Rank 1 = healthiest (lowest nutriscore = better)
        -- Analysts use this to answer: "Is our product in the top 10% of its category?"
        RANK() OVER (
            PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')
            ORDER BY nutriscore_score_raw ASC NULLS LAST
        ) AS healthiness_rank_in_category,

        -- Total products in category (for rank % calculation)
        COUNT(*) OVER (PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown'))
            AS category_product_count

    FROM with_keys
),

-- Step 3: Apply business logic flags and final derived columns
final AS (
    SELECT
        -- ── Foreign keys (links to dimension tables) ──────────────────────
        product_key,
        brand_key,
        category_key,
        country_key,
        nutriscore_key,
        snapshot_date,

        -- ── Degenerate dimensions (useful attributes kept on the fact) ─────
        barcode                         AS product_id,
        COALESCE(NULLIF(TRIM(CAST(primary_brand AS STRING)), ''), 'Unknown')       AS brand_name,
        COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown') AS category_name,
        primary_country                 AS country_name,

        -- ── Nutrition measures per 100g ───────────────────────────────────
        energy_kcal_per_100g,
        proteins_100g,
        fat_100g,
        carbohydrates_100g,
        sugars_100g,
        salt_100g,
        sodium_corrected_100g,
        fiber_100g,

        -- ── Derived measures ──────────────────────────────────────────────
        protein_density_score,
        nutriscore_score_raw,
        ingredient_count,
        nova_group,
        nova_group_label,

        -- ── Tier classifications (EU thresholds from Silver) ──────────────
        sugar_tier,
        fat_tier,
        salt_tier,

        -- ── Protein density tier (Gold business logic) ────────────────────
        -- 'high'   = protein_density_score >= 10 (protein-rich per calorie)
        -- 'medium' = protein_density_score >= 5
        -- 'low'    = anything below 5
        CASE
            WHEN protein_density_score IS NULL THEN 'unknown'
            WHEN protein_density_score >= 10   THEN 'high'
            WHEN protein_density_score >= 5    THEN 'medium'
            ELSE 'low'
        END AS protein_density_tier,

        -- ── Category benchmark averages ───────────────────────────────────
        category_avg_kcal_100g,
        category_avg_sugar_100g,
        category_avg_fat_100g,
        category_avg_salt_100g,
        category_avg_protein_100g,

        -- ── Above/below category average flags ────────────────────────────
        -- These are the "is this product worse than its peers?" flags
        -- Power BI uses these as filter conditions: "show me all products above avg"
        CASE
            WHEN sugars_100g IS NULL OR category_avg_sugar_100g IS NULL THEN NULL
            WHEN sugars_100g > category_avg_sugar_100g THEN TRUE
            ELSE FALSE
        END AS is_above_avg_sugar,

        CASE
            WHEN fat_100g IS NULL OR category_avg_fat_100g IS NULL THEN NULL
            WHEN fat_100g > category_avg_fat_100g THEN TRUE
            ELSE FALSE
        END AS is_above_avg_fat,

        CASE
            WHEN salt_100g IS NULL OR category_avg_salt_100g IS NULL THEN NULL
            WHEN salt_100g > category_avg_salt_100g THEN TRUE
            ELSE FALSE
        END AS is_above_avg_salt,

        -- ── Healthiness ranking ───────────────────────────────────────────
        healthiness_rank_in_category,
        category_product_count,

        -- Healthiness percentile (0 = healthiest, 100 = worst)
        -- "Our product is in the top 20% of its category"
        ROUND(
            (healthiness_rank_in_category - 1.0) / NULLIF(category_product_count - 1, 0) * 100,
            1
        ) AS healthiness_percentile,

        -- ── Regulatory flags ──────────────────────────────────────────────
        nutriscore_grade_reported,
        nutriscore_grade_recalculated,
        nutriscore_grade_mismatch,        -- TRUE = product is potentially mislabeled

        -- ── Pipeline lineage ──────────────────────────────────────────────
        ingest_run_id,
        ingested_at,
        silver_processed_at,
        CURRENT_TIMESTAMP()  AS gold_built_at

    FROM with_category_benchmarks
)

SELECT * FROM final