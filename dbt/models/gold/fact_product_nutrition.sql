-- dbt/models/gold/fact_product_nutrition.sql
-- -------------------------------------------
-- Purpose  : Central fact table — one row per product per snapshot date.
-- Grain    : barcode + snapshot_date
-- Measures : nutrition values, scores, rankings, category benchmarks
-- FK links : product_key, brand_key, category_key, country_key, nutriscore_key

WITH silver AS (
    SELECT * FROM {{ source('silver', 'silver_openfood_products') }}
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
        SHA2(silver.country_iso_code, 256) AS country_key,
        SHA2(
            CASE
                WHEN silver.nutriscore_grade_reported IN ('A', 'B', 'C', 'D', 'E')
                    THEN silver.nutriscore_grade_reported
                ELSE 'Unknown'
            END,
            256
        ) AS nutriscore_key,
        {%- set _rd = var('run_date', none) %}
        {%- if _rd is not none and _rd | string | trim != '' %}
        DATE('{{ _rd }}') AS snapshot_date
        {%- else %}
        CURRENT_DATE() AS snapshot_date
        {%- endif %}
    FROM silver
),

-- Step 2: Calculate category-level benchmark averages
-- These window functions compute averages across ALL products in the same category
-- so each product row can see how it compares to its peers
with_category_benchmarks AS (
    SELECT
        *,

        -- Category benchmarks: AVG only over plausible per-100g values (must match Silver caps).
        ROUND(
            AVG(
                CASE
                    WHEN energy_kcal_per_100g > 0 AND energy_kcal_per_100g <= 900
                        THEN energy_kcal_per_100g
                END
            ) OVER (
                PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')
            ),
            1
        ) AS category_avg_kcal_100g,

        ROUND(
            AVG(
                CASE
                    WHEN sugars_100g >= 0 AND sugars_100g <= 100 THEN sugars_100g
                END
            ) OVER (
                PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')
            ),
            1
        ) AS category_avg_sugar_100g,

        ROUND(
            AVG(
                CASE
                    WHEN fat_100g >= 0 AND fat_100g <= 100 THEN fat_100g
                END
            ) OVER (
                PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')
            ),
            1
        ) AS category_avg_fat_100g,

        ROUND(
            AVG(
                CASE
                    WHEN salt_100g >= 0 AND salt_100g <= 100 THEN salt_100g
                END
            ) OVER (
                PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')
            ),
            1
        ) AS category_avg_salt_100g,

        ROUND(
            AVG(
                CASE
                    WHEN proteins_100g >= 0 AND proteins_100g <= 100 THEN proteins_100g
                END
            ) OVER (
                PARTITION BY COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown')
            ),
            1
        ) AS category_avg_protein_100g,

        -- Healthiness rank within category
        -- Rank 1 = healthiest (lowest nutriscore = better)
        
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

        -- ── Degenerate dimensions (useful attributes kept on the fact) ────
        barcode                         AS product_id,
        COALESCE(NULLIF(TRIM(CAST(primary_brand AS STRING)), ''), 'Unknown')       AS brand_name,
        COALESCE(NULLIF(TRIM(CAST(primary_category AS STRING)), ''), 'Unknown') AS category_name,
        primary_country                 AS country_name,
        country_iso_code,
        data_quality_tier,
        completeness_score,
        is_nutritional_data_complete,

        -- ── Nutrition measures per 100g (consistent rounding) ───────────────
        ROUND(energy_kcal_per_100g, 1)      AS energy_kcal_per_100g,
        ROUND(proteins_100g, 1)             AS proteins_100g,
        ROUND(fat_100g, 1)                  AS fat_100g,
        ROUND(carbohydrates_100g, 1)        AS carbohydrates_100g,
        ROUND(sugars_100g, 1)               AS sugars_100g,
        ROUND(salt_100g, 2)                 AS salt_100g,
        ROUND(sodium_corrected_100g, 2)     AS sodium_corrected_100g,
        ROUND(fiber_100g, 1)                AS fiber_100g,

        -- ── Derived measures ──────────────────────────────────────────────
        ROUND(protein_density_score, 1)     AS protein_density_score,
        nutriscore_score_raw,
        ingredient_count,
        nova_group,
        CASE nova_group_label
            WHEN 'unclassified' THEN 'Unclassified'
            WHEN 'ultra_processed' THEN 'Ultra-processed'
            WHEN 'culinary_ingredient' THEN 'Culinary ingredient'
            WHEN 'unprocessed' THEN 'Unprocessed'
            WHEN 'processed' THEN 'Processed'
            ELSE nova_group_label
        END AS nova_group_label,

        -- ── Tier classifications (EU thresholds from Silver) ──────────────
        sugar_tier,
        fat_tier,
        salt_tier,

        -- ── Protein density tier (Gold business logic) ────────────────────
        -- 'high'   = protein_density_score >= 10 (protein-rich per calorie)
        -- 'medium' = protein_density_score >= 5
        -- 'low'    = anything below 5
        CASE
            WHEN protein_density_score IS NULL THEN 'Unknown'
            WHEN protein_density_score >= 10   THEN 'High'
            WHEN protein_density_score >= 5    THEN 'Medium'
            ELSE 'Low'
        END AS protein_density_tier,

        -- ── Category benchmark averages ───────────────────────────────────
        category_avg_kcal_100g,
        category_avg_sugar_100g,
        category_avg_fat_100g,
        category_avg_salt_100g,
        category_avg_protein_100g,

        -- ── Above/below category average flags ────────────────────────────
        CASE
            WHEN sugars_100g IS NULL OR category_avg_sugar_100g IS NULL THEN 'Unknown'
            WHEN sugars_100g > category_avg_sugar_100g THEN 'Yes'
            ELSE 'No'
        END AS is_above_avg_sugar,

        CASE
            WHEN fat_100g IS NULL OR category_avg_fat_100g IS NULL THEN 'Unknown'
            WHEN fat_100g > category_avg_fat_100g THEN 'Yes'
            ELSE 'No'
        END AS is_above_avg_fat,

        CASE
            WHEN salt_100g IS NULL OR category_avg_salt_100g IS NULL THEN 'Unknown'
            WHEN salt_100g > category_avg_salt_100g THEN 'Yes'
            ELSE 'No'
        END AS is_above_avg_salt,

        -- ── Healthiness ranking ───────────────────────────────────────────
        CAST(healthiness_rank_in_category AS INT) AS healthiness_rank_in_category,
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