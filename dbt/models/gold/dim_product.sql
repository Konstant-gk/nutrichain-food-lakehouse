-- dbt/models/gold/dim_product.sql
-- ---------------------------------
-- Purpose: Product dimension — one row per unique product (barcode).
-- Grain: one product = one barcode.
-- Used by: fact_product_nutrition (joined on product_id = barcode)

WITH source AS (
    -- source(): PySpark owns the Silver Delta table; declared in models/silver/sources.yml
    SELECT * FROM {{ source('silver', 'silver_openfood_products') }}
),

deduped AS (
    -- Same barcode can appear on multiple ingest runs; keep newest.
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY barcode
            ORDER BY last_modified_unix DESC NULLS LAST
        ) AS rn
    FROM source
),

final AS (
    SELECT
        -- Surrogate key: SHA-256 hash of barcode
        SHA2(barcode, 256)          AS product_key,

        -- Natural / business key
        barcode                     AS product_id,

        -- Descriptive attributes
        product_name,
        packaging,
        allergens,
        ingredient_count,

        -- NOVA classification
        nova_group,
        CASE nova_group_label
            WHEN 'unclassified' THEN 'Unclassified'
            WHEN 'ultra_processed' THEN 'Ultra-processed'
            WHEN 'culinary_ingredient' THEN 'Culinary ingredient'
            WHEN 'unprocessed' THEN 'Unprocessed'
            WHEN 'processed' THEN 'Processed'
            ELSE nova_group_label
        END AS nova_group_label,

        completeness_score,
        data_quality_tier,
        is_nutritional_data_complete,

        -- Nutri-Score information
        nutriscore_grade_reported,
        nutriscore_grade_recalculated,
        nutriscore_grade_mismatch,

        -- Same SHA2 rules as dim_brand / dim_category / dim_country.
        SHA2(
            COALESCE(
                NULLIF(TRIM(CAST(primary_brand AS STRING)), ''),
                'Unknown'
            ),
            256
        )    AS brand_key,
        SHA2(
            COALESCE(
                NULLIF(TRIM(CAST(primary_category AS STRING)), ''),
                'Unknown'
            ),
            256
        ) AS category_key,
        SHA2(country_iso_code, 256) AS country_key,

        -- Metadata
        ingest_run_id,
        silver_processed_at,
        CURRENT_TIMESTAMP()         AS gold_built_at

    FROM deduped
    WHERE rn = 1
      AND barcode IS NOT NULL
)

SELECT * FROM final