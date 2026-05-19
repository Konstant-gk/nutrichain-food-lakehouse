-- dbt/models/gold/pipeline_audit.sql
-- ------------------------------------
-- Purpose  : Pipeline health monitoring table for Power BI dashboard.
-- Grain    : one row per Airflow run (ingest_run_id)
-- Used by  : Power BI pipeline monitoring dashboard
--
-- This table answers questions like:
--   "How many products did we ingest on each run?"
--   "What % had nutriscore mismatches (data quality issue)?"
--   "How many are ultra-processed (nova_group = 4)?"
--   "Is data freshness improving or degrading?"

WITH silver AS (
    SELECT * FROM {{ source('silver', 'silver_openfood_products') }}
),

audit AS (
    SELECT
        ingest_run_id,

        -- When was this run ingested?
        MIN(ingested_at)                                AS run_ingested_at,
        MAX(silver_processed_at)                        AS silver_completed_at,

        -- Volume metrics
        COUNT(*)                                        AS total_products_processed,
        COUNT(DISTINCT barcode)                         AS unique_barcodes,
        COUNT(DISTINCT primary_brand)                   AS unique_brands,
        COUNT(DISTINCT primary_category)                AS unique_categories,
        COUNT(DISTINCT primary_country)                 AS unique_countries,

        -- Data quality metrics
        SUM(CASE WHEN nutriscore_grade_mismatch THEN 1 ELSE 0 END)
                                                        AS nutriscore_mismatch_count,
        ROUND(
            SUM(CASE WHEN nutriscore_grade_mismatch THEN 1.0 ELSE 0 END)
            / NULLIF(COUNT(*), 0) * 100, 2
        )                                               AS nutriscore_mismatch_pct,

        -- Completeness metrics (how many rows are missing key nutrition fields)
        SUM(CASE WHEN energy_kcal_per_100g IS NULL THEN 1 ELSE 0 END)
                                                        AS missing_kcal_count,
        SUM(CASE WHEN proteins_100g IS NULL THEN 1 ELSE 0 END)
                                                        AS missing_protein_count,
        SUM(CASE WHEN sugars_100g IS NULL THEN 1 ELSE 0 END)
                                                        AS missing_sugar_count,

        ROUND(
            SUM(CASE WHEN energy_kcal_per_100g IS NULL THEN 1.0 ELSE 0 END)
            / NULLIF(COUNT(*), 0) * 100, 2
        )                                               AS missing_kcal_pct,

        -- Barcode format (monitoring — non-EAN codes are kept, not dropped)
        ROUND(
            SUM(
                CASE
                    WHEN barcode IS NULL
                        OR NOT RLIKE(TRIM(barcode), '^[0-9]{8,14}$')
                    THEN 1.0
                    ELSE 0
                END
            ) / NULLIF(COUNT(*), 0) * 100,
            2
        )                                               AS invalid_ean_pct,

        -- NOVA group distribution
        SUM(CASE WHEN nova_group = 1 THEN 1 ELSE 0 END) AS nova_1_unprocessed_count,
        SUM(CASE WHEN nova_group = 2 THEN 1 ELSE 0 END) AS nova_2_culinary_count,
        SUM(CASE WHEN nova_group = 3 THEN 1 ELSE 0 END) AS nova_3_processed_count,
        SUM(CASE WHEN nova_group = 4 THEN 1 ELSE 0 END) AS nova_4_ultra_processed_count,

        -- Nutri-Score grade distribution
        SUM(CASE WHEN nutriscore_grade_reported = 'A' THEN 1 ELSE 0 END) AS grade_a_count,
        SUM(CASE WHEN nutriscore_grade_reported = 'B' THEN 1 ELSE 0 END) AS grade_b_count,
        SUM(CASE WHEN nutriscore_grade_reported = 'C' THEN 1 ELSE 0 END) AS grade_c_count,
        SUM(CASE WHEN nutriscore_grade_reported = 'D' THEN 1 ELSE 0 END) AS grade_d_count,
        SUM(CASE WHEN nutriscore_grade_reported = 'E' THEN 1 ELSE 0 END) AS grade_e_count,

        -- Sugar tier distribution
        SUM(CASE WHEN sugar_tier = 'Low'    THEN 1 ELSE 0 END) AS sugar_low_count,
        SUM(CASE WHEN sugar_tier = 'Medium' THEN 1 ELSE 0 END) AS sugar_medium_count,
        SUM(CASE WHEN sugar_tier = 'High'   THEN 1 ELSE 0 END) AS sugar_high_count,

        CURRENT_TIMESTAMP() AS audit_built_at

    FROM silver
    GROUP BY ingest_run_id
)

SELECT * FROM audit
ORDER BY run_ingested_at DESC