"""
gold_mart.py
------------
Purpose  : Silver Delta → Gold Star Schema tables for Power BI consumption.

Company context:
    NutriChain Retail Intelligence — Gold is the business-ready layer.
    Supermarket buyers, regulatory teams, and brand managers query these
    tables via Power BI to benchmark products, flag mislabeling, and
    assess portfolio health.

Star Schema built:
    dim_product          — one row per unique product (barcode)
    dim_brand            — one row per unique brand
    dim_category         — one row per unique category
    dim_country          — one row per unique country
    dim_nutriscore       — static lookup: grades A–E with descriptions
    fact_product_nutrition — measures per product per snapshot date

Business logic applied:
    - healthiness_rank: rank within category by nutriscore score (lower = better)
    - category_avg_sugar / fat / salt: benchmark averages
    - is_above_avg_sugar / fat / salt: Boolean flags vs category average
    - protein_density_tier: 'high' / 'medium' / 'low' banding

Runs as: Databricks Python file job task.
Triggered by: Airflow DatabricksRunNowOperator (Task 5 in the DAG).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gold: Silver → Star Schema tables.")
    p.add_argument("--run_id", required=True)
    p.add_argument("--catalog", default="nutrichain_lakehouse")
    p.add_argument("--silver_schema", default="silver")
    p.add_argument("--gold_schema", default="gold")
    p.add_argument("--silver_table", default="silver_openfood_products")
    p.add_argument("--gold_fact_table", default="fact_product_nutrition")
    p.add_argument("--gold_dim_product", default="dim_product")
    p.add_argument("--gold_dim_brand", default="dim_brand")
    p.add_argument("--gold_dim_category", default="dim_category")
    p.add_argument("--gold_dim_country", default="dim_country")
    p.add_argument("--gold_dim_nutriscore", default="dim_nutriscore")
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    catalog = args.catalog.strip()
    silver_schema = args.silver_schema.strip()
    gold_schema = args.gold_schema.strip()

    silver_full = f"{catalog}.{silver_schema}.{args.silver_table}"
    fact_full = f"{catalog}.{gold_schema}.{args.gold_fact_table}"
    dim_product_full = f"{catalog}.{gold_schema}.{args.gold_dim_product}"
    dim_brand_full = f"{catalog}.{gold_schema}.{args.gold_dim_brand}"
    dim_category_full = f"{catalog}.{gold_schema}.{args.gold_dim_category}"
    dim_country_full = f"{catalog}.{gold_schema}.{args.gold_dim_country}"
    dim_nutriscore_full = f"{catalog}.{gold_schema}.{args.gold_dim_nutriscore}"

    logger.info("Gold mart starting. run_id=%s, source=%s", run_id, silver_full)

    spark = SparkSession.builder.appName("gold_nutrichain_mart").getOrCreate()

    silver_df = spark.read.format("delta").table(silver_full)
    total = silver_df.count()
    if total == 0:
        raise RuntimeError(f"Silver table is empty: {silver_full}")
    logger.info("Silver rows: %d", total)

    snapshot_date = run_id[:4] + "-" + run_id[4:6] + "-" + run_id[6:8]

    # ── DIMENSION: dim_nutriscore (static lookup — never changes) ─────────────
    nutriscore_data = [
        ("a", -100, -1,  "Excellent nutritional quality. Recommended."),
        ("b",    0,  2,  "Good nutritional quality."),
        ("c",    3, 10,  "Average nutritional quality."),
        ("d",   11, 18,  "Poor nutritional quality. Limit consumption."),
        ("e",   19, 999, "Very poor nutritional quality. Avoid."),
        ("unknown", None, None, "Score not available."),
    ]
    nutriscore_schema = ["grade", "score_min", "score_max", "description"]
    dim_nutriscore_df = (
        spark.createDataFrame(nutriscore_data, nutriscore_schema)
        .withColumn("gold_built_at", F.current_timestamp())
    )
    dim_nutriscore_df.write.format("delta").mode("overwrite").saveAsTable(dim_nutriscore_full)
    logger.info("Written: %s (%d rows)", dim_nutriscore_full, dim_nutriscore_df.count())

    # ── DIMENSION: dim_brand ──────────────────────────────────────────────────
    # One row per unique brand name found across all products
    dim_brand_df = (
        silver_df.select(
            F.coalesce(
                F.nullif(F.trim(F.col("primary_brand").cast("string")), F.lit("")),
                F.lit("Unknown"),
            ).alias("brand_name")
        )
        .distinct()
        .withColumn("brand_id", F.sha2(F.col("brand_name"), 256))
        .withColumn("gold_built_at", F.current_timestamp())
    )
    dim_brand_df.write.format("delta").mode("overwrite").saveAsTable(dim_brand_full)
    logger.info("Written: %s (%d rows)", dim_brand_full, dim_brand_df.count())

    # ── DIMENSION: dim_category ───────────────────────────────────────────────
    dim_category_df = (
        silver_df.select(
            F.coalesce(
                F.nullif(F.trim(F.col("primary_category").cast("string")), F.lit("")),
                F.lit("Unknown"),
            ).alias("category_name")
        )
        .distinct()
        .withColumn("category_id", F.sha2(F.col("category_name"), 256))
        .withColumn("gold_built_at", F.current_timestamp())
    )
    dim_category_df.write.format("delta").mode("overwrite").saveAsTable(dim_category_full)
    logger.info("Written: %s (%d rows)", dim_category_full, dim_category_df.count())

    # ── DIMENSION: dim_country ────────────────────────────────────────────────
    dim_country_df = (
        silver_df
        .select(F.col("primary_country").alias("country_name"))
        .filter(
            F.col("country_name").isNotNull() & (F.length(F.col("country_name")) > 0)
        )
        .distinct()
        .withColumn("country_id", F.sha2(F.col("country_name"), 256))
        .withColumn("gold_built_at", F.current_timestamp())
    )
    dim_country_df.write.format("delta").mode("overwrite").saveAsTable(dim_country_full)
    logger.info("Written: %s (%d rows)", dim_country_full, dim_country_df.count())

    # ── DIMENSION: dim_product ────────────────────────────────────────────────
    # One row per unique product (barcode = natural key)
    dim_product_df = (
        silver_df
        .select(
            F.col("barcode").alias("product_id"),
            F.col("product_name"),
            F.col("brands_raw"),
            F.col("primary_brand").alias("brand_name"),
            F.col("primary_category").alias("category_name"),
            F.col("primary_country").alias("country_name"),
            F.col("packaging"),
            F.col("allergens"),
            F.col("ingredient_count"),
            F.col("nova_group"),
            F.col("nova_group_label"),
            F.col("nutriscore_grade_reported"),
            F.col("nutriscore_grade_recalculated"),
            F.col("nutriscore_grade_mismatch"),
        )
        .withColumn("gold_built_at", F.current_timestamp())
    )
    dim_product_df.write.format("delta").mode("overwrite").saveAsTable(dim_product_full)
    logger.info("Written: %s (%d rows)", dim_product_full, dim_product_df.count())

    # ── FACT: fact_product_nutrition ──────────────────────────────────────────
    # One row per product per snapshot_date.
    # Snapshot date = the Airflow run date (daily refresh).
    # Foreign keys are SHA-256 hashes of the natural keys.

    # Step A: Add foreign key columns to silver (join keys to dims)
    fact_base_df = (
        silver_df
        .withColumn("product_id", F.col("barcode"))
        .withColumn(
            "_norm_brand",
            F.coalesce(
                F.nullif(F.trim(F.col("primary_brand").cast("string")), F.lit("")),
                F.lit("Unknown"),
            ),
        )
        .withColumn(
            "_norm_category",
            F.coalesce(
                F.nullif(F.trim(F.col("primary_category").cast("string")), F.lit("")),
                F.lit("Unknown"),
            ),
        )
        .withColumn("brand_id", F.sha2(F.col("_norm_brand"), 256))
        .withColumn("category_id", F.sha2(F.col("_norm_category"), 256))
        .withColumn("country_id", F.sha2(F.col("primary_country"), 256))
        .withColumn("nutriscore_grade_key",
                    F.coalesce(F.lower(F.col("nutriscore_grade_reported")),
                               F.lit("unknown")))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )

    # Step B: Category-level benchmark averages
    # These tell analysts: "for THIS product's category, what is the avg sugar?"
    # Then the fact row carries both the product's actual value AND the category avg.
    category_window = Window.partitionBy("_norm_category")

    fact_df = (
        fact_base_df
        .withColumn(
            "category_avg_sugar_100g",
            F.round(F.avg("sugars_100g").over(category_window), 2),
        )
        .withColumn(
            "category_avg_fat_100g",
            F.round(F.avg("fat_100g").over(category_window), 2),
        )
        .withColumn(
            "category_avg_salt_100g",
            F.round(F.avg("salt_100g").over(category_window), 2),
        )
        .withColumn(
            "category_avg_kcal_100g",
            F.round(F.avg("energy_kcal_per_100g").over(category_window), 2),
        )
        # Step C: Boolean flags — is this product worse than its category average?
        .withColumn(
            "is_above_avg_sugar",
            F.when(
                F.col("sugars_100g").isNotNull()
                & F.col("category_avg_sugar_100g").isNotNull(),
                F.col("sugars_100g") > F.col("category_avg_sugar_100g"),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn(
            "is_above_avg_fat",
            F.when(
                F.col("fat_100g").isNotNull()
                & F.col("category_avg_fat_100g").isNotNull(),
                F.col("fat_100g") > F.col("category_avg_fat_100g"),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn(
            "is_above_avg_salt",
            F.when(
                F.col("salt_100g").isNotNull()
                & F.col("category_avg_salt_100g").isNotNull(),
                F.col("salt_100g") > F.col("category_avg_salt_100g"),
            ).otherwise(F.lit(None).cast("boolean")),
        )
        # Step D: Healthiness rank within category (lower nutriscore = healthier)
        .withColumn(
            "healthiness_rank_in_category",
            F.rank().over(
                Window.partitionBy("_norm_category")
                .orderBy(F.col("nutriscore_score_raw").asc_nulls_last())
            ),
        )
        # Step E: Protein density tier
        .withColumn(
            "protein_density_tier",
            F.when(F.col("protein_density_score").isNull(), "unknown")
            .when(F.col("protein_density_score") >= 10.0, "high")
            .when(F.col("protein_density_score") >= 5.0, "medium")
            .otherwise("low"),
        )
    )

    # Step F: Select final fact columns
    fact_final_df = fact_df.select(
        # Keys
        F.col("product_id"),
        F.col("brand_id"),
        F.col("category_id"),
        F.col("country_id"),
        F.col("nutriscore_grade_key"),
        F.col("snapshot_date"),
        # Measures — raw nutrition per 100g
        F.col("energy_kcal_per_100g"),
        F.col("proteins_100g"),
        F.col("fat_100g"),
        F.col("carbohydrates_100g"),
        F.col("sugars_100g"),
        F.col("salt_100g"),
        F.col("sodium_corrected_100g"),
        F.col("fiber_100g"),
        # Derived measures
        F.col("protein_density_score"),
        F.col("nutriscore_score_raw"),
        # Classification columns
        F.col("sugar_tier"),
        F.col("fat_tier"),
        F.col("salt_tier"),
        F.col("nova_group"),
        F.col("nova_group_label"),
        F.col("protein_density_tier"),
        # Business intelligence columns
        F.col("category_avg_sugar_100g"),
        F.col("category_avg_fat_100g"),
        F.col("category_avg_salt_100g"),
        F.col("category_avg_kcal_100g"),
        F.col("is_above_avg_sugar"),
        F.col("is_above_avg_fat"),
        F.col("is_above_avg_salt"),
        F.col("healthiness_rank_in_category"),
        F.col("nutriscore_grade_mismatch"),
        # Lineage
        F.col("ingest_run_id"),
        F.col("ingested_at"),
        F.col("silver_processed_at"),
        F.lit(F.current_timestamp()).alias("gold_built_at"),
    )

    fact_count = fact_final_df.count()
    logger.info("Fact rows: %d", fact_count)
    fact_final_df.write.format("delta").mode("overwrite").saveAsTable(fact_full)
    logger.info("Written: %s", fact_full)

    logger.info(
        "Gold mart complete. run_id=%s | dim_product=%d | fact=%d",
        run_id, dim_product_df.count(), fact_count,
    )
    spark.stop()


if __name__ == "__main__":
    main(parse_args())