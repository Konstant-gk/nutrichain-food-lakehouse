"""
silver_transform.py
-------------------
Purpose  : Bronze Delta → clean, flatten, enrich → Silver Delta.

Company context:
    NutriChain Retail Intelligence — Silver is the "trusted clean" layer.
    Every column here is type-safe, unit-standardized, and deduplicated.
    Analysts can join Silver tables without worrying about kJ vs kcal,
    or whether sodium and salt are consistent.

Transformations applied (all documented in docs/data_dictionary.md):
    1. Explode products array — one row per product
    2. energy_kcal_per_100g  — convert kJ → kcal where needed (÷ 4.184)
    3. sodium_corrected      — fill null sodium from salt ÷ 2.5
    4. serving_standardized  — flag: nutrition values are per 100g (OFF standard)
    5. ingredient_count      — count comma-separated ingredients
    6. nutriscore_verified   — compare reported vs recalculated grade
    7. is_duplicate          — same barcode, keep most recently modified
    8. sugar_tier            — EU threshold classification: low/medium/high
    9. nova_group_label      — map numeric NOVA 1-4 to readable label
   10. primary_category      — extract first category from comma list
   11. primary_country       — extract first country from comma list
   12. row_hash              — SHA-256 of (barcode + last_modified_t) for dedup

Runs as: Databricks Python file job task.
Triggered by: Airflow DatabricksRunNowOperator (Task 4 in the DAG).
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parsed from bronze.raw_products_json. Only fields Silver uses; all STRING so
# new OFF top-level keys and nested blobs (e.g. nutriments) do not break parsing.
_RAW_PRODUCTS_SCHEMA = (
    "array<struct<"
    "code:string,product_name:string,brands:string,categories:string,countries:string,"
    "quantity:string,serving_size:string,energy_100g:string,`energy-kcal_100g`:string,"
    "proteins_100g:string,fat_100g:string,carbohydrates_100g:string,sugars_100g:string,"
    "salt_100g:string,sodium_100g:string,fiber_100g:string,nutriscore_score:string,"
    "nutriscore_grade:string,nova_group:string,ingredients_text:string,allergens:string,"
    "packaging:string,last_modified_t:string"
    ">>"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Silver: Bronze Delta → Silver Delta for one run_id."
    )
    p.add_argument("--run_id", required=True)
    p.add_argument("--catalog", default="nutrichain_lakehouse")
    p.add_argument("--bronze_schema", default="bronze")
    p.add_argument("--silver_schema", default="silver")
    p.add_argument("--bronze_table", default="bronze_openfood_products_raw")
    p.add_argument("--silver_table", default="silver_openfood_products")
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    catalog = args.catalog.strip()
    bronze_schema = args.bronze_schema.strip()
    silver_schema = args.silver_schema.strip()
    bronze_table = args.bronze_table.strip()
    silver_table = args.silver_table.strip()

    required = {
        "run_id": run_id, "catalog": catalog,
        "bronze_schema": bronze_schema, "silver_schema": silver_schema,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"Missing required args: {missing}.", file=sys.stderr)
        sys.exit(1)

    bronze_full = f"{catalog}.{bronze_schema}.{bronze_table}"
    silver_full = f"{catalog}.{silver_schema}.{silver_table}"

    logger.info(
        "Silver transform starting. run_id=%s, source=%s, target=%s",
        run_id, bronze_full, silver_full,
    )

    spark = SparkSession.builder.appName("silver_openfood_transform").getOrCreate()

    # ── Step 1: Read Bronze rows for this run and explode products array ──────
    bronze_df = (
        spark.read.format("delta").table(bronze_full)
        .filter(F.col("ingest_run_id") == run_id)
    )

    row_count = bronze_df.count()
    if row_count == 0:
        raise RuntimeError(
            f"No Bronze rows for run_id={run_id}. Did Bronze job succeed?"
        )
    logger.info("Bronze page-rows for this run: %d", row_count)

    # Parse page-level JSON saved in Bronze (stable string column — see bronze_ingestion).
    bronze_df = bronze_df.withColumn(
        "_products_array",
        F.from_json(F.col("raw_products_json"), _RAW_PRODUCTS_SCHEMA),
    )

    # Explode: each page has N products → one row per product
    exploded_df = bronze_df.select(
        F.explode(F.col("_products_array")).alias("p"),
        F.col("ingest_run_id"),
        F.col("ingested_at"),
    ).filter(F.col("p").isNotNull())

    product_count = exploded_df.count()
    logger.info("Products after explode: %d", product_count)
    if product_count == 0:
        raise RuntimeError(
            "Zero product rows after parsing raw_products_json. "
            "Check Volume JSON, to_json in Bronze, and that raw_products_json is non-null. "
            "Very old Bronze rows without raw_products_json are not supported by this Silver job."
        )

    # ── Step 2: Extract flat columns from each product struct (strings → typed casts)
    flat_df = exploded_df.select(
        F.col("p.code").alias("barcode"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.brands").alias("brands_raw"),
        F.col("p.categories").alias("categories_raw"),
        F.col("p.countries").alias("countries_raw"),
        F.col("p.quantity").alias("quantity_raw"),
        F.col("p.serving_size").alias("serving_size_raw"),
        F.col("p.energy_100g").cast("double").alias("energy_raw_100g"),
        F.col("p.`energy-kcal_100g`").cast("double").alias("energy_kcal_raw_100g"),
        F.col("p.proteins_100g").cast("double").alias("proteins_100g"),
        F.col("p.fat_100g").cast("double").alias("fat_100g"),
        F.col("p.carbohydrates_100g").cast("double").alias("carbohydrates_100g"),
        F.col("p.sugars_100g").cast("double").alias("sugars_100g"),
        F.col("p.salt_100g").cast("double").alias("salt_100g"),
        F.col("p.sodium_100g").cast("double").alias("sodium_raw_100g"),
        F.col("p.fiber_100g").cast("double").alias("fiber_100g"),
        F.col("p.nutriscore_score").cast("int").alias("nutriscore_score_raw"),
        F.col("p.nutriscore_grade").alias("nutriscore_grade_reported"),
        F.col("p.nova_group").cast("int").alias("nova_group"),
        F.col("p.ingredients_text").alias("ingredients_text"),
        F.col("p.allergens").alias("allergens"),
        F.col("p.packaging").alias("packaging"),
        F.col("p.last_modified_t").cast("long").alias("last_modified_unix"),
        F.col("ingest_run_id"),
        F.col("ingested_at"),
    )

    # ── Step 3: Apply all cleaning transformations ────────────────────────────

    silver_df = flat_df

    # 3a. Energy: normalize to kcal per 100g
    # OFF stores kJ in `energy_100g` and sometimes mislabels kJ as `energy-kcal_100g`.
    # Plausible per-100g band tops out ~900 kcal (pure fat). Values above that in the
    # kcal field are treated as kJ and converted (÷ 4.184).
    _MAX_KCAL_PER_100G = 900.0
    silver_df = silver_df.withColumn(
        "energy_kcal_per_100g",
        F.when(
            F.col("energy_kcal_raw_100g").isNotNull()
            & (F.col("energy_kcal_raw_100g") > 0)
            & (F.col("energy_kcal_raw_100g") <= _MAX_KCAL_PER_100G),
            F.col("energy_kcal_raw_100g"),
        )
        .when(
            F.col("energy_raw_100g").isNotNull() & (F.col("energy_raw_100g") > 0),
            F.round(F.col("energy_raw_100g") / 4.184, 2),
        )
        .when(
            F.col("energy_kcal_raw_100g").isNotNull()
            & (F.col("energy_kcal_raw_100g") > _MAX_KCAL_PER_100G),
            F.round(F.col("energy_kcal_raw_100g") / 4.184, 2),
        )
        .otherwise(F.lit(None).cast("double")),
    )
    # After conversion, drop values still outside plausible band (garbage-in from OFF).
    silver_df = silver_df.withColumn(
        "energy_kcal_per_100g",
        F.when(
            F.col("energy_kcal_per_100g").isNotNull()
            & (
                (F.col("energy_kcal_per_100g") < 0)
                | (F.col("energy_kcal_per_100g") > _MAX_KCAL_PER_100G)
            ),
            F.lit(None).cast("double"),
        ).otherwise(F.col("energy_kcal_per_100g")),
    )

    # EAN-style barcode flag for monitoring (OFF has valid non-EAN codes we still keep).
    silver_df = silver_df.withColumn(
        "barcode_is_ean",
        F.when(
            F.col("barcode").isNotNull()
            & F.trim(F.col("barcode")).rlike("^[0-9]{8,14}$"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # 3b. Sodium: fill null sodium from salt (sodium = salt / 2.5)
    # Many products correctly fill only salt_100g, not sodium_100g.
    # The official formula: sodium_g = salt_g / 2.5
    silver_df = silver_df.withColumn(
        "sodium_corrected_100g",
        F.when(
            F.col("sodium_raw_100g").isNotNull() & (F.col("sodium_raw_100g") > 0),
            F.col("sodium_raw_100g"),
        ).when(
            F.col("salt_100g").isNotNull() & (F.col("salt_100g") > 0),
            F.round(F.col("salt_100g") / 2.5, 4),
        ).otherwise(F.lit(None).cast("double")),
    )

    # 3c. Sugar tier — EU traffic light thresholds per 100g
    # LOW:    sugar < 5g
    # MEDIUM: 5g ≤ sugar ≤ 12.5g
    # HIGH:   sugar > 12.5g
    silver_df = silver_df.withColumn(
        "sugar_tier",
        F.when(F.col("sugars_100g").isNull(), "unknown")
        .when(F.col("sugars_100g") < 5.0, "low")
        .when(F.col("sugars_100g") <= 12.5, "medium")
        .otherwise("high"),
    )

    # 3d. Fat tier — EU thresholds per 100g
    # LOW: fat < 3g, MEDIUM: 3–17.5g, HIGH: > 17.5g
    silver_df = silver_df.withColumn(
        "fat_tier",
        F.when(F.col("fat_100g").isNull(), "unknown")
        .when(F.col("fat_100g") < 3.0, "low")
        .when(F.col("fat_100g") <= 17.5, "medium")
        .otherwise("high"),
    )

    # 3e. Salt tier — EU thresholds per 100g
    # LOW: salt < 0.3g, MEDIUM: 0.3–1.5g, HIGH: > 1.5g
    silver_df = silver_df.withColumn(
        "salt_tier",
        F.when(F.col("salt_100g").isNull(), "unknown")
        .when(F.col("salt_100g") < 0.3, "low")
        .when(F.col("salt_100g") <= 1.5, "medium")
        .otherwise("high"),
    )

    # 3f. Protein density score = proteins / kcal * 100
    # Measures how protein-dense a product is relative to its caloric load.
    # A high score = good source of protein per calorie.
    silver_df = silver_df.withColumn(
        "protein_density_score",
        F.when(
            F.col("energy_kcal_per_100g").isNotNull()
            & (F.col("energy_kcal_per_100g") > 0)
            & F.col("proteins_100g").isNotNull(),
            F.round(F.col("proteins_100g") / F.col("energy_kcal_per_100g") * 100, 4),
        ).otherwise(F.lit(None).cast("double")),
    )

    # 3g. NOVA group label — map numeric code to readable text
    # NOVA is a food processing classification system (Monteiro, Brazil 2009):
    # 1 = unprocessed / minimally processed (apple, rice, egg)
    # 2 = processed culinary ingredient (butter, sugar, oil)
    # 3 = processed food (cheese, canned fish, cured meat)
    # 4 = ultra-processed (soft drinks, chips, instant noodles)
    silver_df = silver_df.withColumn(
        "nova_group_label",
        F.when(F.col("nova_group") == 1, "unprocessed")
        .when(F.col("nova_group") == 2, "culinary_ingredient")
        .when(F.col("nova_group") == 3, "processed")
        .when(F.col("nova_group") == 4, "ultra_processed")
        .otherwise("unknown"),
    )

    # 3h. Ingredient count — count comma-separated items in ingredient text
    silver_df = silver_df.withColumn(
        "ingredient_count",
        F.when(
            F.col("ingredients_text").isNotNull() & (F.length(F.col("ingredients_text")) > 0),
            F.size(F.split(F.col("ingredients_text"), ",")),
        ).otherwise(F.lit(0)),
    )

    # 3i. Primary category — first item from comma-separated list
    # Categories raw looks like: "Beverages, Fruit juices, Orange juices"
    silver_df = silver_df.withColumn(
        "primary_category",
        F.trim(F.split(F.col("categories_raw"), ",").getItem(0)),
    )

    # 3j. Primary country — first item from comma-separated list
    silver_df = silver_df.withColumn(
        "primary_country",
        F.trim(F.split(F.col("countries_raw"), ",").getItem(0)),
    )

    # 3k. Primary brand — first item from comma-separated list
    silver_df = silver_df.withColumn(
        "primary_brand",
        F.trim(F.split(F.col("brands_raw"), ",").getItem(0)),
    )

    # 3l. Nutriscore grade verification flag
    # Compare reported grade vs what score implies.
    # Official banding: A=[-∞,-1], B=[0,2], C=[3,10], D=[11,18], E=[19,+∞]
    silver_df = silver_df.withColumn(
        "nutriscore_grade_recalculated",
        F.when(F.col("nutriscore_score_raw") <= -1, "a")
        .when(F.col("nutriscore_score_raw") <= 2, "b")
        .when(F.col("nutriscore_score_raw") <= 10, "c")
        .when(F.col("nutriscore_score_raw") <= 18, "d")
        .when(F.col("nutriscore_score_raw").isNotNull(), "e")
        .otherwise("unknown"),
    )

    silver_df = silver_df.withColumn(
        "nutriscore_grade_mismatch",
        F.when(
            F.col("nutriscore_grade_reported").isNotNull()
            & (F.col("nutriscore_grade_recalculated") != "unknown")
            & (
                F.lower(F.col("nutriscore_grade_reported"))
                != F.col("nutriscore_grade_recalculated")
            ),
            True,
        ).otherwise(False),
    )

    # 3m. Row hash — SHA-256 of barcode + last_modified for dedup
    silver_df = silver_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", F.col("barcode"), F.col("last_modified_unix").cast("string")),
            256,
        ),
    )

    # 3n. Silver processing metadata
    silver_df = silver_df.withColumn("silver_processed_at", F.current_timestamp())

    # ── Step 4: Filter out invalid rows ──────────────────────────────────────
    # Barcode is our primary key. Rows without a barcode cannot be joined
    # in Gold and should not enter Silver.
    silver_df = silver_df.filter(F.col("barcode").isNotNull())

    silver_count = silver_df.count()
    logger.info("Silver rows after cleaning: %d (from %d Bronze products)",
                silver_count, product_count)

    # ── Step 5: Deduplicate ───────────────────────────────────────────────────
    # Keep only the most recently modified version of each barcode.
    # In a daily pipeline, the same product may appear in multiple runs.
    dedup_window = Window.partitionBy("barcode").orderBy(
        F.col("last_modified_unix").desc_nulls_last()
    )

    deduped_df = (
        silver_df
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    deduped_count = deduped_df.count()
    logger.info(
        "After dedup: %d unique products (%d duplicates removed)",
        deduped_count, silver_count - deduped_count,
    )

    # ── Step 6: Write to Silver using MERGE (idempotent) ─────────────────────
    if spark.catalog.tableExists(silver_full):
        from delta.tables import DeltaTable

        silver_delta = DeltaTable.forName(spark, silver_full)
        (
            silver_delta.alias("existing")
            .merge(deduped_df.alias("new"), "existing.barcode = new.barcode")
            .whenMatchedUpdateAll()   # update if the product changed
            .whenNotMatchedInsertAll()  # insert if it's a new product
            .execute()
        )
        logger.info("Silver MERGE complete: %s", silver_full)
    else:
        deduped_df.write.format("delta").mode("overwrite").saveAsTable(silver_full)
        logger.info("Silver table created: %s", silver_full)

    spark.stop()


if __name__ == "__main__":
    main(parse_args())