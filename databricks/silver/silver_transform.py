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
   10. primary_category      — extract + clean OFF category tags
   11. primary_country       — extract + ISO alias lookup (English display name)
   12. row_hash              — SHA-256 of (barcode + last_modified_t) for dedup
   13. Text cleansing         — product name, allergens, packaging
   14. Nutrition rounding     — 2 dp; negatives → null
   15. data_quality_tier      — complete vs sparse (completeness score)

Runs as: Databricks Python file job task.
Triggered by: Airflow DatabricksRunNowOperator (Task 4 in the DAG).

Maintenance: pass --backfill_all to read all Bronze history, dedupe by barcode,
and overwrite Silver (one-time after schema/cleaning changes). Airflow daily runs
use --run_id only (MERGE).
"""

from __future__ import annotations

import argparse
import logging
import sys
from functools import reduce
from operator import add
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.window import Window


def _silver_module_dir() -> Path:
    """Directory containing this module (databricks/silver/)."""
    try:
        return Path(__file__).resolve().parent
    except NameError:
        # Databricks file tasks exec() the script without defining __file__.
        cwd = Path.cwd()
        for base in (cwd, cwd / "databricks" / "silver", cwd / "silver"):
            if (base / "data" / "country_alias_lookup.csv").is_file():
                return base
            if (base / "silver_transform.py").is_file():
                return base
        return cwd / "databricks" / "silver"


_SILVER_DIR = _silver_module_dir()
if str(_SILVER_DIR) not in sys.path:
    sys.path.insert(0, str(_SILVER_DIR))

from silver_cleaning import (  # noqa: E402
    COMPLETENESS_FIELD_NAMES,
    reported_grade_for_mismatch,
    spark_clean_category,
    spark_clean_off_tag_list,
    spark_clean_product_name,
    spark_completeness_score,
    spark_data_quality_tier,
    spark_normalize_nutriscore_reported,
    spark_round_nutrient,
    spark_strip_off_prefix,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_COUNTRY_LOOKUP_CSV = _SILVER_DIR / "data" / "country_alias_lookup.csv"

_NUTRIENT_COLS = (
    "proteins_100g",
    "fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "salt_100g",
    "fiber_100g",
)

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
        description="Silver: Bronze Delta → Silver Delta (per run_id or full backfill)."
    )
    p.add_argument(
        "--run_id",
        default="",
        help="Bronze ingest_run_id for incremental MERGE (required unless --backfill_all).",
    )
    p.add_argument(
        "--backfill_all",
        action="store_true",
        help="Read all Bronze rows, rebuild Silver, overwrite table (maintenance).",
    )
    p.add_argument("--catalog", default="nutrichain_lakehouse")
    p.add_argument("--bronze_schema", default="bronze")
    p.add_argument("--silver_schema", default="silver")
    p.add_argument("--bronze_table", default="bronze_openfood_products_raw")
    p.add_argument("--silver_table", default="silver_openfood_products")
    p.add_argument(
        "--country_lookup_csv",
        default=str(_COUNTRY_LOOKUP_CSV),
        help="ISO country alias CSV (alias, iso_code, display_name).",
    )
    return p.parse_args(argv)


def _load_country_lookup(spark: SparkSession, csv_path: str):
    if not Path(csv_path).is_file():
        raise FileNotFoundError(f"Country alias lookup not found: {csv_path}")
    return (
        spark.read.option("header", True)
        .csv(csv_path)
        .select(
            F.lower(F.trim(F.col("alias"))).alias("alias"),
            F.upper(F.trim(F.col("iso_code"))).alias("iso_code"),
            F.col("display_name"),
        )
        .dropDuplicates(["alias"])
    )


def _country_alias_key(col):
    stripped = spark_strip_off_prefix(col)
    normalized = F.lower(F.regexp_replace(F.trim(stripped), "-", " "))
    return F.regexp_replace(normalized, r"\s+", " ")


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    backfill_all = bool(args.backfill_all)
    catalog = args.catalog.strip()
    bronze_schema = args.bronze_schema.strip()
    silver_schema = args.silver_schema.strip()
    bronze_table = args.bronze_table.strip()
    silver_table = args.silver_table.strip()

    if backfill_all and run_id:
        logger.warning("Both --backfill_all and --run_id set; backfill ignores run_id.")
    if not backfill_all and not run_id:
        print("Provide --run_id for incremental Silver, or --backfill_all for full rebuild.",
              file=sys.stderr)
        sys.exit(1)

    required = {
        "catalog": catalog,
        "bronze_schema": bronze_schema,
        "silver_schema": silver_schema,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"Missing required args: {missing}.", file=sys.stderr)
        sys.exit(1)

    bronze_full = f"{catalog}.{bronze_schema}.{bronze_table}"
    silver_full = f"{catalog}.{silver_schema}.{silver_table}"

    logger.info(
        "Silver transform starting. backfill_all=%s, run_id=%s, source=%s, target=%s",
        backfill_all, run_id or "(all)", bronze_full, silver_full,
    )

    spark = SparkSession.builder.appName("silver_openfood_transform").getOrCreate()
    country_lookup_df = _load_country_lookup(spark, args.country_lookup_csv)
    country_lookup = F.broadcast(country_lookup_df)
    iso_display = F.broadcast(
        country_lookup_df.select(
            F.col("iso_code"),
            F.col("display_name").alias("iso_display_name"),
        ).dropDuplicates(["iso_code"])
    )

    bronze_df = spark.read.format("delta").table(bronze_full)
    if not backfill_all:
        bronze_df = bronze_df.filter(F.col("ingest_run_id") == run_id)

    row_count = bronze_df.count()
    if row_count == 0:
        if backfill_all:
            raise RuntimeError(f"Bronze table is empty: {bronze_full}")
        raise RuntimeError(
            f"No Bronze rows for run_id={run_id}. Did Bronze job succeed?"
        )
    logger.info(
        "Bronze page-rows loaded: %d (%s)",
        row_count,
        "all runs" if backfill_all else f"run_id={run_id}",
    )

    bronze_df = bronze_df.withColumn(
        "_products_array",
        F.from_json(F.col("raw_products_json"), _RAW_PRODUCTS_SCHEMA),
    )

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
            "Check Volume JSON, to_json in Bronze, and that raw_products_json is non-null."
        )

    flat_df = exploded_df.select(
        F.col("p.code").cast("string").alias("barcode"),
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

    silver_df = flat_df

    # Round nutrients; negative values → null (0 is kept).
    for nut_col in _NUTRIENT_COLS:
        silver_df = silver_df.withColumn(nut_col, spark_round_nutrient(F.col(nut_col)))

    # Text cleansing
    silver_df = silver_df.withColumn("product_name", spark_clean_product_name(F.col("product_name")))
    silver_df = silver_df.withColumn("allergens", spark_clean_off_tag_list(F.col("allergens")))
    silver_df = silver_df.withColumn("packaging", spark_clean_off_tag_list(F.col("packaging")))

    silver_df = silver_df.withColumn(
        "primary_category",
        spark_clean_category(F.trim(F.split(F.col("categories_raw"), ",").getItem(0))),
    )

    silver_df = silver_df.withColumn(
        "primary_country_raw",
        F.trim(F.split(F.col("countries_raw"), ",").getItem(0)),
    )
    silver_df = silver_df.withColumn("_country_alias", _country_alias_key(F.col("primary_country_raw")))
    silver_df = silver_df.join(
        country_lookup,
        silver_df["_country_alias"] == country_lookup["alias"],
        "left",
    )
    silver_df = silver_df.withColumn(
        "country_iso_code",
        F.coalesce(
            F.col("iso_code"),
            F.when(
                F.col("_country_alias").rlike("^[a-z]{2}$"),
                F.upper(F.col("_country_alias")),
            ),
            F.lit("XX"),
        ),
    )
    silver_df = silver_df.join(iso_display, on="country_iso_code", how="left")
    silver_df = (
        silver_df.withColumn(
            "primary_country",
            F.coalesce(
                F.col("display_name"),
                F.col("iso_display_name"),
                F.lit("Unknown Country"),
            ),
        )
        .drop(
            "iso_code",
            "display_name",
            "iso_display_name",
            "alias",
            "_country_alias",
            "primary_country_raw",
        )
    )

    silver_df = silver_df.withColumn(
        "primary_brand",
        F.trim(F.split(F.col("brands_raw"), ",").getItem(0)),
    )

    # Energy kcal per 100g
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

    silver_df = silver_df.withColumn(
        "barcode_is_ean",
        F.when(
            F.col("barcode").isNotNull()
            & F.trim(F.col("barcode")).rlike("^[0-9]{8,14}$"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    silver_df = silver_df.withColumn(
        "sodium_corrected_100g",
        F.when(
            F.col("sodium_raw_100g").isNotNull() & (F.col("sodium_raw_100g") > 0),
            F.round(F.col("sodium_raw_100g"), 2),
        ).when(
            F.col("salt_100g").isNotNull() & (F.col("salt_100g") > 0),
            F.round(F.col("salt_100g") / 2.5, 2),
        ).otherwise(F.lit(None).cast("double")),
    )

    silver_df = silver_df.withColumn(
        "sugar_tier",
        F.when(F.col("sugars_100g").isNull(), "unknown")
        .when(F.col("sugars_100g") < 5.0, "low")
        .when(F.col("sugars_100g") <= 12.5, "medium")
        .otherwise("high"),
    )

    silver_df = silver_df.withColumn(
        "fat_tier",
        F.when(F.col("fat_100g").isNull(), "unknown")
        .when(F.col("fat_100g") < 3.0, "low")
        .when(F.col("fat_100g") <= 17.5, "medium")
        .otherwise("high"),
    )

    silver_df = silver_df.withColumn(
        "salt_tier",
        F.when(F.col("salt_100g").isNull(), "unknown")
        .when(F.col("salt_100g") < 0.3, "low")
        .when(F.col("salt_100g") <= 1.5, "medium")
        .otherwise("high"),
    )

    silver_df = silver_df.withColumn(
        "protein_density_score",
        F.when(
            F.col("energy_kcal_per_100g").isNotNull()
            & (F.col("energy_kcal_per_100g") > 0)
            & F.col("proteins_100g").isNotNull(),
            F.round(F.col("proteins_100g") / F.col("energy_kcal_per_100g") * 100, 2),
        ).otherwise(F.lit(None).cast("double")),
    )

    silver_df = silver_df.withColumn(
        "nova_group_label",
        F.when(F.col("nova_group") == 1, "unprocessed")
        .when(F.col("nova_group") == 2, "culinary_ingredient")
        .when(F.col("nova_group") == 3, "processed")
        .when(F.col("nova_group") == 4, "ultra_processed")
        .otherwise("unclassified"),
    )

    silver_df = silver_df.withColumn(
        "ingredient_count",
        F.when(
            F.col("ingredients_text").isNotNull() & (F.length(F.col("ingredients_text")) > 0),
            F.size(F.split(F.col("ingredients_text"), ",")),
        ).otherwise(F.lit(0)),
    )

    silver_df = silver_df.withColumn(
        "nutriscore_grade_reported",
        spark_normalize_nutriscore_reported(F.col("nutriscore_grade_reported")),
    )

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
            reported_grade_for_mismatch(F.col("nutriscore_grade_reported")).isNotNull()
            & (F.col("nutriscore_grade_recalculated") != "unknown")
            & (
                reported_grade_for_mismatch(F.col("nutriscore_grade_reported"))
                != F.col("nutriscore_grade_recalculated")
            ),
            True,
        ).otherwise(False),
    )

    completeness_cols = [F.col(name) for name in COMPLETENESS_FIELD_NAMES]
    silver_df = silver_df.withColumn(
        "completeness_score",
        spark_completeness_score(completeness_cols),
    )
    silver_df = silver_df.withColumn(
        "data_quality_tier",
        spark_data_quality_tier(F.col("completeness_score")),
    )

    nutrition_cols = [
        F.col("energy_kcal_per_100g"),
        F.col("proteins_100g"),
        F.col("fat_100g"),
        F.col("carbohydrates_100g"),
        F.col("sugars_100g"),
        F.col("salt_100g"),
        F.col("fiber_100g"),
    ]
    any_nutrition = reduce(
        add,
        [F.when(c.isNotNull(), 1).otherwise(0) for c in nutrition_cols],
    )
    silver_df = silver_df.withColumn(
        "is_nutritional_data_complete",
        any_nutrition > 0,
    )

    silver_df = silver_df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", F.col("barcode"), F.col("last_modified_unix").cast("string")),
            256,
        ),
    )

    silver_df = silver_df.withColumn("silver_processed_at", F.current_timestamp())

    silver_df = silver_df.filter(F.col("barcode").isNotNull())

    silver_count = silver_df.count()
    logger.info(
        "Silver rows after cleaning: %d (from %d Bronze products)",
        silver_count,
        product_count,
    )

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
        deduped_count,
        silver_count - deduped_count,
    )

    if backfill_all:
        (
            deduped_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_full)
        )
        logger.info("Silver BACKFILL overwrite complete: %s", silver_full)
    elif spark.catalog.tableExists(silver_full):
        from delta.tables import DeltaTable

        silver_delta = DeltaTable.forName(spark, silver_full)
        (
            silver_delta.alias("existing")
            .merge(deduped_df.alias("new"), "existing.barcode = new.barcode")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Silver MERGE complete: %s", silver_full)
    else:
        deduped_df.write.format("delta").mode("overwrite").saveAsTable(silver_full)
        logger.info("Silver table created: %s", silver_full)

    spark.stop()


if __name__ == "__main__":
    main(parse_args())
