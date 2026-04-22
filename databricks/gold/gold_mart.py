"""
gold_mart.py
------------
Purpose  : Read Silver Delta table -> build Gold analytical tables.
Runs as  : Databricks Job triggered by Airflow after silver_transform succeeds.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Airflow -> Databricks notebook_params contract
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "openfda_lakehouse")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("silver_table", "silver_openfda_drug_labeling")
dbutils.widgets.text("gold_table", "gold_label_core")
dbutils.widgets.text("gold_flags_table", "gold_label_section_flags")

run_id = dbutils.widgets.get("run_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
silver_table = dbutils.widgets.get("silver_table").strip()
gold_table = dbutils.widgets.get("gold_table").strip()
gold_flags_table = dbutils.widgets.get("gold_flags_table").strip()

required = {
    "run_id": run_id,
    "catalog": catalog,
    "silver_schema": silver_schema,
    "gold_schema": gold_schema,
    "silver_table": silver_table,
    "gold_table": gold_table,
    "gold_flags_table": gold_flags_table,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise ValueError(
        f"Missing required notebook params: {missing}. "
        "Airflow DatabricksRunNowOperator must pass all required notebook_params."
    )

SILVER_FULL = f"{catalog}.{silver_schema}.{silver_table}"
GOLD_CORE_TABLE = f"{catalog}.{gold_schema}.{gold_table}"
GOLD_FLAGS_TABLE = f"{catalog}.{gold_schema}.{gold_flags_table}"


def main() -> None:
    logger.info(
        "Gold mart starting. run_id=%s, source=%s, core=%s, flags=%s",
        run_id,
        SILVER_FULL,
        GOLD_CORE_TABLE,
        GOLD_FLAGS_TABLE,
    )

    spark = SparkSession.builder.appName("gold_openfda_mart").getOrCreate()

    silver_df = spark.read.format("delta").table(SILVER_FULL)
    total = silver_df.count()
    if total == 0:
        raise RuntimeError(f"Silver table is empty: {SILVER_FULL}")
    logger.info("Silver rows available: %d", total)

    # Deterministic "latest label per set_id"
    latest_window = Window.partitionBy("set_id").orderBy(
        F.col("effective_time").desc_nulls_last(),
        F.col("silver_processed_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )

    latest_silver_df = (
        silver_df
        .withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    gold_core_df = (
        latest_silver_df
        .select(
            F.col("set_id"),
            F.col("brand_name"),
            F.col("generic_name"),
            F.col("manufacturer_name"),
            F.col("effective_time"),
            F.col("ingest_run_id"),
            F.col("silver_processed_at"),
        )
        .withColumn("gold_built_at", F.current_timestamp())
    )

    core_count = gold_core_df.count()
    logger.info("Gold core rows: %d", core_count)

    gold_core_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_CORE_TABLE)
    logger.info("Written: %s", GOLD_CORE_TABLE)

    gold_flags_df = (
        latest_silver_df
        .select(
            F.col("set_id"),
            F.col("brand_name"),
            F.when(
                F.col("indications_and_usage").isNotNull() &
                (F.length(F.col("indications_and_usage")) > 0), 1
            ).otherwise(0).alias("has_indications"),
            F.when(
                F.col("warnings").isNotNull() &
                (F.length(F.col("warnings")) > 0), 1
            ).otherwise(0).alias("has_warnings"),
            F.when(
                F.col("adverse_reactions").isNotNull() &
                (F.length(F.col("adverse_reactions")) > 0), 1
            ).otherwise(0).alias("has_adverse_reactions"),
            F.when(
                F.col("dosage_and_administration").isNotNull() &
                (F.length(F.col("dosage_and_administration")) > 0), 1
            ).otherwise(0).alias("has_dosage"),
        )
        .withColumn("gold_built_at", F.current_timestamp())
    )

    flags_count = gold_flags_df.count()
    logger.info("Gold flags rows: %d", flags_count)

    gold_flags_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_FLAGS_TABLE)
    logger.info("Written: %s", GOLD_FLAGS_TABLE)

    logger.info(
        "Gold mart complete. Core=%d rows, Flags=%d rows, run_id=%s",
        core_count,
        flags_count,
        run_id,
    )
    spark.stop()


if __name__ == "__main__":
    main()