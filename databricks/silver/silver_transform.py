"""
silver_transform.py
-------------------
Purpose  : Read Bronze Delta table -> clean, flatten, deduplicate -> write Silver Delta.
Runs as  : Databricks Job triggered by Airflow after bronze_ingestion succeeds.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sha2, concat_ws

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Airflow -> Databricks notebook_params contract
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "openfda_lakehouse")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("bronze_table", "bronze_openfda_drug_labeling_raw")
dbutils.widgets.text("silver_table", "silver_openfda_drug_labeling")

run_id = dbutils.widgets.get("run_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
bronze_table = dbutils.widgets.get("bronze_table").strip()
silver_table = dbutils.widgets.get("silver_table").strip()

required = {
    "run_id": run_id,
    "catalog": catalog,
    "bronze_schema": bronze_schema,
    "silver_schema": silver_schema,
    "bronze_table": bronze_table,
    "silver_table": silver_table,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise ValueError(
        f"Missing required notebook params: {missing}. "
        "Airflow DatabricksRunNowOperator must pass all required notebook_params."
    )

BRONZE_FULL = f"{catalog}.{bronze_schema}.{bronze_table}"
SILVER_FULL = f"{catalog}.{silver_schema}.{silver_table}"


def main() -> None:
    logger.info(
        "Silver transform starting. run_id=%s, source=%s, target=%s",
        run_id,
        BRONZE_FULL,
        SILVER_FULL,
    )

    spark = SparkSession.builder.appName("silver_openfda_transform").getOrCreate()

    bronze_df = (
        spark.read.format("delta").table(BRONZE_FULL)
        .filter(F.col("ingest_run_id") == run_id)
    )

    row_count = bronze_df.count()
    if row_count == 0:
        raise RuntimeError(
            f"No Bronze rows for run_id={run_id} in {BRONZE_FULL}. "
            "Did Bronze run and write rows for this run_id?"
        )
    logger.info("Bronze rows for this run: %d", row_count)

    exploded_df = bronze_df.select(
        F.explode(F.col("results")).alias("label"),
        F.col("ingest_run_id"),
        F.col("ingested_at"),
    )

    silver_df = exploded_df.select(
        F.col("label.set_id").alias("set_id"),
        F.array_join(F.col("label.openfda.brand_name"), " | ").alias("brand_name"),
        F.array_join(F.col("label.openfda.generic_name"), " | ").alias("generic_name"),
        F.array_join(F.col("label.openfda.manufacturer_name"), " | ").alias("manufacturer_name"),
        F.array_join(F.col("label.indications_and_usage"), " ").alias("indications_and_usage"),
        F.array_join(F.col("label.warnings"), " ").alias("warnings"),
        F.array_join(F.col("label.adverse_reactions"), " ").alias("adverse_reactions"),
        F.array_join(F.col("label.dosage_and_administration"), " ").alias("dosage_and_administration"),
        F.col("label.effective_time").alias("effective_time"),
        F.col("ingest_run_id"),
        F.col("ingested_at"),
    )

    silver_df = (
        silver_df
        .withColumn("row_hash", sha2(concat_ws("|", F.col("set_id"), F.col("effective_time")), 256))
        .withColumn("silver_processed_at", F.current_timestamp())
        .filter(F.col("set_id").isNotNull())
    )

    silver_count = silver_df.count()
    logger.info("Silver rows to write: %d", silver_count)

    if spark.catalog.tableExists(SILVER_FULL):
        from delta.tables import DeltaTable
        silver_delta = DeltaTable.forName(spark, SILVER_FULL)
        (
            silver_delta.alias("existing")
            .merge(silver_df.alias("new"), "existing.row_hash = new.row_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Silver MERGE complete: %s", SILVER_FULL)
    else:
        silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_FULL)
        logger.info("Silver table created: %s", SILVER_FULL)

    spark.stop()


if __name__ == "__main__":
    main()