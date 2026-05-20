"""
bronze_ingestion.py
-------------------
Purpose  : Read Open Food Facts JSON from Databricks UC Volume → write Bronze Delta.

Company context:
    NutriChain Retail Intelligence — Bronze is the raw evidence layer.
    We land data exactly as received from the API with metadata added
    for lineage. No cleaning happens here. Bronze is the source of truth
    for "what did the API actually return on this date?"

Runs as: Databricks Python file job task.
Triggered by: Airflow DatabricksRunNowOperator (Task 3 in the DAG).
Parameters: CLI args passed via job_parameters from Airflow.
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fixed write schema; Silver parses raw_products_json with its own struct definition.
BRONZE_WRITE_COLUMNS = (
    "ingest_run_id",
    "ingest_layer",
    "ingested_at",
    "source_system",
    "source_url",
    "api_page_number",
    "page_number",
    "page_size",
    "fetched_at",
    "total_products_reported",
    "raw_products_json",
)


def _migrate_legacy_bronze_schema(spark: SparkSession, bronze_table: str) -> None:
    """Drop legacy ``products`` struct via CREATE OR REPLACE (DROP COLUMN often blocked on UC)."""
    if not spark.catalog.tableExists(bronze_table):
        return
    column_names = {f.name for f in spark.table(bronze_table).schema.fields}
    if "products" not in column_names:
        return
    logger.warning(
        "Bronze table %s still has legacy column products; rebuilding without it.",
        bronze_table,
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE {bronze_table} "
        f"AS SELECT * EXCEPT (products) FROM {bronze_table}"
    )


def _align_to_write_schema(bronze_df, run_id: str):
    """Select BRONZE_WRITE_COLUMNS; fill missing columns with null."""
    df = bronze_df
    if "api_page_number" not in df.columns and "page_number" in df.columns:
        df = df.withColumn("api_page_number", F.col("page_number"))
    for col_name in BRONZE_WRITE_COLUMNS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None))
    return (
        df.withColumn("ingest_run_id", F.lit(run_id))
        .select(*BRONZE_WRITE_COLUMNS)
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bronze ingestion: Open Food Facts JSON on Volume → Delta table."
    )
    p.add_argument(
        "--run_date",
        required=True,
        help="Calendar date folder on Volume (yyyymmdd).",
    )
    p.add_argument(
        "--run_id",
        required=True,
        help="Batch id (yyyymmdd_HH00). Must match Volume subfolder name.",
    )
    p.add_argument("--catalog", default="nutrichain_lakehouse")
    p.add_argument("--schema", default="bronze")
    p.add_argument("--table", default="bronze_openfood_products_raw")
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_date = args.run_date.strip()
    run_id = args.run_id.strip()
    if not run_date or not run_id:
        print(
            "run_date and run_id are required. Pass them via Airflow job_parameters.",
            file=sys.stderr,
        )
        sys.exit(1)

    catalog = args.catalog.strip()
    schema = args.schema.strip()
    table = args.table.strip()

    bronze_table = f"{catalog}.{schema}.{table}"
    volume_input = (
        f"/Volumes/{catalog}/{schema}/raw_json_landing/{run_date}/{run_id}/"
    )

    logger.info(
        "Bronze ingestion starting. run_date=%s, run_id=%s, source=%s, target=%s",
        run_date, run_id, volume_input, bronze_table,
    )

    spark = SparkSession.builder.appName("bronze_openfood_ingestion").getOrCreate()

    # One JSON object per file (not JSONL).
    raw_df = spark.read.option("multiLine", "true").json(volume_input)

    row_count = raw_df.count()
    if row_count == 0:
        raise RuntimeError(
            f"Zero records read from {volume_input}. "
            "Check that upload_to_volume completed successfully for this run_id."
        )
    logger.info("Read %d page-level records from Volume.", row_count)

     # Serialize products array to string; inferred struct column breaks append over time.
    bronze_df = (
        raw_df
        .withColumn("ingest_layer", F.lit("bronze"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_system", F.lit("open_food_facts_api"))
        .withColumn("source_url", F.col("source_url"))
        .withColumn("page_number", F.col("page_number").cast("int"))
        .withColumn("page_size", F.col("page_size").cast("int"))
        .withColumn(
            "total_products_reported",
            F.col("total_products_reported").cast("long"),
        )
        .withColumn("raw_products_json", F.to_json(F.col("products")))
        .drop("products")
    )
    bronze_df = _align_to_write_schema(bronze_df, run_id)

    _migrate_legacy_bronze_schema(spark, bronze_table)

    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )

    final_count = bronze_df.count()
    logger.info(
        "Bronze write complete. Table=%s, page_rows=%d", bronze_table, final_count
    )
    spark.stop()


if __name__ == "__main__":
    main(parse_args())