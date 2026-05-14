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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bronze ingestion: Open Food Facts JSON on Volume → Delta table."
    )
    p.add_argument(
        "--run_id",
        required=True,
        help="Ingestion run id (yyyymmdd). Must match Volume subfolder name.",
    )
    p.add_argument("--catalog", default="nutrichain_lakehouse")
    p.add_argument("--schema", default="bronze")
    p.add_argument("--table", default="bronze_openfood_products_raw")
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    if not run_id:
        print(
            "run_id is empty. Pass it via Airflow job_parameters.",
            file=sys.stderr,
        )
        sys.exit(1)

    catalog = args.catalog.strip()
    schema = args.schema.strip()
    table = args.table.strip()

    bronze_table = f"{catalog}.{schema}.{table}"
    # Volume path must match what upload.py wrote to
    volume_input = f"/Volumes/{catalog}/{schema}/raw_json_landing/{run_id}/"

    logger.info(
        "Bronze ingestion starting. run_id=%s, source=%s, target=%s",
        run_id, volume_input, bronze_table,
    )

    spark = SparkSession.builder.appName("bronze_openfood_ingestion").getOrCreate()

    # Read all JSON files from this run's Volume subfolder
    # multiLine=true because each file is a single large JSON object, not JSONL
    raw_df = spark.read.option("multiLine", "true").json(volume_input)

    row_count = raw_df.count()
    if row_count == 0:
        raise RuntimeError(
            f"Zero records read from {volume_input}. "
            "Check that upload_to_volume completed successfully for this run_id."
        )
    logger.info("Read %d page-level records from Volume.", row_count)

    # Bronze adds lineage metadata and one string column for all product payloads.
    # Do NOT persist inferred struct/array column "products" on append: Open Food
    # Facts evolves nested fields between pages/runs → Delta schema merge fails with
    # DELTA_FAILED_TO_MERGE_FIELDS on "products". raw_products_json is stable (string).
    # Silver parses this JSON with a fixed array<struct<...>> schema (see silver_transform).
    bronze_df = (
        raw_df
        .withColumn("ingest_run_id", F.lit(run_id))
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

    bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table)

    final_count = bronze_df.count()
    logger.info(
        "Bronze write complete. Table=%s, page_rows=%d", bronze_table, final_count
    )
    spark.stop()


if __name__ == "__main__":
    main(parse_args())