"""
bronze_ingestion.py
-------------------
Runs as a Databricks **Python file** job task. Input: UC Volume JSON files. Output: Bronze Delta.

Configuration is passed on the **command line** (see argparse below). Airflow should trigger the job
using ``job_parameters`` on run-now; the Databricks job must map those to argv — see
``databricks/JOB_PARAMETER_SETUP.md``.
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
    p = argparse.ArgumentParser(description="Bronze ingestion: openFDA JSON on Volume -> Delta table.")
    p.add_argument(
        "--run_id",
        required=True,
        help="Ingestion run id (yyyymmdd). Must match Airflow fetch/upload and the Volume subfolder name.",
    )
    p.add_argument("--catalog", default="openfda_lakehouse", help="Unity Catalog name.")
    p.add_argument("--schema", default="bronze", help="Schema for the bronze table and raw_json_landing volume.")
    p.add_argument(
        "--table",
        default="bronze_openfda_drug_labeling_raw",
        help="Delta table name (unqualified) for bronze output.",
    )
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    if not run_id:
        print(
            "run_id is empty. This job must be run with a non-empty run_id, "
            "e.g. from Airflow via DatabricksRunNowOperator with job_parameters including run_id.",
            file=sys.stderr,
        )
        sys.exit(1)

    catalog = args.catalog.strip()
    schema = args.schema.strip()
    table = args.table.strip()

    bronze_table = f"{catalog}.{schema}.{table}"
    volume_input = f"/Volumes/{catalog}/{schema}/raw_json_landing/{run_id}/"

    logger.info(
        "Bronze ingestion starting. run_id=%s, table=%s, source=%s",
        run_id,
        bronze_table,
        volume_input,
    )

    spark = SparkSession.builder.appName("bronze_openfda_ingestion").getOrCreate()

    raw_df = spark.read.option("multiLine", "true").json(volume_input)

    row_count = raw_df.count()
    if row_count == 0:
        raise RuntimeError(
            f"Zero records read from {volume_input}. "
            "Check that Airflow upload_to_volume completed successfully for this run_id."
        )

    logger.info("Read %d records from Volume.", row_count)

    bronze_df = (
        raw_df.withColumn("ingest_run_id", F.lit(run_id))
        .withColumn("ingest_layer", F.lit("bronze"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_system", F.lit("openfda_api"))
        .withColumn("raw_json", F.to_json(F.col("results")))
    )

    bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table)

    logger.info("Bronze write complete. Table=%s, rows=%d", bronze_table, row_count)
    spark.stop()


if __name__ == "__main__":
    main(parse_args())
