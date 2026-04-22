"""
silver_transform.py
-------------------
Read Bronze Delta -> clean, flatten, deduplicate -> write Silver Delta.

Runs as a Databricks **Python file** job. Parameters are **CLI args**; Airflow passes values via
``job_parameters`` on run-now. Wire the job in Databricks as described in
``databricks/JOB_PARAMETER_SETUP.md``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sha2, concat_ws

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Silver: Bronze Delta -> Silver Delta for one run_id.")
    p.add_argument("--run_id", required=True, help="Ingestion run id; filters bronze rows.")
    p.add_argument("--catalog", default="openfda_lakehouse")
    p.add_argument("--bronze_schema", default="bronze")
    p.add_argument("--silver_schema", default="silver")
    p.add_argument("--bronze_table", default="bronze_openfda_drug_labeling_raw")
    p.add_argument("--silver_table", default="silver_openfda_drug_labeling")
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    catalog = args.catalog.strip()
    bronze_schema = args.bronze_schema.strip()
    silver_schema = args.silver_schema.strip()
    bronze_table = args.bronze_table.strip()
    silver_table = args.silver_table.strip()

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
        print(f"Missing required args: {missing}.", file=sys.stderr)
        sys.exit(1)

    bronze_full = f"{catalog}.{bronze_schema}.{bronze_table}"
    silver_full = f"{catalog}.{silver_schema}.{silver_table}"

    logger.info(
        "Silver transform starting. run_id=%s, source=%s, target=%s",
        run_id,
        bronze_full,
        silver_full,
    )

    spark = SparkSession.builder.appName("silver_openfda_transform").getOrCreate()

    bronze_df = spark.read.format("delta").table(bronze_full).filter(
        F.col("ingest_run_id") == run_id
    )

    row_count = bronze_df.count()
    if row_count == 0:
        raise RuntimeError(
            f"No Bronze rows for run_id={run_id} in {bronze_full}. "
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
        silver_df.withColumn("row_hash", sha2(concat_ws("|", F.col("set_id"), F.col("effective_time")), 256))
        .withColumn("silver_processed_at", F.current_timestamp())
        .filter(F.col("set_id").isNotNull())
    )

    silver_count = silver_df.count()
    logger.info("Silver rows to write: %d", silver_count)

    if spark.catalog.tableExists(silver_full):
        from delta.tables import DeltaTable

        silver_delta = DeltaTable.forName(spark, silver_full)
        (
            silver_delta.alias("existing")
            .merge(silver_df.alias("new"), "existing.row_hash = new.row_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Silver MERGE complete: %s", silver_full)
    else:
        silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_full)
        logger.info("Silver table created: %s", silver_full)

    spark.stop()


if __name__ == "__main__":
    main(parse_args())
