"""
gold_mart.py
------------
Read Silver Delta -> build Gold analytical tables (core + section flags).

Runs as a Databricks **Python file** job. Parameters are **CLI args**; Airflow passes values via
``job_parameters``. See ``databricks/JOB_PARAMETER_SETUP.md`` for Databricks job wiring.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gold: Silver Delta -> gold core and flags tables.")
    p.add_argument("--run_id", required=True, help="Logged for lineage; same logical day as the pipeline run.")
    p.add_argument("--catalog", default="openfda_lakehouse")
    p.add_argument("--silver_schema", default="silver")
    p.add_argument("--gold_schema", default="gold")
    p.add_argument("--silver_table", default="silver_openfda_drug_labeling")
    p.add_argument("--gold_table", default="gold_openfda_drug_labeling")
    p.add_argument("--gold_flags_table", default="gold_label_section_flags")
    return p.parse_args(argv)


def main(args: argparse.Namespace) -> None:
    run_id = args.run_id.strip()
    catalog = args.catalog.strip()
    silver_schema = args.silver_schema.strip()
    gold_schema = args.gold_schema.strip()
    silver_table = args.silver_table.strip()
    gold_table = args.gold_table.strip()
    gold_flags_table = args.gold_flags_table.strip()

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
        print(f"Missing required args: {missing}.", file=sys.stderr)
        sys.exit(1)

    silver_full = f"{catalog}.{silver_schema}.{silver_table}"
    gold_core_table = f"{catalog}.{gold_schema}.{gold_table}"
    gold_flags_fqn = f"{catalog}.{gold_schema}.{gold_flags_table}"

    logger.info(
        "Gold mart starting. run_id=%s, source=%s, core=%s, flags=%s",
        run_id,
        silver_full,
        gold_core_table,
        gold_flags_fqn,
    )

    spark = SparkSession.builder.appName("gold_openfda_mart").getOrCreate()

    silver_df = spark.read.format("delta").table(silver_full)
    total = silver_df.count()
    if total == 0:
        raise RuntimeError(f"Silver table is empty: {silver_full}")
    logger.info("Silver rows available: %d", total)

    # Deterministic "latest label per set_id"
    latest_window = Window.partitionBy("set_id").orderBy(
        F.col("effective_time").desc_nulls_last(),
        F.col("silver_processed_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )

    latest_silver_df = (
        silver_df.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    gold_core_df = (
        latest_silver_df.select(
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

    gold_core_df.write.format("delta").mode("overwrite").saveAsTable(gold_core_table)
    logger.info("Written: %s", gold_core_table)

    gold_flags_df = (
        latest_silver_df.select(
            F.col("set_id"),
            F.col("brand_name"),
            F.when(
                F.col("indications_and_usage").isNotNull() & (F.length(F.col("indications_and_usage")) > 0),
                1,
            )
            .otherwise(0)
            .alias("has_indications"),
            F.when(
                F.col("warnings").isNotNull() & (F.length(F.col("warnings")) > 0),
                1,
            )
            .otherwise(0)
            .alias("has_warnings"),
            F.when(
                F.col("adverse_reactions").isNotNull() & (F.length(F.col("adverse_reactions")) > 0),
                1,
            )
            .otherwise(0)
            .alias("has_adverse_reactions"),
            F.when(
                F.col("dosage_and_administration").isNotNull() & (F.length(F.col("dosage_and_administration")) > 0),
                1,
            )
            .otherwise(0)
            .alias("has_dosage"),
        )
        .withColumn("gold_built_at", F.current_timestamp())
    )

    flags_count = gold_flags_df.count()
    logger.info("Gold flags rows: %d", flags_count)

    gold_flags_df.write.format("delta").mode("overwrite").saveAsTable(gold_flags_fqn)
    logger.info("Written: %s", gold_flags_fqn)

    logger.info(
        "Gold mart complete. Core=%d rows, Flags=%d rows, run_id=%s",
        core_count,
        flags_count,
        run_id,
    )
    spark.stop()


if __name__ == "__main__":
    main(parse_args())
