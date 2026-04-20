"""
bronze_ingestion.py
-------------------
Purpose  : Read raw JSON files from DBFS and write a Bronze Delta table.
           This script runs INSIDE Databricks

Data flow: DBFS raw JSON files  →  Spark DataFrame  →  Bronze Delta table (append)

How to run:
    Option A (manual): Open this file in Databricks workspace → Run All.
    Option B (API):    Triggered via Databricks Jobs API from Airflow or GitHub Actions.
    Option C (widget): Add a Databricks widget for run_id and run interactively.

Prerequisites:
    - JSON files already uploaded to DBFS by the Airflow upload task.
    - The catalog and schema (database) must exist in Databricks.
    - Run this SQL in a Databricks notebook first to create the schema:
        CREATE SCHEMA IF NOT EXISTS bronze;
"""

from __future__ import annotations

import sys
import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("bronze_ingestion")

# ── Configuration ──────────────────────────────────────────────────────────────

# Align folder layout with src/openfda/upload.py (DBFS_BASE + run_id subfolder).
CATALOG_NAME = os.environ.get("CATALOG", "hive_metastore")
SCHEMA_NAME = "bronze"
TABLE_NAME = "bronze_openfda_drug_label_raw"
FULL_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

DBFS_BASE = "dbfs:/FileStore/openfda/bronze/raw"
BRONZE_AUDIT_TABLE = os.environ.get("BRONZE_AUDIT_TABLE", "").strip() or None



def get_run_id() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip()
    return datetime.now(timezone.utc).strftime("%Y%m%d")



def create_schema_if_not_exists(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
    logger.info("Schema ready: %s.%s", CATALOG_NAME, SCHEMA_NAME)



def read_raw_json(spark: SparkSession, run_id: str):
    """One JSON document per file → multiLine=true (not JSONL)."""
    input_path = f"{DBFS_BASE}/{run_id}/"
    logger.info("Reading JSON from: %s", input_path)
    return (
        # Spark reads all .json files in the folder in parallel.
        # Tells Spark to read each entire file as one JSON record,
        spark.read.option("multiLine", "true") 
        .option("mode", "PERMISSIVE")
        .json(input_path)
)



def add_bronze_metadata(raw_df, run_id: str):
    """
    results_fingerprint = SHA-256 (hex) of canonical JSON text of `results`.
    Used for Silver dedupe — not a security boundary.
    """
    raw_json_col = F.to_json(F.col("results"))
    return (
        raw_df
        .withColumn("ingest_run_id", F.lit(run_id))  # Lineage
        .withColumn("ingested_at", F.current_timestamp())  #Audit
        .withColumn("ingested_layer", F.lit("bronze"))
        .withColumn("source_system", F.lit("openfda_drug_label_api"))
        .withColumn("raw_results_json", raw_json_col) #results array as raw JSON str
        .withColumn("results_fingerprint", F.sha2(raw_json_col, 256)) # hash
    )



def _run_light_validation(row_count: int, run_id: str) -> Dict[str, Any]:
    checks: Dict[str, Any] = {"run_id": run_id, "row_count": row_count, "non_empty":row_count > 0}
    if row_count == 0:
        logger.error("Validation failed: zero rows for run_id=%s", run_id)
    else:
        logger.info("Validation OK: row_count=%d run_id=%s", row_count, run_id)
    return checks



def _append_audit_row(
    spark: SparkSession,
    *,
    audit_table: str,
    run_id: str,
    row_count: int,
    target_table: str,
    status: str,
    error_message: Optional[str],
    ) -> None:
    """Optional one-row append — create the audit Delta table first."""
    ended = datetime.now(timezone.utc).isoformat()
    row: Dict[str, Any] = {
        "ingest_run_id": run_id,
        "job_name": "bronze_openfda_dbfs_landing",
        "ended_at_utc": ended,
        "status": status,
        "target_table": target_table,
        "rows_written": int(row_count),
        "error message": error_message or "",
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(audit_table)
    logger.info("Wrote audit row to %s", audit_table)



def write_bronze_delta(bronze_df, run_id: str) -> int:
    """Single count() for logging + validation + write path."""
    row_count = bronze_df.count()
    logger.info("Writing %d rows to %s (append)", row_count, FULL_TABLE)
    (
        bronze_df.write.format("detla")
        .mode("append")
        .option("mergeSchema", "true")  # Allow schema evolution if API adds new fields.
        .saveAsTable(FULL_TABLE) # Register in Hive metastore so notebooks can query it.
    )
    logger.info("Bronze write complete: %s rows=%d run_id=%s", FULL_TABLE, row_count, run_id)
    return row_count



def run_bronze_job() -> None:
    """
    Main entry point. Orchestrates the full Bronze ingestion for one run.

    Call sequence:
        1. Get run_id (from args or today's date).
        2. Create Spark session.
        3. Create schema if needed.
        4. Read raw JSON from DBFS.
        5. Add metadata columns.
        6. Write to Delta table.
        7. Log success metrics.
    """
    run_id = get_run_id()
    logger.info("=== Bronze DBFS landing start run_id=%s ===", run_id)

    spark = SparkSession.builder.appName("openfda_bronze_dbfs_landing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    audit_status = "FAILED"
    rows_written = 0

    try:
        create_schema_if_not_exists(spark)  # 1. Create schema if needed.
        raw_df = read_raw_json(spark, run_id)  # 2. Read raw JSON from DBFS.

        if not raw_df.take(1):
            logger.error("No rows under %s/%s/ – check upload + run_id", DBFS_BASE, run_id)
            return       # Exit without writing anything

        bronze_df = add_bronze_metadata(raw_df, run_id)  # 3. Add Bronze metadata cols.
        rows_written = write_bronze_delta(bronze_df, run_id)   # 4. Write to Delta.
        _run_light_validation(rows_written, run_id)  # 5. Run light validation.
        audit_status = "SUCCESS"
        logger.info(
        "=== Bronze ingestion complete. run_id=%s, rows=%d ===",
        run_id, rows_written
    )

    except Exception as exc: 
      logger.exception("Bronze job failed: %s", exc)
      raise

    finally:
        if BRONZE_AUDIT_TABLE and audit_status == "SUCCESS":
            try:
                _append_audit_row(
                    spark,
                    audit_table=BRONZE_AUDIT_TABLE,
                    run_id=run_id,
                    row_count=rows_written,
                    target_table=FULL_TABLE,
                    status=audit_status,
                    error_message=None,
                )
            except Exception as exc:
                logger.warning("Audit write failed (non-fatal): %s", exc)

        if os.environ.get("BRONZE_STOP_SPARK", "").lower() in ("1", "true", "yes"):
            spark.stop()
            logger.info("spark.stop() called (BRONZE_STOP_SPARK set).")

if __name__ == "__main__":
    run_bronze_job()

