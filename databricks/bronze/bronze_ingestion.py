"""
bronze_ingestion.py
-------------------
Runs as a Databricks Job. Input: UC Volume JSON files. Output: Bronze Delta table.

HOW IT GETS ITS CONFIG:
    Airflow triggers this job and passes notebook_params with catalog, schema,
    table names, and run_id. The notebook reads them via dbutils.widgets.
    No .env file. No os.environ. No local file system. Just widgets.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Read parameters that Airflow passed at trigger time ---
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "openfda_lakehouse")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("table", "bronze_openfda_drug_labeling_raw")

run_id = dbutils.widgets.get("run_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")

if not run_id:
    raise ValueError(
        "run_id was not passed. This notebook must be triggered by Airflow "
        "via DatabricksRunNowOperator with notebook_params including run_id."
    )

BRONZE_TABLE = f"{catalog}.{schema}.{table}"
VOLUME_INPUT = f"/Volumes/{catalog}/{schema}/raw_json_landing/{run_id}/"

logger.info(
    "Bronze ingestion starting. run_id=%s, table=%s, source=%s",
    run_id,
    BRONZE_TABLE,
    VOLUME_INPUT,
)

spark = SparkSession.builder.appName("bronze_openfda_ingestion").getOrCreate()

raw_df = spark.read.option("multiLine", "true").json(VOLUME_INPUT)

row_count = raw_df.count()
if row_count == 0:
    raise RuntimeError(
        f"Zero records read from {VOLUME_INPUT}. "
        "Check that Airflow upload_to_volume completed successfully for this run_id."
    )

logger.info("Read %d records from Volume.", row_count)

bronze_df = (
    raw_df
    .withColumn("ingest_run_id", F.lit(run_id))
    .withColumn("ingest_layer", F.lit("bronze"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_system", F.lit("openfda_api"))
    .withColumn("raw_json", F.to_json(F.col("results")))
)

bronze_df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

logger.info("Bronze write complete. Table=%s, rows=%d", BRONZE_TABLE, row_count)
spark.stop()