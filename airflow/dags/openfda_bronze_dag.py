"""
openfda_bronze_dag.py
---------------------
Purpose : Airflow DAG — full pipeline from API fetch to Gold Delta table.

Task order:
    1. fetch_openfda_pages   — calls openFDA API, saves JSON locally
    2. upload_to_volume      — uploads JSON to Databricks UC Volume
    3. trigger_bronze_job    — Databricks job: Volume → Bronze Delta table
    4. trigger_silver_job    — Databricks job: Bronze Delta → Silver Delta
    5. trigger_gold_job      — Databricks job: Silver Delta → Gold Delta

WHY AIRFLOW DOES ALL EXTERNAL CALLS:
    Databricks Free Edition restricts outbound internet from serverless compute.
    Calls to openFDA from inside a Databricks notebook may fail with DNS errors.
    Airflow runs on your laptop inside Docker — no internet restrictions.
    Rule: all external API calls happen in Airflow. Databricks only transforms.

HOW TO TRIGGER MANUALLY:
    airflow dags trigger openfda_drug_label_bronze_daily

WHAT "SUCCESS" LOOKS LIKE:
    All 5 tasks green in Airflow UI.
    Bronze, Silver, Gold Delta tables have new rows with today's ingest_run_id.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Our library code — separate from DAG logic so it can be unit tested
from openfda.fetch import fetch_all_pages
from openfda.upload import upload_run_to_volume  # updated function name

logger = logging.getLogger(__name__)

LOCAL_OUTPUT_DIR = "/tmp/openfda_bronze"
MAX_PAGES = int(os.getenv("OPENFDA_MAX_PAGES", "100"))
CATALOG = os.getenv("DATABRICKS_CATALOG", "openfda_lakehouse")
BRONZE_SCHEMA = os.getenv("DATABRICKS_BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("DATABRICKS_SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.getenv("DATABRICKS_GOLD_SCHEMA", "gold")
BRONZE_TABLE = os.getenv("DATABRICKS_BRONZE_TABLE", "bronze_openfda_drug_labeling_raw")
SILVER_TABLE = os.getenv("DATABRICKS_SILVER_TABLE", "silver_openfda_drug_labeling")
GOLD_TABLE = os.getenv("DATABRICKS_GOLD_TABLE", "gold_openfda_drug_labeling")


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def task_fetch(**context) -> dict:
    """
    Task 1: Fetch pages from openFDA and save JSON files to /tmp/ on this machine.
    """
    execution_date = context["ds"]       # e.g. "2025-04-20"
    run_id = execution_date.replace("-", "")  # e.g. "20250420"
    api_key = os.getenv("OPENFDA_API_KEY")   # optional, higher rate limit

    metadata = fetch_all_pages(
        output_dir=f"{LOCAL_OUTPUT_DIR}/{run_id}",
        run_id=run_id,
        max_pages=MAX_PAGES,
        api_key=api_key,
    )

    logger.info("Fetch complete: %s", metadata)
    # Return dict is pushed to XCom automatically — Task 2 reads it
    return metadata


def task_upload(**context) -> list:
    """
    Task 2: Upload local JSON files to Databricks UC Volume via Files API.
    """
    task_instance = context["task_instance"]
    metadata = task_instance.xcom_pull(task_ids="fetch_openfda_pages")

    run_id = metadata["run_id"]
    local_dir = metadata["output_dir"]

    uploaded = upload_run_to_volume(local_dir=local_dir, run_id=run_id)
    logger.info("Upload complete. %d files sent to Volume.", len(uploaded))
    return uploaded


# --- DAG definition ---
with DAG(
    dag_id="openfda_drug_labeling_bronze_daily",
    description="Fetch openFDA drug labels and run full Bronze→Silver→Gold pipeline.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # 02:00 every day
    catchup=False,
    tags=["bronze", "silver", "gold", "openfda", "ingestion"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_openfda_pages",
        python_callable=task_fetch,
    )

    upload_task = PythonOperator(
        task_id="upload_to_volume",
        python_callable=task_upload,
    )

    # Task 3: Trigger Bronze Databricks Job
    bronze_job = DatabricksRunNowOperator(
    task_id="trigger_bronze_job",
    databricks_conn_id="databricks_default",
    job_id="{{ var.value.databricks_bronze_job_id }}",
    notebook_params={
        "run_id": "{{ ds_nodash }}",       
        "catalog": CATALOG,                 
        "schema": BRONZE_SCHEMA,            
        "table": BRONZE_TABLE,           
    },
    wait_for_termination=True,
    )

    # Task 4: Silver — only runs after Bronze job succeeds
    silver_job = DatabricksRunNowOperator(
    task_id="trigger_silver_job",
    databricks_conn_id="databricks_default",
    job_id="{{ var.value.databricks_silver_job_id }}",
    notebook_params={
        "run_id": "{{ ds_nodash }}",
        "catalog": CATALOG,
        "bronze_schema": BRONZE_SCHEMA,
        "silver_schema": SILVER_SCHEMA,
        "bronze_table": BRONZE_TABLE,
        "silver_table": SILVER_TABLE,
    },
    wait_for_termination=True,
    )

    # Task 5: Gold — only runs after Silver job succeeds
    gold_job = DatabricksRunNowOperator(
    task_id="trigger_gold_job",
    databricks_conn_id="databricks_default",
    job_id="{{ var.value.databricks_gold_job_id }}",
    notebook_params={
        "run_id": "{{ ds_nodash }}",
        "catalog": CATALOG,
        "silver_schema": SILVER_SCHEMA,
        "gold_schema": GOLD_SCHEMA,
        "silver_table": SILVER_TABLE,
        "gold_table": GOLD_TABLE,
    },
    wait_for_termination=True,
    )

    # If upload fails, Bronze never triggers. If Bronze fails, Silver never triggers.
    fetch_task >> upload_task >> bronze_job >> silver_job >> gold_job