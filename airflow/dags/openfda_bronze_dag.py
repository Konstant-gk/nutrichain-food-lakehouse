"""
openfda_bronze_dag.py
---------------------
Purpose  : Airflow DAG that orchestrates daily Bronze ingestion from openFDA.

Task sequence:
    fetch_openfda_pages  →  upload_to_dbfs

What this file does:
    Defines the pipeline schedule, task dependencies, retry behavior, and
    passes parameters to the library functions in src/openfda/.

What this file does NOT do:
    Contains no API call logic, no file I/O, no upload logic.
    All of that lives in src/openfda/fetch.py and src/openfda/upload.py.
    This DAG is a thin orchestration wrapper — easy to read, easy to modify.

How to trigger manually (useful for testing):
    In the Airflow UI: DAGs → openfda_drug_label_bronze_daily → Trigger DAG
    Or via CLI: airflow dags trigger openfda_drug_label_bronze_daily

How to view task logs:
    Airflow UI → DAG run → click any task → Logs tab
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Library functions import
from src.openfda.fetch import fetch_all_pages
from src.openfda.upload import upload_run_to_dbfs

logger = logging.getLogger(__name__)

# Local directory for temp json files
LOCAL_OUTPUT_BASE = "/tmp/openfda_bronze"

# API pages to fetch per rum
MAX_PAGES = int(os.getenv("OPENFDA_MAX_PAGES", "50"))

#DAG default arguments
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_Delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Task Functions
def task_fetch(**context) -> dict:
    """
    Task 1: Fetch pages from openFDA and save JSON files to local disk.
    The execution_date comes from Airflow context. Using "ds" (date string) makes the pipeline idempotent.
    """
    
    # context["ds"] is the execution date as a string: "2025-04-20"
    execution_date = context["ds"] 
    run_id = execution_date.replace("-","")
    output_dir = f"{LOCAL_OUTPUT_BASE}/{run_id}"
    api_key = os.getenv("OPENFDA_API_KEY")

    logger.info(
        "Starting fetch for run_id=%s, max_pages=%d, output_dir=%s", run_id, MAX_PAGES, output_dir
    )

    # Call library function to fetch pages from openFDA
    summary = fetch_all_pages(
        output_dir=output_dir,
        run_id=run_id,
        max_pages=MAX_PAGES,
        api_key=api_key,
    )

    logger.info("Fetch task complete: %s", summary)
    return summary


def task_upload(**context) -> list:
    """
    Task 2: This task reads the return value from task_fetch using XCom pull.
    XCom (Cross-Communication) is Airflow's built-in system for passing
    small values between tasks.
    """

    task_instance = context["task_instance"]
    fetch_summary = task_instance.xcom_pull(task_ids="fetch_openfda_pages")

    if not fetch_summary:
        raise ValueError(
            "No XCom data from fetch_openfda_pages."
            "Did the fetch task complete successfully?"
        )

    run_id = fetch_summary["run_id"]
    local_dir = fetch_summary["output_dir"]
    pages_fetched = fetch_summary.get("pages_fetched", 0)

    logger.info(
        "Starting upload for run_id=%s, %d pages fetched, from %s",
        run_id, pages_fetched, local_dir
    )

    # Call library function to upload files to DBFS
    uploaded_paths = upload_run_to_dbfs(local_dir=local_dir, run_id=run_id)

    logger.info(
        "Upload task complete. %d files uploaded to DBFS.", len(uploaded_paths)
    )

    return uploaded_paths


# DAG Definition
with DAG(
    dag_id="openfda_drug_label_bronze_daily",

    description=(
        "Daily batch ingestion: fetch openFDA drug labels"
        "and land JSON files in Databricks DBFS Bronze zone"
    ),

    default_args=default_args, 
    start_date=datetime(2025, 1, 1),  #Past date to not backfill old runs
    schedule_interval="0 2 * * *", # Cron expression: daily at 2:00 AM
    catchup=False, # Disable backfill by default
    tags=["bronze", "openfda", "ingestion"], # Tags for filtering

) as dag:

    #Task 1
    fetch_task = PythonOperator(
        task_id="fetch_openfda_pages",
        python_callable=task_fetch,
    )

    #Task 2
    upload_task = PythonOperator(
        task_id="upload_to_dbfs",
        python_callable=task_upload,
    )

    # The >> operator sets the execution order. If fetch_task fails (and retries are exhausted), upload_task never runs.
    fetch_task >> upload_task