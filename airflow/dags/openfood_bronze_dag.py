"""
openfood_bronze_dag.py
----------------------
Purpose : Airflow DAG — full NutriChain pipeline from API fetch to Gold Delta.

Company context:
    NutriChain Retail Intelligence ingests Open Food Facts product data daily,
    cleans and enriches it through Bronze → Silver → Gold, and serves it to
    Power BI dashboards for supermarket category health reports.

Task order:
    1. fetch_openfood_pages    — calls Open Food Facts API, saves JSON locally
    2. upload_to_volume        — uploads JSON to Databricks UC Volume
    3. trigger_bronze_job      — Volume JSON → Bronze Delta table
    4. trigger_silver_job      — Bronze Delta → Silver Delta (cleaned + enriched)
    5. run_dbt_gold_models     — dbt builds Gold Delta (single owner; no PySpark gold job)

WHY AIRFLOW DOES THE API FETCH (not Databricks):
    Databricks Free Edition restricts outbound internet from serverless compute.
    Airflow runs inside Docker on your local machine — no internet restrictions.
    Rule: all external API calls happen in Airflow. Databricks only transforms.

HOW TO TRIGGER MANUALLY:
    airflow dags trigger nutrichain_openfood_daily
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator

from openfood.config import load_airflow_task_defaults, load_openfood_fetch_settings
from openfood.fetch import fetch_all_pages
from openfood.upload import upload_run_to_volume

logger = logging.getLogger(__name__)

LOCAL_OUTPUT_DIR = "/tmp/nutrichain_openfood"

CATALOG = os.getenv("DATABRICKS_CATALOG", "nutrichain_lakehouse")
BRONZE_SCHEMA = os.getenv("DATABRICKS_BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("DATABRICKS_SILVER_SCHEMA", "silver")
BRONZE_TABLE = os.getenv("DATABRICKS_BRONZE_TABLE", "bronze_openfood_products_raw")
SILVER_TABLE = os.getenv("DATABRICKS_SILVER_TABLE", "silver_openfood_products")

_airflow_defaults = load_airflow_task_defaults()
default_args = {
    "owner": "nutrichain_de_team",
    "depends_on_past": False,
    "retries": _airflow_defaults.retries,
    "retry_delay": timedelta(seconds=_airflow_defaults.retry_delay_seconds),
    "email_on_failure": False,
}


def task_fetch(**context) -> dict:
    """
    Task 1: Call Open Food Facts API and save JSON pages to /tmp/ on Airflow host.
    Returns summary dict → pushed to XCom automatically for Task 2 to read.
    """
    run_id = context["ds"].replace("-", "")  # "2025-04-20" → "20250420"
    fetch_settings = load_openfood_fetch_settings()
    logger.info(
        "Starting fetch for run_id=%s, max_pages=%d, records_per_page=%d",
        run_id,
        fetch_settings.max_pages,
        fetch_settings.records_per_page,
    )

    metadata = fetch_all_pages(
        output_dir=f"{LOCAL_OUTPUT_DIR}/{run_id}",
        run_id=run_id,
    )
    logger.info("Fetch complete: %s", metadata)
    return metadata


def task_upload(**context) -> list:
    """
    Task 2: Upload local JSON files from /tmp/ to Databricks UC Volume.
    Reads run_id and output_dir from XCom (set by task_fetch).
    """
    task_instance = context["task_instance"]
    metadata = task_instance.xcom_pull(task_ids="fetch_openfood_pages")

    if not metadata:
        raise ValueError(
            "XCom from fetch_openfood_pages is empty. "
            "Did Task 1 succeed and return a summary dict?"
        )

    run_id = metadata["run_id"]
    local_dir = metadata["output_dir"]

    uploaded = upload_run_to_volume(local_dir=local_dir, run_id=run_id)
    logger.info("Upload complete. %d files sent to Volume.", len(uploaded))
    return uploaded


with DAG(
    dag_id="nutrichain_openfood_daily",
    description="NutriChain: Ingest Open Food Facts products → Bronze → Silver → Gold.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # Every day at 02:00 UTC
    catchup=False,
    tags=["nutrichain", "bronze", "silver", "gold", "openfood", "ingestion"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_openfood_pages",
        python_callable=task_fetch,
    )

    upload_task = PythonOperator(
        task_id="upload_to_volume",
        python_callable=task_upload,
    )

    bronze_job = DatabricksRunNowOperator(
        task_id="trigger_bronze_job",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_bronze_job_id }}",
        job_parameters={
            "run_id": "{{ ds_nodash }}",
            "catalog": CATALOG,
            "schema": BRONZE_SCHEMA,
            "table": BRONZE_TABLE,
        },
        wait_for_termination=True,
    )

    silver_job = DatabricksRunNowOperator(
        task_id="trigger_silver_job",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_silver_job_id }}",
        job_parameters={
            "run_id": "{{ ds_nodash }}",
            "catalog": CATALOG,
            "bronze_schema": BRONZE_SCHEMA,
            "silver_schema": SILVER_SCHEMA,
            "bronze_table": BRONZE_TABLE,
            "silver_table": SILVER_TABLE,
        },
        wait_for_termination=True,
    )

    # dbt is installed in the Airflow image via requirements.txt (no separate venv).
    # ../dbt is mounted at /opt/airflow/dbt in docker-compose; profiles.yml uses env_var().
    dbt_run_task = BashOperator(
        task_id="run_dbt_gold_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt seed --profiles-dir /opt/airflow/dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt "
            "--select path:models/gold "
            '--vars "{\"run_date\": \"{{ ds }}\"}"'
        ),
    )

    dbt_test_task = BashOperator(
        task_id="test_dbt_gold_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir /opt/airflow/dbt "
            "--select path:models/gold"
        ),
    )

    # chain:
    fetch_task >> upload_task >> bronze_job >> silver_job >> dbt_run_task >> dbt_test_task