"""
upload.py
---------
Purpose  : Upload local JSON files from a run directory to Databricks DBFS.

Data flow: local disk (JSON files)  →  HTTPS POST  →  Databricks DBFS

Why base64?
    The Databricks /dbfs/put API accepts JSON request bodies. JSON is text.
    File contents are raw bytes, which may include characters that break JSON.
    Base64 encoding converts any bytes to safe ASCII text, which JSON handles
    perfectly. Databricks decodes it back on the other side.

Why one file at a time?
    The Community Edition /dbfs/put endpoint works best with files under 1 MB.
    Our files are ~100-200 KB each. Uploading one at a time is simpler, more
    reliable, and easier to retry if one file fails.

Prerequisites:
    DATABRICKS_HOST  env var
    DATABRICKS_TOKEN env var
"""

import os
import base64
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

DBFS_BASE_PATH = "/FileStore/openfda/bronze/raw"

def get_dbfs_credentials() -> tuple[str, str]:
 
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    if not host:
        raise EnvironmentError(
            "DATABRICKS_HOST is not set"
            "Add it to your .env file or environment"
        )
    if not token:
        raise EnvironmentError(
            "DATABRICKS_TOKEN is not set"
            "Add it to your .env file or environment"
        )

    return host, token

def upload_file_to_dbfs(
    local_filepath: Path,
    dbfs_target_path: str,
    host: str,
    token: str,
) -> bool:
    
    # Read the file as raw bytes.
    with open(local_filepath, "rb") as f:
        file_bytes = f.read()

    # Encode bytes to base64 string. The Databricks API requires this.
    file_b64 = base64.b64encode(file_bytes).decode("utf-8")
    
    # Build the JSON payload for the /dbfs/put endpoint.
    payload = {
        "path": dbfs_target_path,
        "contents": file_b64,
        "overwrite": True
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    url = f"{host}/api/2.0/dbfs/put"

    logger.info(
        "Uploading %s -> dbfs:%s", local_filepath.name, dbfs_target_path
    )

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    logger.info("Upload successful: %s", local_filepath.name)
    return True



def upload_run_to_dbfs(local_dir: str, run_id: str) -> list[str]:
    
    host, token = get_dbfs_credentials()

    # Build the DBFS subfolder for this specific run.
    dbfs_run_folder = f"{DBFS_BASE_PATH}/{run_id}"

    local_path = Path(local_dir)
    json_files = sorted(local_path.glob("*.json"))

    if not json_files:
        logger.warning("No JSON files found in %s. Nothing to upload.", local_dir)
        return[]

    logger.info(
        "Starting upload of %d files from %s to dbfs:%s",
        len(json_files), local_dir, dbfs_run_folder
    )

    uploaded_paths = []

    for local_file in json_files:
        dbfs_target = f"{dbfs_run_folder}/{local_file.name}"

        upload_file_to_dbfs(
            local_filepath=local_file,
            dbfs_target_path=dbfs_target,
            host=host,
            token=token,
        )

        uploaded_paths.append(dbfs_target)

    logger.info(
        "Upload complete. %d files uploaded to dbfs:%s",
        len(uploaded_paths), dbfs_run_folder
    )

    return uploaded_paths
    
