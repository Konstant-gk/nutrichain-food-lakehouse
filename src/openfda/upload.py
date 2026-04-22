"""
upload.py
---------
Purpose : Upload local JSON files from a run directory to a Databricks
          Unity Catalog Volume using the Files API.

Prerequisites:
    DATABRICKS_HOST  env var — e.g. https://adb-xxxx.azuredatabricks.net
    DATABRICKS_TOKEN env var — Databricks personal access token
    A UC Volume must exist at /Volumes/dev/openfda/landing/
    (create it once in Databricks UI: Catalog → your schema → New Volume)
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Defaults can be overridden from environment.
DEFAULT_VOLUME_BASE_PATH = "/Volumes/openfda_lakehouse/bronze/raw_json_landing"
DEFAULT_MAX_FILE_MB = "10"
DEFAULT_TIMEOUT_SECONDS = "60"
DEFAULT_MAX_RETRIES = "3"
DEFAULT_BACKOFF_SECONDS = "1.5"


def get_files_api_credentials() -> tuple[str, str]:
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    if not host:
        raise EnvironmentError(
            "DATABRICKS_HOST is not set. Add it to your .env/environment."
        )
    if not token:
        raise EnvironmentError(
            "DATABRICKS_TOKEN is not set. Add it to your .env/environment."
        )
    return host, token


def _validate_volume_base_path(path: str) -> str:
    normalized = path.strip().rstrip("/")
    if not normalized.startswith("/Volumes/"):
        raise ValueError(
            f"DATABRICKS_VOLUME_PATH must start with '/Volumes/'. Got: {path}"
        )
    return normalized


def _get_max_file_size_bytes() -> int:
    raw_value = os.getenv("DATABRICKS_MAX_FILE_MB", DEFAULT_MAX_FILE_MB)
    try:
        mb = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"DATABRICKS_MAX_FILE_MB must be an integer. Got: {raw_value}"
        ) from exc

    if mb <= 0:
        raise ValueError("DATABRICKS_MAX_FILE_MB must be > 0.")
    return mb * 1024 * 1024


def _put_file_with_retry(
    url: str,
    token: str,
    local_file: Path,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }

    for attempt in range(max_retries + 1):
        try:
            # Stream file bytes directly; avoids loading full file into memory.
            with open(local_file, "rb") as file_handle:
                response = requests.put(
                    url,
                    headers=headers,
                    data=file_handle,
                    timeout=timeout_seconds,
                )
        except requests.RequestException as exc:
            if attempt < max_retries:
                sleep_s = backoff_seconds * (2 ** attempt)
                logger.warning(
                    "Network/upload error for %s (attempt %d/%d): %s. Retrying in %.1fs",
                    local_file.name,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"Upload failed for {local_file.name}: {exc}") from exc

        if response.status_code in (200, 201, 204):
            return

        retryable_statuses = {408, 429, 500, 502, 503, 504}
        if response.status_code in retryable_statuses and attempt < max_retries:
            sleep_s = backoff_seconds * (2 ** attempt)
            logger.warning(
                "Retryable HTTP status for %s: %s (attempt %d/%d). Retrying in %.1fs",
                local_file.name,
                response.status_code,
                attempt + 1,
                max_retries + 1,
                sleep_s,
            )
            time.sleep(sleep_s)
            continue

        raise RuntimeError(
            f"Upload failed for {local_file.name}: "
            f"HTTP {response.status_code} - {response.text}"
        )

    raise RuntimeError(f"Upload failed for {local_file.name}: retries exhausted.")


def upload_run_to_volume(local_dir: str, run_id: str) -> list[str]:
    host, token = get_files_api_credentials()

    volume_base_path = _validate_volume_base_path(
        os.getenv("DATABRICKS_VOLUME_PATH", DEFAULT_VOLUME_BASE_PATH)
    )
    max_file_size_bytes = _get_max_file_size_bytes()

    timeout_seconds = int(os.getenv("DATABRICKS_UPLOAD_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))
    max_retries = int(os.getenv("DATABRICKS_UPLOAD_MAX_RETRIES", DEFAULT_MAX_RETRIES))
    backoff_seconds = float(os.getenv("DATABRICKS_UPLOAD_BACKOFF_SECONDS", DEFAULT_BACKOFF_SECONDS))

    local_path = Path(local_dir)
    json_files = sorted(local_path.glob("*.json"))
    if not json_files:
        logger.warning("No JSON files found in %s; nothing uploaded.", local_dir)
        return []

    volume_run_path = f"{volume_base_path}/{run_id}"
    uploaded_paths: list[str] = []

    logger.info(
        "Starting upload of %d JSON files from %s to %s",
        len(json_files),
        local_dir,
        volume_run_path,
    )

    for local_file in json_files:
        file_size = local_file.stat().st_size
        if file_size > max_file_size_bytes:
            raise RuntimeError(
                f"File too large for configured cap: {local_file.name} "
                f"({file_size} bytes > {max_file_size_bytes} bytes). "
                "Split upstream or increase DATABRICKS_MAX_FILE_MB."
            )

        volume_file_path = f"{volume_run_path}/{local_file.name}"
        url = f"{host}/api/2.0/fs/files{volume_file_path}"

        logger.info("Uploading %s -> %s", local_file.name, volume_file_path)

        _put_file_with_retry(
            url=url,
            token=token,
            local_file=local_file,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )

        uploaded_paths.append(volume_file_path)
        logger.info("Uploaded %s", local_file.name)

    logger.info(
        "Upload complete. %d files uploaded to %s",
        len(uploaded_paths),
        volume_run_path,
    )
    return uploaded_paths