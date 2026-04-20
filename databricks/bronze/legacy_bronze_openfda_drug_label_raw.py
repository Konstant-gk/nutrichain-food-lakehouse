"""
Bronze ingestion for openFDA drug labeling endpoint.

Data flow:
1) Read pages from openFDA API (`/drug/label.json`) using limit/skip pagination.
2) Preserve each returned record as raw JSON with ingestion metadata.
3) Append rows to a Delta bronze table.
4) Run automated validation checks and write run-level audit telemetry.

How to run in Databricks:
- Attach a serverless notebook session (or run as a Job task).
- Make sure the target table catalog/schema exists.
- Optionally store API key in Databricks Secrets and pass secret scope/key.
- Call `ingest_openfda_drug_labels(...)` with a config.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from pyspark.dbutils import DBUtils
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class OpenFdaIngestionConfig:
    """Configuration for openFDA drug-label ingestion.

    Args:
        target_table: Fully-qualified Delta table for bronze rows.
        endpoint_url: API endpoint URL for openFDA drug labels.
        limit: Page size requested from API (openFDA max is typically 1000).
        start_skip: Offset for the first page.
        max_pages: Max number of pages per run (use small values for smoke tests).
        request_timeout_seconds: Timeout per HTTP request.
        retry_attempts: Retry count per page if transient request errors happen.
        retry_backoff_seconds: Base delay between retries.
        throttle_seconds: Delay between successful page requests (polite rate control).
        api_key: Optional direct API key value.
        secret_scope: Optional Databricks secret scope name for API key lookup.
        secret_key: Optional Databricks secret key name for API key lookup.
    """

    target_table: str
    endpoint_url: str = "https://api.fda.gov/drug/label.json"
    limit: int = 1000
    start_skip: int = 0
    max_pages: int = 5
    request_timeout_seconds: int = 60
    retry_attempts: int = 3
    retry_backoff_seconds: float = 2.0
    throttle_seconds: float = 0.25
    api_key: Optional[str] = None
    secret_scope: Optional[str] = None
    secret_key: Optional[str] = None
    request_backoff_jitter_seconds: float = 0.5
    validation_min_rows: int = 1
    fail_on_duplicate_hashes: bool = False
    audit_table: Optional[str] = None
    max_error_message_length: int = 1000
    job_name: str = "openfda_bronze_drug_label_ingestion"


@dataclass(frozen=True)
class ValidationSummary:
    """Strongly-typed automated validation output."""

    is_valid: bool
    metrics: Dict[str, Any]
    checks: List[Dict[str, Any]]


LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Configure default logger once for notebook and job use."""
    if LOGGER.handlers:
        return

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


def _log_event(level: str, event: str, **context: Any) -> None:
    """Emit structured JSON logs for easier operations filtering."""
    payload = {"event": event, **context}
    message = json.dumps(payload, default=str, sort_keys=True)
    log_method = getattr(LOGGER, level, LOGGER.info)
    log_method(message)


def _parse_table_identifier(target_table: str) -> Tuple[str, str, str]:
    """Parse table identifier into (catalog, schema, table_name).

    Supports:
    - catalog.schema.table
    - schema.table (defaults catalog to `hive_metastore`)
    """
    parts = target_table.split(".")
    if len(parts) == 3:
        parsed = (parts[0], parts[1], parts[2])
    elif len(parts) == 2:
        parsed = ("hive_metastore", parts[0], parts[1])
    else:
        raise ValueError(
            f"Unsupported target_table format: {target_table}. Use catalog.schema.table or schema.table."
        )

    identifier_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    invalid_identifiers = [part for part in parsed if not identifier_pattern.match(part)]
    if invalid_identifiers:
        raise ValueError(
            "Table identifiers must be alphanumeric or underscore and cannot start with a digit: "
            f"{invalid_identifiers}"
        )

    return parsed[0], parsed[1], parsed[2]


def ensure_bronze_table_with_spark_sql(spark: SparkSession, target_table: str) -> None:
    """Create schema and bronze Delta table using Spark SQL if they do not exist."""
    catalog, schema, table_name = _parse_table_identifier(target_table)
    fq_schema = f"{catalog}.{schema}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fq_schema}.{table_name} (
            ingest_run_id STRING,
            batch_id STRING,
            page_number INT,
            page_skip INT,
            page_limit INT,
            request_url STRING,
            api_endpoint STRING,
            ingested_at_utc STRING,
            source_record_id STRING,
            effective_time STRING,
            record_hash STRING,
            raw_json STRING
        )
        USING DELTA
        """
    )


def ensure_ingestion_audit_table_with_spark_sql(spark: SparkSession, audit_table: str) -> None:
    """Create ingestion audit table for run-level observability."""
    catalog, schema, table_name = _parse_table_identifier(audit_table)
    fq_schema = f"{catalog}.{schema}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fq_schema}.{table_name} (
            ingest_run_id STRING,
            job_name STRING,
            started_at_utc STRING,
            ended_at_utc STRING,
            status STRING,
            target_table STRING,
            pages_fetched INT,
            rows_written_total INT,
            retries_total INT,
            last_successful_skip INT,
            validation_is_valid BOOLEAN,
            validation_payload_json STRING,
            error_message STRING
        )
        USING DELTA
        """
    )


def _utc_now_iso() -> str:
    """Return current UTC timestamp as ISO 8601 text."""
    return datetime.now(timezone.utc).isoformat()


def _build_ingest_run_id() -> str:
    """Build a compact run identifier for grouping one ingestion execution."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _resolve_api_key(spark: SparkSession, config: OpenFdaIngestionConfig) -> Optional[str]:
    """Resolve API key from direct config or Databricks Secrets.

    Priority:
    1) Explicit `config.api_key`
    2) Databricks Secrets lookup if `secret_scope` and `secret_key` are set
    3) None
    """
    if config.api_key:
        return config.api_key

    if config.secret_scope and config.secret_key:
        try:
            dbutils_helper = DBUtils(spark)
            return dbutils_helper.secrets.get(config.secret_scope, config.secret_key)  # type: ignore[name-defined]
        except NameError as exc:
            raise RuntimeError(
                "Databricks `dbutils` is unavailable. Run inside Databricks or pass api_key directly."
            ) from exc

    return None


def _request_page(
    session: requests.Session,
    config: OpenFdaIngestionConfig,
    api_key: Optional[str],
    skip: int,
) -> Tuple[str, Dict[str, Any], int]:
    """Request one API page with retries and return (final_url, payload_json, attempts_used)."""
    params: Dict[str, Any] = {"limit": config.limit, "skip": skip}
    if api_key:
        params = {"api_key": api_key, **params}

    for attempt in range(1, config.retry_attempts + 1):
        try:
            _log_event(
                "info",
                "openfda_page_request_attempt",
                skip=skip,
                attempt=attempt,
                retry_attempts=config.retry_attempts,
            )
            response = session.get(
                config.endpoint_url,
                params=params,
                timeout=config.request_timeout_seconds,
            )
            response.raise_for_status()
            return response.url, response.json(), attempt
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            retryable_statuses = {408, 429, 500, 502, 503, 504}
            is_retryable = status_code in retryable_statuses
            if attempt == config.retry_attempts or not is_retryable:
                raise RuntimeError(
                    f"openFDA HTTP error (status={status_code}, retryable={is_retryable}, skip={skip})"
                ) from exc
            sleep_seconds = config.retry_backoff_seconds * attempt + config.request_backoff_jitter_seconds
            _log_event(
                "warning",
                "openfda_page_request_retry_http_error",
                skip=skip,
                attempt=attempt,
                status_code=status_code,
                sleep_seconds=sleep_seconds,
            )
            time.sleep(sleep_seconds)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == config.retry_attempts:
                raise RuntimeError(f"openFDA network timeout/connection failure (skip={skip})") from exc
            sleep_seconds = config.retry_backoff_seconds * attempt + config.request_backoff_jitter_seconds
            _log_event(
                "warning",
                "openfda_page_request_retry_connection_error",
                skip=skip,
                attempt=attempt,
                sleep_seconds=sleep_seconds,
            )
            time.sleep(sleep_seconds)
        except requests.RequestException as exc:
            if attempt == config.retry_attempts:
                raise RuntimeError(f"openFDA request exception (skip={skip})") from exc
            sleep_seconds = config.retry_backoff_seconds * attempt + config.request_backoff_jitter_seconds
            _log_event(
                "warning",
                "openfda_page_request_retry_generic_error",
                skip=skip,
                attempt=attempt,
                sleep_seconds=sleep_seconds,
            )
            time.sleep(sleep_seconds)

    # This line is defensive and should never be reached because retries either return or raise.
    raise RuntimeError("Page request loop ended unexpectedly.")


def _records_to_bronze_rows(
    records: List[Dict[str, Any]],
    *,
    ingest_run_id: str,
    batch_id: str,
    page_number: int,
    page_skip: int,
    page_limit: int,
    request_url: str,
    endpoint_name: str,
) -> List[Dict[str, Any]]:
    """Convert API records to append-only bronze rows with metadata."""
    ingested_at_utc = _utc_now_iso()
    output_rows: List[Dict[str, Any]] = []

    for record in records:
        raw_json = json.dumps(record, ensure_ascii=False, sort_keys=True)
        record_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        output_rows.append(
            {
                "ingest_run_id": ingest_run_id,
                "batch_id": batch_id,
                "page_number": page_number,
                "page_skip": page_skip,
                "page_limit": page_limit,
                "request_url": request_url,
                "api_endpoint": endpoint_name,
                "ingested_at_utc": ingested_at_utc,
                "source_record_id": record.get("id"),
                "effective_time": record.get("effective_time"),
                "record_hash": record_hash,
                "raw_json": raw_json,
            }
        )

    return output_rows


def _write_bronze_append(spark: SparkSession, target_table: str, rows: List[Dict[str, Any]]) -> int:
    """Append rows to a Delta table and return written-row count."""
    if not rows:
        return 0

    dataframe = spark.createDataFrame(rows)
    (
        dataframe.write.format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )
    return dataframe.count()


def _truncate_error_message(message: str, max_length: int) -> str:
    """Keep failure payloads bounded for log/audit storage."""
    if len(message) <= max_length:
        return message
    return f"{message[: max_length - 3]}..."


def run_automated_validation(
    spark: SparkSession,
    target_table: str,
    ingest_run_id: str,
    expected_pages_fetched: int,
    min_rows_required: int,
    fail_on_duplicate_hashes: bool,
) -> ValidationSummary:
    """Run production-style validation and return pass/fail checks plus metrics."""
    summary_row = (
        spark.sql(
            f"""
            SELECT
                ingest_run_id,
                COUNT(*) AS row_count,
                COUNT(DISTINCT batch_id) AS batch_count,
                MIN(page_skip) AS min_page_skip,
                MAX(page_skip) AS max_page_skip
            FROM {target_table}
            WHERE ingest_run_id = '{ingest_run_id}'
            GROUP BY ingest_run_id
            """
        )
        .limit(1)
        .collect()
    )
    summary_metrics = summary_row[0].asDict() if summary_row else {}

    duplicate_metrics = (
        spark.sql(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT record_hash) AS distinct_record_hashes,
                (COUNT(*) - COUNT(DISTINCT record_hash)) AS duplicate_hash_rows
            FROM {target_table}
            WHERE ingest_run_id = '{ingest_run_id}'
            """
        )
        .limit(1)
        .collect()[0]
        .asDict()
    )

    row_count = int(summary_metrics.get("row_count", 0))
    batch_count = int(summary_metrics.get("batch_count", 0))
    duplicate_hash_rows = int(duplicate_metrics.get("duplicate_hash_rows", 0))

    checks: List[Dict[str, Any]] = []
    checks.append(
        {
            "name": "minimum_row_count",
            "status": "PASS" if row_count >= min_rows_required else "FAIL",
            "severity": "CRITICAL",
            "message": f"row_count={row_count}, min_rows_required={min_rows_required}",
        }
    )
    checks.append(
        {
            "name": "pages_vs_batches_alignment",
            "status": "PASS" if batch_count == expected_pages_fetched else "FAIL",
            "severity": "CRITICAL",
            "message": f"batch_count={batch_count}, expected_pages_fetched={expected_pages_fetched}",
        }
    )
    duplicate_status = "FAIL" if fail_on_duplicate_hashes and duplicate_hash_rows > 0 else "WARN"
    if duplicate_hash_rows == 0:
        duplicate_status = "PASS"
    checks.append(
        {
            "name": "duplicate_hash_observation",
            "status": duplicate_status,
            "severity": "ERROR" if fail_on_duplicate_hashes else "WARNING",
            "message": f"duplicate_hash_rows={duplicate_hash_rows}, fail_on_duplicate_hashes={fail_on_duplicate_hashes}",
        }
    )

    failing_statuses = {"FAIL"}
    is_valid = all(check["status"] not in failing_statuses for check in checks)
    metrics = {
        "summary": summary_metrics,
        "duplicates": duplicate_metrics,
        "expected_pages_fetched": expected_pages_fetched,
    }
    return ValidationSummary(is_valid=is_valid, metrics=metrics, checks=checks)


def _write_ingestion_audit_record(
    spark: SparkSession,
    audit_table: str,
    *,
    ingest_run_id: str,
    job_name: str,
    started_at_utc: str,
    ended_at_utc: str,
    status: str,
    target_table: str,
    pages_fetched: int,
    rows_written_total: int,
    retries_total: int,
    last_successful_skip: int,
    validation_summary: Optional[ValidationSummary],
    error_message: Optional[str],
) -> None:
    """Append one run-level audit row to the Delta audit table."""
    payload = {
        "ingest_run_id": ingest_run_id,
        "job_name": job_name,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
        "status": status,
        "target_table": target_table,
        "pages_fetched": pages_fetched,
        "rows_written_total": rows_written_total,
        "retries_total": retries_total,
        "last_successful_skip": last_successful_skip,
        "validation_is_valid": validation_summary.is_valid if validation_summary else False,
        "validation_payload_json": json.dumps(
            {
                "checks": validation_summary.checks if validation_summary else [],
                "metrics": validation_summary.metrics if validation_summary else {},
            },
            default=str,
            sort_keys=True,
        ),
        "error_message": error_message,
    }
    spark.createDataFrame([payload]).write.format("delta").mode("append").saveAsTable(audit_table)


def ingest_openfda_drug_labels(
    spark: SparkSession,
    config: OpenFdaIngestionConfig,
) -> Dict[str, Any]:
    """Ingest openFDA drug-label pages into bronze Delta table.

    This is append-only for bronze by design. Idempotent business output is achieved
    later in silver by deduplicating with `record_hash` and source identifiers.
    """
    _configure_logging()
    ingest_run_id = _build_ingest_run_id()
    started_at_utc = _utc_now_iso()
    api_key = _resolve_api_key(spark, config)
    endpoint_name = "drug/label.json"
    status = "RUNNING"
    error_message: Optional[str] = None
    validation_summary: Optional[ValidationSummary] = None
    retries_total = 0
    rows_written_total = 0
    pages_fetched = 0
    last_successful_skip = -1

    _log_event(
        "info",
        "ingestion_run_started",
        ingest_run_id=ingest_run_id,
        job_name=config.job_name,
        target_table=config.target_table,
        endpoint_url=config.endpoint_url,
        max_pages=config.max_pages,
        limit=config.limit,
        start_skip=config.start_skip,
    )

    try:
        ensure_bronze_table_with_spark_sql(spark, config.target_table)
        if config.audit_table:
            ensure_ingestion_audit_table_with_spark_sql(spark, config.audit_table)

        with requests.Session() as session:
            for page_index in range(config.max_pages):
                skip = config.start_skip + page_index * config.limit
                request_url, payload, attempts_used = _request_page(session, config, api_key, skip)
                retries_total += max(0, attempts_used - 1)
                records = payload.get("results", [])

                if not records:
                    _log_event("info", "openfda_empty_page_stopping", skip=skip, page_index=page_index)
                    break

                pages_fetched += 1
                page_number = page_index + 1
                batch_id = f"{ingest_run_id}_skip_{skip}"

                bronze_rows = _records_to_bronze_rows(
                    records,
                    ingest_run_id=ingest_run_id,
                    batch_id=batch_id,
                    page_number=page_number,
                    page_skip=skip,
                    page_limit=config.limit,
                    request_url=request_url,
                    endpoint_name=endpoint_name,
                )

                rows_written = _write_bronze_append(spark, config.target_table, bronze_rows)
                rows_written_total += rows_written
                last_successful_skip = skip
                _log_event(
                    "info",
                    "openfda_page_written",
                    ingest_run_id=ingest_run_id,
                    page_number=page_number,
                    skip=skip,
                    rows_in_page=len(records),
                    rows_written=rows_written,
                    rows_written_total=rows_written_total,
                )

                # If API returns fewer than limit, we reached final available page for this query.
                if len(records) < config.limit:
                    _log_event(
                        "info",
                        "openfda_short_page_stopping",
                        page_number=page_number,
                        skip=skip,
                        rows_in_page=len(records),
                        configured_limit=config.limit,
                    )
                    break

                time.sleep(config.throttle_seconds)

        validation_summary = run_automated_validation(
            spark=spark,
            target_table=config.target_table,
            ingest_run_id=ingest_run_id,
            expected_pages_fetched=pages_fetched,
            min_rows_required=config.validation_min_rows,
            fail_on_duplicate_hashes=config.fail_on_duplicate_hashes,
        )
        _log_event(
            "info",
            "ingestion_automated_validation_completed",
            ingest_run_id=ingest_run_id,
            is_valid=validation_summary.is_valid,
            checks=validation_summary.checks,
        )
        if not validation_summary.is_valid:
            raise RuntimeError("Automated validation failed. Check validation checks in logs/audit table.")

        status = "SUCCESS"
        return {
            "ingest_run_id": ingest_run_id,
            "target_table": config.target_table,
            "pages_fetched": pages_fetched,
            "rows_written_total": rows_written_total,
            "retries_total": retries_total,
            "validation": {
                "is_valid": validation_summary.is_valid,
                "checks": validation_summary.checks,
                "metrics": validation_summary.metrics,
            },
            "status": status,
            "started_at_utc": started_at_utc,
            "ended_at_utc": _utc_now_iso(),
        }
    except Exception as exc:
        status = "FAILED"
        error_message = _truncate_error_message(str(exc), config.max_error_message_length)
        _log_event(
            "error",
            "ingestion_run_failed",
            ingest_run_id=ingest_run_id,
            error_message=error_message,
        )
        LOGGER.exception("Ingestion failure traceback")
        raise
    finally:
        ended_at_utc = _utc_now_iso()
        if config.audit_table:
            try:
                _write_ingestion_audit_record(
                    spark=spark,
                    audit_table=config.audit_table,
                    ingest_run_id=ingest_run_id,
                    job_name=config.job_name,
                    started_at_utc=started_at_utc,
                    ended_at_utc=ended_at_utc,
                    status=status,
                    target_table=config.target_table,
                    pages_fetched=pages_fetched,
                    rows_written_total=rows_written_total,
                    retries_total=retries_total,
                    last_successful_skip=last_successful_skip,
                    validation_summary=validation_summary,
                    error_message=error_message,
                )
            except Exception:
                LOGGER.exception("Failed to write ingestion audit record")
        _log_event(
            "info",
            "ingestion_run_finished",
            ingest_run_id=ingest_run_id,
            status=status,
            pages_fetched=pages_fetched,
            rows_written_total=rows_written_total,
            retries_total=retries_total,
            last_successful_skip=last_successful_skip,
        )


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()

    demo_config = OpenFdaIngestionConfig(
        target_table="hive_metastore.default.bronze_openfda_drug_label_raw",
        limit=1000,
        start_skip=0,
        max_pages=2,  # Small safe run for smoke testing.
        secret_scope=None,  # Example: "openfda_scope"
        secret_key=None,  # Example: "api_key"
        api_key=None,  # Prefer secret scope/key in shared environments.
        audit_table="hive_metastore.default.openfda_ingestion_audit",
        validation_min_rows=1,
        fail_on_duplicate_hashes=False,
    )

    result = ingest_openfda_drug_labels(spark_session, demo_config)
    print(
        "Ingestion finished:",
        {
            "ingest_run_id": result["ingest_run_id"],
            "target_table": result["target_table"],
            "pages_fetched": result["pages_fetched"],
            "rows_written_total": result["rows_written_total"],
            "retries_total": result["retries_total"],
            "status": result["status"],
        },
    )

