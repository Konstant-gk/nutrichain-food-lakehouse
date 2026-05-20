"""
config.py
---------
Read Open Food Facts ingest and Airflow task settings from environment variables.

Required keys are validated at read time (no silent defaults in code).
See env.example.txt for names and documented values.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class OpenFoodConfigError(ValueError):
    """Missing or invalid OPENFOOD_* / AIRFLOW_* environment variable."""


def _require_raw(name: str) -> str:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        raise OpenFoodConfigError(
            f"{name} is not set. Copy env.example.txt to .env and set {name}."
        )
    return raw


def _require_positive_int(name: str) -> int:
    raw = _require_raw(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise OpenFoodConfigError(f"{name}={raw!r} is not an integer.") from exc
    if value < 1:
        raise OpenFoodConfigError(f"{name} must be >= 1; got {raw!r}.")
    return value


def _require_non_negative_int(name: str) -> int:
    raw = _require_raw(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise OpenFoodConfigError(f"{name}={raw!r} is not an integer.") from exc
    if value < 0:
        raise OpenFoodConfigError(f"{name} must be >= 0; got {raw!r}.")
    return value


def _require_positive_float(name: str) -> float:
    raw = _require_raw(name)
    try:
        value = float(raw)
    except ValueError as exc:
        raise OpenFoodConfigError(f"{name}={raw!r} is not a number.") from exc
    if value <= 0:
        raise OpenFoodConfigError(f"{name} must be > 0; got {raw!r}.")
    return value


@dataclass(frozen=True)
class OpenFoodFetchSettings:
    max_pages: int
    records_per_page: int
    polite_delay_seconds: float
    page_max_retries: int


@dataclass(frozen=True)
class AirflowTaskDefaults:
    retries: int
    retry_delay_seconds: int


def load_openfood_fetch_settings() -> OpenFoodFetchSettings:
    """Return page size, max pages, polite delay, and per-page retry limit."""
    return OpenFoodFetchSettings(
        max_pages=_require_positive_int("OPENFOOD_MAX_PAGES"),
        records_per_page=_require_positive_int("OPENFOOD_RECORDS_PER_PAGE"),
        polite_delay_seconds=_require_positive_float("OPENFOOD_POLITE_DELAY_SECONDS"),
        page_max_retries=_require_positive_int("OPENFOOD_PAGE_MAX_RETRIES"),
    )


def load_airflow_task_defaults() -> AirflowTaskDefaults:
    """Return task retry count and delay for the Airflow DAG default_args."""
    return AirflowTaskDefaults(
        retries=_require_non_negative_int("AIRFLOW_TASK_RETRIES"),
        retry_delay_seconds=_require_positive_int("AIRFLOW_TASK_RETRY_DELAY_SECONDS"),
    )
