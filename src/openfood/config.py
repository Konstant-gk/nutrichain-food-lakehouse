"""
config.py
---------
Read Open Food Facts ingest and Airflow task settings from environment variables.

Documented defaults live in env.example.txt at the repo root — not in Python constants,
so dev/staging/prod cannot drift from two different hardcoded fallbacks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class OpenFoodConfigError(ValueError):
    """Raised when a required OPENFOOD_* or AIRFLOW_* env var is missing or invalid."""


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
    """Load OPENFOOD_* fetch tuning from the environment."""
    return OpenFoodFetchSettings(
        max_pages=_require_positive_int("OPENFOOD_MAX_PAGES"),
        records_per_page=_require_positive_int("OPENFOOD_RECORDS_PER_PAGE"),
        polite_delay_seconds=_require_positive_float("OPENFOOD_POLITE_DELAY_SECONDS"),
        page_max_retries=_require_positive_int("OPENFOOD_PAGE_MAX_RETRIES"),
    )


def load_airflow_task_defaults() -> AirflowTaskDefaults:
    """Load Airflow default_args retry settings from the environment."""
    return AirflowTaskDefaults(
        retries=_require_non_negative_int("AIRFLOW_TASK_RETRIES"),
        retry_delay_seconds=_require_positive_int("AIRFLOW_TASK_RETRY_DELAY_SECONDS"),
    )
