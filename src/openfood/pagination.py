"""
pagination.py
-------------
Persist Open Food Facts API page offset between Airflow runs.

Each successful fetch advances `next_page_start` so the next run requests
pages N+1..N+max_pages instead of always re-fetching pages 1..max_pages.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_STATE_PATH = "/tmp/nutrichain_openfood/pagination_state.json"


def get_state_path() -> Path:
    raw = (os.environ.get("OPENFOOD_PAGINATION_STATE_PATH") or "").strip()
    return Path(raw if raw else DEFAULT_STATE_PATH)


def load_next_page_start() -> int:
    """Return the API page number to start from on the next fetch (default 1)."""
    path = get_state_path()
    if not path.exists():
        return 1
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read pagination state at %s: %s. Starting at page 1.", path, exc)
        return 1
    value = data.get("next_page_start", 1)
    try:
        page = int(value)
    except (TypeError, ValueError):
        logger.warning("Invalid next_page_start=%r in %s. Starting at page 1.", value, path)
        return 1
    return max(1, page)


def save_next_page_start(next_page_start: int) -> None:
    """Persist the API page number for the next pipeline run."""
    if next_page_start < 1:
        raise ValueError(f"next_page_start must be >= 1; got {next_page_start}")
    path = get_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"next_page_start": next_page_start}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Pagination state saved: next_page_start=%d → %s", next_page_start, path)
