"""
pagination.py
-------------
Persist Open Food Facts API page offset between Airflow runs.

Global state (``OPENFOOD_PAGINATION_STATE_PATH``): after each successful page,
``next_page_start`` moves forward so the next 4h tick does not restart at page 1.

Per-run checkpoint (``.fetch_run_checkpoint.json`` in the batch output dir): pins
``batch_page_start..batch_page_end`` for one ``run_id``. Airflow task retries must
reuse that window; otherwise a retry would open a fresh N-page slice and leave
the previous batch half-fetched.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_STATE_PATH = "/tmp/nutrichain_openfood/pagination_state.json"
RUN_CHECKPOINT_FILENAME = ".fetch_run_checkpoint.json"
_PAGE_FILE_RE = re.compile(r"^run_(?P<run_id>.+)_page_(?P<page>\d{6})\.json$")


def get_state_path() -> Path:
    raw = (os.environ.get("OPENFOOD_PAGINATION_STATE_PATH") or "").strip()
    return Path(raw if raw else DEFAULT_STATE_PATH)


def load_next_page_start() -> int:
    """Read global ``next_page_start`` (1 if state file missing or corrupt)."""
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
    """Write global offset for the next scheduled fetch."""
    if next_page_start < 1:
        raise ValueError(f"next_page_start must be >= 1; got {next_page_start}")
    path = get_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"next_page_start": next_page_start}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Pagination state saved: next_page_start=%d → %s", next_page_start, path)


def _run_checkpoint_path(output_dir: str | Path) -> Path:
    return Path(output_dir) / RUN_CHECKPOINT_FILENAME


def load_run_checkpoint(output_dir: str | Path) -> dict | None:
    """Return checkpoint dict for this output dir, or None."""
    path = _run_checkpoint_path(output_dir)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read run checkpoint at %s: %s", path, exc)
        return None
    return data if isinstance(data, dict) else None


def save_run_checkpoint(
    output_dir: str | Path,
    *,
    run_id: str,
    batch_page_start: int,
    batch_page_end: int,
    next_api_page: int,
) -> None:
    """Save batch window and next API page for one ``run_id`` (retry-safe)."""
    if batch_page_start < 1 or batch_page_end < batch_page_start:
        raise ValueError(
            f"Invalid batch window: start={batch_page_start}, end={batch_page_end}"
        )
    if not (batch_page_start <= next_api_page <= batch_page_end + 1):
        raise ValueError(
            f"next_api_page={next_api_page} outside batch "
            f"[{batch_page_start}, {batch_page_end}]"
        )
    path = _run_checkpoint_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "batch_page_start": batch_page_start,
        "batch_page_end": batch_page_end,
        "next_api_page": next_api_page,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "Run checkpoint saved: run_id=%s batch=%d..%d next_api_page=%d → %s",
        run_id,
        batch_page_start,
        batch_page_end,
        next_api_page,
        path,
    )


def list_fetched_api_pages(output_dir: str | Path, run_id: str) -> list[int]:
    """Sorted page numbers from existing ``run_{run_id}_page_*.json`` files."""
    root = Path(output_dir)
    if not root.is_dir():
        return []
    pages: list[int] = []
    for path in root.glob(f"run_{run_id}_page_*.json"):
        match = _PAGE_FILE_RE.match(path.name)
        if not match or match.group("run_id") != run_id:
            continue
        pages.append(int(match.group("page")))
    return sorted(pages)


def resolve_fetch_window(
    output_dir: str | Path,
    run_id: str,
    max_pages: int,
    *,
    page_start_override: int | None = None,
) -> tuple[int, int, int]:
    """Pick batch_page_start, batch_page_end, and first API page to request.

    Order: existing checkpoint for ``run_id`` → else infer from saved JSON →
    else new window from global offset or ``page_start_override``.
    Returns ``(batch_page_start, batch_page_end, next_api_page)``.
    """
    if max_pages < 1:
        raise ValueError(f"max_pages must be >= 1; got {max_pages}")

    existing_pages = list_fetched_api_pages(output_dir, run_id)
    checkpoint = load_run_checkpoint(output_dir)

    if checkpoint and checkpoint.get("run_id") == run_id:
        batch_start = int(checkpoint["batch_page_start"])
        batch_end = int(checkpoint["batch_page_end"])
        next_page = int(checkpoint.get("next_api_page", batch_start))
        logger.info(
            "Resuming DAG run %s from checkpoint: batch %d..%d, next_api_page=%d",
            run_id,
            batch_start,
            batch_end,
            next_page,
        )
    elif existing_pages:
        batch_start = existing_pages[0]
        batch_end = batch_start + max_pages - 1
        next_page = existing_pages[-1] + 1
        logger.info(
            "Resuming DAG run %s from %d saved file(s): batch %d..%d, next_api_page=%d",
            run_id,
            len(existing_pages),
            batch_start,
            batch_end,
            next_page,
        )
    else:
        batch_start = (
            max(1, int(page_start_override))
            if page_start_override is not None
            else load_next_page_start()
        )
        batch_end = batch_start + max_pages - 1
        next_page = batch_start
        logger.info(
            "Starting DAG run %s: batch %d..%d (%d pages)",
            run_id,
            batch_start,
            batch_end,
            max_pages,
        )

    if existing_pages:
        next_page = max(next_page, existing_pages[-1] + 1)

    next_page = max(batch_start, next_page)
    if next_page > batch_end + 1:
        next_page = batch_end + 1

    save_run_checkpoint(
        output_dir,
        run_id=run_id,
        batch_page_start=batch_start,
        batch_page_end=batch_end,
        next_api_page=next_page,
    )
    return batch_start, batch_end, next_page
