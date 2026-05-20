"""
fetch.py
--------
Purpose  : Fetch food product pages from the Open Food Facts REST API
           and save each page as a JSON file on local disk.

Data flow: Open Food Facts API → requests.get() → local .json file per page

Run locally: ``python -m src.openfood.fetch`` (OPENFOOD_* in .env; no API key).
Kept outside the DAG so pytest can mock ``requests`` without Airflow.
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from .config import load_openfood_fetch_settings
from .pagination import (
    load_next_page_start,
    resolve_fetch_window,
    save_next_page_start,
    save_run_checkpoint,
)

logger = logging.getLogger(__name__)

# v2 search endpoint (cgi/search.pl is legacy).
BASE_URL = "https://world.openfoodfacts.org/api/v2/search"

# Retry these before failing the page (429 rate limit, 5xx upstream).
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# TCP drops and timeouts before any HTTP status (e.g. RemoteDisconnected).
_TRANSIENT_NETWORK_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)

# Cap backoff so long sleeps do not lose the Airflow worker heartbeat.
_MAX_RETRY_WAIT_SECONDS = 60

# OFF rejects generic library User-Agent strings.
_DEFAULT_USER_AGENT = (
    "NutriChainFoodLakehouse/1.0 "
    "(set OPENFOOD_USER_AGENT in .env to your email or project URL)"
)


def _request_headers() -> dict[str, str]:
    ua = (os.environ.get("OPENFOOD_USER_AGENT") or "").strip()
    return {"User-Agent": ua if ua else _DEFAULT_USER_AGENT}


REQUESTED_FIELDS = ",".join([
    "code",
    "product_name",
    "brands",
    "categories",
    "countries",
    "quantity",
    "serving_size",
    "energy_100g",
    "energy-kcal_100g",
    "proteins_100g",
    "fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "salt_100g",
    "sodium_100g",
    "fiber_100g",
    "nutriscore_score",
    "nutriscore_grade",
    "nova_group",
    "ingredients_text",
    "allergens",
    "packaging",
    "image_url",
    "last_modified_t",
    "pnns_groups_1",
    "pnns_groups_2",
])


def fetch_all_pages(
    output_dir: str,
    run_id: str,
    *,
    page_start: int | None = None,
) -> dict:
    """Download up to OPENFOOD_MAX_PAGES pages into ``output_dir``.

    New batch: ``resolve_fetch_window`` sets ``batch_page_start..batch_page_end`` from
    global ``next_page_start``. Retries reuse that window via ``.fetch_run_checkpoint.json``
    and skip non-empty JSON already on disk.

    Args:
        output_dir : Local folder where JSON files will be written.
        run_id     : Unique batch id (e.g. YYYYMMDD_HH00).
        page_start : Optional override for API page number (1-based). If omitted, uses
                     persisted state from OPENFOOD_PAGINATION_STATE_PATH.

    Returns:
        dict: run_id, run_date, pages_fetched, records_total, output_dir, page_start,
              page_end, next_page_start.

    Raises:
        OpenFoodConfigError: Invalid/missing OPENFOOD_* env.
        RuntimeError: Retries exhausted or timeout on a page.
    """
    settings = load_openfood_fetch_settings()
    max_pages = settings.max_pages
    records_per_page = settings.records_per_page
    polite_delay_s = settings.polite_delay_seconds
    page_max_retries = settings.page_max_retries

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    override = max(1, int(page_start)) if page_start is not None else None
    batch_page_start, batch_page_end, first_api_page = resolve_fetch_window(
        output_dir,
        run_id,
        max_pages,
        page_start_override=override,
    )
    resumed = first_api_page > batch_page_start

    pages_fetched = 0
    records_total = 0
    last_api_page = batch_page_start - 1

    for api_page in range(first_api_page, batch_page_end + 1):
        batch_slot = api_page - batch_page_start + 1
        filename = f"run_{run_id}_page_{api_page:06d}.json"
        filepath = Path(output_dir) / filename

        if filepath.exists() and filepath.stat().st_size > 0:
            try:
                with open(filepath, encoding="utf-8") as f:
                    cached = json.load(f)
                cached_count = len(cached.get("products", []))
            except (json.JSONDecodeError, OSError):
                cached_count = 0

            if cached_count > 0:
                logger.info(
                    "Skipping API page %d (batch slot %d/%d) — already saved at %s",
                    api_page,
                    batch_slot,
                    max_pages,
                    filename,
                )
                pages_fetched += 1
                records_total += cached_count
                last_api_page = api_page
                save_next_page_start(api_page + 1)
                save_run_checkpoint(
                    output_dir,
                    run_id=run_id,
                    batch_page_start=batch_page_start,
                    batch_page_end=batch_page_end,
                    next_api_page=api_page + 1,
                )
                continue
        params = {
            "search_terms": "",
            "search_simple": 1,
            "action": "process",
            "json": 1,
            "page_size": records_per_page,
            "page": api_page,
            "fields": REQUESTED_FIELDS,
            "sort_by": "last_modified_t",
        }

        max_retries = page_max_retries
        response = None
        last_status = None

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Fetching API page %d (batch slot %d/%d), attempt %d/%d",
                    api_page,
                    batch_slot,
                    max_pages,
                    attempt + 1,
                    max_retries,
                )
                response = requests.get(
                    BASE_URL,
                    params=params,
                    headers=_request_headers(),
                    timeout=30,
                )
                last_status = response.status_code

                if response.status_code in RETRYABLE_STATUS_CODES:
                    # Back off with jitter so parallel retries do not align.
                    base_wait = min(2 ** attempt, _MAX_RETRY_WAIT_SECONDS)
                    jitter = random.uniform(0, 3)
                    wait_s = base_wait + jitter

                    logger.warning(
                        "Retryable status %d on page %d (attempt %d/%d). "
                        "Waiting %.1fs before retry.",
                        response.status_code, api_page, attempt + 1, max_retries, wait_s,
                    )
                    time.sleep(wait_s)
                    continue

                # 4xx (except rate limit) fail fast.
                response.raise_for_status()
                break

            except _TRANSIENT_NETWORK_ERRORS as exc:
                logger.warning(
                    "Transient network error on API page %d (attempt %d/%d): %s",
                    api_page,
                    attempt + 1,
                    max_retries,
                    exc,
                )
                if attempt == max_retries - 1:
                    raise RuntimeError(
                        f"API page {api_page} failed after {max_retries} attempts: {exc}"
                    ) from exc
                wait_s = min(2 ** attempt, _MAX_RETRY_WAIT_SECONDS) + random.uniform(0, 3)
                time.sleep(wait_s)

        if response is None:
            raise RuntimeError(f"No response received for API page {api_page}.")

        if last_status in RETRYABLE_STATUS_CODES:
            raise RuntimeError(
                f"API page {api_page} still returning {last_status} after "
                f"{max_retries} retries. The API may be down. "
                f"Try again later, increase OPENFOOD_PAGE_MAX_RETRIES / "
                f"OPENFOOD_POLITE_DELAY_SECONDS, or reduce OPENFOOD_MAX_PAGES."
            )

        data = response.json()
        products = data.get("products", [])

        if not products:
            logger.info(
                "Empty products on API page %d. Dataset exhausted. Stopping early.",
                api_page,
            )
            save_next_page_start(1)
            break

        payload = {
            "ingest_run_id": run_id,
            "api_page_number": api_page,
            "page_number": api_page,
            "page_size": len(products),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source_url": BASE_URL,
            "total_products_reported": data.get("count", 0),
            "products": products,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        pages_fetched += 1
        records_total += len(products)
        last_api_page = api_page

        logger.info(
            "✓ Saved API page %d → %s (%d products, total so far: %d)",
            api_page, filename, len(products), records_total,
        )

        save_next_page_start(api_page + 1)
        save_run_checkpoint(
            output_dir,
            run_id=run_id,
            batch_page_start=batch_page_start,
            batch_page_end=batch_page_end,
            next_api_page=api_page + 1,
        )

        # Required courtesy delay between pages on the public API.
        time.sleep(polite_delay_s)

    batch_complete = last_api_page >= batch_page_end

    if pages_fetched == 0:
        next_page_start = 1
        logger.info(
            "No products fetched (exhausted at page %d). Resetting pagination to page 1.",
            first_api_page,
        )
    elif batch_complete:
        next_page_start = batch_page_end + 1
        save_next_page_start(next_page_start)
        logger.info(
            "Batch window %d..%d complete. Next scheduled run starts at API page %d.",
            batch_page_start,
            batch_page_end,
            next_page_start,
        )
    else:
        next_page_start = last_api_page + 1
        logger.info(
            "Batch window %d..%d incomplete (stopped at page %d). "
            "Retry will resume at API page %d.",
            batch_page_start,
            batch_page_end,
            last_api_page,
            next_page_start,
        )

    run_date = run_id[:8] if len(run_id) >= 8 and run_id[:8].isdigit() else run_id

    summary = {
        "run_id": run_id,
        "run_date": run_date,
        "pages_fetched": pages_fetched,
        "records_total": records_total,
        "output_dir": output_dir,
        "page_start": batch_page_start,
        "page_end": last_api_page if pages_fetched else None,
        "batch_page_start": batch_page_start,
        "batch_page_end": batch_page_end,
        "next_page_start": next_page_start,
        "resumed": resumed,
    }
    logger.info("Fetch complete: %s", summary)
    return summary