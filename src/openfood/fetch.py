"""
fetch.py
--------
Purpose  : Fetch food product pages from the Open Food Facts REST API
           and save each page as a JSON file on local disk.

Data flow: Open Food Facts API → requests.get() → local .json file per page

Company context:
    NutriChain Retail Intelligence ingests product nutrition data from
    Open Food Facts to power category health reports for supermarket clients.

Why this file is separate from the DAG:
    Pure Python with no Airflow dependency. pytest can import and test
    this module without Docker or Airflow running. The DAG is a thin wrapper.

How to run manually:
    python -m src.openfood.fetch

Prerequisites:
    No API key required. Open Food Facts is fully public.
    Set OPENFOOD_* vars in .env (see env.example.txt): MAX_PAGES, RECORDS_PER_PAGE,
    POLITE_DELAY_SECONDS, PAGE_MAX_RETRIES, USER_AGENT.
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

logger = logging.getLogger(__name__)

# Open Food Facts v2 search API
# This is the stable modern endpoint — cgi/search.pl is the legacy one
BASE_URL = "https://world.openfoodfacts.org/api/v2/search"

# Status codes that mean "server is busy, try again later"
# 429 = rate limited, 503 = temporarily unavailable, 502 = bad gateway
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# OpenFood blocks anonymous library defaults;
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


def fetch_all_pages(output_dir: str, run_id: str) -> dict:
    """
    Fetch up to OPENFOOD_MAX_PAGES pages from Open Food Facts and save each as JSON.

    Args:
        output_dir : Local folder where JSON files will be written.
        run_id     : Unique string identifying this pipeline run (YYYYMMDD).

    Returns:
        dict: run_id, pages_fetched, records_total, output_dir.

    Raises:
        OpenFoodConfigError : If required OPENFOOD_* env vars are missing or invalid.
        RuntimeError : If max retries are exhausted on any page.
    """
    settings = load_openfood_fetch_settings()
    max_pages = settings.max_pages
    records_per_page = settings.records_per_page
    polite_delay_s = settings.polite_delay_seconds
    page_max_retries = settings.page_max_retries

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    pages_fetched = 0
    records_total = 0

    for page_num in range(1, max_pages + 1):
        params = {
            "search_terms": "",
            "search_simple": 1,
            "action": "process",
            "json": 1,
            "page_size": records_per_page,
            "page": page_num,
            "fields": REQUESTED_FIELDS,
            "sort_by": "last_modified_t",
        }

        max_retries = page_max_retries
        response = None
        last_status = None

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Fetching page %d/%d, attempt %d/%d",
                    page_num, max_pages, attempt + 1, max_retries,
                )
                response = requests.get(
                    BASE_URL,
                    params=params,
                    headers=_request_headers(),
                    timeout=30,
                )
                last_status = response.status_code

                # ── KEY FIX: retry on ALL server-side errors, not just 429 ──
                if response.status_code in RETRYABLE_STATUS_CODES:
                    # Exponential backoff WITH jitter
                    # Jitter = small random extra wait so multiple retries
                    # don't all hit the server at exactly the same second
                    base_wait = 2 ** attempt          # 1, 2, 4, 8, 16 seconds
                    jitter = random.uniform(0, 3)   # random 0-3 extra seconds
                    wait_s = base_wait + jitter

                    logger.warning(
                        "Retryable status %d on page %d (attempt %d/%d). "
                        "Waiting %.1fs before retry.",
                        response.status_code, page_num, attempt + 1, max_retries, wait_s,
                    )
                    time.sleep(wait_s)
                    continue  # go to next attempt, do NOT call raise_for_status

                # For non-retryable errors (400, 401, 403, 404) → crash immediately
                response.raise_for_status()
                break  # success — exit the retry loop

            except requests.exceptions.Timeout:
                logger.error(
                    "Timeout on page %d, attempt %d/%d",
                    page_num, attempt + 1, max_retries,
                )
                if attempt == max_retries - 1:
                    raise RuntimeError(
                        f"Page {page_num} timed out after {max_retries} attempts."
                    )
                wait_s = 2 ** attempt + random.uniform(0, 1)
                time.sleep(wait_s)

        # ── After retry loop: check if we actually got a good response ────────
        if response is None:
            raise RuntimeError(f"No response received for page {page_num}.")

        if last_status in RETRYABLE_STATUS_CODES:
            raise RuntimeError(
                f"Page {page_num} still returning {last_status} after "
                f"{max_retries} retries. The API may be down. "
                f"Try again later, increase OPENFOOD_PAGE_MAX_RETRIES / "
                f"OPENFOOD_POLITE_DELAY_SECONDS, or reduce OPENFOOD_MAX_PAGES."
            )

        data = response.json()
        products = data.get("products", [])

        if not products:
            logger.info(
                "Empty products on page %d. Dataset exhausted. Stopping early.",
                page_num,
            )
            break

        payload = {
            "ingest_run_id": run_id,
            "page_number": page_num,
            "page_size": len(products),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source_url": BASE_URL,
            "total_products_reported": data.get("count", 0),
            "products": products,
        }

        filename = f"run_{run_id}_page_{page_num:04d}.json"
        filepath = Path(output_dir) / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

        pages_fetched += 1
        records_total += len(products)

        logger.info(
            "✓ Saved page %d → %s (%d products, total so far: %d)",
            page_num, filename, len(products), records_total,
        )

        # Polite delay between pages — NEVER remove this for a free public API
        time.sleep(polite_delay_s)

    summary = {
        "run_id": run_id,
        "pages_fetched": pages_fetched,
        "records_total": records_total,
        "output_dir": output_dir,
    }
    logger.info("Fetch complete: %s", summary)
    return summary