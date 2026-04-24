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
    OPENFOOD_MAX_PAGES env var (optional, defaults to 50).
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Open Food Facts search API — returns products with all nutrition fields
BASE_URL = "https://world.openfoodfacts.org/cgi/search.pl"
PAGE_SIZE = 100
POLITE_DELAY_SECONDS = 1.0  # Be polite to a free public API

# Only request the fields we actually need — keeps response size small
# and avoids downloading 200 fields we will never use
REQUESTED_FIELDS = ",".join([
    "code",                      # barcode — our primary key
    "product_name",
    "brands",
    "categories",
    "countries",
    "quantity",
    "serving_size",
    "energy_100g",               # may be kJ or kcal — we fix in Silver
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
    "last_modified_t",           # Unix timestamp — tells us data freshness
    "pnns_groups_1",             # Parent nutrition category (OFF system)
    "pnns_groups_2",             # Sub nutrition category
])


def fetch_all_pages(
    output_dir: str,
    run_id: str,
    max_pages: int = 50,
) -> dict:
    """
    Fetch up to max_pages pages from Open Food Facts and save each as JSON.

    Open Food Facts uses page= (1-indexed) + page_size= for pagination,
    unlike openFDA which used skip=. We adapt accordingly.

    Each saved file contains raw API results PLUS lineage metadata fields
    (run_id, page_number, fetched_at) baked directly into the file so the
    file is self-describing even if moved or renamed.

    Args:
        output_dir : Local folder where JSON files will be written.
                     Created automatically if it does not exist.
        run_id     : Unique string identifying this pipeline run.
                     Format: "20250420" (YYYYMMDD from Airflow execution date).
        max_pages  : Maximum number of pages to fetch in one run.
                     Keeps cost and time bounded on free tier.

    Returns:
        dict: run_id, pages_fetched, records_total, output_dir.
              Airflow XCom carries this to the upload task automatically.

    Raises:
        requests.HTTPError : For non-retryable HTTP errors.
        RuntimeError       : If max retries are exhausted on any page.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    pages_fetched = 0
    records_total = 0

    for page_num in range(1, max_pages + 1):
        params = {
            "search_terms": "",          # empty = all products
            "search_simple": 1,
            "action": "process",
            "json": 1,
            "page_size": PAGE_SIZE,
            "page": page_num,
            "fields": REQUESTED_FIELDS,
            "sort_by": "last_modified_t",  # most recently updated first
        }

        max_retries = 4
        response = None

        for attempt in range(max_retries):
            try:
                logger.info(
                    "Fetching page %d/%d, attempt %d",
                    page_num, max_pages, attempt + 1,
                )
                response = requests.get(BASE_URL, params=params, timeout=30)

                if response.status_code == 429:
                    wait_s = 2 ** attempt
                    logger.warning(
                        "Rate limited (429) on page %d. Waiting %ds.", page_num, wait_s
                    )
                    time.sleep(wait_s)
                    continue

                response.raise_for_status()
                break

            except requests.exceptions.Timeout:
                logger.error("Timeout on page %d, attempt %d", page_num, attempt + 1)
                if attempt == max_retries - 1:
                    raise RuntimeError(
                        f"Page {page_num} timed out after {max_retries} attempts."
                    )
                time.sleep(2 ** attempt)

        if response is None:
            raise RuntimeError(f"No response received for page {page_num}.")

        if response.status_code == 429:
            raise RuntimeError(
                f"Page {page_num} still rate-limited after {max_retries} attempts."
            )

        data = response.json()
        products = data.get("products", [])

        if not products:
            logger.info(
                "Empty products list at page %d. Dataset exhausted. Stopping.",
                page_num,
            )
            break

        payload = {
            "ingest_run_id": run_id,
            "page_number": page_num,
            "page_size": len(products),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source_url": BASE_URL,
            "total_products_reported": data.get("count", 0),  # OFF tells us total
            "products": products,
        }

        filename = f"run_{run_id}_page_{page_num:04d}.json"
        filepath = Path(output_dir) / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)  # ensure_ascii=False
            # because product names contain accents, Chinese chars, Arabic, etc.

        pages_fetched += 1
        records_total += len(products)

        logger.info(
            "Saved page %d → %s (%d products, total so far: %d)",
            page_num, filename, len(products), records_total,
        )
        time.sleep(POLITE_DELAY_SECONDS)

    summary = {
        "run_id": run_id,
        "pages_fetched": pages_fetched,
        "records_total": records_total,
        "output_dir": output_dir,
    }
    logger.info("Fetch complete: %s", summary)
    return summary