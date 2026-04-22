"""
fetch.py
--------
Purpose  : Fetch drug label pages from the openFDA REST API and save each
           page as a small JSON file on local disk.

Data flow: openFDA API  →  requests.get()  →  local .json file per page

Why this file exists separately from the DAG:
    Pure Python logic has no Airflow dependency. This means pytest can import and test this module without Docker or Airflow running. The DAG file is just a thin wrapper that calls this function.

How to run manually (for testing outside Airflow):
    python -m src.openfda.fetch

Prerequisites:
    OPENFDA_API_KEY environment variable.
"""

import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import requests

# Configure logging so every important event is recorder with a timestamp
logger = logging.getLogger(__name__)

# The openFDA drug label endpoint. Public API URL
BASE_URL = "https://api.fda.gov/drug/label.json"
PAGE_SIZE = 100
POLITE_DELAY_SECONDS = 0.5

def fetch_all_pages(
    output_dir: str,
    run_id: str,
    max_pages: int = 50,
    api_key: str | None = None,
    ) -> dict:
    """
    Fetch up to max_pages pages from openFDA and save each as a JSON file.

    Each saved file contains the raw API results PLUS metadata fields
    (run_id, page_number, fetched_at) that Bronze will use for lineage.
    We bake metadata into the file here rather than adding it later because if a file gets separated from its context, it still carries its own provenance information.

    Args:
        output_dir : Local folder path where JSON files will be written.
                     Created automatically if it does not exist.
        run_id     : Unique string identifying this pipeline run.
                     Used in filenames and in the metadata inside each file.
                     Format we use: "20250420" (YYYYMMDD from execution date).
        max_pages  : Maximum number of pages to fetch in one run.
                     Keeps compute bounded on the free tier.
        api_key    : openFDA API key for higher rate limits.
                     Never passed as a hardcoded value.

    Returns:
        dict with keys: run_id, pages_fetched, records_total, output_dir.
        The Airflow task returns this dict, which gets stored in XCom so the next task (upload) can read it without re-computing anything.

    Raises:
        requests.HTTPError  : For non-retryable HTTP errors (4xx that are not 429).
        RuntimeError        : If max retries are exhausted on any page.
    """

    # Create the output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    pages_fetched = 0
    records_total = 0

    for page_num in range(max_pages):

        skip = page_num * PAGE_SIZE
        params: dict = {"limit": PAGE_SIZE, "skip": skip}
        if api_key:
            params["api_key"] = api_key

        max_retries = 4
        response = None

        # Retry loop with exponential backoff
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Fetching page %d/%d (skip=%d), attempt %d",
                    page_num + 1, max_pages, skip, attempt + 1
                )

                response = requests.get(
                    BASE_URL,
                    params=params,
                    timeout=30
                )

                if response.status_code == 429:
                    wait_seconds = 2 ** attempt
                    logger.warning(
                        "Rate limited (429) on page %d. Waiting %ds before retry.",
                        page_num + 1, wait_seconds
                    )
                    time.sleep(wait_seconds)
                    continue

                # for other bad status codes, raise an error
                response.raise_for_status()
                break

            except requests.exceptions.Timeout:
                logger.error(
                    "Timeout on page %d, attempt %d", page_num + 1, attempt + 1
                )
                if attempt == max_retries - 1:

                    raise RuntimeError(
                        f"Page {page_num + 1} timed out after {max_retries} attempts."
                    )
                time.sleep(2 ** attempt)

        if response is None:
            raise RuntimeError(f"No response received for page {page_num + 1}.")

        # if all retries rate-limited, the last response is still a 429
        if response.status_code == 429:
            raise RuntimeError(
                f"Page {page_num + 1} still rate-limited after {max_retries} attempts. "
                "Increase the POLITE_DELAY_SECONDS or reduce MAX_PAGES."
            )

        # Parse the response JSON
        data = response.json()
        results = data.get("results", [])

        if not results:
            logger.info(
                "Empty results at page %d. Dataset exhausted. Stopping.", page_num + 1
            )
            break

        # add metadata in the json file
        payload = {
            "ingest_run_id": run_id,
            "page_number": page_num + 1,
            "skip_offset": skip,
            "page_size": len(results),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source_url": BASE_URL,
            "results": results,            
        }

        filename = f"run_{run_id}_page_{page_num + 1:04d}.json"
        filepath = Path(output_dir) / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        pages_fetched +=1
        records_total += len(results)

        logger.info(
            "Saved page %d -> %s (%d records, total so far: %d)",
            page_num + 1, filename, len(results), records_total
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
