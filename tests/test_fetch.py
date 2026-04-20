"""
test_fetch.py
-------------
Unit tests for src/openfda/fetch.py.

What "unit test" means here:
    We test the fetch logic IN ISOLATION from the real openFDA API.
    We replace the requests.get() function with a fake (a "mock") that
    returns exactly the response we choose. This makes tests:
    - Fast (no network calls)
    - Reliable (no dependency on internet or API availability)
    - Precise (we can test edge cases like rate limits or empty results)

How to run:
    pytest tests/test_fetch.py -v
    pytest tests/ --cov=src --cov-report=term-missing  (with coverage report)
"""

import json
import os
import tempfile
from unittest.mock import patch, MagicMock, call

import pytest

from src.openfda.fetch import fetch_all_pages, PAGE_SIZE


# ── Helper: build a fake requests.Response ────────────────────────────────────

def make_fake_response(results: list, status_code: int = 200) -> MagicMock:
    """
    Create a fake requests.Response object that behaves like a real one.

    MagicMock creates an object that accepts any attribute access or method call.
    We configure specific behaviors: .status_code, .json(), and .raise_for_status().

    Args:
        results     : List of records to include in the fake response body.
        status_code : HTTP status code to simulate.

    Returns:
        A MagicMock configured to behave like a requests.Response.
    """
    mock_response = MagicMock()
    mock_response.status_code = status_code

    # .json() is called by our code after a successful request.
    # We make it return a dict that matches the real openFDA response structure.
    mock_response.json.return_value = {
        "meta":    {"total": len(results), "skip": 0, "limit": PAGE_SIZE},
        "results": results,
    }

    # .raise_for_status() raises an exception for 4xx/5xx codes.
    # For status_code=200 we want it to do nothing (no error).
    # For status_code=500 we want it to raise.
    if status_code >= 400:
        mock_response.raise_for_status.side_effect = Exception(
            f"HTTP {status_code} Error"
        )
    else:
        mock_response.raise_for_status.return_value = None  # Does nothing.

    return mock_response


def make_fake_record(record_id: str = "abc123") -> dict:
    """Build a minimal fake drug label record matching openFDA structure."""
    return {
        "id":      record_id,
        "openfda": {"brand_name": ["TESTDRUG"], "generic_name": ["testdrug"]},
        "indications_and_usage": ["For testing purposes only."],
    }


# ── Test class ────────────────────────────────────────────────────────────────

class TestFetchAllPages:
    """Tests for the fetch_all_pages() function."""

    def test_saves_one_json_file_per_page(self):
        """
        When the API returns one page of results followed by an empty page,
        exactly one JSON file should be created on disk.

        This tests the core happy path: fetch works, file is saved.
        """
        fake_results = [make_fake_record("drug_001")]

        with tempfile.TemporaryDirectory() as tmp_dir:
            # patch() replaces requests.get with our fake for the duration
            # of this test. The "with" block means the real requests.get
            # is restored after the test finishes.
            with patch("src.openfda.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response(fake_results),  # Page 1: has data.
                    make_fake_response([]),             # Page 2: empty → stop.
                ]

                summary = fetch_all_pages(
                    output_dir=tmp_dir,
                    run_id="test20250420",
                    max_pages=5,
                )

            # Check that exactly one file was created.
            created_files = sorted(os.listdir(tmp_dir))
            assert len(created_files) == 1, (
                f"Expected 1 JSON file, got {len(created_files)}: {created_files}"
            )

            # Check the summary dict has correct counts.
            assert summary["pages_fetched"] == 1
            assert summary["records_total"] == 1
            assert summary["run_id"] == "test20250420"

    def test_file_contents_contain_metadata(self):
        """
        Each saved JSON file must contain lineage metadata fields:
        ingest_run_id, page_number, fetched_at, and the results list.

        This tests that Bronze will have the metadata it needs.
        """
        fake_results = [make_fake_record("drug_002")]

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfda.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response(fake_results),
                    make_fake_response([]),
                ]

                fetch_all_pages(
                    output_dir=tmp_dir,
                    run_id="metarun",
                    max_pages=5,
                )

            # Read the saved file and check its contents.
            saved_file = sorted(os.listdir(tmp_dir))[0]
            with open(os.path.join(tmp_dir, saved_file)) as f:
                content = json.load(f)

            assert content["ingest_run_id"] == "metarun"
            assert content["page_number"] == 1
            assert "fetched_at" in content
            assert isinstance(content["results"], list)
            assert len(content["results"]) == 1

    def test_stops_immediately_on_empty_first_page(self):
        """
        If the very first API page returns no results, no files should
        be created and pages_fetched should be 0.

        This handles the case where the API returns nothing — either
        because there is no data or the API is down.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfda.fetch.requests.get") as mock_get:
                mock_get.return_value = make_fake_response([])

                summary = fetch_all_pages(
                    output_dir=tmp_dir,
                    run_id="emptyrun",
                    max_pages=10,
                )

            assert summary["pages_fetched"] == 0
            assert summary["records_total"] == 0
            assert len(os.listdir(tmp_dir)) == 0

    def test_run_id_appears_in_filename(self):
        """
        The run_id must appear in the filename of every saved file.
        This ensures files from different runs never accidentally overwrite
        each other, and makes traceability trivial.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfda.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response([make_fake_record()]),
                    make_fake_response([]),
                ]

                fetch_all_pages(
                    output_dir=tmp_dir,
                    run_id="uniquerun999",
                    max_pages=5,
                )

            filenames = os.listdir(tmp_dir)
            assert len(filenames) == 1
            assert "uniquerun999" in filenames[0], (
                f"run_id 'uniquerun999' not found in filename: {filenames[0]}"
            )

    def test_fetches_multiple_pages(self):
        """
        When max_pages=3 and the API returns results for all 3 pages,
        3 files should be created with cumulative record counts.
        """
        fake_page = [make_fake_record(f"drug_{i}") for i in range(PAGE_SIZE)]

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfda.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response(fake_page),   # Page 1
                    make_fake_response(fake_page),   # Page 2
                    make_fake_response(fake_page),   # Page 3
                    # max_pages=3 so we never ask for page 4.
                ]

                summary = fetch_all_pages(
                    output_dir=tmp_dir,
                    run_id="multipage",
                    max_pages=3,
                )

            assert summary["pages_fetched"] == 3
            assert summary["records_total"] == PAGE_SIZE * 3
            assert len(os.listdir(tmp_dir)) == 3

    def test_api_key_included_in_request_when_provided(self):
        """
        When an api_key is passed, it must appear in the request parameters.
        If it is missing, the API uses lower rate limits and we get throttled.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfda.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response([make_fake_record()]),
                    make_fake_response([]),
                ]

                fetch_all_pages(
                    output_dir=tmp_dir,
                    run_id="keytest",
                    max_pages=2,
                    api_key="my_secret_key_123",
                )

            # Check the first call included the api_key in params.
            first_call_kwargs = mock_get.call_args_list[0]
            params_used = first_call_kwargs[1]["params"]  # keyword arg "params"
            assert params_used.get("api_key") == "my_secret_key_123"