"""
test_fetch.py
-------------
Unit tests for src/openfood/fetch.py — NutriChain Retail Intelligence.

We mock requests.get() so tests run instantly without network calls.
Tests cover: happy path, empty response, metadata in files, multi-page,
run_id in filenames, and User-Agent header wiring.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src.openfood.config import load_openfood_fetch_settings
from src.openfood.fetch import _DEFAULT_USER_AGENT, fetch_all_pages


@pytest.fixture(autouse=True)
def _no_time_sleep(monkeypatch):
    """Retries and polite delays use sleep; tests stay fast without real waits."""
    monkeypatch.setattr("src.openfood.fetch.time.sleep", lambda *_args, **_kwargs: None)


@pytest.fixture(autouse=True)
def _pagination_state_tmp(monkeypatch, tmp_path):
    """Isolate pagination state so tests do not share offsets."""
    state_file = tmp_path / "pagination_state.json"
    monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", str(state_file))
    monkeypatch.setattr(
        "src.openfood.fetch.load_next_page_start",
        lambda: 1,
    )
    monkeypatch.setattr(
        "src.openfood.fetch.save_next_page_start",
        lambda _next: None,
    )


def make_fake_response(products: list, status_code: int = 200) -> MagicMock:
    """Build a fake requests.Response matching Open Food Facts API shape."""
    page_size = load_openfood_fetch_settings().records_per_page
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.json.return_value = {
        "count": len(products),
        "page": 1,
        "page_size": page_size,
        "products": products,
    }
    if status_code >= 400:
        mock_response.raise_for_status.side_effect = Exception(
            f"HTTP {status_code} Error"
        )
    else:
        mock_response.raise_for_status.return_value = None
    return mock_response


def make_fake_product(barcode: str = "3017620422003") -> dict:
    """Build a minimal fake Open Food Facts product record."""
    return {
        "code": barcode,
        "product_name": "Nutella",
        "brands": "Ferrero",
        "categories": "Spreads, Sweet spreads",
        "countries": "France, Germany",
        "energy_100g": 2255.0,
        "energy-kcal_100g": 539.0,
        "proteins_100g": 6.3,
        "fat_100g": 30.9,
        "sugars_100g": 56.3,
        "salt_100g": 0.107,
        "sodium_100g": None,
        "nutriscore_score": 26,
        "nutriscore_grade": "e",
        "nova_group": 4,
        "last_modified_t": 1700000000,
    }


class TestFetchAllPages:

    def test_saves_one_json_file_per_page(self):
        """Happy path: one page of products → one file saved, correct summary."""
        fake_products = [make_fake_product("111")]

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfood.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response(fake_products),
                    make_fake_response([]),  # page 2: empty → stop
                ]
                summary = fetch_all_pages(output_dir=tmp_dir, run_id="20250420")

            files = os.listdir(tmp_dir)
            assert len(files) == 1
            assert summary["pages_fetched"] == 1
            assert summary["records_total"] == 1
            assert summary["run_id"] == "20250420"

    def test_file_contents_contain_metadata_and_products(self):
        """Each saved file must carry lineage metadata + the products list."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfood.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response([make_fake_product("222")]),
                    make_fake_response([]),
                ]
                fetch_all_pages(output_dir=tmp_dir, run_id="metarun")

            saved_file = sorted(os.listdir(tmp_dir))[0]
            with open(os.path.join(tmp_dir, saved_file), encoding="utf-8") as f:
                content = json.load(f)

            assert content["ingest_run_id"] == "metarun"
            assert content["page_number"] == 1
            assert "fetched_at" in content
            assert "source_url" in content
            assert isinstance(content["products"], list)
            assert len(content["products"]) == 1

    def test_stops_on_empty_first_page(self):
        """If first page has no products, no files created, summary shows 0."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfood.fetch.requests.get") as mock_get:
                mock_get.return_value = make_fake_response([])
                summary = fetch_all_pages(output_dir=tmp_dir, run_id="emptyrun")

            assert summary["pages_fetched"] == 0
            assert summary["records_total"] == 0
            assert len(os.listdir(tmp_dir)) == 0

    def test_run_id_appears_in_filename(self):
        """run_id must appear in the filename for traceability."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfood.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response([make_fake_product()]),
                    make_fake_response([]),
                ]
                fetch_all_pages(output_dir=tmp_dir, run_id="uniquerun999")

            filenames = os.listdir(tmp_dir)
            assert "uniquerun999" in filenames[0]

    def test_fetches_multiple_pages(self):
        """OPENFOOD_MAX_PAGES=3 with full pages → 3 files, correct record count."""
        records_per_page = load_openfood_fetch_settings().records_per_page
        fake_page = [make_fake_product(f"prod_{i}") for i in range(records_per_page)]

        with patch.dict(os.environ, {"OPENFOOD_MAX_PAGES": "3"}):
            with tempfile.TemporaryDirectory() as tmp_dir:
                with patch("src.openfood.fetch.requests.get") as mock_get:
                    mock_get.side_effect = [
                        make_fake_response(fake_page),
                        make_fake_response(fake_page),
                        make_fake_response(fake_page),
                    ]
                    summary = fetch_all_pages(output_dir=tmp_dir, run_id="multipage")

                assert summary["pages_fetched"] == 3
                assert summary["records_total"] == records_per_page * 3
                assert len(os.listdir(tmp_dir)) == 3

    def test_rate_limit_retries(self):
        """429 on first attempt should retry and succeed on second attempt."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch("src.openfood.fetch.requests.get") as mock_get:
                rate_limited = make_fake_response([], status_code=429)
                rate_limited.raise_for_status.return_value = None
                success = make_fake_response([make_fake_product()])
                empty = make_fake_response([])
                mock_get.side_effect = [rate_limited, success, empty]

                summary = fetch_all_pages(output_dir=tmp_dir, run_id="retrytest")

            assert summary["pages_fetched"] == 1

    def test_opens_with_user_agent_from_env_when_set(self):
        """OPENFOOD_USER_AGENT must be sent so OFF does not return 403."""
        custom_ua = "NutriChainCustom/1.0 (pytest-override)"
        with patch.dict(os.environ, {"OPENFOOD_USER_AGENT": custom_ua}):
            with tempfile.TemporaryDirectory() as tmp_dir:
                with patch("src.openfood.fetch.requests.get") as mock_get:
                    mock_get.side_effect = [
                        make_fake_response([make_fake_product()]),
                        make_fake_response([]),
                    ]
                    fetch_all_pages(output_dir=tmp_dir, run_id="uatest")

                first = mock_get.call_args_list[0]
                assert first.kwargs["headers"]["User-Agent"] == custom_ua

    def test_opens_with_default_user_agent_when_env_unset(self):
        """Without env, still send a non-library default User-Agent."""
        with patch.dict(os.environ, {"OPENFOOD_USER_AGENT": ""}):
            with tempfile.TemporaryDirectory() as tmp_dir:
                with patch("src.openfood.fetch.requests.get") as mock_get:
                    mock_get.side_effect = [
                        make_fake_response([make_fake_product()]),
                        make_fake_response([]),
                    ]
                    fetch_all_pages(output_dir=tmp_dir, run_id="defaultua")

                ua = mock_get.call_args_list[0].kwargs["headers"]["User-Agent"]
                assert ua == _DEFAULT_USER_AGENT
