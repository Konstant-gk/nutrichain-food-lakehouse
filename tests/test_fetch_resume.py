"""Fetch must finish the same page window on Airflow retry, not start a new batch."""

import json
import os
from unittest.mock import patch

from src.openfood.fetch import fetch_all_pages
from src.openfood.pagination import load_run_checkpoint


def make_fake_response(products: list, status_code: int = 200):
    from unittest.mock import MagicMock
    from src.openfood.config import load_openfood_fetch_settings

    page_size = load_openfood_fetch_settings().records_per_page
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.json.return_value = {
        "count": len(products),
        "page": 1,
        "page_size": page_size,
        "products": products,
    }
    mock_response.raise_for_status.return_value = None
    return mock_response


def make_fake_product(barcode: str) -> dict:
    return {
        "code": barcode,
        "product_name": "Test",
        "last_modified_t": 1700000000,
    }


class TestFetchResumeWithinBatch:

    def test_retry_continues_same_batch_window(self, monkeypatch, tmp_path):
        """Second attempt resumes inside the original batch_page_end cap."""
        monkeypatch.setattr("src.openfood.fetch.time.sleep", lambda *_a, **_k: None)
        state_file = tmp_path / "pagination_state.json"
        monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", str(state_file))

        out_dir = tmp_path / "out"
        run_id = "20260518_1600"

        import requests

        with patch.dict(os.environ, {"OPENFOOD_MAX_PAGES": "5"}):
            # Attempt 1: pages 10-11 succeed, page 12 fails after in-loop retries.
            calls = {"n": 0}

            def flaky_get(*_args, **_kwargs):
                calls["n"] += 1
                if calls["n"] <= 2:
                    return make_fake_response([make_fake_product(f"p{calls['n']}")])
                raise requests.exceptions.ConnectionError("network down")

            with patch("src.openfood.fetch.requests.get", side_effect=flaky_get):
                try:
                    fetch_all_pages(
                        output_dir=str(out_dir),
                        run_id=run_id,
                        page_start=10,
                    )
                except RuntimeError:
                    pass

            checkpoint = load_run_checkpoint(out_dir)
            assert checkpoint is not None
            assert checkpoint["batch_page_start"] == 10
            assert checkpoint["batch_page_end"] == 14
            assert checkpoint["next_api_page"] == 12

            # Attempt 2: should finish 12-14 only (batch slots 3-5), not 12-16.
            with patch("src.openfood.fetch.requests.get") as mock_get_retry:
                mock_get_retry.side_effect = [
                    make_fake_response([make_fake_product("c")]),
                    make_fake_response([make_fake_product("d")]),
                    make_fake_response([make_fake_product("e")]),
                ]
                summary = fetch_all_pages(
                    output_dir=str(out_dir),
                    run_id=run_id,
                )

        assert summary["resumed"] is True
        assert summary["batch_page_start"] == 10
        assert summary["batch_page_end"] == 14
        assert summary["next_page_start"] == 15
        assert summary["pages_fetched"] == 3

        saved_pages = sorted(
            int(p.name.split("_page_")[1].split(".")[0])
            for p in out_dir.glob("run_*_page_*.json")
        )
        assert saved_pages == [10, 11, 12, 13, 14]
        assert mock_get_retry.call_count == 3

    def test_skips_existing_json_without_api_call(self, monkeypatch, tmp_path):
        monkeypatch.setattr("src.openfood.fetch.time.sleep", lambda *_a, **_k: None)
        state_file = tmp_path / "pagination_state.json"
        monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", str(state_file))

        out_dir = tmp_path / "out"
        run_id = "20260518_2000"
        out_dir.mkdir(parents=True)

        existing = {
            "ingest_run_id": run_id,
            "api_page_number": 20,
            "products": [make_fake_product("cached")],
        }
        (out_dir / f"run_{run_id}_page_000020.json").write_text(
            json.dumps(existing),
            encoding="utf-8",
        )

        with patch.dict(os.environ, {"OPENFOOD_MAX_PAGES": "3"}):
            with patch("src.openfood.fetch.requests.get") as mock_get:
                mock_get.side_effect = [
                    make_fake_response([make_fake_product("fresh21")]),
                    make_fake_response([make_fake_product("fresh22")]),
                ]
                summary = fetch_all_pages(
                    output_dir=str(out_dir),
                    run_id=run_id,
                    page_start=20,
                )

        assert mock_get.call_count == 2
        assert summary["page_start"] == 20
        assert summary["batch_page_end"] == 22
        assert summary["resumed"] is True
