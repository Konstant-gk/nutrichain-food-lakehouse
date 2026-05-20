"""After a full batch, global next_page_start moves past batch_page_end."""

import os
import tempfile
from unittest.mock import patch

from src.openfood.fetch import fetch_all_pages
from src.openfood.pagination import load_next_page_start


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


class TestFetchPagination:

    def test_advances_page_offset_after_run(self, monkeypatch):
        monkeypatch.setattr("src.openfood.fetch.time.sleep", lambda *_a, **_k: None)

        with tempfile.TemporaryDirectory() as tmp_dir:
            state_file = os.path.join(tmp_dir, "state.json")
            monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", state_file)

            with patch.dict(os.environ, {"OPENFOOD_MAX_PAGES": "2"}):
                with patch("src.openfood.fetch.requests.get") as mock_get:
                    mock_get.side_effect = [
                        make_fake_response([make_fake_product("111")]),
                        make_fake_response([make_fake_product("222")]),
                    ]
                    summary = fetch_all_pages(
                        output_dir=os.path.join(tmp_dir, "out"),
                        run_id="20250420_0900",
                        page_start=101,
                    )

            assert summary["page_start"] == 101
            assert summary["next_page_start"] == 103
            assert load_next_page_start() == 103
