"""Tests for openfood.pagination — API page offset persistence."""

import json
import tempfile
from pathlib import Path

import pytest

from src.openfood.pagination import load_next_page_start, save_next_page_start


class TestPaginationState:

    def test_defaults_to_page_one_when_missing(self, monkeypatch, tmp_path):
        state_file = tmp_path / "pagination_state.json"
        monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", str(state_file))
        assert load_next_page_start() == 1

    def test_loads_saved_page(self, monkeypatch, tmp_path):
        state_file = tmp_path / "pagination_state.json"
        monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", str(state_file))
        state_file.write_text(json.dumps({"next_page_start": 201}), encoding="utf-8")
        assert load_next_page_start() == 201

    def test_save_and_reload(self, monkeypatch, tmp_path):
        state_file = tmp_path / "pagination_state.json"
        monkeypatch.setenv("OPENFOOD_PAGINATION_STATE_PATH", str(state_file))
        save_next_page_start(301)
        assert load_next_page_start() == 301
