"""Shared pytest fixtures for offline ingest tests."""

import os

import pytest

# Small page counts and zero delay; mirrors env.example.txt keys, not production values.

_TEST_OPENFOOD_ENV = {
    "OPENFOOD_MAX_PAGES": "10",
    "OPENFOOD_RECORDS_PER_PAGE": "10",
    "OPENFOOD_POLITE_DELAY_SECONDS": "0.01",
    "OPENFOOD_PAGE_MAX_RETRIES": "5",
    "OPENFOOD_USER_AGENT": "NutriChainTest/1.0 (pytest)",
}


@pytest.fixture(autouse=True)
def _openfood_env(monkeypatch):
    for key, value in _TEST_OPENFOOD_ENV.items():
        monkeypatch.setenv(key, value)
