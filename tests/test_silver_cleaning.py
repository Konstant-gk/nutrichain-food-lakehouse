"""Unit tests for databricks/silver/silver_cleaning.py (no Spark required)."""

import csv
import sys
from pathlib import Path

_SILVER_DIR = Path(__file__).resolve().parents[1] / "databricks" / "silver"
sys.path.insert(0, str(_SILVER_DIR))

from silver_cleaning import (  # noqa: E402
    clean_category_name,
    clean_off_tag_list,
    clean_product_name,
    country_lookup_key,
    normalize_nutriscore_grade_reported,
    strip_off_lang_prefix,
)


def test_strip_off_lang_prefix_double():
    assert strip_off_lang_prefix("de:en:germany") == "germany"


def test_clean_category_name():
    assert clean_category_name("en:tartare-sauces") == "Tartare Sauces"
    assert clean_category_name("Вода") == "Unknown Category"


def test_country_lookup_key():
    assert country_lookup_key("en:South Korea") == "south korea"
    assert country_lookup_key("za") == "za"


def test_clean_product_name():
    assert clean_product_name("  CHOCOLATE &amp; COOKIES  ") == "Chocolate & Cookies"
    assert clean_product_name(None) == "Unknown Product"


def test_country_alias_csv_exists():
    path = _SILVER_DIR / "data" / "country_alias_lookup.csv"
    assert path.is_file()
    aliases = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            aliases[row["alias"]] = row["iso_code"]
    assert "za" in aliases
    assert aliases["za"] == "ZA"
