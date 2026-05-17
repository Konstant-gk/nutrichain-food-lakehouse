"""Unit tests for src/openfood/silver_cleaning.py."""

from src.openfood.silver_cleaning import (
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
    assert clean_category_name("fr:pates-a-tartiner") == "Pates A Tartiner"
    assert clean_category_name("Вода") == "Unknown Category"
    assert clean_category_name("") == "Unknown Category"


def test_country_lookup_key():
    assert country_lookup_key("en:South Korea") == "south korea"
    assert country_lookup_key("  GR  ") == "gr"
    assert country_lookup_key("Alemania") == "alemania"
    assert country_lookup_key("de:en:germany") == "germany"


def test_country_alias_csv_covers_common_off_tokens():
    import csv
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / "databricks"
        / "silver"
        / "data"
        / "country_alias_lookup.csv"
    )
    aliases = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            aliases[row["alias"]] = (row["iso_code"], row["display_name"])

    for token in ("za", "gr", "en:south korea", "alemania", "deutschland", "en:ca"):
        assert token in aliases, f"missing alias: {token}"
        assert aliases[token][0] != "XX"


def test_clean_product_name():
    assert clean_product_name("  CHOCOLATE &amp; COOKIES  ") == "Chocolate & Cookies"
    assert clean_product_name(None) == "Unknown Product"
    assert clean_product_name("   ") == "Unknown Product"


def test_clean_off_tag_list():
    assert clean_off_tag_list("en:gluten,en:milk,en:soybeans") == "Gluten, Milk, Soybeans"
    assert clean_off_tag_list(None) == "Unknown"
    assert clean_off_tag_list("") == "Unknown"


def test_normalize_nutriscore_grade_reported():
    assert normalize_nutriscore_grade_reported("") == "Unknown"
    assert normalize_nutriscore_grade_reported(None) == "Unknown"
    assert normalize_nutriscore_grade_reported("not-applicable") == "Not Applicable"
    assert normalize_nutriscore_grade_reported("e") == "E"
