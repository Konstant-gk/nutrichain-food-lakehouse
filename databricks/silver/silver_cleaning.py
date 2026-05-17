"""
silver_cleaning.py
------------------
Text and nutrition cleansing helpers for silver_transform.py (PySpark).

Lives under databricks/silver/ so Databricks jobs do not depend on src/openfood/.
"""

from __future__ import annotations

import html
import re
from functools import reduce
from operator import add
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import Column

try:
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover
    F = None  # type: ignore[assignment]

COMPLETENESS_FIELD_NAMES = (
    "product_name",
    "ingredients_text",
    "packaging",
    "allergens",
    "nova_group",
    "nutriscore_score_raw",
    "energy_kcal_per_100g",
    "proteins_100g",
    "fat_100g",
    "sugars_100g",
)
COMPLETENESS_COMPLETE_MIN = 3

_OFF_LANG_PREFIX = re.compile(r"^([a-z]{2}:)+", re.IGNORECASE)
_NON_ASCII = re.compile(r"[^\x00-\x7F]")

_PACKAGING_ALIASES = {
    "bolsa": "Bag",
    "plastique": "Plastic",
    "plástico": "Plastic",
    "plastico": "Plastic",
    "pp-tub": "Plastic Tub",
    "pp tub": "Plastic Tub",
    "pet-pot": "Pet Pot",
    "pet-bottle": "Pet Bottle",
    "aluminium-tray": "Aluminium Tray",
    "paper-sleeve": "Paper Sleeve",
}


def strip_off_lang_prefix(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.strip()
    while True:
        match = _OFF_LANG_PREFIX.match(text)
        if not match:
            break
        text = text[match.end() :].strip()
    return text


def clean_category_name(value: Optional[str]) -> str:
    text = strip_off_lang_prefix(value)
    if not text:
        return "Unknown Category"
    text = text.replace("-", " ").strip()
    if _NON_ASCII.search(text):
        return "Unknown Category"
    return text.title()


def country_lookup_key(value: Optional[str]) -> str:
    text = strip_off_lang_prefix(value).strip().lower()
    text = text.replace("-", " ")
    return " ".join(text.split())


def clean_product_name(value: Optional[str]) -> str:
    if not value or not str(value).strip():
        return "Unknown Product"
    text = html.unescape(str(value).strip())
    return text.title()


def clean_off_tag_list(value: Optional[str], unknown_label: str = "Unknown") -> str:
    if not value or not str(value).strip():
        return unknown_label
    parts = [strip_off_lang_prefix(p).replace("-", " ") for p in str(value).split(",")]
    cleaned: list[str] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        key = part.lower()
        if key in _PACKAGING_ALIASES:
            cleaned.append(_PACKAGING_ALIASES[key])
        else:
            cleaned.append(part.title())
    if not cleaned:
        return unknown_label
    return ", ".join(cleaned)


def normalize_nutriscore_grade_reported(value: Optional[str]) -> str:
    if not value or not str(value).strip():
        return "Unknown"
    text = str(value).strip().lower().replace("_", " ").replace("-", " ")
    if text in {"not applicable", "na", "n a"}:
        return "Not Applicable"
    if text in {"a", "b", "c", "d", "e"}:
        return text.upper()
    if text == "unknown":
        return "Unknown"
    return str(value).strip().title()


def reported_grade_for_mismatch(value: "Column") -> "Column":
    return (
        F.when(value.isin("Unknown", "Not Applicable"), F.lit(None))
        .when(value.isin("A", "B", "C", "D", "E"), F.lower(value))
        .otherwise(F.lower(F.trim(value)))
    )


def spark_strip_off_prefix(col: "Column") -> "Column":
    out = F.trim(col)
    for _ in range(5):
        out = F.regexp_replace(out, r"(?i)^[a-z]{2}:", "")
        out = F.trim(out)
    return out


def spark_clean_category(col: "Column") -> "Column":
    stripped = spark_strip_off_prefix(col)
    spaced = F.regexp_replace(stripped, "-", " ")
    titled = F.initcap(F.trim(spaced))
    empty = col.isNull() | (F.length(F.trim(col)) == 0)
    non_latin = F.length(F.regexp_replace(F.trim(spaced), r"[\x00-\x7F]", "")) > 0
    return (
        F.when(empty, F.lit("Unknown Category"))
        .when(non_latin, F.lit("Unknown Category"))
        .otherwise(titled)
    )


def spark_clean_product_name(col: "Column") -> "Column":
    trimmed = F.trim(col)
    unescaped = F.regexp_replace(trimmed, "&amp;", "&")
    titled = F.initcap(unescaped)
    return (
        F.when(trimmed.isNull() | (F.length(trimmed) == 0), F.lit("Unknown Product"))
        .otherwise(titled)
    )


def spark_clean_off_tag_list(col: "Column", unknown_label: str = "Unknown") -> "Column":
    stripped = F.regexp_replace(col, r"(?i)(^|,\s*)[a-z]{2}:", "$1")
    parts = F.split(stripped, ",")
    joined = F.array_join(
        F.transform(
            parts,
            lambda p: F.initcap(F.regexp_replace(F.trim(p), "-", " ")),
        ),
        ", ",
    )
    return (
        F.when(col.isNull() | (F.length(F.trim(col)) == 0), F.lit(unknown_label))
        .otherwise(joined)
    )


def spark_normalize_nutriscore_reported(col: "Column") -> "Column":
    trimmed = F.lower(F.trim(col))
    return (
        F.when(col.isNull() | (F.length(F.trim(col)) == 0), F.lit("Unknown"))
        .when(
            trimmed.isin("not applicable", "not-applicable", "na", "n/a"),
            F.lit("Not Applicable"),
        )
        .when(trimmed.isin("a", "b", "c", "d", "e"), F.upper(trimmed))
        .when(trimmed == "unknown", F.lit("Unknown"))
        .otherwise(F.initcap(F.trim(col)))
    )


def spark_round_nutrient(col: "Column") -> "Column":
    return F.when(col < 0, F.lit(None).cast("double")).otherwise(F.round(col, 2))


def spark_completeness_score(cols: list["Column"]) -> "Column":
    filled = [
        F.when(c.isNotNull() & (F.length(F.trim(c.cast("string"))) > 0), 1).otherwise(0)
        for c in cols
    ]
    return reduce(add, filled)


def spark_data_quality_tier(score: "Column") -> "Column":
    return F.when(score >= COMPLETENESS_COMPLETE_MIN, F.lit("complete")).otherwise(
        F.lit("sparse")
    )
