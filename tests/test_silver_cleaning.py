"""Unit tests for silver completeness rules (no Spark required)."""

from databricks.silver.silver_cleaning import (
    COMPLETENESS_COMPLETE_MIN,
    COMPLETENESS_FIELD_NAMES,
    MAX_KCAL_PER_100G,
    MAX_MACRO_NUTRIENT_PER_100G,
)


def test_completeness_field_count():
    assert len(COMPLETENESS_FIELD_NAMES) == 10


def test_sparse_when_five_or_more_fields_missing():
    # 5+ missing → score <= 4 → sparse; complete needs score >= 6.
    assert COMPLETENESS_COMPLETE_MIN == 6


def test_per_100g_plausibility_caps():
    assert MAX_MACRO_NUTRIENT_PER_100G == 100.0
    assert MAX_KCAL_PER_100G == 900.0
