"""Tests for the instruments subpackage registry and dispatch helpers."""

from helix_dagster.instruments import (
    EXTERNAL_LEAF_DATA_TYPES,
    HELIX_DERIVED_DATA_TYPES,
    HELIX_LEAF_DATA_TYPES,
    MAXIMA_DERIVED_DATA_TYPES,
    MAXIMA_LEAF_DATA_TYPES,
    OUT_OF_SCOPE_DATA_TYPES,
    all_in_scope_data_types,
    instrument_for_data_type,
    is_in_scope,
    role_for_data_type,
)


def test_instrument_helix_and_maxima_recognized():
    assert instrument_for_data_type("pdv_alpss_output") == "HELIX"
    assert instrument_for_data_type("xrd_raw") == "MAXIMA"


def test_role_for_helix_alpss_output_is_derived():
    assert role_for_data_type("pdv_alpss_output") == "derived"


def test_role_for_maxima_xrd_raw_is_leaf():
    assert role_for_data_type("xrd_raw") == "leaf"


def test_unknown_data_type_returns_none():
    assert instrument_for_data_type("not_a_thing") is None
    assert role_for_data_type("not_a_thing") is None
    assert is_in_scope("not_a_thing") is False


def test_out_of_scope_data_type_returns_none():
    assert "xrd_metadata" in OUT_OF_SCOPE_DATA_TYPES
    assert instrument_for_data_type("xrd_metadata") is None
    assert role_for_data_type("xrd_metadata") is None
    assert is_in_scope("xrd_metadata") is False


def test_all_in_scope_data_types_non_empty():
    result = all_in_scope_data_types()
    assert len(result) >= 5
    assert "xrd_raw" in result
    assert "pdv_alpss_output" in result
    assert "xrd_derived" in result


def test_pdv_trace_is_not_in_scope_for_new_dag():
    assert is_in_scope("pdv_trace") is False
    assert "pdv_trace" in EXTERNAL_LEAF_DATA_TYPES


def test_data_type_has_only_one_owner():
    all_sets = [
        HELIX_LEAF_DATA_TYPES,
        HELIX_DERIVED_DATA_TYPES,
        MAXIMA_LEAF_DATA_TYPES,
        MAXIMA_DERIVED_DATA_TYPES,
    ]
    for dt in all_in_scope_data_types():
        containing = [s for s in all_sets if dt in s]
        assert len(containing) == 1, f"{dt} appears in {len(containing)} sets"
