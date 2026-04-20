"""Tests for Dagster asset checks."""

import pandas as pd
from dagster import build_asset_context

from helix_dagster.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    pdv_match_rate,
    zero_inventory,
)


def test_zero_inventory_fails_on_empty():
    ctx = build_asset_context()
    result = zero_inventory(ctx, pdv_trace_inventory=[])
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_zero_inventory_passes_with_items():
    ctx = build_asset_context()
    result = zero_inventory(ctx, pdv_trace_inventory=[{"_id": "a"}])
    assert result.passed


def test_igsn_validity_rate_passes_above_threshold():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"valid_igsn": ["A", "B", "C", "D", None]}),
        "igsn_issues": [],
    }
    result = igsn_validity_rate(ctx, validated_rows=validated)
    assert result.passed  # 4/5 = 80%


def test_igsn_validity_rate_warns_below_threshold():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"valid_igsn": ["A", None, None, None, None]}),
        "igsn_issues": [],
    }
    result = igsn_validity_rate(ctx, validated_rows=validated)
    assert not result.passed  # 1/5 = 20%
    assert result.severity.value == "WARN"


def test_pdv_match_rate_passes():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"PDV_FileName": ["shot1", "shot2"]}),
        "igsn_issues": [],
    }
    xrefs = {"matches": {0: {}, 1: {}}, "pdv_issues": []}
    result = pdv_match_rate(ctx, pdv_cross_references=xrefs, validated_rows=validated)
    assert result.passed  # 2/2 = 100%


def test_pdv_match_rate_warns():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"PDV_FileName": ["shot1", "shot2", "shot3", "shot4"]}),
        "igsn_issues": [],
    }
    xrefs = {"matches": {0: {}}, "pdv_issues": []}
    result = pdv_match_rate(ctx, pdv_cross_references=xrefs, validated_rows=validated)
    assert not result.passed  # 1/4 = 25%


def test_igsn_consistency_passes():
    ctx = build_asset_context()
    xrefs = {"matches": {}, "pdv_issues": []}
    result = igsn_consistency(ctx, pdv_cross_references=xrefs)
    assert result.passed


def test_igsn_consistency_errors_on_mismatch():
    ctx = build_asset_context()
    xrefs = {
        "matches": {},
        "pdv_issues": [
            {"type": "igsn_mismatch", "spreadsheet_igsn": "A", "item_igsn": "B"},
        ],
    }
    result = igsn_consistency(ctx, pdv_cross_references=xrefs)
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_enrichment_success_rate_passes():
    ctx = build_asset_context()
    enriched = {"written_count": 9, "write_errors": [], "coord_failures": 0}
    xrefs = {"matches": {i: {} for i in range(10)}, "pdv_issues": []}
    result = enrichment_success_rate(
        ctx, enriched_pdv_metadata=enriched, pdv_cross_references=xrefs
    )
    assert result.passed  # 9/10 = 90%


def test_enrichment_success_rate_warns():
    ctx = build_asset_context()
    enriched = {"written_count": 5, "write_errors": [{}] * 5, "coord_failures": 0}
    xrefs = {"matches": {i: {} for i in range(10)}, "pdv_issues": []}
    result = enrichment_success_rate(
        ctx, enriched_pdv_metadata=enriched, pdv_cross_references=xrefs
    )
    assert not result.passed  # 5/10 = 50%


def test_coord_transform_check_all_ok():
    ctx = build_asset_context()
    enriched = {
        "coord_failures": 0,
        "version_counter": {"HELIX/v2": 3},
        "yaml_sha256": "abc" * 21 + "a",
        "written_count": 3,
    }
    result = coord_transform_check(ctx, enriched_pdv_metadata=enriched)
    assert result.passed is True


def test_coord_transform_check_failures():
    ctx = build_asset_context()
    enriched = {
        "coord_failures": 2,
        "version_counter": {"HELIX/v2": 1},
        "yaml_sha256": "abc" * 21 + "a",
        "written_count": 3,
    }
    result = coord_transform_check(ctx, enriched_pdv_metadata=enriched)
    assert result.passed is False
    assert "2" in result.description


def test_coord_transform_check_no_version_resolved():
    ctx = build_asset_context()
    enriched = {
        "coord_failures": 0,
        "version_counter": {},
        "yaml_sha256": "abc" * 21 + "a",
        "written_count": 3,
    }
    result = coord_transform_check(ctx, enriched_pdv_metadata=enriched)
    assert result.passed is False


def test_coord_transform_check_missing_sha():
    ctx = build_asset_context()
    enriched = {
        "coord_failures": 0,
        "version_counter": {"HELIX/v2": 3},
        "yaml_sha256": None,
        "written_count": 3,
    }
    result = coord_transform_check(ctx, enriched_pdv_metadata=enriched)
    assert result.passed is False
    assert "sha" in result.description.lower()
