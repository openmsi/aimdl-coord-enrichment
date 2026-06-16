"""Tests for the retargeted helix_spreadsheet asset checks.

Each check now reads a single asset's bundled output dict (pdv_log,
pdv_data, or pdv_processing_manifest).
"""

import pandas as pd
from dagster import build_asset_context

from aimdl_coord_enrichment.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    manifest_written,
    pdv_match_rate,
    zero_pdv_inventory,
)


def test_zero_pdv_inventory_fails_on_empty():
    ctx = build_asset_context()
    result = zero_pdv_inventory(ctx, pdv_data={"inventory_count": 0})
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_zero_pdv_inventory_passes_with_items():
    ctx = build_asset_context()
    result = zero_pdv_inventory(ctx, pdv_data={"inventory_count": 5})
    assert result.passed


def test_igsn_validity_rate_passes_above_threshold():
    ctx = build_asset_context()
    pdv_log = {"dataframe": pd.DataFrame({"valid_igsn": ["A", "B", "C", "D", None]})}
    result = igsn_validity_rate(ctx, pdv_log=pdv_log)
    assert result.passed  # 4/5 = 80%


def test_igsn_validity_rate_warns_below_threshold():
    ctx = build_asset_context()
    pdv_log = {"dataframe": pd.DataFrame({"valid_igsn": ["A", None, None, None, None]})}
    result = igsn_validity_rate(ctx, pdv_log=pdv_log)
    assert not result.passed  # 1/5 = 20%
    assert result.severity.value == "WARN"


def test_pdv_match_rate_passes():
    ctx = build_asset_context()
    pdv_data = {"rows_with_pdv": 2, "matched_count": 2}
    result = pdv_match_rate(ctx, pdv_data=pdv_data)
    assert result.passed  # 2/2 = 100%


def test_pdv_match_rate_warns():
    ctx = build_asset_context()
    pdv_data = {"rows_with_pdv": 4, "matched_count": 1}
    result = pdv_match_rate(ctx, pdv_data=pdv_data)
    assert not result.passed  # 1/4 = 25%


def test_igsn_consistency_passes():
    ctx = build_asset_context()
    result = igsn_consistency(ctx, pdv_data={"pdv_issues": []})
    assert result.passed


def test_igsn_consistency_errors_on_mismatch():
    ctx = build_asset_context()
    pdv_data = {
        "pdv_issues": [
            {"type": "igsn_mismatch", "spreadsheet_igsn": "A", "item_igsn": "B"},
        ],
    }
    result = igsn_consistency(ctx, pdv_data=pdv_data)
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_enrichment_success_rate_passes():
    ctx = build_asset_context()
    pdv_data = {"written_count": 9, "matched_count": 10, "write_errors": []}
    result = enrichment_success_rate(ctx, pdv_data=pdv_data)
    assert result.passed  # 9/10 = 90%


def test_enrichment_success_rate_warns():
    ctx = build_asset_context()
    pdv_data = {"written_count": 5, "matched_count": 10, "write_errors": [{}] * 5}
    result = enrichment_success_rate(ctx, pdv_data=pdv_data)
    assert not result.passed  # 5/10 = 50%


def test_coord_transform_check_all_ok():
    ctx = build_asset_context()
    pdv_data = {
        "coord_failures": 0,
        "version_counter": {"HELIX/v2": 3},
        "yaml_sha256": "abc" * 21 + "a",
        "written_count": 3,
    }
    result = coord_transform_check(ctx, pdv_data=pdv_data)
    assert result.passed is True


def test_coord_transform_check_failures():
    ctx = build_asset_context()
    pdv_data = {
        "coord_failures": 2,
        "version_counter": {"HELIX/v2": 1},
        "yaml_sha256": "abc" * 21 + "a",
        "written_count": 3,
    }
    result = coord_transform_check(ctx, pdv_data=pdv_data)
    assert result.passed is False
    assert "2" in result.description


def test_coord_transform_check_no_version_resolved():
    ctx = build_asset_context()
    pdv_data = {
        "coord_failures": 0,
        "version_counter": {},
        "yaml_sha256": "abc" * 21 + "a",
        "written_count": 3,
    }
    result = coord_transform_check(ctx, pdv_data=pdv_data)
    assert result.passed is False


def test_coord_transform_check_missing_sha():
    ctx = build_asset_context()
    pdv_data = {
        "coord_failures": 0,
        "version_counter": {"HELIX/v2": 3},
        "yaml_sha256": None,
        "written_count": 3,
    }
    result = coord_transform_check(ctx, pdv_data=pdv_data)
    assert result.passed is False
    assert "sha" in result.description.lower()


def test_manifest_written_passes():
    ctx = build_asset_context()
    manifest = {"manifest_written": True, "status": "completed_clean"}
    result = manifest_written(ctx, pdv_processing_manifest=manifest)
    assert result.passed
    assert result.severity.value == "ERROR"


def test_manifest_written_fails():
    ctx = build_asset_context()
    manifest = {"manifest_written": False, "status": "completed_clean"}
    result = manifest_written(ctx, pdv_processing_manifest=manifest)
    assert not result.passed
    assert result.severity.value == "ERROR"
