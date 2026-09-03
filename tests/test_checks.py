"""Tests for the helix_spreadsheet asset-check decision helpers.

The checks read each partition's latest materialization metadata from the
event log (see checks.py / check_support.py) and delegate the verdict to
these pure helpers, which take a flat metadata dict. Testing the helpers
directly keeps the data logic covered without standing up an instance.
"""

from aimdl_coord_enrichment.checks import (
    eval_coord_transform,
    eval_enrichment_success_rate,
    eval_igsn_consistency,
    eval_igsn_validity,
    eval_manifest_written,
    eval_pdv_match_rate,
    eval_zero_traces,
)


def test_zero_traces_fails_on_empty_partition():
    result = eval_zero_traces({"traces_in_partition": 0})
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_zero_traces_passes_with_traces():
    result = eval_zero_traces({"traces_in_partition": 5})
    assert result.passed


def test_igsn_validity_rate_passes_above_threshold():
    result = eval_igsn_validity({"row_count": 5, "valid_igsn_count": 4})
    assert result.passed  # 4/5 = 80%


def test_igsn_validity_rate_warns_below_threshold():
    result = eval_igsn_validity({"row_count": 5, "valid_igsn_count": 1})
    assert not result.passed  # 1/5 = 20%
    assert result.severity.value == "WARN"


def test_igsn_validity_rate_empty_passes():
    result = eval_igsn_validity({"row_count": 0, "valid_igsn_count": 0})
    assert result.passed


def test_pdv_match_rate_passes():
    result = eval_pdv_match_rate(
        {"traces_in_partition": 2, "paired_count": 2, "log_items": 1}
    )
    assert result.passed  # 2/2 = 100%


def test_pdv_match_rate_warns():
    result = eval_pdv_match_rate(
        {"traces_in_partition": 4, "paired_count": 1, "log_items": 1}
    )
    assert not result.passed  # 1/4 = 25%


def test_pdv_match_rate_passes_when_no_log_is_tagged():
    """A partition whose log is not tagged upstream has no rows to pair
    against. Nothing this pipeline can do and nothing to act on, so it passes;
    log_items records the condition without turning the run red."""
    result = eval_pdv_match_rate(
        {"traces_in_partition": 12, "paired_count": 0, "log_items": 0}
    )
    assert result.passed
    assert result.metadata["log_items"].value == 0


def test_igsn_consistency_passes():
    result = eval_igsn_consistency({"igsn_mismatch_count": 0})
    assert result.passed


def test_igsn_consistency_errors_on_mismatch():
    result = eval_igsn_consistency({"igsn_mismatch_count": 1})
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_enrichment_success_rate_passes():
    result = eval_enrichment_success_rate(
        {"items_enriched": 9, "items_simulated": 0, "paired_count": 10}
    )
    assert result.passed  # 9/10 = 90%


def test_enrichment_success_rate_warns():
    result = eval_enrichment_success_rate(
        {"items_enriched": 5, "items_simulated": 0, "paired_count": 10,
         "write_errors_count": 5}
    )
    assert not result.passed  # 5/10 = 50%


def test_enrichment_success_rate_counts_dry_run_simulated():
    """A dry run (enriched=0, simulated=matched) reads as a representative pass."""
    result = eval_enrichment_success_rate(
        {"items_enriched": 0, "items_simulated": 10, "paired_count": 10}
    )
    assert result.passed  # (0+10)/10 = 100%


def test_coord_transform_check_all_ok():
    result = eval_coord_transform({
        "coordinate_transform_failures": 0,
        "transform_version_count": 1,
        "yaml_sha256_present": True,
        "items_enriched": 3,
        "items_simulated": 0,
    })
    assert result.passed is True


def test_coord_transform_check_failures():
    result = eval_coord_transform({
        "coordinate_transform_failures": 2,
        "transform_version_count": 1,
        "yaml_sha256_present": True,
        "items_enriched": 3,
        "items_simulated": 0,
    })
    assert result.passed is False
    assert "2" in result.description


def test_coord_transform_check_no_version_resolved():
    result = eval_coord_transform({
        "coordinate_transform_failures": 0,
        "transform_version_count": 0,
        "yaml_sha256_present": True,
        "items_enriched": 3,
        "items_simulated": 0,
    })
    assert result.passed is False


def test_coord_transform_check_no_version_ok_when_nothing_attempted():
    """Empty partition (no writes attempted) shouldn't fail on missing versions."""
    result = eval_coord_transform({
        "coordinate_transform_failures": 0,
        "transform_version_count": 0,
        "yaml_sha256_present": True,
        "items_enriched": 0,
        "items_simulated": 0,
    })
    assert result.passed is True


def test_coord_transform_check_missing_sha():
    result = eval_coord_transform({
        "coordinate_transform_failures": 0,
        "transform_version_count": 1,
        "yaml_sha256_present": False,
        "items_enriched": 3,
        "items_simulated": 0,
    })
    assert result.passed is False
    assert "sha" in result.description.lower()


def test_manifest_written_passes():
    result = eval_manifest_written({"manifest_written": True, "status": "completed_clean"})
    assert result.passed
    assert result.severity.value == "ERROR"


def test_manifest_written_fails():
    result = eval_manifest_written({"manifest_written": False, "status": "completed_clean"})
    assert not result.passed
    assert result.severity.value == "ERROR"
