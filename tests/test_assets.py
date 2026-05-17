"""Integration tests for the Dagster asset-based pipeline."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from dagster import build_asset_context

from helix_dagster.assets import (
    validated_rows as validated_rows_fn,
    pdv_cross_references as pdv_cross_references_fn,
)
from helix_dagster.coordinates import _COORD_TRANSFORMER


def test_validated_rows_pure():
    """Call validated_rows asset function directly with a sample DataFrame."""
    df = pd.DataFrame(
        [
            {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001"},
            {"Sample_IGSN": "INVALID", "PDV_FileName": "shot002"},
            {"Sample_IGSN": float("nan"), "PDV_FileName": "shot003"},
            {"Sample_IGSN": "XYZABC67890-sub1", "PDV_FileName": "shot004"},
        ]
    )

    ctx = build_asset_context()
    result = validated_rows_fn(context=ctx, raw_experiment_log=df)

    out_df = result["dataframe"]
    issues = result["igsn_issues"]

    # Row 0: valid IGSN
    assert out_df.loc[0, "valid_igsn"] == "ABCDEF12345"
    # Row 1: invalid format
    assert pd.isna(out_df.loc[1, "valid_igsn"])
    # Row 2: missing (NaN)
    assert pd.isna(out_df.loc[2, "valid_igsn"])
    # Row 3: valid with suffix
    assert out_df.loc[3, "valid_igsn"] == "XYZABC67890-sub1"

    # Should have 2 issues: invalid_format + missing
    assert len(issues) == 2
    issue_types = {i["issue"] for i in issues}
    assert "invalid_format" in issue_types
    assert "missing" in issue_types


def test_pdv_cross_references_pure():
    """Call pdv_cross_references asset function with mock inputs."""
    df = pd.DataFrame(
        [
            {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001", "valid_igsn": "ABCDEF12345"},
            {"Sample_IGSN": "ABCDEF12346", "PDV_FileName": "shot999", "valid_igsn": "ABCDEF12346"},
            {"Sample_IGSN": "ABCDEF12347", "PDV_FileName": float("nan"), "valid_igsn": "ABCDEF12347"},
        ]
    )

    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "a1", "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
        {"name": "shot002_ch1.tdms", "_id": "b1", "meta": {"igsn": "ABCDEF12346", "data_type": "pdv_trace"}},
    ]

    validated = {"dataframe": df, "igsn_issues": []}

    ctx = build_asset_context()
    result = pdv_cross_references_fn(
        context=ctx,
        validated_rows=validated,
        pdv_trace_inventory=pdv_items,
    )

    matches = result["matches"]
    issues = result["pdv_issues"]

    # Row 0 matched shot001
    assert 0 in matches
    assert matches[0]["_id"] == "a1"

    # Row 1 not found
    assert 1 not in matches

    # Row 2 skipped (NaN filename) — no match, no issue
    assert 2 not in matches

    # Only one issue: not_found for row 1
    assert len(issues) == 1
    assert issues[0]["type"] == "not_found"
    assert issues[0]["row"] == 1


def test_asset_dag_loads():
    """Verify the Dagster Definitions object loads with all assets and checks."""
    from helix_dagster import defs

    repo = defs.get_repository_def()
    asset_keys = {ak.to_user_string() for ak in repo.asset_graph.get_all_asset_keys()}

    expected_assets = {
        "experiment_log_source",
        "raw_experiment_log",
        "pdv_trace_inventory",
        "validated_rows",
        "pdv_cross_references",
        "enriched_pdv_metadata",
        "alpss_results_inventory",
        "quality_report",
        "processing_manifest",
    }
    for name in expected_assets:
        assert name in asset_keys, f"Missing asset: {name}"

    # Verify asset checks are registered
    check_keys = {
        str(ck) for ck in repo.asset_graph.asset_check_keys
    }
    assert len(check_keys) >= 5, f"Expected at least 5 asset checks, got {len(check_keys)}"


def test_helix_folder_sensor_run_config_validates():
    """The sensor's RunRequest must validate against process_helix_assets_job.

    Regression test for the bug where helix_folder_sensor configured
    only `raw_experiment_log` while three ops took ExperimentLogConfig.
    """
    from dagster import validate_run_config
    from helix_dagster import defs

    job = defs.resolve_job_def("process_helix_assets_job")

    # Reproduce the exact run_config shape the sensor emits.
    run_config = {
        "ops": {
            "experiment_log_source": {
                "config": {
                    "item_id": "fake_id_for_validation",
                    "filename": "anything.csv",
                }
            }
        }
    }
    # Will raise DagsterInvalidConfigError on shape mismatch.
    validate_run_config(job, run_config)


def test_igsn_mismatch_detection():
    """Verify that IGSN mismatches between spreadsheet and Girder item are flagged."""
    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001",
         "valid_igsn": "ABCDEF12345"},
    ])
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "a1",
         "meta": {"igsn": "XXXXXX99999", "data_type": "pdv_trace"}},
    ]
    validated = {"dataframe": df, "igsn_issues": []}
    ctx = build_asset_context()
    result = pdv_cross_references_fn(
        context=ctx,
        validated_rows=validated,
        pdv_trace_inventory=pdv_items,
    )
    issues = result["pdv_issues"]
    mismatch_issues = [i for i in issues if i["type"] == "igsn_mismatch"]
    assert len(mismatch_issues) == 1
    assert mismatch_issues[0]["spreadsheet_igsn"] == "ABCDEF12345"
    assert mismatch_issues[0]["item_igsn"] == "XXXXXX99999"


def test_quality_report_alpss_completeness():
    """Verify quality_report includes ALPSS completeness metrics."""
    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001",
         "valid_igsn": "ABCDEF12345"},
        {"Sample_IGSN": "ABCDEF12346", "PDV_FileName": "shot002",
         "valid_igsn": "ABCDEF12346"},
    ])
    validated = {"dataframe": df, "igsn_issues": []}
    pdv_xrefs = {
        "matches": {0: {"_id": "a1"}, 1: {"_id": "b1"}},
        "pdv_issues": [],
    }
    enriched = {"written_count": 2, "write_errors": [], "coord_failures": 0}
    alpss_items = [
        {"meta": {"igsn": "ABCDEF12345", "data_type": "pdv_alpss_result"}},
        # ABCDEF12346 has no ALPSS result
    ]

    ctx = build_asset_context()
    from helix_dagster.assets import quality_report as quality_report_fn
    report = quality_report_fn(
        context=ctx,
        validated_rows=validated,
        pdv_cross_references=pdv_xrefs,
        enriched_pdv_metadata=enriched,
        alpss_results_inventory=alpss_items,
    )
    assert report["alpss_completeness"]["igsns_with_alpss_results"] == 1
    assert report["alpss_completeness"]["igsns_without_alpss_results"] == 1
    assert "ABCDEF12346" in report["alpss_completeness"]["missing_igsns"]
    assert report["summary"]["alpss_coverage_pct"] == 50.0


def test_processing_manifest_clean():
    """Test manifest for a run with no issues."""
    from unittest.mock import MagicMock
    from helix_dagster.assets import processing_manifest as manifest_fn

    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001",
         "valid_igsn": "ABCDEF12345"},
    ])
    validated = {"dataframe": df, "igsn_issues": []}
    xrefs = {"matches": {0: {"_id": "a1"}}, "pdv_issues": []}
    enriched = {"written_count": 1, "write_errors": [], "coord_failures": 0}
    report = {"igsn_issues": [], "pdv_issues": [], "write_errors": [],
              "alpss_completeness": {"matched_igsns": 1, "igsns_with_alpss_results": 1,
                                     "igsns_without_alpss_results": 0, "missing_igsns": []},
              "summary": {"total_igsn_issues": 0, "total_pdv_issues": 0,
                          "total_write_errors": 0, "alpss_coverage_pct": 100.0}}

    source = {"item_id": "test_item_123", "filename": "test.csv"}
    mock_girder = MagicMock()

    ctx = build_asset_context()
    result = manifest_fn(
        context=ctx,
        experiment_log_source=source,
        quality_report=report,
        validated_rows=validated,
        pdv_cross_references=xrefs,
        enriched_pdv_metadata=enriched,
        girder=mock_girder,
    )

    assert result["status"] == "completed_clean"
    assert result["total_rows"] == 1
    assert result["rows_enriched"] == 1
    assert result["issues_summary"]["igsn_invalid"] == 0
    mock_girder.addMetadataToItem.assert_called_once()


def test_processing_manifest_with_warnings():
    """Test manifest for a run with issues."""
    from unittest.mock import MagicMock
    from helix_dagster.assets import processing_manifest as manifest_fn

    df = pd.DataFrame([
        {"Sample_IGSN": "INVALID", "PDV_FileName": "shot001",
         "valid_igsn": None},
    ])
    validated = {
        "dataframe": df,
        "igsn_issues": [{"issue": "invalid_format", "value": "INVALID", "row": 0}],
    }
    xrefs = {"matches": {}, "pdv_issues": [{"type": "not_found", "row": 0}]}
    enriched = {"written_count": 0, "write_errors": [], "coord_failures": 0}
    report = {"igsn_issues": validated["igsn_issues"],
              "pdv_issues": xrefs["pdv_issues"], "write_errors": [],
              "alpss_completeness": {"matched_igsns": 0, "igsns_with_alpss_results": 0,
                                     "igsns_without_alpss_results": 0, "missing_igsns": []},
              "summary": {"alpss_coverage_pct": 0.0}}

    source = {"item_id": "test_item_456", "filename": "test.csv"}
    mock_girder = MagicMock()

    ctx = build_asset_context()
    result = manifest_fn(
        context=ctx,
        experiment_log_source=source,
        quality_report=report,
        validated_rows=validated,
        pdv_cross_references=xrefs,
        enriched_pdv_metadata=enriched,
        girder=mock_girder,
    )

    assert result["status"] == "completed_with_warnings"
    assert result["issues_summary"]["igsn_invalid"] == 1
    assert result["issues_summary"]["pdv_not_found"] == 1


def test_enriched_pdv_metadata_writes_provenance():
    """Verify enriched_pdv_metadata writes coord_provenance alongside coords."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    from helix_dagster.assets import (
        enriched_pdv_metadata as enrich_fn,
    )

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "Sample_IGSN": "ABCDEF12345",
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    validated = {"dataframe": df, "igsn_issues": []}
    xrefs = {
        "matches": {
            0: {"_id": "pdvitem1", "name": "shot001_ch1.tdms",
                "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
        },
        "pdv_issues": [],
    }
    source = {"item_id": "src_spreadsheet_id", "filename": "test.csv"}
    mock_girder = MagicMock()

    ctx = build_asset_context()
    result = enrich_fn(
        context=ctx,
        experiment_log_source=source,
        pdv_cross_references=xrefs,
        validated_rows=validated,
        girder=mock_girder,
    )

    mock_girder.addMetadataToItem.assert_called_once()
    call_args = mock_girder.addMetadataToItem.call_args
    item_id_arg, payload = call_args[0]
    assert item_id_arg == "pdvitem1"
    assert "Station_X" in payload
    assert "Sample_X" in payload
    assert "coord_provenance" in payload
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "HELIX"
    assert prov["transform_version"] is not None
    assert prov["station_coord_source"]["kind"] == "helix_experiment_log"
    assert prov["station_coord_source"]["spreadsheet_item_id"] == "src_spreadsheet_id"
    assert prov["station_coord_source"]["spreadsheet_row_index"] == 0

    assert result["naive_timestamps_count"] == 0
    assert result["written_count"] == 1
    assert result["version_counter"]


def test_enriched_pdv_metadata_version_boundary_dispatch():
    """Two rows with identical station coords and different timestamps
    straddling HELIX v1/v2 must produce different Sample_X/Y values.
    """
    import pytest
    from datetime import datetime, timezone
    from unittest.mock import MagicMock
    from dagster import build_asset_context
    from helix_dagster.assets import (
        enriched_pdv_metadata as enriched_fn,
    )
    from helix_dagster.coordinates import _COORD_TRANSFORMER

    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")

    # Two rows: same station coords, different timestamps.
    # Pick an (x,y) where v1 and v2 predict clearly different outputs.
    # Station (8, 8):  v1 -> (32, 8);  v2 -> (8, 8).
    df = pd.DataFrame([
        {
            "Timestamp": "2025-06-01T12:00:00+00:00",  # pre-2026-04-01 -> v1
            "Sample_IGSN": "ABCDEF00001",
            "valid_igsn": "ABCDEF00001",
            "PDV_FileName": "shot_v1_ch1",
            "Flyer_Row": 1,
            "Flyer_Column": 1,
            "Flyer_X_Position_Final_mm": 8.0,
            "Flyer_Y_Position_Final_mm": 8.0,
        },
        {
            "Timestamp": "2026-05-01T12:00:00+00:00",  # post-boundary -> v2
            "Sample_IGSN": "ABCDEF00002",
            "valid_igsn": "ABCDEF00002",
            "PDV_FileName": "shot_v2_ch1",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 8.0,
            "Flyer_Y_Position_Final_mm": 8.0,
        },
    ])

    validated = {"dataframe": df, "igsn_issues": []}
    matches = {
        0: {"_id": "itemv1", "meta": {"igsn": "ABCDEF00001"}, "name": "shot_v1_ch1.csv"},
        1: {"_id": "itemv2", "meta": {"igsn": "ABCDEF00002"}, "name": "shot_v2_ch1.csv"},
    }
    xrefs = {"matches": matches, "pdv_issues": []}

    captured = []
    mock_girder = MagicMock()
    mock_girder.addMetadataToItem.side_effect = lambda item_id, meta: captured.append(
        (item_id, meta)
    )

    source = {"item_id": "src_sheet_item", "filename": "test.csv"}
    ctx = build_asset_context()
    result = enriched_fn(
        context=ctx,
        experiment_log_source=source,
        pdv_cross_references=xrefs,
        validated_rows=validated,
        girder=mock_girder,
    )

    assert result["written_count"] == 2
    assert len(captured) == 2

    by_id = {iid: m for iid, m in captured}
    v1_payload = by_id["itemv1"]
    v2_payload = by_id["itemv2"]

    # Provenance records the resolved version per row
    assert "v1" in v1_payload["coord_provenance"]["transform_version"]
    assert "v2" in v2_payload["coord_provenance"]["transform_version"]

    # Same inputs, different transforms → different Sample_X/Y
    assert (v1_payload["Sample_X"], v1_payload["Sample_Y"]) != (
        v2_payload["Sample_X"],
        v2_payload["Sample_Y"],
    )

    # Exact values per the YAML calibration points.
    # Station (8, 8): v1 -> (32, 8),  v2 -> (8, 8)
    assert v1_payload["Sample_X"] == pytest.approx(32.0, abs=1e-4)
    assert v1_payload["Sample_Y"] == pytest.approx(8.0, abs=1e-4)
    assert v2_payload["Sample_X"] == pytest.approx(8.0, abs=1e-4)
    assert v2_payload["Sample_Y"] == pytest.approx(8.0, abs=1e-4)

    # The asset's version_counter should reflect both versions
    assert result["version_counter"].get("HELIX/v1", 0) == 1
    assert result["version_counter"].get("HELIX/v2", 0) == 1
