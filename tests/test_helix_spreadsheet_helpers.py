"""Unit tests for the pure helpers in aimdl_coord_enrichment.spreadsheet.

These replace the coverage the old test_validated_rows_pure /
test_pdv_cross_references_pure asset tests gave, now that the in-memory
stages are context-free functions rather than assets.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER
from aimdl_coord_enrichment.spreadsheet import (
    count_rows_with_pdv,
    match_pdv_rows,
    normalize_experiment_log,
    summarize_pdv_processing,
    validate_log_rows,
    write_pdv_metadata,
    write_processing_manifest,
)


def test_normalize_experiment_log_renames_columns():
    df = pd.DataFrame([
        {"Sample_ID": "ABCDEF12345", "Flyer_X_Position_Corrected (mm)": 10.5},
    ])
    out = normalize_experiment_log(df)
    assert "Sample_IGSN" in out.columns
    assert "Flyer_X_Position_Final_mm" in out.columns
    assert "Sample_ID" not in out.columns


def test_validate_log_rows():
    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345"},
        {"Sample_IGSN": "INVALID"},
        {"Sample_IGSN": float("nan")},
        {"Sample_IGSN": "XYZABC67890-sub1"},
    ])
    out_df, issues = validate_log_rows(df)

    assert out_df.loc[0, "valid_igsn"] == "ABCDEF12345"
    assert pd.isna(out_df.loc[1, "valid_igsn"])
    assert pd.isna(out_df.loc[2, "valid_igsn"])
    assert out_df.loc[3, "valid_igsn"] == "XYZABC67890-sub1"

    assert len(issues) == 2
    issue_types = {i["issue"] for i in issues}
    assert issue_types == {"invalid_format", "missing"}


def test_match_pdv_rows_match_and_not_found_and_nan():
    df = pd.DataFrame([
        {"PDV_FileName": "shot001", "valid_igsn": "ABCDEF12345"},
        {"PDV_FileName": "shot999", "valid_igsn": "ABCDEF12346"},
        {"PDV_FileName": float("nan"), "valid_igsn": "ABCDEF12347"},
    ])
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "a1",
         "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
        {"name": "shot002_ch1.tdms", "_id": "b1",
         "meta": {"igsn": "ABCDEF12346", "data_type": "pdv_trace"}},
    ]
    matches, issues = match_pdv_rows(df, pdv_items)

    assert matches[0]["_id"] == "a1"
    assert 1 not in matches
    assert 2 not in matches
    assert len(issues) == 1
    assert issues[0]["type"] == "not_found"
    assert issues[0]["row"] == 1


def test_match_pdv_rows_flags_igsn_mismatch():
    df = pd.DataFrame([
        {"PDV_FileName": "shot001", "valid_igsn": "ABCDEF12345"},
    ])
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "a1",
         "meta": {"igsn": "XXXXXX99999", "data_type": "pdv_trace"}},
    ]
    matches, issues = match_pdv_rows(df, pdv_items)
    assert 0 in matches
    mismatches = [i for i in issues if i["type"] == "igsn_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["spreadsheet_igsn"] == "ABCDEF12345"
    assert mismatches[0]["item_igsn"] == "XXXXXX99999"


def test_count_rows_with_pdv():
    df = pd.DataFrame([
        {"PDV_FileName": "shot001"},
        {"PDV_FileName": ""},
        {"PDV_FileName": float("nan")},
        {"PDV_FileName": "shot004"},
    ])
    assert count_rows_with_pdv(df) == 2


def test_summarize_pdv_processing_clean():
    df = pd.DataFrame([{"valid_igsn": "ABCDEF12345"}])
    pdv_log = {"dataframe": df, "igsn_issues": []}
    pdv_data = {
        "pdv_issues": [],
        "write_errors": [],
        "matched_count": 1,
        "written_count": 1,
        "coord_failures": 0,
    }
    summary = summarize_pdv_processing(pdv_log, pdv_data)
    assert summary["status"] == "completed_clean"
    assert summary["has_issues"] is False
    assert summary["total_rows"] == 1
    assert summary["rows_valid_igsn"] == 1
    assert summary["rows_enriched"] == 1
    assert all(v == 0 for v in summary["issues_summary"].values())


def test_summarize_pdv_processing_with_warnings():
    df = pd.DataFrame([{"valid_igsn": None}])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [{"issue": "invalid_format", "row": 0}],
    }
    pdv_data = {
        "pdv_issues": [{"type": "not_found", "row": 0}],
        "write_errors": [],
        "matched_count": 0,
        "written_count": 0,
        "coord_failures": 0,
    }
    summary = summarize_pdv_processing(pdv_log, pdv_data)
    assert summary["status"] == "completed_with_warnings"
    assert summary["issues_summary"]["igsn_invalid"] == 1
    assert summary["issues_summary"]["pdv_not_found"] == 1


def test_write_processing_manifest_success():
    girder = MagicMock()
    summary = {
        "status": "completed_clean",
        "issues_summary": {"igsn_invalid": 0},
        "total_rows": 1,
        "rows_valid_igsn": 1,
        "rows_matched_pdv": 1,
        "rows_enriched": 1,
    }
    manifest = write_processing_manifest(girder, "item123", summary, run_id="run1")
    assert manifest["status"] == "completed_clean"
    assert manifest["dagster_run_id"] == "run1"
    assert "write_failed" not in manifest
    girder.addMetadataToItem.assert_called_once()
    item_id_arg, payload = girder.addMetadataToItem.call_args[0]
    assert item_id_arg == "item123"
    assert "processing_status" in payload


def test_write_processing_manifest_write_failure():
    girder = MagicMock()
    girder.addMetadataToItem.side_effect = RuntimeError("boom")
    summary = {
        "status": "completed_clean",
        "issues_summary": {},
        "total_rows": 0,
        "rows_valid_igsn": 0,
        "rows_matched_pdv": 0,
        "rows_enriched": 0,
    }
    manifest = write_processing_manifest(girder, "item123", summary, run_id="run1")
    assert manifest["write_failed"] is True


def test_write_pdv_metadata_writes_coords_and_provenance():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    matches = {
        0: {"_id": "pdvitem1", "name": "shot001_ch1.tdms",
            "meta": {"igsn": "ABCDEF12345"}},
    }
    girder = MagicMock()
    summary = write_pdv_metadata(
        girder, df, matches,
        run_id="run1",
        source_item_id="src_sheet",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )

    assert summary["written_count"] == 1
    assert summary["write_errors"] == []
    assert summary["version_counter"]
    girder.addMetadataToItem.assert_called_once()
    item_id_arg, payload = girder.addMetadataToItem.call_args[0]
    assert item_id_arg == "pdvitem1"
    assert "Station_X" in payload
    assert "Sample_X" in payload
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "HELIX"
    assert prov["station_coord_source"]["spreadsheet_item_id"] == "src_sheet"
    assert prov["station_coord_source"]["spreadsheet_row_index"] == 0


def test_write_pdv_metadata_per_row_source_item_id():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
            "_source_item_id": "log_item_B",
        },
    ])
    matches = {0: {"_id": "pdvitem1", "name": "shot001_ch1.tdms", "meta": {}}}
    girder = MagicMock()
    write_pdv_metadata(
        girder, df, matches,
        run_id="run1",
        source_item_id="log_item_A_fallback",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )
    _, payload = girder.addMetadataToItem.call_args[0]
    assert payload["coord_provenance"]["station_coord_source"]["spreadsheet_item_id"] == "log_item_B"


def test_write_pdv_metadata_records_write_errors():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    matches = {0: {"_id": "pdvitem1", "name": "shot001_ch1.tdms", "meta": {}}}
    girder = MagicMock()
    girder.addMetadataToItem.side_effect = RuntimeError("girder down")
    summary = write_pdv_metadata(
        girder, df, matches,
        run_id="run1",
        source_item_id="src",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )
    assert summary["written_count"] == 0
    assert len(summary["write_errors"]) == 1
    assert summary["write_errors"][0]["row"] == 0
