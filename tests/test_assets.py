"""Integration tests for the 3-asset partitioned helix_spreadsheet flow."""

import io
from unittest.mock import MagicMock

import pandas as pd
import pytest
from dagster import build_asset_context

from aimdl_coord_enrichment.assets import (
    HelixSpreadsheetConfig,
    pdv_data as pdv_data_fn,
    pdv_log as pdv_log_fn,
    pdv_processing_manifest as pdv_processing_manifest_fn,
)
from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER

LIVE = HelixSpreadsheetConfig(dry_run=False)
DRY = HelixSpreadsheetConfig(dry_run=True)

PARTITION_KEY = "ABCDEF12345//2026-04-16"


def _raw_log_csv():
    """A small experiment-log CSV using raw (pre-COLUMN_MAP) column names."""
    df = pd.DataFrame([
        {
            "Timestamp": "2026-04-16T17:00:00+00:00",
            "Sample_ID": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Corrected (mm)": 10.5,
            "Flyer_Y_Position_Corrected (mm)": 20.3,
        },
        {
            "Timestamp": "2026-04-16T17:05:00+00:00",
            "Sample_ID": "INVALID",
            "PDV_FileName": "shot999",
            "Flyer_Row": 1,
            "Flyer_Column": 3,
            "Flyer_X_Position_Corrected (mm)": 11.0,
            "Flyer_Y_Position_Corrected (mm)": 21.0,
        },
    ])
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def _mock_girder(log_items, pdv_items):
    """MagicMock girder dispatching .get on path + .downloadFile + .addMetadataToItem."""
    client = MagicMock()
    csv_bytes = _raw_log_csv()

    def fake_get(path, parameters=None):
        if path == "aimdl/partition/details":
            return log_items
        if path == "aimdl/datafiles":
            return pdv_items
        if path.startswith("item/") and path.endswith("/files"):
            item_id = path.split("/")[1]
            return [{"_id": f"file-{item_id}"}]
        raise AssertionError(f"unexpected client.get: {path} {parameters}")

    def fake_download(file_id, buf):
        buf.write(csv_bytes)

    client.get.side_effect = fake_get
    client.downloadFile.side_effect = fake_download
    return client


def test_asset_dag_loads():
    """Verify the Dagster Definitions object loads with the 3 assets and checks."""
    from aimdl_coord_enrichment import defs

    repo = defs.get_repository_def()
    asset_keys = {ak.to_user_string() for ak in repo.asset_graph.get_all_asset_keys()}

    expected_assets = {"pdv_log", "pdv_data", "pdv_processing_manifest"}
    for name in expected_assets:
        assert name in asset_keys, f"Missing asset: {name}"

    # Old assets must be gone.
    for gone in ("experiment_log_source", "raw_experiment_log", "validated_rows",
                 "pdv_cross_references", "enriched_pdv_metadata",
                 "alpss_results_inventory", "quality_report", "processing_manifest"):
        assert gone not in asset_keys, f"Old asset still present: {gone}"

    check_keys = {str(ck) for ck in repo.asset_graph.asset_check_keys}
    assert len(check_keys) >= 5, f"Expected at least 5 asset checks, got {len(check_keys)}"


def test_pdv_log_reads_partition():
    """pdv_log fetches log items for its partition, normalizes, validates IGSNs."""
    log_items = [{"_id": "logitem1", "name": "log.csv"}]
    girder = _mock_girder(log_items, pdv_items=[])

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_log_fn(context=ctx, girder=girder)

    assert result["partition_key"] == PARTITION_KEY
    assert result["source_item_ids"] == ["logitem1"]
    df = result["dataframe"]
    # COLUMN_MAP applied: Sample_ID -> Sample_IGSN, Corrected -> Final_mm
    assert "Sample_IGSN" in df.columns
    assert "Flyer_X_Position_Final_mm" in df.columns
    assert "valid_igsn" in df.columns
    # Row 0 valid, row 1 invalid
    assert df.loc[0, "valid_igsn"] == "ABCDEF12345"
    assert pd.isna(df.loc[1, "valid_igsn"])
    assert df.loc[0, "_source_item_id"] == "logitem1"
    # One invalid_format IGSN issue
    assert len(result["igsn_issues"]) == 1


def test_pdv_data_matches_and_writes():
    """pdv_data fetches inventory, matches rows, writes coord metadata."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": "2026-04-16T17:00:00+00:00",
            "Sample_IGSN": "ABCDEF12345",
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
            "_source_item_id": "logitem1",
        },
    ])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [],
        "source_item_ids": ["logitem1"],
        "partition_key": PARTITION_KEY,
    }
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "pdvitem1",
         "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
    ]
    girder = _mock_girder(log_items=[], pdv_items=pdv_items)

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_data_fn(context=ctx, config=LIVE, pdv_log=pdv_log, girder=girder)

    assert result["inventory_count"] == 1
    assert result["matched_count"] == 1
    assert result["rows_with_pdv"] == 1
    assert result["written_count"] == 1
    assert result["pdv_issues"] == []
    assert result["version_counter"]
    girder.addMetadataToItem.assert_called_once()
    item_id_arg, payload = girder.addMetadataToItem.call_args[0]
    assert item_id_arg == "pdvitem1"
    assert "Sample_X" in payload
    assert payload["coord_provenance"]["station_coord_source"]["spreadsheet_item_id"] == "logitem1"


def test_pdv_processing_manifest_writes_status():
    """pdv_processing_manifest summarizes and writes meta.processing_status."""
    df = pd.DataFrame([{"valid_igsn": "ABCDEF12345"}])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [],
        "source_item_ids": ["logitem1"],
        "partition_key": PARTITION_KEY,
    }
    pdv_data = {
        "pdv_issues": [],
        "write_errors": [],
        "matched_count": 1,
        "written_count": 1,
        "coord_failures": 0,
    }
    girder = MagicMock()

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_processing_manifest_fn(
        context=ctx, config=LIVE, pdv_log=pdv_log, pdv_data=pdv_data, girder=girder
    )

    assert result["status"] == "completed_clean"
    assert result["manifest_written"] is True
    girder.addMetadataToItem.assert_called_once()
    item_id_arg, payload = girder.addMetadataToItem.call_args[0]
    assert item_id_arg == "logitem1"
    assert "processing_status" in payload


def test_pdv_processing_manifest_flags_write_failure():
    """manifest_written is False when a source-item write raises."""
    df = pd.DataFrame([{"valid_igsn": "ABCDEF12345"}])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [],
        "source_item_ids": ["logitem1"],
        "partition_key": PARTITION_KEY,
    }
    pdv_data = {
        "pdv_issues": [],
        "write_errors": [],
        "matched_count": 1,
        "written_count": 1,
        "coord_failures": 0,
    }
    girder = MagicMock()
    girder.addMetadataToItem.side_effect = RuntimeError("girder down")

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_processing_manifest_fn(
        context=ctx, config=LIVE, pdv_log=pdv_log, pdv_data=pdv_data, girder=girder
    )
    assert result["manifest_written"] is False


def test_pdv_data_version_boundary_dispatch():
    """Two rows with identical station coords and timestamps straddling the
    HELIX v1/v2 boundary must produce different Sample_X/Y values.
    """
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")

    # Station (8, 8): v1 -> (32, 8); v2 -> (8, 8).
    df = pd.DataFrame([
        {
            "Timestamp": "2025-06-01T12:00:00+00:00",  # pre-2026-04-01 -> v1
            "Sample_IGSN": "ABCDEF00001",
            "valid_igsn": "ABCDEF00001",
            "PDV_FileName": "shot_v1_ch1",
            "Flyer_X_Position_Final_mm": 8.0,
            "Flyer_Y_Position_Final_mm": 8.0,
            "_source_item_id": "logitem1",
        },
        {
            "Timestamp": "2026-05-01T12:00:00+00:00",  # post-boundary -> v2
            "Sample_IGSN": "ABCDEF00002",
            "valid_igsn": "ABCDEF00002",
            "PDV_FileName": "shot_v2_ch1",
            "Flyer_X_Position_Final_mm": 8.0,
            "Flyer_Y_Position_Final_mm": 8.0,
            "_source_item_id": "logitem1",
        },
    ])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [],
        "source_item_ids": ["logitem1"],
        "partition_key": PARTITION_KEY,
    }
    pdv_items = [
        {"_id": "itemv1", "meta": {"igsn": "ABCDEF00001"}, "name": "shot_v1_ch1.csv"},
        {"_id": "itemv2", "meta": {"igsn": "ABCDEF00002"}, "name": "shot_v2_ch1.csv"},
    ]
    captured = []
    girder = _mock_girder(log_items=[], pdv_items=pdv_items)
    girder.addMetadataToItem.side_effect = lambda item_id, meta: captured.append(
        (item_id, meta)
    )

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_data_fn(context=ctx, config=LIVE, pdv_log=pdv_log, girder=girder)

    assert result["written_count"] == 2
    by_id = {iid: m for iid, m in captured}
    assert "v1" in by_id["itemv1"]["coord_provenance"]["transform_version"]
    assert "v2" in by_id["itemv2"]["coord_provenance"]["transform_version"]
    assert by_id["itemv1"]["Sample_X"] == pytest.approx(32.0, abs=1e-4)
    assert by_id["itemv2"]["Sample_X"] == pytest.approx(8.0, abs=1e-4)
    assert result["version_counter"].get("HELIX/v1", 0) == 1
    assert result["version_counter"].get("HELIX/v2", 0) == 1


def test_pdv_data_dry_run_skips_writes():
    """With dry_run=True, pdv_data computes but performs no Girder writes."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": "2026-04-16T17:00:00+00:00",
            "Sample_IGSN": "ABCDEF12345",
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
            "_source_item_id": "logitem1",
        },
    ])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [],
        "source_item_ids": ["logitem1"],
        "partition_key": PARTITION_KEY,
    }
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "pdvitem1",
         "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
    ]
    girder = _mock_girder(log_items=[], pdv_items=pdv_items)

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_data_fn(context=ctx, config=DRY, pdv_log=pdv_log, girder=girder)

    girder.addMetadataToItem.assert_not_called()
    assert result["dry_run"] is True
    assert result["matched_count"] == 1
    assert result["written_count"] == 0
    assert result["simulated_count"] == 1
    # Transform still computed even though nothing was written.
    assert result["version_counter"]


def test_pdv_processing_manifest_dry_run_skips_write():
    """With dry_run=True, the manifest is computed but not written; the
    representative manifest_written flag is still True."""
    df = pd.DataFrame([{"valid_igsn": "ABCDEF12345"}])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [],
        "source_item_ids": ["logitem1"],
        "partition_key": PARTITION_KEY,
    }
    pdv_data = {
        "pdv_issues": [],
        "write_errors": [],
        "matched_count": 1,
        "written_count": 0,
        "simulated_count": 1,
        "coord_failures": 0,
    }
    girder = MagicMock()

    ctx = build_asset_context(partition_key=PARTITION_KEY)
    result = pdv_processing_manifest_fn(
        context=ctx, config=DRY, pdv_log=pdv_log, pdv_data=pdv_data, girder=girder
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["dry_run"] is True
    assert result["manifest_written"] is True
    assert result["status"] == "completed_clean"
