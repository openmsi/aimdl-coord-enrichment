"""End-to-end integration test for the coord_enrichment DAG.

Runs the full asset graph against a mock Girder pre-loaded with the
JHAMAL00018-009 fixture (25 scan points -> 25 xrd_raw + 25 xrf_raw
items plus instructions.txt). Asserts 50 enrichment writes and a
manifest write.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from dagster import build_asset_context

from helix_dagster.coordinates import _COORD_TRANSFORMER
from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import (
    CoordTransformSnapshot,
    coord_transform_config_snapshot,
)
from helix_dagster.coord_enrichment.enrichment_leaves import enriched_maxima_raw
from helix_dagster.coord_enrichment.inventory import enrichable_items_inventory
from helix_dagster.coord_enrichment.manifest import coord_enrichment_manifest
from helix_dagster.coord_enrichment.pdv_observer import helix_pdv_coverage_observer
from helix_dagster.coord_enrichment.provenance_tagging import provenance_tagged_items
from helix_dagster.coord_enrichment.report import coord_enrichment_report

FIXTURES = Path(__file__).parent / "fixtures"
EXAMPLE_TS = datetime(2026, 4, 16, 16, 56, 16, tzinfo=timezone.utc)
TRACKING_ITEM_ID = "fake-tracking-item-999"

pytestmark = pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="coordinate-transformer not configured",
)


def _load_instructions():
    return json.loads((FIXTURES / "instructions_example.json").read_text())


def _make_items(data_type: str, count: int = 25):
    """Build a list of mock Girder items for a single data_type."""
    items = []
    for i in range(count):
        items.append({
            "_id": f"{data_type}-{i:03d}",
            "name": f"scan_point_{i}_master.h5" if data_type == "xrd_raw" else f"scan_point_{i}.xrf",
            "folderId": "raw-folder-1",
            "meta": {
                "data_type": data_type,
                "igsn": "JHAMAL00018-009",
                "experiment_date": EXAMPLE_TS.isoformat(),
            },
        })
    return items


def _snapshot():
    return CoordTransformSnapshot(
        yaml_path="test.yaml",
        yaml_sha256="abc123",
        transformer_version="0.3.0",
    )


def _mock_transform(station_x, station_y, instrument="MAXIMA", timestamp=None):
    return (station_x + 100, station_y + 100, "MAXIMA/v1")


@pytest.fixture
def xrd_items():
    return _make_items("xrd_raw", 25)


@pytest.fixture
def xrf_items():
    return _make_items("xrf_raw", 25)


@pytest.fixture
def girder_mock():
    return MagicMock()


def _run_full_dag(xrd_items, xrf_items, girder_mock):
    """Run the full coord_enrichment DAG manually in dependency order."""
    parsed = _load_instructions()
    instr_item = {"_id": "instr-abc"}

    inventory = {
        "MAXIMA/xrd_raw": xrd_items,
        "MAXIMA/xrf_raw": xrf_items,
        "HELIX/pdv_alpss_output": [],
        "HELIX/pdv_alpss_result": [],
        "HELIX/pdv_alpss_results": [],
        "MAXIMA/xrd_derived": [],
    }

    snap = _snapshot()
    config_live = CoordEnrichmentConfig(
        dry_run=False,
        manifest_tracking_item_id=TRACKING_ITEM_ID,
    )

    tagging_ctx = build_asset_context()
    with patch(
        "helix_dagster.coord_enrichment.provenance_tagging.fetch_all_aimdl_datafiles",
        return_value=[],
    ):
        tagging_result = provenance_tagged_items(
            tagging_ctx, config_live, inventory, girder_mock,
        )

    common_patches = {
        "helix_dagster.coord_enrichment.cache.find_run_folder_id": "run-folder-1",
        "helix_dagster.coord_enrichment.cache.fetch_instructions_for_run": (instr_item, parsed),
    }

    partition_results = {}
    for pkey in ["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"]:
        ctx = build_asset_context(partition_key=pkey)
        with patch(
            "helix_dagster.coord_enrichment.cache.find_run_folder_id",
            return_value="run-folder-1",
        ), patch(
            "helix_dagster.coord_enrichment.cache.fetch_instructions_for_run",
            return_value=(instr_item, parsed),
        ), patch(
            "helix_dagster.coord_enrichment.enrichment_leaves.transform_station_to_sample",
            side_effect=_mock_transform,
        ), patch(
            "helix_dagster.coord_enrichment.enrichment_leaves._experiment_date",
            return_value=EXAMPLE_TS,
        ):
            partition_results[pkey] = enriched_maxima_raw(
                ctx, config_live, inventory, snap, girder_mock,
            )

    observer_ctx = build_asset_context()
    with patch(
        "helix_dagster.coord_enrichment.pdv_observer.fetch_all_aimdl_datafiles",
        return_value=[],
    ):
        observer_result = helix_pdv_coverage_observer(observer_ctx, girder_mock)

    report_ctx = build_asset_context()
    xrd_report = coord_enrichment_report(
        report_ctx,
        partition_results["MAXIMA/xrd_raw"],
        {},  # enriched_helix_alpss — not exercised in this e2e
        {},  # enriched_maxima_derived — not exercised in this e2e
        tagging_result,
        observer_result,
    )

    manifest_ctx = build_asset_context()
    manifest_result = coord_enrichment_manifest(
        manifest_ctx, config_live, xrd_report, girder_mock,
    )

    return partition_results, tagging_result, xrd_report, manifest_result


def test_e2e_50_enrichment_writes_plus_manifest(xrd_items, xrf_items, girder_mock):
    """Full DAG: 25 xrd_raw + 25 xrf_raw = 50 enrichment writes + 1 manifest."""
    partition_results, _, _, manifest_result = _run_full_dag(
        xrd_items, xrf_items, girder_mock,
    )

    xrd_counts = partition_results["MAXIMA/xrd_raw"]["counts"]
    xrf_counts = partition_results["MAXIMA/xrf_raw"]["counts"]
    assert xrd_counts["written"] == 25
    assert xrf_counts["written"] == 25

    all_calls = girder_mock.addMetadataToItem.call_args_list
    enrichment_calls = [
        c for c in all_calls
        if "coord_enrichment_status" not in (c[0][1] if len(c[0]) > 1 else {})
    ]
    manifest_calls = [
        c for c in all_calls
        if "coord_enrichment_status" in (c[0][1] if len(c[0]) > 1 else {})
    ]
    assert len(enrichment_calls) == 50
    assert len(manifest_calls) == 1
    assert manifest_calls[0][0][0] == TRACKING_ITEM_ID


def test_e2e_provenance_payload_shape(xrd_items, xrf_items, girder_mock):
    """All enrichment payloads include Sample_X/Y and correct provenance."""
    _run_full_dag(xrd_items, xrf_items, girder_mock)

    all_calls = girder_mock.addMetadataToItem.call_args_list
    enrichment_calls = [
        c for c in all_calls
        if "coord_enrichment_status" not in (c[0][1] if len(c[0]) > 1 else {})
    ]

    for c in enrichment_calls:
        payload = c[0][1]
        assert "Sample_X" in payload
        assert "Sample_Y" in payload
        assert "Station_X" in payload
        assert "Station_Y" in payload
        assert "coord_provenance" in payload
        prov = payload["coord_provenance"]
        assert prov["station_coord_source"]["kind"] == "maxima_instructions"
        assert prov["instrument"] == "MAXIMA"


def test_e2e_scan_point_16_coords(xrd_items, xrf_items, girder_mock):
    """Scan-point index 16 produces Station_X=11.0, Station_Y=-5.0."""
    _run_full_dag(xrd_items, xrf_items, girder_mock)

    all_calls = girder_mock.addMetadataToItem.call_args_list
    item16_calls = [
        c for c in all_calls if c[0][0] == "xrd_raw-016"
    ]
    assert len(item16_calls) == 1
    payload = item16_calls[0][0][1]
    assert payload["Station_X"] == 11.0
    assert payload["Station_Y"] == -5.0
    assert payload["Sample_X"] == 111.0
    assert payload["Sample_Y"] == 95.0


def test_e2e_manifest_written(xrd_items, xrf_items, girder_mock):
    """Manifest is written to the configured tracking item."""
    _, _, _, manifest_result = _run_full_dag(xrd_items, xrf_items, girder_mock)

    assert "write_skipped" not in manifest_result
    assert "write_failed" not in manifest_result
    assert manifest_result["dry_run"] is False
    assert manifest_result["pipeline_version"] == "0.4.0"
    assert manifest_result["job"] == "coord_enrichment_job"
