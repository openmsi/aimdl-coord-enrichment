"""End-to-end integration test for the coord_enrichment DAG.

Runs the full asset graph against a mock Girder pre-loaded with the
JHAMAL00018-009 fixture (25 scan points -> 25 xrd_raw + 25 xrf_raw
items plus instructions.txt). Asserts 50 enrichment writes and a
manifest write.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    MetadataValue,
    build_asset_context,
)

from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER
from aimdl_coord_enrichment.coord_enrichment.config import CoordEnrichmentConfig
from aimdl_coord_enrichment.coord_enrichment.config_snapshot import CoordTransformSnapshot
from aimdl_coord_enrichment.coord_enrichment.enrichment_leaves import enriched_maxima_run
from aimdl_coord_enrichment.coord_enrichment.manifest import coord_enrichment_manifest
from aimdl_coord_enrichment.coord_enrichment.pdv_observer import helix_pdv_coverage_observer
from aimdl_coord_enrichment.coord_enrichment.provenance_tagging import (
    helix_alpss_provenance_tagged,
)
from aimdl_coord_enrichment.coord_enrichment.report import coord_enrichment_report

FIXTURES = Path(__file__).parent / "fixtures"
EXAMPLE_TS = datetime(2026, 4, 16, 16, 56, 16, tzinfo=timezone.utc)
AIMDL_KEY = f"JHAMAL00018-009//{EXAMPLE_TS.isoformat()}"
TRACKING_ITEM_ID = "fake-tracking-item-999"

pytestmark = pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="coordinate-transformer not configured",
)


def _load_instructions_bytes() -> bytes:
    return (FIXTURES / "instructions_example.json").read_bytes()


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
    mock = MagicMock()
    instructions_bytes = _load_instructions_bytes()

    def _get(path, parameters=None):
        if path.startswith("item/") and path.endswith("/files"):
            return [{"_id": f"file-for-{path.split('/')[1]}"}]
        raise AssertionError(f"unexpected girder.get({path!r}, {parameters!r})")

    mock.get.side_effect = _get

    def _download(file_id, buf):
        buf.write(instructions_bytes)

    mock.downloadFile.side_effect = _download
    return mock


def _report_maxima_raw_event(instance, partition_key, data_type, aimdl_key, result):
    """Persist a runless materialization for one enriched_maxima_run partition.

    The report asset rebuilds per-partition state by reading these
    events back; the keys must match what
    enriched_maxima_run.add_output_metadata writes.
    """
    counts = result["counts"]
    instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=AssetKey("enriched_maxima_run"),
            partition=str(partition_key),
            metadata={
                "partition": MetadataValue.text(str(partition_key)),
                "data_type": MetadataValue.text(data_type),
                "aimdl_key": MetadataValue.text(aimdl_key),
                "seen": MetadataValue.int(counts["seen"]),
                "written": MetadataValue.int(counts["written"]),
                "simulated_dry_run": MetadataValue.int(counts["simulated_dry_run"]),
                "skipped_no_change": MetadataValue.int(counts["skipped_no_change"]),
                "coord_failures": MetadataValue.int(counts["coord_failures"]),
                "resolution_errors": MetadataValue.int(counts["resolution_errors"]),
                "instructions_errors": MetadataValue.int(
                    len(result["instructions_errors"])
                ),
                "transform_versions_used": MetadataValue.text(
                    ", ".join(
                        f"{k}={v}" for k, v in
                        sorted(result["version_counter"].items())
                    ) or "none"
                ),
            },
        )
    )


def _run_full_dag(xrd_items, xrf_items, girder_mock):
    """Run the full coord_enrichment DAG manually in dependency order."""
    instr_item = {"_id": "instr-abc", "name": "instructions.txt"}

    # The tagger fetches its own ALPSS items now; this dict is served through
    # that fetch. Empty HELIX ALPSS lists mean it has nothing to do.
    inventory = {
        "HELIX/pdv_trace": [],
        "HELIX/pdv_alpss_output": [],
        "HELIX/pdv_alpss_result": [],
        "HELIX/pdv_alpss_results": [],
        "MAXIMA/xrd_raw": xrd_items,
        "MAXIMA/xrf_raw": xrf_items,
        "MAXIMA/xrd_derived": [],
    }

    snap = _snapshot()
    config_live = CoordEnrichmentConfig(
        dry_run=False,
        manifest_tracking_item_id=TRACKING_ITEM_ID,
    )

    with DagsterInstance.ephemeral() as instance:
        # The dynamic dim must know about AIMDL_KEY before the report
        # tries to enumerate maxima_raw partition keys.
        instance.add_dynamic_partitions("maxima_run", [AIMDL_KEY])

        tagging_ctx = build_asset_context(instance=instance)
        with patch(
            "aimdl_coord_enrichment.coord_enrichment.provenance_tagging.fetch_all_aimdl_datafiles",
            return_value=[],
        ), patch(
            "aimdl_coord_enrichment.coord_enrichment.provenance_tagging.fetch_items_by_partition",
            side_effect=lambda g, dt: inventory.get(f"HELIX/{dt}", []),
        ):
            tagging_result = helix_alpss_provenance_tagged(
                tagging_ctx, config_live, girder_mock,
            )

        fetch_mapping = {
            ("xrd_raw", AIMDL_KEY): xrd_items,
            ("xrf_raw", AIMDL_KEY): xrf_items,
            ("xrd_metadata", AIMDL_KEY): [instr_item],
        }

        def _fake_fetch(girder, data_type, key):
            return list(fetch_mapping.get((data_type, key), []))

        # One partition covers the whole run — both data types together.
        partition_results = {}
        ctx = build_asset_context(partition_key=AIMDL_KEY, instance=instance)
        with patch(
            "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.fetch_partition_details",
            side_effect=_fake_fetch,
        ), patch(
            "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.transform_station_to_sample",
            side_effect=_mock_transform,
        ):
            leaf_result = enriched_maxima_run(ctx, config_live, snap, girder_mock)
        partition_results["run"] = leaf_result
        _report_maxima_raw_event(
            instance, AIMDL_KEY, "run", AIMDL_KEY, leaf_result,
        )

        observer_ctx = build_asset_context(instance=instance)
        with patch(
            "aimdl_coord_enrichment.coord_enrichment.pdv_observer.fetch_all_aimdl_datafiles",
            return_value=[],
        ):
            observer_result = helix_pdv_coverage_observer(observer_ctx, girder_mock)

        report_ctx = build_asset_context(instance=instance)
        xrd_report = coord_enrichment_report(
            report_ctx, tagging_result, observer_result,
        )

        manifest_ctx = build_asset_context(instance=instance)
        manifest_result = coord_enrichment_manifest(
            manifest_ctx, config_live, xrd_report, girder_mock,
        )

    return partition_results, tagging_result, xrd_report, manifest_result


def test_e2e_50_enrichment_writes_plus_manifest(xrd_items, xrf_items, girder_mock):
    """Full DAG: 25 xrd_raw + 25 xrf_raw = 50 enrichment writes + 1 manifest."""
    partition_results, _, report, manifest_result = _run_full_dag(
        xrd_items, xrf_items, girder_mock,
    )

    counts = partition_results["run"]["counts"]
    per_type = partition_results["run"]["per_data_type"]
    # One partition, both data types: 25 xrd_raw + 25 xrf_raw = 50 writes.
    assert per_type["xrd_raw"] == 25
    assert per_type["xrf_raw"] == 25
    assert counts["written"] == 50

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

    # Report sees the two materialized maxima_raw partitions and counts
    # the helix/derived leaves as unmaterialized (no events reported).
    # One run partition now covers both data types (was 2 partitions).
    assert report["summary"]["leaf_partitions_covered"] == 1
    assert report["leaves_unmaterialized"]["enriched_helix_alpss"] == 3
    assert report["leaves_unmaterialized"]["enriched_maxima_run"] == 0


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
    assert manifest_result["pipeline_version"] == "0.6.0"
    assert manifest_result["job"] == "unknown"  # direct invocation, no job context
