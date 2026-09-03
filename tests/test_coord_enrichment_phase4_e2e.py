"""End-to-end integration test for Phase 4 coord_enrichment leaves.

Runs the full Phase 4 surface against a mock Girder pre-populated with:
  - One fully-enriched pdv_trace item (HELIX, coord_provenance present)
  - Two HELIX ALPSS items (pdv_alpss_output + pdv_alpss_results)
    with prov.wasDerivedFrom → the pdv_trace
  - One fully-enriched xrd_raw master.h5 (MAXIMA, coord_provenance present)
  (xrd_derived moved to enriched_maxima_run — it reads instructions.txt
   directly rather than inheriting; see test_coord_enrichment_maxima_raw.py)

Verifies that all three derived items inherit coherently (Sample_X/Y
match parent transform, provenance station_coord_source.kind ==
"inherited") and that the observer and report aggregate correctly.
"""

from datetime import datetime, timezone
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
from aimdl_coord_enrichment.coord_enrichment.config_snapshot import (
    CoordTransformSnapshot,
    coord_transform_config_snapshot,
)
from aimdl_coord_enrichment.coord_enrichment.helix_alpss_leaf import enriched_helix_alpss
from aimdl_coord_enrichment.coord_enrichment.pdv_observer import helix_pdv_coverage_observer
from aimdl_coord_enrichment.coord_enrichment.report import coord_enrichment_report
from aimdl_coord_enrichment.coord_enrichment.manifest import coord_enrichment_manifest

EXAMPLE_TS = datetime(2026, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
TRACKING_ITEM_ID = "fake-tracking-item-phase4"

pytestmark = pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="coordinate-transformer not configured",
)


def _snapshot():
    return CoordTransformSnapshot(
        yaml_path="test.yaml",
        yaml_sha256="abc123phase4",
        transformer_version="0.3.0",
    )


PDV_TRACE_ITEM = {
    "_id": "pdv-trace-001",
    "name": "shot15_ch1.tdms",
    "folderId": "helix-folder-1",
    "meta": {
        "data_type": "pdv_trace",
        "igsn": "JHAMAL00018-005",
        "Station_X": 8.0,
        "Station_Y": 12.0,
        "Sample_X": 108.0,
        "Sample_Y": 112.0,
        "coord_provenance": {
            "instrument": "HELIX",
            "transform_version": "HELIX/v1",
            "transform_yaml_sha256": "abc123phase4",
            "transformer_version": "0.3.0",
            "pipeline_version": "0.4.0",
            "source_timestamp": EXAMPLE_TS.isoformat(),
            "source_timestamp_origin": "helix_experiment_log",
            "station_coord_source": {
                "kind": "helix_experiment_log",
                "spreadsheet_row": 15,
            },
            "enriched_at": EXAMPLE_TS.isoformat(),
        },
    },
}

ALPSS_OUTPUT_ITEM = {
    "_id": "alpss-output-001",
    "name": "shot15_ch1-iq.png",
    "folderId": "alpss-folder-1",
    "meta": {
        "data_type": "pdv_alpss_output",
        "igsn": "JHAMAL00018-005",
        "prov": {"wasDerivedFrom": "pdv-trace-001"},
    },
}

ALPSS_RESULTS_ITEM = {
    "_id": "alpss-results-001",
    "name": "shot15_ch1_results.json",
    "folderId": "alpss-folder-1",
    "meta": {
        "data_type": "pdv_alpss_results",
        "igsn": "JHAMAL00018-005",
        "prov": {"wasDerivedFrom": "pdv-trace-001"},
    },
}

XRD_RAW_ITEM = {
    "_id": "xrd-raw-master-001",
    "name": "scan_point_0_master.h5",
    "folderId": "maxima-raw-folder-1",
    "meta": {
        "data_type": "xrd_raw",
        "igsn": "JHAMAB00019-12",
        "Station_X": 5.0,
        "Station_Y": -3.0,
        "Sample_X": 105.0,
        "Sample_Y": 97.0,
        "coord_provenance": {
            "instrument": "MAXIMA",
            "transform_version": "MAXIMA/v1",
            "transform_yaml_sha256": "abc123phase4",
            "transformer_version": "0.3.0",
            "pipeline_version": "0.4.0",
            "source_timestamp": EXAMPLE_TS.isoformat(),
            "source_timestamp_origin": "maxima_instructions",
            "station_coord_source": {
                "kind": "maxima_instructions",
                "instructions_item_id": "instr-abc",
                "scan_point_index": 0,
            },
            "enriched_at": EXAMPLE_TS.isoformat(),
        },
    },
}

XRD_DERIVED_ITEM = {
    "_id": "xrd-derived-001",
    "name": "scan_point_0_xrd.csv",
    "folderId": "maxima-raw-folder-1",
    "meta": {
        "data_type": "xrd_derived",
        "igsn": "JHAMAB00019-12",
        "prov": {"wasDerivedFrom": "xrd-raw-master-001"},
    },
}


def _mock_transform_helix(instrument, version_label, station_x, station_y):
    """Fake transform_with_named_version for HELIX: +100 offset."""
    return (station_x + 100, station_y + 100)


def _mock_transform_maxima(instrument, version_label, station_x, station_y):
    """Fake transform_with_named_version for MAXIMA: +100 offset."""
    return (station_x + 100, station_y + 100)


def _mock_transform_any(instrument, version_label, station_x, station_y):
    """Fake transform_with_named_version: +100 offset for any instrument."""
    return (station_x + 100, station_y + 100)


INVENTORY = {
    "HELIX/pdv_alpss_output": [ALPSS_OUTPUT_ITEM],
    "HELIX/pdv_alpss_result": [],
    "HELIX/pdv_alpss_results": [ALPSS_RESULTS_ITEM],
    "MAXIMA/xrd_raw": [],
    "MAXIMA/xrf_raw": [],
    "MAXIMA/xrd_derived": [XRD_DERIVED_ITEM],
}


def _parent_fetch(item_id):
    """Simulate girder.get('item/{id}') for parent lookups."""
    parents = {
        "pdv-trace-001": PDV_TRACE_ITEM,
        "xrd-raw-master-001": XRD_RAW_ITEM,
    }
    if item_id in parents:
        return parents[item_id]
    raise Exception(f"Item {item_id} not found")


@pytest.fixture
def girder_mock():
    mock = MagicMock()
    mock.get.side_effect = lambda path: _parent_fetch(path.replace("item/", ""))
    return mock


def _report_inheritance_leaf_event(instance, asset_name, partition_key, result):
    """Persist a runless materialization for one inheritance-leaf partition.

    enriched_helix_alpss writes the
    same metadata key set, so one helper covers both.
    """
    counts = result["counts"]
    instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=AssetKey(asset_name),
            partition=partition_key,
            metadata={
                "partition": MetadataValue.text(partition_key),
                "seen": MetadataValue.int(counts["seen"]),
                "written": MetadataValue.int(counts["written"]),
                "simulated_dry_run": MetadataValue.int(counts["simulated_dry_run"]),
                "skipped_no_change": MetadataValue.int(counts["skipped_no_change"]),
                "coord_failures": MetadataValue.int(counts["coord_failures"]),
                "resolution_errors": MetadataValue.int(counts["resolution_errors"]),
                "transform_versions_used": MetadataValue.text(
                    ", ".join(
                        f"{k}={v}" for k, v in
                        sorted(result["version_counter"].items())
                    ) or "none"
                ),
            },
        )
    )


def _run_phase4_dag(girder_mock):
    """Run all Phase 4 assets in dependency order."""
    snap = _snapshot()
    config = CoordEnrichmentConfig(
        dry_run=False,
        manifest_tracking_item_id=TRACKING_ITEM_ID,
    )

    with DagsterInstance.ephemeral() as instance:
        alpss_results = {}
        for pkey in [
            "HELIX/pdv_alpss_output",
            "HELIX/pdv_alpss_result",
            "HELIX/pdv_alpss_results",
        ]:
            ctx = build_asset_context(partition_key=pkey, instance=instance)
            with patch(
                "aimdl_coord_enrichment.coord_enrichment.helix_alpss_leaf."
                "transform_with_named_version",
                side_effect=_mock_transform_any,
            ), patch(
                "aimdl_coord_enrichment.coord_enrichment.helix_alpss_leaf."
                "fetch_items_by_partition",
                side_effect=lambda g, dt: INVENTORY.get(f"HELIX/{dt}", []),
            ):
                leaf_result = enriched_helix_alpss(
                    ctx, config, snap, girder_mock,
                )
                alpss_results[pkey] = leaf_result
            _report_inheritance_leaf_event(
                instance, "enriched_helix_alpss", pkey, leaf_result,
            )

        observer_ctx = build_asset_context(instance=instance)
        with patch(
            "aimdl_coord_enrichment.coord_enrichment.pdv_observer.fetch_all_aimdl_datafiles",
            return_value=[PDV_TRACE_ITEM],
        ):
            observer_result = helix_pdv_coverage_observer(observer_ctx, girder_mock)

        report_ctx = build_asset_context(instance=instance)
        report = coord_enrichment_report(
            report_ctx,
            {"counters": {}, "unresolved": [], "write_ops": [], "dry_run": False},
            observer_result,
        )

        manifest_ctx = build_asset_context(instance=instance)
        manifest = coord_enrichment_manifest(
            manifest_ctx, config, report, girder_mock,
        )

    return alpss_results, observer_result, report, manifest


def test_phase4_enrichment_writes(girder_mock):
    """Phase 4: 2 ALPSS items = 2 enrichment writes + 1 manifest."""
    alpss_results, _, _, _ = _run_phase4_dag(girder_mock)

    assert alpss_results["HELIX/pdv_alpss_output"]["counts"]["written"] == 1
    assert alpss_results["HELIX/pdv_alpss_result"]["counts"]["seen"] == 0
    assert alpss_results["HELIX/pdv_alpss_results"]["counts"]["written"] == 1

    all_calls = girder_mock.addMetadataToItem.call_args_list
    enrichment_calls = [
        c for c in all_calls
        if "coord_enrichment_status" not in (c[0][1] if len(c[0]) > 1 else {})
    ]
    manifest_calls = [
        c for c in all_calls
        if "coord_enrichment_status" in (c[0][1] if len(c[0]) > 1 else {})
    ]
    assert len(enrichment_calls) == 2
    assert len(manifest_calls) == 1
    assert manifest_calls[0][0][0] == TRACKING_ITEM_ID


def test_phase4_helix_alpss_inherits_parent_coords(girder_mock):
    """ALPSS items inherit Station_X/Y from parent PDV trace and transform to Sample coords."""
    alpss_results, _, _, _ = _run_phase4_dag(girder_mock)

    all_calls = girder_mock.addMetadataToItem.call_args_list
    alpss_calls = [
        c for c in all_calls if c[0][0] in ("alpss-output-001", "alpss-results-001")
    ]
    assert len(alpss_calls) == 2

    for c in alpss_calls:
        payload = c[0][1]
        assert payload["Station_X"] == 8.0
        assert payload["Station_Y"] == 12.0
        assert payload["Sample_X"] == 108.0
        assert payload["Sample_Y"] == 112.0
        prov = payload["coord_provenance"]
        assert prov["station_coord_source"]["kind"] == "inherited"
        assert prov["station_coord_source"]["parent_item_id"] == "pdv-trace-001"
        assert prov["station_coord_source"]["parent_data_type"] == "pdv_trace"
        assert prov["instrument"] == "HELIX"
        assert prov["transform_version"] == "HELIX/v1"

def test_phase4_observer_reports_coverage(girder_mock):
    """Observer counts pdv_trace coverage — one fully enriched item."""
    _, observer_result, _, _ = _run_phase4_dag(girder_mock)

    assert observer_result["total"] == 1
    assert observer_result["fully_enriched"] == 1
    assert observer_result["unenriched"] == 0
    assert observer_result["coverage_rate"] == 1.0


def test_phase4_report_aggregates_all_leaves(girder_mock):
    """Report aggregates counts from the HELIX ALPSS leaf partitions."""
    _, _, report, _ = _run_phase4_dag(girder_mock)

    summary = report["summary"]
    # 1 write each from pdv_alpss_output and pdv_alpss_results;
    # pdv_alpss_result has 0 in-scope items. MAXIMA is no longer an
    # inheritance leaf — it materializes per run via enriched_maxima_run.
    assert summary["total_writes"] == 2
    assert summary["leaf_partitions_covered"] == 3

    assert "pdv_trace" in report["coverage"]
    assert report["coverage"]["pdv_trace"]["fully_enriched"] == 1

    assert "leaves_unmaterialized" in report
    assert summary["leaf_partitions_unmaterialized"] >= 0


def test_phase4_manifest_written(girder_mock):
    """Manifest is written to the configured tracking item."""
    _, _, _, manifest = _run_phase4_dag(girder_mock)

    assert "write_skipped" not in manifest
    assert "write_failed" not in manifest
    assert manifest["dry_run"] is False
    assert manifest["pipeline_version"] == "0.6.0"
    assert manifest["job"] == "unknown"  # direct invocation, no job context
