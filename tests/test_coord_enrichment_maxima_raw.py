"""Integration-style tests for enriched_maxima_raw asset."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import CoordTransformSnapshot
from helix_dagster.coord_enrichment.enrichment_leaves import (
    enriched_maxima_raw,
    enrichment_success_rate_maxima_raw,
    no_coord_transform_failures_maxima_raw,
)

FIXTURES = Path(__file__).parent / "fixtures"
EXAMPLE_TS = datetime(2026, 4, 16, 16, 56, 16, tzinfo=timezone.utc)


def _load_instructions():
    return json.loads((FIXTURES / "instructions_example.json").read_text())


def _snapshot(yaml_sha="abc123", version="0.3.0"):
    return CoordTransformSnapshot(
        yaml_path="test.yaml",
        yaml_sha256=yaml_sha,
        transformer_version=version,
    )


def _xrd_raw_item(item_id="item1", index=0, *, experiment_date=None, coord_prov=None):
    meta = {
        "data_type": "xrd_raw",
        "igsn": "JHAMAB00019-12",
    }
    if experiment_date is not None:
        meta["experiment_date"] = experiment_date
    if coord_prov is not None:
        meta["coord_provenance"] = coord_prov
    return {
        "_id": item_id,
        "name": f"scan_point_{index}_master.h5",
        "folderId": "raw-folder-1",
        "meta": meta,
    }


def _mock_transform(station_x, station_y, instrument="MAXIMA", timestamp=None):
    return (25.0, 25.0, "MAXIMA/v1")


def _mock_transform_none(station_x, station_y, instrument="MAXIMA", timestamp=None):
    return (None, None, None)


def _run_asset(items, *, partition_key="MAXIMA/xrd_raw", dry_run=True, snapshot=None,
               transform_fn=None):
    """Run enriched_maxima_raw with full mocking."""
    inventory = {
        "MAXIMA/xrd_raw": [],
        "MAXIMA/xrf_raw": [],
    }
    inventory[partition_key] = items

    girder = MagicMock()
    config = CoordEnrichmentConfig(dry_run=dry_run)
    snap = snapshot or _snapshot()
    ctx = build_asset_context(partition_key=partition_key)

    parsed = _load_instructions()
    instr_item = {"_id": "instr-abc"}

    with patch(
        "helix_dagster.coord_enrichment.enrichment_leaves._experiment_date",
        side_effect=lambda item: _get_experiment_date(item),
    ), patch(
        "helix_dagster.coord_enrichment.cache.find_run_folder_id",
        return_value="run-folder-1",
    ), patch(
        "helix_dagster.coord_enrichment.cache.fetch_instructions_for_run",
        return_value=(instr_item, parsed),
    ), patch(
        "helix_dagster.coord_enrichment.enrichment_leaves.transform_station_to_sample",
        side_effect=transform_fn or _mock_transform,
    ):
        result = enriched_maxima_raw(ctx, config, inventory, snap, girder)

    return result, girder


def _get_experiment_date(item):
    from helix_dagster.instruments.types import ResolutionError
    raw = (item.get("meta") or {}).get("experiment_date")
    if raw is None:
        raise ResolutionError(f"item {item.get('_id')} missing meta.experiment_date")
    return datetime.fromisoformat(raw)


def test_single_item_dry_run():
    item = _xrd_raw_item(experiment_date=EXAMPLE_TS.isoformat())
    result, girder = _run_asset([item], dry_run=True)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["seen"] == 1
    assert result["counts"]["simulated_dry_run"] == 1
    assert result["counts"]["written"] == 0
    assert "MAXIMA/v1" in result["version_counter"]


def test_single_item_live_writes_expected_payload():
    item = _xrd_raw_item(experiment_date=EXAMPLE_TS.isoformat())
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    call_args = girder.addMetadataToItem.call_args
    item_id_arg = call_args[0][0]
    payload = call_args[0][1]

    assert item_id_arg == "item1"
    assert "Station_X" in payload
    assert "Sample_X" in payload
    assert payload["Sample_X"] == 25.0
    assert payload["Sample_Y"] == 25.0
    assert payload["Station_X"] == -4.0
    assert payload["Station_Y"] == -10.0
    assert "coord_provenance" in payload
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "MAXIMA"
    assert prov["station_coord_source"]["kind"] == "maxima_instructions"
    assert prov["station_coord_source"]["scan_point_index"] == 0
    assert result["counts"]["written"] == 1


def test_skip_when_stored_prov_unchanged():
    from helix_dagster.provenance import build_coord_provenance
    from helix_dagster import __version__

    stored_prov = build_coord_provenance(
        instrument="MAXIMA",
        transform_version="MAXIMA/v1",
        transform_yaml_sha256="abc123",
        transformer_version="0.3.0",
        pipeline_version=__version__,
        source_timestamp=EXAMPLE_TS,
        source_timestamp_origin="meta.experiment_date",
        station_coord_source={
            "kind": "maxima_instructions",
            "instructions_item_id": "instr-abc",
            "scan_point_index": 0,
        },
        dagster_run_id="old-run",
    )

    item = _xrd_raw_item(experiment_date=EXAMPLE_TS.isoformat(), coord_prov=stored_prov)
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["skipped_no_change"] == 1


def test_overwrite_when_yaml_sha_differs():
    stored_prov = {
        "instrument": "MAXIMA",
        "transform_version": "MAXIMA/v1",
        "transform_yaml_sha256": "OLD_SHA",
        "transformer_version": "0.3.0",
        "station_coord_source": {
            "kind": "maxima_instructions",
            "instructions_item_id": "instr-abc",
            "scan_point_index": 0,
        },
    }
    item = _xrd_raw_item(experiment_date=EXAMPLE_TS.isoformat(), coord_prov=stored_prov)
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    assert result["counts"]["written"] == 1


def test_missing_experiment_date_recorded_as_resolution_error():
    item = _xrd_raw_item()
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 1
    assert len(result["resolution_errors"]) == 1
    assert result["resolution_errors"][0]["stage"] == "experiment_date"


def test_partition_filter_applies():
    xrd_item = _xrd_raw_item("xrd1", 0, experiment_date=EXAMPLE_TS.isoformat())
    xrf_item = _xrd_raw_item("xrf1", 1, experiment_date=EXAMPLE_TS.isoformat())
    xrf_item["meta"]["data_type"] = "xrf_raw"

    inventory = {
        "MAXIMA/xrd_raw": [xrd_item],
        "MAXIMA/xrf_raw": [xrf_item],
    }

    girder = MagicMock()
    config = CoordEnrichmentConfig(dry_run=False)
    snap = _snapshot()
    ctx = build_asset_context(partition_key="MAXIMA/xrd_raw")

    parsed = _load_instructions()
    instr_item = {"_id": "instr-abc"}

    with patch(
        "helix_dagster.coord_enrichment.enrichment_leaves._experiment_date",
        side_effect=_get_experiment_date,
    ), patch(
        "helix_dagster.coord_enrichment.cache.find_run_folder_id",
        return_value="run-folder-1",
    ), patch(
        "helix_dagster.coord_enrichment.cache.fetch_instructions_for_run",
        return_value=(instr_item, parsed),
    ), patch(
        "helix_dagster.coord_enrichment.enrichment_leaves.transform_station_to_sample",
        side_effect=_mock_transform,
    ):
        result = enriched_maxima_raw(ctx, config, inventory, snap, girder)

    assert result["counts"]["seen"] == 1
    assert result["partition_key"] == "MAXIMA/xrd_raw"
    girder.addMetadataToItem.assert_called_once()
    written_id = girder.addMetadataToItem.call_args[0][0]
    assert written_id == "xrd1"
