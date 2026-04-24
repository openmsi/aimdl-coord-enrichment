"""Integration-style tests for enriched_maxima_raw asset.

The asset is partitioned on
``MultiPartitionsDefinition({data_type, run})`` and fetches its own
items via the scoped ``/aimdl/partition/details`` endpoint. These
tests monkeypatch ``fetch_partition_details`` to return per-partition
items and xrd_metadata entries, and mock the Girder client for
instructions.txt download.
"""

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import MultiPartitionKey, build_asset_context

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import CoordTransformSnapshot
from helix_dagster.coord_enrichment.enrichment_leaves import (
    enriched_maxima_raw,
)

FIXTURES = Path(__file__).parent / "fixtures"
EXAMPLE_TS = datetime(2026, 4, 16, 16, 56, 16, tzinfo=timezone.utc)
EXAMPLE_KEY = "JHAMAB00019-12//2026-04-16T16:56:16+00:00"


def _load_instructions_bytes() -> bytes:
    return (FIXTURES / "instructions_example.json").read_bytes()


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


def _instr_item(item_id="instr-abc"):
    return {"_id": item_id, "name": "instructions.txt"}


def _mock_transform(station_x, station_y, instrument="MAXIMA", timestamp=None):
    return (25.0, 25.0, "MAXIMA/v1")


def _make_girder(instructions_bytes: bytes | None = None) -> MagicMock:
    """Mock Girder client. `item/<id>/files` returns one file; downloadFile
    writes the given bytes into the supplied buffer.
    """
    girder = MagicMock()

    def _get(path, parameters=None):
        if path.startswith("item/") and path.endswith("/files"):
            return [{"_id": f"file-for-{path.split('/')[1]}"}]
        raise AssertionError(f"unexpected girder.get({path!r}, {parameters!r})")

    girder.get.side_effect = _get

    def _download(file_id, buf):
        if instructions_bytes is None:
            raise AssertionError("downloadFile called but no bytes configured")
        buf.write(instructions_bytes)

    girder.downloadFile.side_effect = _download
    return girder


def _make_fetch_partition_details(mapping: dict[tuple[str, str], list[dict]]):
    """Build a replacement for fetch_partition_details keyed on (data_type, key)."""

    def _fake(girder, data_type, key):
        return list(mapping.get((data_type, key), []))

    return _fake


def _run_asset(
    *,
    data_type="xrd_raw",
    aimdl_key=EXAMPLE_KEY,
    raw_items,
    metadata_items,
    dry_run=True,
    transform_fn=None,
    instructions_bytes=None,
):
    """Invoke enriched_maxima_raw with the given per-partition fixtures."""
    if instructions_bytes is None:
        instructions_bytes = _load_instructions_bytes()
    girder = _make_girder(instructions_bytes=instructions_bytes)
    config = CoordEnrichmentConfig(dry_run=dry_run)
    snap = _snapshot()
    ctx = build_asset_context(
        partition_key=MultiPartitionKey(
            {"data_type": data_type, "run": aimdl_key}
        )
    )

    mapping = {
        (data_type, aimdl_key): raw_items,
        ("xrd_metadata", aimdl_key): metadata_items,
    }
    fake_fetch = _make_fetch_partition_details(mapping)

    with patch(
        "helix_dagster.coord_enrichment.enrichment_leaves.fetch_partition_details",
        side_effect=fake_fetch,
    ), patch(
        "helix_dagster.coord_enrichment.enrichment_leaves.transform_station_to_sample",
        side_effect=transform_fn or _mock_transform,
    ):
        result = enriched_maxima_raw(ctx, config, snap, girder)

    return result, girder


def test_happy_path_dry_run_two_items():
    items = [
        _xrd_raw_item("item-a", 0, experiment_date=EXAMPLE_TS.isoformat()),
        _xrd_raw_item("item-b", 1, experiment_date=EXAMPLE_TS.isoformat()),
    ]
    result, girder = _run_asset(
        raw_items=items,
        metadata_items=[_instr_item()],
        dry_run=True,
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["seen"] == 2
    assert result["counts"]["simulated_dry_run"] == 2
    assert result["counts"]["written"] == 0
    assert result["counts"]["resolution_errors"] == 0
    assert result["instructions_errors"] == []
    assert result["data_type"] == "xrd_raw"
    assert result["aimdl_key"] == EXAMPLE_KEY
    assert "MAXIMA/v1" in result["version_counter"]


def test_happy_path_live_writes_payload():
    items = [
        _xrd_raw_item("item-a", 0, experiment_date=EXAMPLE_TS.isoformat()),
        _xrd_raw_item("item-b", 1, experiment_date=EXAMPLE_TS.isoformat()),
    ]
    result, girder = _run_asset(
        raw_items=items,
        metadata_items=[_instr_item()],
        dry_run=False,
    )

    assert result["counts"]["written"] == 2
    assert girder.addMetadataToItem.call_count == 2
    first_call = girder.addMetadataToItem.call_args_list[0]
    item_id_arg = first_call[0][0]
    payload = first_call[0][1]
    assert item_id_arg == "item-a"
    assert payload["Sample_X"] == 25.0
    assert payload["Sample_Y"] == 25.0
    assert payload["Station_X"] == -4.0
    assert payload["Station_Y"] == -10.0
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "MAXIMA"
    assert prov["station_coord_source"]["kind"] == "maxima_instructions"
    assert prov["station_coord_source"]["scan_point_index"] == 0
    assert prov["station_coord_source"]["instructions_item_id"] == "instr-abc"


def test_missing_instructions_marks_all_items_as_errors():
    items = [
        _xrd_raw_item("item-a", 0, experiment_date=EXAMPLE_TS.isoformat()),
        _xrd_raw_item("item-b", 1, experiment_date=EXAMPLE_TS.isoformat()),
    ]
    result, girder = _run_asset(
        raw_items=items,
        metadata_items=[],
        dry_run=False,
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 2
    assert len(result["resolution_errors"]) == 2
    for err in result["resolution_errors"]:
        assert err["stage"] == "instructions"
    assert len(result["instructions_errors"]) == 1
    assert result["instructions_errors"][0]["stage"] == "instructions_missing"


def test_multiple_instructions_first_wins_and_warns():
    items = [
        _xrd_raw_item("item-a", 0, experiment_date=EXAMPLE_TS.isoformat()),
    ]
    metadata_items = [
        _instr_item("instr-first"),
        _instr_item("instr-second"),
        # Unrelated xrd_metadata entry that must be ignored.
        {"_id": "other", "name": "beam_profile.png"},
    ]
    result, girder = _run_asset(
        raw_items=items,
        metadata_items=metadata_items,
        dry_run=False,
    )

    assert result["counts"]["written"] == 1
    assert len(result["instructions_errors"]) == 1
    assert result["instructions_errors"][0]["stage"] == "instructions_duplicate"
    call = girder.addMetadataToItem.call_args_list[0]
    prov = call[0][1]["coord_provenance"]
    assert prov["station_coord_source"]["instructions_item_id"] == "instr-first"


def test_bad_scan_point_name_recorded_as_resolution_error():
    bad = _xrd_raw_item("item-bad", experiment_date=EXAMPLE_TS.isoformat())
    bad["name"] = "not_a_scan_point.h5"
    result, girder = _run_asset(
        raw_items=[bad],
        metadata_items=[_instr_item()],
        dry_run=False,
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 1
    assert result["resolution_errors"][0]["stage"] == "scan_point_lookup"


def test_missing_experiment_date_recorded_as_resolution_error():
    item = _xrd_raw_item("item-no-date")  # no experiment_date
    result, girder = _run_asset(
        raw_items=[item],
        metadata_items=[_instr_item()],
        dry_run=False,
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 1
    assert result["resolution_errors"][0]["stage"] == "experiment_date"


def test_xrf_partition_key_selects_xrf_items():
    items = [
        {
            "_id": "xrf-1",
            "name": "scan_point_0.xrf",
            "folderId": "raw-folder-1",
            "meta": {
                "data_type": "xrf_raw",
                "igsn": "JHAMAB00019-12",
                "experiment_date": EXAMPLE_TS.isoformat(),
            },
        },
    ]
    result, girder = _run_asset(
        data_type="xrf_raw",
        raw_items=items,
        metadata_items=[_instr_item()],
        dry_run=False,
    )

    assert result["data_type"] == "xrf_raw"
    assert result["counts"]["written"] == 1
    assert girder.addMetadataToItem.call_args_list[0][0][0] == "xrf-1"
