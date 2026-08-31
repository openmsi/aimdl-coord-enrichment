"""Integration-style tests for enriched_maxima_run asset.

The asset is partitioned on a single dynamic dimension keyed on the
AIMD-L run key, and fetches its own items via the scoped
``/aimdl/partition/details`` endpoint — once per in-scope MAXIMA
data_type. These tests monkeypatch ``fetch_partition_details`` to
return per-(data_type, run) items and xrd_metadata entries, and mock
the Girder client for the instructions.txt download.
"""

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from aimdl_coord_enrichment.coord_enrichment.config import CoordEnrichmentConfig
from aimdl_coord_enrichment.coord_enrichment.config_snapshot import CoordTransformSnapshot
from aimdl_coord_enrichment.coord_enrichment.enrichment_leaves import (
    enriched_maxima_run,
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
    """Invoke enriched_maxima_run with the given per-partition fixtures."""
    if instructions_bytes is None:
        instructions_bytes = _load_instructions_bytes()
    girder = _make_girder(instructions_bytes=instructions_bytes)
    config = CoordEnrichmentConfig(dry_run=dry_run)
    snap = _snapshot()
    ctx = build_asset_context(partition_key=aimdl_key)

    mapping = {
        (data_type, aimdl_key): raw_items,
        ("xrd_metadata", aimdl_key): metadata_items,
    }
    fake_fetch = _make_fetch_partition_details(mapping)

    with patch(
        "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.fetch_partition_details",
        side_effect=fake_fetch,
    ), patch(
        "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.transform_station_to_sample",
        side_effect=transform_fn or _mock_transform,
    ):
        result = enriched_maxima_run(ctx, config, snap, girder)

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
    assert result["per_data_type"]["xrd_raw"] == 2
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


def test_missing_instructions_excludes_all_items():
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
    # A run with no instructions.txt has no coordinates for anything in it.
    # That is out of scope, not a failure.
    assert result["counts"]["resolution_errors"] == 0
    assert result["excluded"]["total"] == 2
    assert result["excluded"]["by_reason"] == {"no_instructions": 2}
    assert result["resolution_errors"] == []
    # The run-level diagnostic is still recorded, so the reason is visible.
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


def test_bad_scan_point_name_is_excluded_not_an_error():
    bad = _xrd_raw_item("item-bad", experiment_date=EXAMPLE_TS.isoformat())
    bad["name"] = "not_a_scan_point.h5"
    result, girder = _run_asset(
        raw_items=[bad],
        metadata_items=[_instr_item()],
        dry_run=False,
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 0
    assert result["excluded"]["by_reason"] == {"unparseable_name": 1}
    assert result["excluded"]["examples"]["unparseable_name"] == ["not_a_scan_point.h5"]


def test_missing_experiment_date_is_excluded_not_an_error():
    item = _xrd_raw_item("item-no-date")  # no experiment_date
    result, girder = _run_asset(
        raw_items=[item],
        metadata_items=[_instr_item()],
        dry_run=False,
    )

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 0
    assert result["excluded"]["by_reason"] == {"no_experiment_date": 1}


def test_xrf_items_are_enriched_in_the_same_run_partition():
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

    assert result["per_data_type"]["xrf_raw"] == 1
    assert result["counts"]["written"] == 1
    assert girder.addMetadataToItem.call_args_list[0][0][0] == "xrf-1"


def test_one_run_partition_covers_every_maxima_data_type():
    """The point of the run-scoped leaf: raw measurements and derived products
    from the same run are enriched together, from one instructions.txt.

    Storage nests raw/ inside the run folder and lineage runs the other way,
    but neither shape is a partition boundary — the run is.
    """
    ts = EXAMPLE_TS.isoformat()

    def _item(iid, name, dt, index):
        return {"_id": iid, "name": name, "folderId": "f",
                "meta": {"data_type": dt, "igsn": "JHAMAB00019-12",
                         "experiment_date": ts}}

    per_type = {
        "xrd_raw": [_item("r1", "scan_point_0_master.h5", "xrd_raw", 0)],
        "xrf_raw": [_item("f1", "scan_point_0.xrf", "xrf_raw", 0)],
        "xrd_derived": [_item("d1", "scan_point_0.tiff", "xrd_derived", 0)],
        "xrd_visualization": [_item("v1", "scan_point_0_scan.jpg",
                                    "xrd_visualization", 0)],
    }
    mapping = {(dt, EXAMPLE_KEY): items for dt, items in per_type.items()}
    mapping[("xrd_metadata", EXAMPLE_KEY)] = [_instr_item()]

    girder = _make_girder(instructions_bytes=_load_instructions_bytes())
    ctx = build_asset_context(partition_key=EXAMPLE_KEY)
    with patch(
        "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.fetch_partition_details",
        side_effect=_make_fetch_partition_details(mapping),
    ), patch(
        "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves."
        "transform_station_to_sample",
        side_effect=_mock_transform,
    ):
        result = enriched_maxima_run(
            ctx, CoordEnrichmentConfig(dry_run=False), _snapshot(), girder,
        )

    assert result["counts"]["seen"] == 4
    assert result["counts"]["written"] == 4
    assert result["per_data_type"] == {
        "xrd_raw": 1, "xrf_raw": 1, "xrd_derived": 1, "xrd_visualization": 1,
    }
    written_ids = {c[0][0] for c in girder.addMetadataToItem.call_args_list}
    assert written_ids == {"r1", "f1", "d1", "v1"}

    # All four resolve to the same scan point, so they share coordinates.
    payloads = [c[0][1] for c in girder.addMetadataToItem.call_args_list]
    assert {(p["Station_X"], p["Station_Y"]) for p in payloads} == {
        (payloads[0]["Station_X"], payloads[0]["Station_Y"])
    }
    for payload in payloads:
        src = payload["coord_provenance"]["station_coord_source"]
        assert src["kind"] == "maxima_instructions"
        assert src["scan_point_index"] == 0


def test_derived_item_records_parent_lineage_without_depending_on_it():
    """prov.wasDerivedFrom is carried into the provenance as a cross-reference,
    but the coordinate still comes from instructions.txt — an item with no
    prov link enriches identically."""
    ts = EXAMPLE_TS.isoformat()
    with_prov = {"_id": "d1", "name": "scan_point_0.tiff", "folderId": "f",
                 "meta": {"data_type": "xrd_derived", "igsn": "JHAMAB00019-12",
                          "experiment_date": ts,
                          "prov": {"wasDerivedFrom": "parent-master-h5"}}}
    without_prov = {"_id": "d2", "name": "scan_point_0.tiff", "folderId": "f",
                    "meta": {"data_type": "xrd_derived", "igsn": "JHAMAB00019-12",
                             "experiment_date": ts}}
    mapping = {("xrd_derived", EXAMPLE_KEY): [with_prov, without_prov],
               ("xrd_metadata", EXAMPLE_KEY): [_instr_item()]}

    girder = _make_girder(instructions_bytes=_load_instructions_bytes())
    ctx = build_asset_context(partition_key=EXAMPLE_KEY)
    with patch(
        "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.fetch_partition_details",
        side_effect=_make_fetch_partition_details(mapping),
    ), patch(
        "aimdl_coord_enrichment.coord_enrichment.enrichment_leaves."
        "transform_station_to_sample",
        side_effect=_mock_transform,
    ):
        result = enriched_maxima_run(
            ctx, CoordEnrichmentConfig(dry_run=False), _snapshot(), girder,
        )

    assert result["counts"]["written"] == 2
    by_id = {c[0][0]: c[0][1] for c in girder.addMetadataToItem.call_args_list}
    linked = by_id["d1"]["coord_provenance"]["station_coord_source"]
    unlinked = by_id["d2"]["coord_provenance"]["station_coord_source"]
    assert linked["parent_item_id"] == "parent-master-h5"
    assert "parent_item_id" not in unlinked
    # Same coordinates either way — the parent link is not load-bearing.
    assert (by_id["d1"]["Station_X"], by_id["d1"]["Station_Y"]) == (
        by_id["d2"]["Station_X"], by_id["d2"]["Station_Y"]
    )


def test_excluded_items_leave_the_success_rate_denominator():
    """A partition of entirely non-standard files must not read as a failure.

    Excluding them from the numerator alone would leave the WARN in place,
    which is the behaviour this policy exists to remove.
    """
    from aimdl_coord_enrichment.coord_enrichment.check_support import (
        evaluate_success_rate,
    )

    # 10 items, 6 excluded, all 4 in-scope written.
    ok = evaluate_success_rate(
        seen=10, written=4, simulated_dry_run=0, skipped_no_change=0,
        resolution_errors=0, write_errors_count=0, partition_label="p",
        excluded=6,
    )
    assert ok.passed
    assert ok.metadata["in_scope"].value == 4
    assert "4/4" in ok.description

    # Everything excluded → passes, and says so rather than reporting 0%.
    none_left = evaluate_success_rate(
        seen=10, written=0, simulated_dry_run=0, skipped_no_change=0,
        resolution_errors=0, write_errors_count=0, partition_label="p",
        excluded=10,
    )
    assert none_left.passed
    assert "No in-scope items" in none_left.description

    # A genuine failure among in-scope items still warns.
    real = evaluate_success_rate(
        seen=10, written=1, simulated_dry_run=0, skipped_no_change=0,
        resolution_errors=0, write_errors_count=3, partition_label="p",
        excluded=6,
    )
    assert not real.passed
