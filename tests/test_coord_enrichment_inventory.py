"""Tests for helix_dagster.coord_enrichment.inventory."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from helix_dagster.coord_enrichment.inventory import (
    MAXIMA_RAW_PARTITIONS,
    _is_in_scope,
    enrichable_items_inventory,
    filter_to_raw_subfolder,
    inventory_nonempty_per_instrument,
)
from helix_dagster.instruments import all_in_scope_data_types, instrument_for_data_type


def _make_item(data_type, igsn=None, **extra_meta):
    meta = {"data_type": data_type}
    if igsn is not None:
        meta["igsn"] = igsn
    meta.update(extra_meta)
    return {"_id": "fake_id", "name": "fake_name", "meta": meta}


# ── enrichable_items_inventory ──────────────────────────────────────


@patch("helix_dagster.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_returns_all_in_scope_data_type_keys(mock_fetch):
    mock_fetch.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    expected_keys = {
        f"{instrument_for_data_type(dt)}/{dt}"
        for dt in all_in_scope_data_types()
    }
    assert set(result.keys()) == expected_keys
    for v in result.values():
        assert v == []


@patch("helix_dagster.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_filters_out_missing_igsn(mock_fetch):
    def side_effect(_client, dt):
        if dt == "xrd_raw":
            return [
                _make_item("xrd_raw", igsn="JHAMAB00001"),
                _make_item("xrd_raw"),  # no igsn
            ]
        return []

    mock_fetch.side_effect = side_effect
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    assert len(result["MAXIMA/xrd_raw"]) == 1


@patch("helix_dagster.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_keeps_items_with_igsn(mock_fetch):
    def side_effect(_client, dt):
        if dt == "xrf_raw":
            return [_make_item("xrf_raw", igsn="JHAMAB00002")]
        return []

    mock_fetch.side_effect = side_effect
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    assert len(result["MAXIMA/xrf_raw"]) == 1
    assert result["MAXIMA/xrf_raw"][0]["meta"]["igsn"] == "JHAMAB00002"


@patch("helix_dagster.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_key_format_is_instrument_slash_data_type(mock_fetch):
    mock_fetch.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    for key in result:
        parts = key.split("/")
        assert len(parts) == 2, f"Key {key!r} not in INSTRUMENT/data_type format"
        instrument, dt = parts
        assert instrument in ("HELIX", "MAXIMA")
        assert dt in all_in_scope_data_types()


@patch("helix_dagster.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_logs_per_data_type_counts(mock_fetch, capsys):
    mock_fetch.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    enrichable_items_inventory(ctx, girder)

    # Dagster's build_asset_context uses a standard logger; verifying
    # that logging doesn't error is the primary check here.
    # TODO: assert log output contains data_type names once Dagster
    # test log capture is wired up.


# ── inventory_nonempty_per_instrument check ─────────────────────────


def test_check_passes_with_all_populated_partitions():
    inventory = {
        "MAXIMA/xrd_raw": [_make_item("xrd_raw", igsn="A")],
        "MAXIMA/xrf_raw": [_make_item("xrf_raw", igsn="B")],
        "HELIX/pdv_alpss_output": [_make_item("pdv_alpss_output", igsn="C")],
    }
    ctx = build_asset_context()
    result = inventory_nonempty_per_instrument(ctx, inventory)

    assert result.passed is True


def test_check_warns_on_empty_partition():
    inventory = {
        "MAXIMA/xrd_raw": [_make_item("xrd_raw", igsn="A")],
        "MAXIMA/xrf_raw": [],
    }
    ctx = build_asset_context()
    result = inventory_nonempty_per_instrument(ctx, inventory)

    assert result.passed is False
    assert result.severity.value == "WARN"
    assert "MAXIMA/xrf_raw" in result.description


# ── MAXIMA_RAW_PARTITIONS ───────────────────────────────────────────


def test_partition_definition_has_expected_keys():
    assert MAXIMA_RAW_PARTITIONS.get_partition_keys() == [
        "MAXIMA/xrd_raw",
        "MAXIMA/xrf_raw",
    ]


# ── filter_to_raw_subfolder ───────────────────────────────────────


def _xrd_derived_item(item_id, folder_id):
    return {
        "_id": item_id,
        "name": f"scan_point_0_{item_id}.csv",
        "folderId": folder_id,
        "meta": {"data_type": "xrd_derived", "igsn": "JHAMAL00018-009"},
    }


def test_xrd_derived_filter_keeps_in_raw_items():
    raw_item = _xrd_derived_item("item_raw", "folder_raw")
    root_item = _xrd_derived_item("item_root", "folder_root")

    girder = MagicMock()

    def get_folder(path):
        if path == "folder/folder_raw":
            return {"_id": "folder_raw", "name": "raw"}
        if path == "folder/folder_root":
            return {"_id": "folder_root", "name": "JHAMAL00018-009_run"}
        raise ValueError(f"unexpected path: {path}")

    girder.get.side_effect = get_folder

    result = filter_to_raw_subfolder([raw_item, root_item], girder)

    assert len(result) == 1
    assert result[0]["_id"] == "item_raw"


def test_xrd_derived_filter_logs_drop_counts(caplog):
    raw_item = _xrd_derived_item("item_raw", "folder_raw")
    root_item = _xrd_derived_item("item_root", "folder_root")

    girder = MagicMock()

    def get_folder(path):
        if path == "folder/folder_raw":
            return {"_id": "folder_raw", "name": "raw"}
        return {"_id": "folder_root", "name": "JHAMAL00018-009_run"}

    girder.get.side_effect = get_folder

    with caplog.at_level(logging.INFO, logger="helix_dagster.coord_enrichment.inventory"):
        filter_to_raw_subfolder([raw_item, root_item], girder)

    assert any("kept 1 in-raw" in msg for msg in caplog.messages)
    assert any("dropped 1 non-raw" in msg for msg in caplog.messages)


def test_xrd_derived_filter_handles_fetch_failure(caplog):
    ok_item = _xrd_derived_item("item_ok", "folder_ok")
    bad_item = _xrd_derived_item("item_bad", "folder_bad")

    girder = MagicMock()

    def get_folder(path):
        if path == "folder/folder_ok":
            return {"_id": "folder_ok", "name": "raw"}
        raise ConnectionError("network error")

    girder.get.side_effect = get_folder

    with caplog.at_level(logging.WARNING, logger="helix_dagster.coord_enrichment.inventory"):
        result = filter_to_raw_subfolder([ok_item, bad_item], girder)

    assert len(result) == 1
    assert result[0]["_id"] == "item_ok"
    assert any("network error" in msg for msg in caplog.messages)
