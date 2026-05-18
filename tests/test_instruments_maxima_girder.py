"""Tests for MAXIMA Girder-backed helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from aimdl_coord_enrichment.instruments.maxima import (
    fetch_instructions_for_run,
    find_master_h5_item_id,
    find_run_folder_id,
    heal_maxima_derived_parent,
    resolve_leaf_coords,
)
from aimdl_coord_enrichment.instruments.types import ResolutionError

FIXTURE = Path(__file__).parent / "fixtures" / "instructions_example.json"
FIXTURE_BYTES = FIXTURE.read_bytes()


def _make_girder_mock(routes: dict | None = None):
    """Build a MagicMock with a .get() that dispatches on the first arg."""
    mock = MagicMock()
    if routes:

        def _get(path, parameters=None):
            for key, val in routes.items():
                if path == key:
                    return val() if callable(val) else val
            raise AssertionError(f"unexpected girder.get({path!r}, {parameters!r})")

        mock.get.side_effect = _get
    return mock


# -- find_run_folder_id ------------------------------------------------------


def test_find_run_folder_id_from_raw_item():
    item = {"_id": "item1", "folderId": "raw_f"}
    girder = _make_girder_mock({
        "folder/raw_f": {"_id": "raw_f", "name": "raw", "parentId": "run_id"},
    })
    assert find_run_folder_id(item, girder) == "run_id"


def test_find_run_folder_id_from_root_item():
    item = {"_id": "item2", "folderId": "run_f"}
    girder = _make_girder_mock({
        "folder/run_f": {"_id": "run_f", "name": "JHAMAL00019-12_scan", "parentId": "parent"},
    })
    assert find_run_folder_id(item, girder) == "run_f"


def test_find_run_folder_id_missing_folder_id_raises():
    with pytest.raises(ResolutionError, match="has no folderId"):
        find_run_folder_id({"_id": "x"}, MagicMock())


# -- fetch_instructions_for_run ----------------------------------------------


def test_fetch_instructions_success():
    instr_item = {"_id": "instr1", "name": "instructions.txt"}

    def _get(path, parameters=None):
        if path == "item":
            return [instr_item, {"_id": "other", "name": "scan_point_0.xrf"}]
        if path == "item/instr1/files":
            return [{"_id": "file1"}]
        raise AssertionError(f"unexpected: {path}")

    girder = MagicMock()
    girder.get.side_effect = _get

    def _download(file_id, buf):
        buf.write(FIXTURE_BYTES)

    girder.downloadFile.side_effect = _download

    item, parsed = fetch_instructions_for_run("run_folder", girder)
    assert item["_id"] == "instr1"
    assert "scan_points" in parsed["sample"]
    assert len(parsed["sample"]["scan_points"]) == 25


def test_fetch_instructions_missing_raises():
    girder = MagicMock()
    girder.get.return_value = []
    with pytest.raises(ResolutionError, match="has no instructions.txt"):
        fetch_instructions_for_run("run_folder", girder)


def test_fetch_instructions_multiple_raises():
    girder = MagicMock()
    girder.get.return_value = [
        {"_id": "a", "name": "instructions.txt"},
        {"_id": "b", "name": "instructions.txt"},
    ]
    with pytest.raises(ResolutionError, match="multiple"):
        fetch_instructions_for_run("run_folder", girder)


# -- resolve_leaf_coords -----------------------------------------------------


def _resolve_girder_mock():
    """Mock for resolve_leaf_coords end-to-end tests."""
    instr_item = {"_id": "instr1", "name": "instructions.txt"}

    def _get(path, parameters=None):
        if path == "folder/raw_f":
            return {"_id": "raw_f", "name": "raw", "parentId": "run_id"}
        if path == "item" and parameters and parameters.get("folderId") == "run_id":
            return [instr_item]
        if path == "item/instr1/files":
            return [{"_id": "file1"}]
        raise AssertionError(f"unexpected: {path}")

    girder = MagicMock()
    girder.get.side_effect = _get

    def _download(file_id, buf):
        buf.write(FIXTURE_BYTES)

    girder.downloadFile.side_effect = _download
    return girder


def test_resolve_leaf_coords_end_to_end():
    item = {
        "_id": "leaf1",
        "name": "scan_point_17_master.h5",
        "folderId": "raw_f",
        "meta": {"experiment_date": "2026-04-16T16:56:16+00:00"},
    }
    girder = _resolve_girder_mock()
    result = resolve_leaf_coords(item, girder)
    assert result.station_x == 11.0
    assert result.station_y == 0.0
    assert result.source_timestamp_origin == "meta.experiment_date"
    assert result.station_coord_source["kind"] == "maxima_instructions"
    assert result.station_coord_source["scan_point_index"] == 17


def test_resolve_leaf_coords_missing_experiment_date_raises():
    item = {
        "_id": "leaf2",
        "name": "scan_point_17_master.h5",
        "folderId": "raw_f",
        "meta": {},
    }
    girder = _resolve_girder_mock()
    with pytest.raises(ResolutionError, match="missing meta.experiment_date"):
        resolve_leaf_coords(item, girder)


def test_resolve_leaf_coords_bad_filename_raises():
    item = {"_id": "leaf3", "name": "foo.tiff", "folderId": "raw_f", "meta": {}}
    girder = MagicMock()
    with pytest.raises(ResolutionError, match="does not encode a scan_point index"):
        resolve_leaf_coords(item, girder)
    girder.get.assert_not_called()


# -- find_master_h5_item_id --------------------------------------------------


def test_find_master_h5_item_id_found():
    def _get(path, parameters=None):
        if path == "folder":
            return [{"_id": "raw_sub", "name": "raw"}]
        if path == "item":
            return [
                {"_id": "m0", "name": "scan_point_0_master.h5"},
                {"_id": "m3", "name": "scan_point_3_master.h5"},
            ]
        raise AssertionError(f"unexpected: {path}")

    girder = MagicMock()
    girder.get.side_effect = _get
    assert find_master_h5_item_id("run_f", 3, girder) == "m3"


def test_find_master_h5_item_id_missing_raw_subfolder():
    girder = MagicMock()
    girder.get.return_value = []
    assert find_master_h5_item_id("run_f", 0, girder) is None


# -- heal_maxima_derived_parent -----------------------------------------------


def test_heal_maxima_derived_parent_success():
    derived = {
        "_id": "d1",
        "name": "scan_point_3_scan.png",
        "folderId": "raw_f",
        "meta": {"prov": {"wasDerivedFrom": "wrong_id"}},
    }

    def _get(path, parameters=None):
        if path == "folder/raw_f":
            return {"_id": "raw_f", "name": "raw", "parentId": "run_id"}
        if path == "folder" and parameters and parameters.get("parentId") == "run_id":
            return [{"_id": "raw_sub", "name": "raw"}]
        if path == "item" and parameters and parameters.get("folderId") == "raw_sub":
            return [{"_id": "master3", "name": "scan_point_3_master.h5"}]
        raise AssertionError(f"unexpected: {path}, {parameters}")

    girder = MagicMock()
    girder.get.side_effect = _get
    assert heal_maxima_derived_parent(derived, girder) == "master3"


def test_heal_maxima_derived_parent_unparseable_name():
    derived = {"_id": "d2", "name": "random_file.csv", "folderId": "f1"}
    assert heal_maxima_derived_parent(derived, MagicMock()) is None
