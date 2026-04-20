"""Contract tests for instruments/ dispatch helpers.

These tests exercise only the public API of the instruments subpackage
(resolve_parent_item_id, resolve_leaf) — never calling helix.py or
maxima.py directly — so they catch regressions in any layer.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from helix_dagster.instruments import (
    resolve_leaf,
    resolve_parent_item_id,
)

FIXTURE = Path(__file__).parent / "fixtures" / "instructions_example.json"
FIXTURE_BYTES = FIXTURE.read_bytes()


# -- helpers -----------------------------------------------------------------

def _alpss_item(name: str = "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png"):
    return {
        "_id": "alpss1",
        "name": name,
        "meta": {"data_type": "pdv_alpss_output"},
    }


def _pdv_trace(stem: str = "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1"):
    return {"_id": "pdv1", "name": f"{stem}.csv"}


def _xrd_derived_item():
    return {
        "_id": "d1",
        "name": "scan_point_3_scan.png",
        "folderId": "raw_f",
        "meta": {"data_type": "xrd_derived", "prov": {"wasDerivedFrom": "old"}},
    }


def _xrd_raw_item(index: int = 0):
    return {
        "_id": "leaf1",
        "name": f"scan_point_{index}_master.h5",
        "folderId": "raw_f",
        "meta": {
            "data_type": "xrd_raw",
            "experiment_date": "2026-04-16T16:56:16+00:00",
        },
    }


def _maxima_girder_mock():
    """Girder mock sufficient for both heal_maxima_derived_parent and
    resolve_leaf_coords through the dispatch layer."""
    instr_item = {"_id": "instr1", "name": "instructions.txt"}

    def _get(path, parameters=None):
        if path == "folder/raw_f":
            return {"_id": "raw_f", "name": "raw", "parentId": "run_id"}
        if path == "item" and parameters and parameters.get("folderId") == "run_id":
            return [instr_item]
        if path == "item/instr1/files":
            return [{"_id": "file1"}]
        if path == "folder" and parameters and parameters.get("parentId") == "run_id":
            return [{"_id": "raw_sub", "name": "raw"}]
        if path == "item" and parameters and parameters.get("folderId") == "raw_sub":
            return [
                {"_id": "master3", "name": "scan_point_3_master.h5"},
            ]
        raise AssertionError(f"unexpected girder.get({path!r}, {parameters!r})")

    girder = MagicMock()
    girder.get.side_effect = _get

    def _download(file_id, buf):
        buf.write(FIXTURE_BYTES)

    girder.downloadFile.side_effect = _download
    return girder


# -- resolve_parent_item_id: HELIX ------------------------------------------

def test_resolve_parent_item_id_dispatch_helix_alpss():
    item = _alpss_item()
    trace = _pdv_trace()
    result = resolve_parent_item_id(item, pdv_inventory=[trace])
    assert result == "pdv1"


def test_resolve_parent_item_id_helix_requires_inventory_kwarg():
    item = _alpss_item()
    with pytest.raises(TypeError, match="pdv_inventory"):
        resolve_parent_item_id(item)


# -- resolve_parent_item_id: MAXIMA ------------------------------------------

def test_resolve_parent_item_id_dispatch_maxima_derived():
    item = _xrd_derived_item()
    girder = _maxima_girder_mock()
    result = resolve_parent_item_id(item, girder=girder)
    assert result == "master3"


def test_resolve_parent_item_id_maxima_requires_girder_kwarg():
    item = _xrd_derived_item()
    with pytest.raises(TypeError, match="girder"):
        resolve_parent_item_id(item)


# -- resolve_parent_item_id: edge cases -------------------------------------

def test_resolve_parent_item_id_returns_none_for_leaf():
    item = _xrd_raw_item()
    assert resolve_parent_item_id(item) is None


def test_resolve_parent_item_id_returns_none_for_out_of_scope():
    item = {"_id": "x", "meta": {"data_type": "xrd_metadata"}}
    assert resolve_parent_item_id(item) is None


def test_resolve_parent_item_id_returns_none_for_missing_data_type():
    assert resolve_parent_item_id({"_id": "x"}) is None


# -- resolve_leaf ------------------------------------------------------------

def test_resolve_leaf_dispatch_maxima_xrd_raw():
    item = _xrd_raw_item(index=17)
    girder = _maxima_girder_mock()
    result = resolve_leaf(item, girder=girder)
    assert result.station_x == 11.0
    assert result.station_y == 0.0
    assert result.station_coord_source["kind"] == "maxima_instructions"
    assert result.station_coord_source["scan_point_index"] == 17


def test_resolve_leaf_raises_for_derived():
    item = _xrd_derived_item()
    with pytest.raises(TypeError, match="non-leaf"):
        resolve_leaf(item, girder=MagicMock())


def test_resolve_leaf_raises_for_helix_alpss():
    item = _alpss_item()
    with pytest.raises(TypeError, match="non-leaf"):
        resolve_leaf(item, girder=MagicMock())


def test_resolve_leaf_requires_girder_kwarg():
    item = _xrd_raw_item()
    with pytest.raises(TypeError, match="girder"):
        resolve_leaf(item)
