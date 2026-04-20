"""Tests for helix_pdv_coverage_observer asset and check."""

from unittest.mock import patch

from dagster import build_asset_context

from helix_dagster.coord_enrichment.pdv_observer import (
    PDV_COVERAGE_WARN_THRESHOLD,
    helix_pdv_coverage_observer,
    pdv_coverage_above_threshold,
)


def _item(igsn=None, station_x=None, station_y=None, coord_prov=None):
    meta = {"data_type": "pdv_trace"}
    if igsn is not None:
        meta["igsn"] = igsn
    if station_x is not None:
        meta["Station_X"] = station_x
    if station_y is not None:
        meta["Station_Y"] = station_y
    if coord_prov is not None:
        meta["coord_provenance"] = coord_prov
    return {"_id": "item-1", "name": "trace.h5", "meta": meta}


def _run_observer(items):
    ctx = build_asset_context()
    with patch(
        "helix_dagster.coord_enrichment.pdv_observer.fetch_all_aimdl_datafiles",
        return_value=items,
    ):
        return helix_pdv_coverage_observer(ctx, None)


def test_observer_counts_fully_enriched():
    items = [
        _item("IGSN001", 1.0, 2.0, {"version": "v1"}),
        _item("IGSN002", 3.0, 4.0, {"version": "v1"}),
    ]
    result = _run_observer(items)
    assert result["fully_enriched"] == 2
    assert result["partial"] == 0
    assert result["unenriched"] == 0
    assert result["missing_igsn"] == 0


def test_observer_counts_partial():
    items = [
        _item("IGSN001", 1.0, 2.0),  # coords only
        _item("IGSN002", coord_prov={"version": "v1"}),  # prov only
    ]
    result = _run_observer(items)
    assert result["partial"] == 2
    assert result["fully_enriched"] == 0


def test_observer_counts_unenriched():
    items = [_item("IGSN001"), _item("IGSN002"), _item("IGSN003")]
    result = _run_observer(items)
    assert result["unenriched"] == 3
    assert result["fully_enriched"] == 0
    assert result["partial"] == 0


def test_observer_counts_missing_igsn():
    items = [_item(), _item()]
    result = _run_observer(items)
    assert result["missing_igsn"] == 2
    assert result["unenriched"] == 0
    assert result["fully_enriched"] == 0
    assert result["partial"] == 0


def test_observer_coverage_rate():
    items = [
        _item("IGSN001", 1.0, 2.0, {"version": "v1"}),
        _item("IGSN002"),
        _item("IGSN003"),
        _item("IGSN004"),
    ]
    result = _run_observer(items)
    assert result["coverage_rate"] == 0.25
    assert result["total"] == 4


def test_observer_empty():
    result = _run_observer([])
    assert result["total"] == 0
    assert result["coverage_rate"] == 0.0


def test_pdv_coverage_check_passes_above_threshold():
    obs = {"coverage_rate": PDV_COVERAGE_WARN_THRESHOLD + 0.1,
           "fully_enriched": 6, "total": 10}
    ctx = build_asset_context()
    result = pdv_coverage_above_threshold(ctx, obs)
    assert result.passed


def test_pdv_coverage_check_warns_below_threshold():
    obs = {"coverage_rate": PDV_COVERAGE_WARN_THRESHOLD - 0.1,
           "fully_enriched": 4, "total": 10}
    ctx = build_asset_context()
    result = pdv_coverage_above_threshold(ctx, obs)
    assert not result.passed
