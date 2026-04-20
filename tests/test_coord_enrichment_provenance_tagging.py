"""Tests for provenance_tagged_items asset and checks."""

from unittest.mock import MagicMock

import pytest
from dagster import AssetCheckSeverity, build_asset_context

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.provenance_tagging import (
    all_helix_alpss_tagged,
    maxima_prov_targets_resolve,
    provenance_tagged_items,
)


def _make_alpss_item(item_id, name, *, igsn="JHAMAC00003-S1R4C3", prov=None):
    meta = {"data_type": "pdv_alpss_output", "igsn": igsn}
    if prov is not None:
        meta["prov"] = prov
    return {"_id": item_id, "name": name, "meta": meta}


def _make_pdv_trace(item_id, name, *, igsn="JHAMAC00003-S1R4C3"):
    return {"_id": item_id, "name": name, "meta": {"data_type": "pdv_trace", "igsn": igsn}}


def _make_xrd_derived(item_id, name, *, igsn="JHAMAB00019-12", prov=None):
    meta = {"data_type": "xrd_derived", "igsn": igsn}
    if prov is not None:
        meta["prov"] = prov
    return {"_id": item_id, "name": name, "meta": meta}


def _empty_inventory():
    """Inventory with all partition keys but empty lists."""
    return {
        "HELIX/pdv_alpss_output": [],
        "HELIX/pdv_alpss_result": [],
        "HELIX/pdv_alpss_results": [],
        "MAXIMA/xrd_derived": [],
        "MAXIMA/xrd_raw": [],
        "MAXIMA/xrf_raw": [],
    }


def _run_asset(inventory, girder, *, dry_run=False, pdv_traces=None, monkeypatch=None):
    """Helper to run provenance_tagged_items with mocks."""
    if pdv_traces is not None and monkeypatch is not None:
        monkeypatch.setattr(
            "helix_dagster.coord_enrichment.provenance_tagging.fetch_all_aimdl_datafiles",
            lambda g, dt: pdv_traces,
        )
    config = CoordEnrichmentConfig(dry_run=dry_run)
    ctx = build_asset_context()
    return provenance_tagged_items(ctx, config, inventory, girder)


# --- Test 1: HELIX ALPSS missing prov gets written ---

def test_helix_alpss_missing_prov_written():
    alpss = _make_alpss_item(
        "alpss1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png",
    )
    pdv = _make_pdv_trace(
        "pdv1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1.csv",
    )
    inv = _empty_inventory()
    inv["HELIX/pdv_alpss_output"] = [alpss]
    inv["HELIX/pdv_trace"] = [pdv]

    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    girder.addMetadataToItem.assert_called_once_with(
        "alpss1", {"prov": {"wasDerivedFrom": "pdv1"}}
    )
    assert result["counters"]["HELIX/pdv_alpss_output"]["written"] == 1


# --- Test 2: already_correct → no write ---

def test_helix_alpss_already_correct_no_write():
    alpss = _make_alpss_item(
        "alpss1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png",
        prov={"wasDerivedFrom": "pdv1"},
    )
    pdv = _make_pdv_trace(
        "pdv1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1.csv",
    )
    inv = _empty_inventory()
    inv["HELIX/pdv_alpss_output"] = [alpss]
    inv["HELIX/pdv_trace"] = [pdv]

    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counters"]["HELIX/pdv_alpss_output"]["already_correct"] == 1


# --- Test 3: no parent → unresolved ---

def test_helix_alpss_no_parent_recorded_as_unresolved():
    alpss = _make_alpss_item(
        "alpss1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png",
    )
    inv = _empty_inventory()
    inv["HELIX/pdv_alpss_output"] = [alpss]
    inv["HELIX/pdv_trace"] = []  # empty PDV inventory

    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counters"]["HELIX/pdv_alpss_output"]["unresolvable"] == 1
    assert len(result["unresolved"]) == 1
    assert result["unresolved"][0]["item_id"] == "alpss1"


# --- Test 4: MAXIMA dangling prov overwritten ---

def test_maxima_xrd_derived_dangling_prov_overwritten(monkeypatch):
    xrd = _make_xrd_derived(
        "xrd1", "scan_point_0_scan.png",
        prov={"wasDerivedFrom": "DANGLING", "wasGeneratedBy": "amdee_xrd-0.1.4"},
    )
    inv = _empty_inventory()
    inv["MAXIMA/xrd_derived"] = [xrd]

    monkeypatch.setattr(
        "helix_dagster.instruments.maxima.heal_maxima_derived_parent",
        lambda item, girder: "master_h5_id",
    )
    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    girder.addMetadataToItem.assert_called_once_with(
        "xrd1",
        {"prov": {"wasDerivedFrom": "master_h5_id", "wasGeneratedBy": "amdee_xrd-0.1.4"}},
    )
    assert result["counters"]["MAXIMA/xrd_derived"]["overwritten"] == 1


# --- Test 5: prov merge preserves other keys ---

def test_maxima_prov_preserves_other_keys(monkeypatch):
    xrd = _make_xrd_derived(
        "xrd1", "scan_point_0_scan.png",
        prov={"wasDerivedFrom": "OLD", "wasGeneratedBy": "amdee_xrd-0.1.4"},
    )
    inv = _empty_inventory()
    inv["MAXIMA/xrd_derived"] = [xrd]

    monkeypatch.setattr(
        "helix_dagster.instruments.maxima.heal_maxima_derived_parent",
        lambda item, girder: "NEW_PARENT",
    )
    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    call_args = girder.addMetadataToItem.call_args
    written_prov = call_args[0][1]["prov"]
    assert written_prov["wasDerivedFrom"] == "NEW_PARENT"
    assert written_prov["wasGeneratedBy"] == "amdee_xrd-0.1.4"


# --- Test 6: dry_run does not call Girder ---

def test_dry_run_does_not_call_girder():
    alpss = _make_alpss_item(
        "alpss1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png",
    )
    pdv = _make_pdv_trace(
        "pdv1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1.csv",
    )
    inv = _empty_inventory()
    inv["HELIX/pdv_alpss_output"] = [alpss]
    inv["HELIX/pdv_trace"] = [pdv]

    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=True)

    girder.addMetadataToItem.assert_not_called()
    assert result["counters"]["HELIX/pdv_alpss_output"]["skipped_dry_run"] == 1
    assert len(result["write_ops"]) == 1
    assert result["write_ops"][0]["simulated"] is True


# --- Test 7: adapter exception → unresolved, no crash ---

def test_unresolved_adapter_exception_does_not_crash_asset(monkeypatch):
    alpss = _make_alpss_item(
        "alpss1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png",
    )
    inv = _empty_inventory()
    inv["HELIX/pdv_alpss_output"] = [alpss]
    inv["HELIX/pdv_trace"] = [_make_pdv_trace("pdv1", "dummy.csv")]

    monkeypatch.setattr(
        "helix_dagster.instruments.helix.find_parent_pdv_item_id",
        lambda item, inv: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counters"]["HELIX/pdv_alpss_output"]["unresolvable"] == 1
    assert len(result["unresolved"]) == 1


# --- Test 8: check passes when no unresolved ---

def test_check_all_helix_alpss_tagged_passes_when_empty_unresolved():
    ctx = build_asset_context()
    prov_result = {"unresolved": [], "counters": {}, "write_ops": [], "dry_run": False}
    check_result = all_helix_alpss_tagged(ctx, prov_result)
    assert check_result.passed is True


# --- Test 9: check errors on HELIX unresolved ---

def test_check_all_helix_alpss_tagged_errors_on_helix_unresolved():
    ctx = build_asset_context()
    prov_result = {
        "unresolved": [
            {"partition": "HELIX/pdv_alpss_output", "item_id": "a1", "name": "foo.png"},
        ],
        "counters": {},
        "write_ops": [],
        "dry_run": False,
    }
    check_result = all_helix_alpss_tagged(ctx, prov_result)
    assert check_result.passed is False
    assert check_result.severity == AssetCheckSeverity.ERROR


# --- Test 10: MAXIMA check ignores HELIX unresolved ---

def test_check_maxima_prov_targets_resolve_ignores_helix_unresolved():
    ctx = build_asset_context()
    prov_result = {
        "unresolved": [
            {"partition": "HELIX/pdv_alpss_output", "item_id": "a1", "name": "foo.png"},
        ],
        "counters": {},
        "write_ops": [],
        "dry_run": False,
    }
    check_result = maxima_prov_targets_resolve(ctx, prov_result)
    assert check_result.passed is True


# --- Test 11: fetches pdv_trace when inventory lacks key ---

def test_fetches_pdv_trace_when_inventory_lacks_key(monkeypatch):
    alpss = _make_alpss_item(
        "alpss1",
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png",
    )
    pdv_traces = [
        _make_pdv_trace("pdv1", "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1.csv"),
        _make_pdv_trace("pdv2", "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot02_ch1.csv"),
    ]
    inv = _empty_inventory()
    inv["HELIX/pdv_alpss_output"] = [alpss]
    # No HELIX/pdv_trace key — forces the fallback fetch

    fetch_calls = []
    def mock_fetch(girder, dt):
        fetch_calls.append(dt)
        return pdv_traces

    monkeypatch.setattr(
        "helix_dagster.coord_enrichment.provenance_tagging.fetch_all_aimdl_datafiles",
        mock_fetch,
    )

    girder = MagicMock()
    result = _run_asset(inv, girder, dry_run=False)

    assert len(fetch_calls) == 1
    assert fetch_calls[0] == "pdv_trace"
    girder.addMetadataToItem.assert_called_once()
    assert result["counters"]["HELIX/pdv_alpss_output"]["written"] == 1
