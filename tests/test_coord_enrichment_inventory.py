"""Tests for aimdl_coord_enrichment.coord_enrichment.inventory."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from aimdl_coord_enrichment.coord_enrichment.inventory import (
    MAXIMA_RUN_PARTITIONS,
    PARTITION_AWARE_DATA_TYPES,
    _is_in_scope,
    enrichable_items_inventory,
    inventory_nonempty_per_instrument,
)
from aimdl_coord_enrichment.instruments import all_in_scope_data_types, instrument_for_data_type


def _make_item(data_type, igsn=None, **extra_meta):
    meta = {"data_type": data_type}
    if igsn is not None:
        meta["igsn"] = igsn
    meta.update(extra_meta)
    return {"_id": "fake_id", "name": "fake_name", "meta": meta}


# ── enrichable_items_inventory ──────────────────────────────────────


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_returns_all_in_scope_data_type_keys(mock_datafiles, mock_partition):
    mock_datafiles.return_value = []
    mock_partition.return_value = []
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


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_filters_out_missing_igsn(mock_datafiles, mock_partition):
    def partition_side_effect(_client, dt):
        if dt == "xrd_raw":
            return [
                _make_item("xrd_raw", igsn="JHAMAB00001"),
                _make_item("xrd_raw"),  # no igsn
            ]
        return []

    mock_partition.side_effect = partition_side_effect
    mock_datafiles.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    assert len(result["MAXIMA/xrd_raw"]) == 1


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_keeps_items_with_igsn(mock_datafiles, mock_partition):
    def partition_side_effect(_client, dt):
        if dt == "xrf_raw":
            return [_make_item("xrf_raw", igsn="JHAMAB00002")]
        return []

    mock_partition.side_effect = partition_side_effect
    mock_datafiles.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    assert len(result["MAXIMA/xrf_raw"]) == 1
    assert result["MAXIMA/xrf_raw"][0]["meta"]["igsn"] == "JHAMAB00002"


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_key_format_is_instrument_slash_data_type(
    mock_datafiles, mock_partition,
):
    mock_datafiles.return_value = []
    mock_partition.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    for key in result:
        parts = key.split("/")
        assert len(parts) == 2, f"Key {key!r} not in INSTRUMENT/data_type format"
        instrument, dt = parts
        assert instrument in ("HELIX", "MAXIMA")
        assert dt in all_in_scope_data_types()


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_logs_per_data_type_counts(
    mock_datafiles, mock_partition, capsys,
):
    mock_datafiles.return_value = []
    mock_partition.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    enrichable_items_inventory(ctx, girder)

    # Dagster's build_asset_context uses a standard logger; verifying
    # that logging doesn't error is the primary check here.
    # TODO: assert log output contains data_type names once Dagster
    # test log capture is wired up.


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_uses_partition_api_for_maxima_types(
    mock_datafiles, mock_partition,
):
    """MAXIMA partition-aware types must route through the partition API."""
    mock_datafiles.return_value = []
    mock_partition.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    enrichable_items_inventory(ctx, girder)

    partition_dts = {call.args[1] for call in mock_partition.call_args_list}
    # The inventory only fetches in-scope types; xrd_metadata is
    # partition-aware but out-of-scope, so it's not fetched.
    expected = PARTITION_AWARE_DATA_TYPES & all_in_scope_data_types()
    assert partition_dts == expected
    # Those same types must NOT have been fetched via /aimdl/datafiles.
    datafiles_dts = {call.args[1] for call in mock_datafiles.call_args_list}
    assert datafiles_dts.isdisjoint(PARTITION_AWARE_DATA_TYPES)


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_inventory_uses_datafiles_for_helix(mock_datafiles, mock_partition):
    """HELIX data types (pdv_alpss_*) still use /aimdl/datafiles."""
    mock_datafiles.return_value = []
    mock_partition.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    enrichable_items_inventory(ctx, girder)

    datafiles_dts = {call.args[1] for call in mock_datafiles.call_args_list}
    helix_dts = all_in_scope_data_types() - PARTITION_AWARE_DATA_TYPES
    assert helix_dts, "HELIX data types expected in scope"
    assert helix_dts <= datafiles_dts


@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_items_by_partition")
@patch("aimdl_coord_enrichment.coord_enrichment.inventory.fetch_all_aimdl_datafiles")
def test_items_carry_full_meta_through_inventory(mock_datafiles, mock_partition):
    """Regression: partition-sourced items keep full meta (experiment_date)."""
    def partition_side_effect(_client, dt):
        if dt == "xrd_raw":
            return [
                _make_item(
                    "xrd_raw",
                    igsn="JHAMAB00007",
                    experiment_date="2026-04-16",
                    prov={"wasDerivedFrom": "parent-id"},
                ),
            ]
        return []

    mock_partition.side_effect = partition_side_effect
    mock_datafiles.return_value = []
    girder = MagicMock()
    ctx = build_asset_context()

    result = enrichable_items_inventory(ctx, girder)

    assert len(result["MAXIMA/xrd_raw"]) == 1
    meta = result["MAXIMA/xrd_raw"][0]["meta"]
    assert meta["experiment_date"] == "2026-04-16"
    assert meta["prov"] == {"wasDerivedFrom": "parent-id"}


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


# ── MAXIMA_RUN_PARTITIONS ───────────────────────────────────────────


def test_partition_definition_shape():
    """One partition per AIMD-L run — no data_type dimension.

    A run is the unit of work: one instructions.txt covers every file the run
    produced, so splitting by data_type would re-fetch and re-parse it per type.
    """
    from dagster import DagsterInstance, DynamicPartitionsDefinition

    assert isinstance(MAXIMA_RUN_PARTITIONS, DynamicPartitionsDefinition)
    assert MAXIMA_RUN_PARTITIONS.name == "maxima_run"
    with DagsterInstance.ephemeral() as instance:
        assert MAXIMA_RUN_PARTITIONS.get_partition_keys(
            dynamic_partitions_store=instance
        ) == []


def test_registered_run_keys_are_the_partition_keys():
    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions("maxima_run", ["IGSN1//T1", "IGSN2//T2"])
        keys = MAXIMA_RUN_PARTITIONS.get_partition_keys(
            dynamic_partitions_store=instance
        )
    assert sorted(keys) == ["IGSN1//T1", "IGSN2//T2"]


