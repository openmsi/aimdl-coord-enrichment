"""Integration-style tests for enriched_maxima_derived asset."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import CoordTransformSnapshot
from helix_dagster.coord_enrichment.check_support import (
    evaluate_coord_failures,
    evaluate_provenance_valid,
    evaluate_success_rate,
)
from helix_dagster.coord_enrichment.maxima_derived_leaf import (
    enriched_maxima_derived,
)


def _success_rate(result):
    """Apply the success-rate decision to a leaf-return dict.

    The @asset_check wrapper reads these same numbers from the event
    log; this exercises the pure decision logic directly.
    """
    c = result["counts"]
    return evaluate_success_rate(
        seen=c["seen"],
        written=c["written"],
        simulated_dry_run=c["simulated_dry_run"],
        skipped_no_change=c["skipped_no_change"],
        resolution_errors=c["resolution_errors"],
        write_errors_count=len(result.get("write_errors", [])),
        partition_label=result["partition_key"],
    )


def _provenance_valid(result):
    """Apply the provenance-valid decision to a leaf-return dict."""
    inherit = [
        e for e in result.get("resolution_errors", [])
        if e.get("stage") == "inherit_from_parent"
    ]
    examples = ", ".join(
        f"{e.get('item_id', '?')}: {e.get('error', '')}" for e in inherit[:3]
    ) or "none"
    return evaluate_provenance_valid(len(inherit), examples)

EXAMPLE_TS = datetime(2026, 4, 16, 16, 56, 16, tzinfo=timezone.utc)

PARTITION_KEY = "MAXIMA/xrd_derived"


def _snapshot(yaml_sha="abc123", version="0.3.0"):
    return CoordTransformSnapshot(
        yaml_path="test.yaml",
        yaml_sha256=yaml_sha,
        transformer_version=version,
    )


def _parent(
    item_id="parent_master_h5",
    data_type="xrd_raw",
    station_x=11.0,
    station_y=-5.0,
    source_timestamp="2026-04-16T16:56:16+00:00",
    transform_version="MAXIMA/v1",
    include_prov=True,
):
    meta = {
        "Station_X": station_x,
        "Station_Y": station_y,
        "data_type": data_type,
    }
    if include_prov:
        meta["coord_provenance"] = {
            "instrument": "MAXIMA",
            "transform_version": transform_version,
            "source_timestamp": source_timestamp,
        }
    return {"_id": item_id, "meta": meta}


def _derived_item(
    item_id="derived1",
    name="scan_point_17_xrd.csv",
    parent_id="parent_master_h5",
    *,
    coord_prov=None,
):
    meta = {
        "data_type": "xrd_derived",
        "igsn": "JHAMAB00019-12",
        "prov": {"wasDerivedFrom": parent_id},
    }
    if coord_prov is not None:
        meta["coord_provenance"] = coord_prov
    return {"_id": item_id, "name": name, "meta": meta}


def _empty_inventory():
    return {PARTITION_KEY: []}


def _run_asset(
    items,
    *,
    dry_run=True,
    snapshot=None,
    parent=None,
    transform_fn=None,
):
    """Run enriched_maxima_derived with full mocking."""
    inventory = _empty_inventory()
    inventory[PARTITION_KEY] = items

    girder = MagicMock()
    parent_item = parent or _parent()
    girder.get.return_value = parent_item

    config = CoordEnrichmentConfig(dry_run=dry_run)
    snap = snapshot or _snapshot()
    ctx = build_asset_context(partition_key=PARTITION_KEY)

    tfn = transform_fn or (lambda inst, ver, sx, sy: (25.0, 25.0))

    with patch(
        "helix_dagster.coord_enrichment.maxima_derived_leaf.transform_with_named_version",
        side_effect=tfn,
    ):
        result = enriched_maxima_derived(ctx, config, inventory, snap, girder)

    return result, girder


# ---- Core tests ----


def test_single_derived_item_live():
    item = _derived_item()
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    call_args = girder.addMetadataToItem.call_args
    item_id_arg = call_args[0][0]
    payload = call_args[0][1]

    assert item_id_arg == "derived1"
    assert payload["Station_X"] == 11.0
    assert payload["Station_Y"] == -5.0
    assert payload["Sample_X"] == 25.0
    assert payload["Sample_Y"] == 25.0
    assert "coord_provenance" in payload
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "MAXIMA"
    assert prov["transform_version"] == "MAXIMA/v1"
    assert prov["source_timestamp_origin"] == "inherited_from_parent"
    assert prov["station_coord_source"]["kind"] == "inherited"
    assert prov["station_coord_source"]["parent_item_id"] == "parent_master_h5"
    assert prov["station_coord_source"]["parent_data_type"] == "xrd_raw"
    assert result["counts"]["written"] == 1


def test_dry_run_does_not_write():
    item = _derived_item()
    result, girder = _run_asset([item], dry_run=True)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["seen"] == 1
    assert result["counts"]["simulated_dry_run"] == 1
    assert result["counts"]["written"] == 0
    assert "MAXIMA/v1" in result["version_counter"]


def test_skips_when_parent_not_enriched():
    parent = _parent(station_x=None)

    item = _derived_item()
    result, girder = _run_asset([item], dry_run=False, parent=parent)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 1
    assert len(result["resolution_errors"]) == 1
    assert result["resolution_errors"][0]["stage"] == "inherit_from_parent"


def test_skips_when_stored_prov_identical():
    from helix_dagster.provenance import build_coord_provenance
    from helix_dagster import __version__

    stored_prov = build_coord_provenance(
        instrument="MAXIMA",
        transform_version="MAXIMA/v1",
        transform_yaml_sha256="abc123",
        transformer_version="0.3.0",
        pipeline_version=__version__,
        source_timestamp=EXAMPLE_TS,
        source_timestamp_origin="inherited_from_parent",
        station_coord_source={
            "kind": "inherited",
            "parent_item_id": "parent_master_h5",
            "parent_data_type": "xrd_raw",
        },
        dagster_run_id="old-run",
    )

    item = _derived_item(coord_prov=stored_prov)
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["skipped_no_change"] == 1


def test_overwrites_when_yaml_sha_differs():
    stored_prov = {
        "instrument": "MAXIMA",
        "transform_version": "MAXIMA/v1",
        "transform_yaml_sha256": "OLD_SHA",
        "transformer_version": "0.3.0",
        "station_coord_source": {
            "kind": "inherited",
            "parent_item_id": "parent_master_h5",
            "parent_data_type": "xrd_raw",
        },
    }
    item = _derived_item(coord_prov=stored_prov)
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    assert result["counts"]["written"] == 1


def test_inherits_parent_transform_version_verbatim():
    pytest.skip(
        "MAXIMA v2 not registered in YAML; test becomes live when a "
        "second version is added."
    )


def test_coord_failure_counted():
    def fail_transform(inst, ver, sx, sy):
        return (None, None)

    item = _derived_item()
    result, girder = _run_asset([item], dry_run=False, transform_fn=fail_transform)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["coord_failures"] == 1


# ---- Check tests ----


def test_enrichment_success_rate_check_passes():
    item = _derived_item()
    result, _ = _run_asset([item], dry_run=False)
    assert _success_rate(result).passed is True


def test_enrichment_success_rate_check_fails_on_all_errors():
    parent = _parent(station_x=None)
    item = _derived_item()
    result, _ = _run_asset([item], dry_run=False, parent=parent)
    assert _success_rate(result).passed is False


def test_enrichment_success_rate_check_empty_partition():
    result, _ = _run_asset([], dry_run=False)
    assert _success_rate(result).passed is True


def test_no_coord_transform_failures_check_passes():
    item = _derived_item()
    result, _ = _run_asset([item], dry_run=False)
    assert evaluate_coord_failures(result["counts"]["coord_failures"]).passed is True


def test_no_coord_transform_failures_check_fails():
    def fail_transform(inst, ver, sx, sy):
        return (None, None)

    item = _derived_item()
    result, _ = _run_asset([item], dry_run=False, transform_fn=fail_transform)
    assert evaluate_coord_failures(result["counts"]["coord_failures"]).passed is False


# ---- Lineage + provenance_valid tests ----


def test_enriched_maxima_derived_depends_on_raw():
    """Step 6 lineage rewiring: derived depends on raw."""
    from dagster import AssetKey

    assert AssetKey(["enriched_maxima_raw"]) in enriched_maxima_derived.dependency_keys


def test_maxima_xrd_derived_provenance_valid_detects_missing_prov():
    fake_derived_output = {
        "resolution_errors": [
            {
                "item_id": "X1",
                "name": "foo.tif",
                "stage": "inherit_from_parent",
                "error": "derived item X1 has no prov.wasDerivedFrom or prov.isPartOf",
            },
            {
                "item_id": "X2",
                "name": "bar.tif",
                "stage": "experiment_date",
                "error": "item X2 missing meta.experiment_date",
            },
        ],
    }
    result = _provenance_valid(fake_derived_output)
    assert result.passed is False
    assert result.metadata["unresolved_count"].value == 1


def test_maxima_xrd_derived_provenance_valid_passes_on_clean():
    fake_derived_output = {"resolution_errors": []}
    result = _provenance_valid(fake_derived_output)
    assert result.passed is True


# ---- Step 1 precondition tests ----


def test_fast_fails_when_no_parents_enriched():
    unenriched_parents = [
        _parent(item_id=f"p{i}", station_x=None, include_prov=False)
        for i in range(3)
    ]
    inventory = {
        PARTITION_KEY: [_derived_item()],
        "MAXIMA/xrd_raw": unenriched_parents,
    }
    girder = MagicMock()
    config = CoordEnrichmentConfig(dry_run=True)
    snap = _snapshot()
    ctx = build_asset_context(partition_key=PARTITION_KEY)

    with pytest.raises(Exception) as excinfo:
        enriched_maxima_derived(ctx, config, inventory, snap, girder)

    msg = str(excinfo.value)
    assert "0/3" in msg
    assert "enriched_maxima_raw" in msg


def test_warns_when_some_parents_unenriched():
    raw_items = [
        _parent(item_id="p_ok", station_x=11.0, include_prov=True),
        _parent(item_id="p_bad", station_x=None, include_prov=False),
    ]
    inventory = {
        PARTITION_KEY: [],
        "MAXIMA/xrd_raw": raw_items,
    }
    girder = MagicMock()
    config = CoordEnrichmentConfig(dry_run=True)
    snap = _snapshot()
    ctx = build_asset_context(partition_key=PARTITION_KEY)

    warning_spy = MagicMock()
    ctx.log.warning = warning_spy

    result = enriched_maxima_derived(ctx, config, inventory, snap, girder)

    warning_spy.assert_called()
    fmt = warning_spy.call_args[0][0]
    assert "partial" in fmt.lower() or "parents enriched" in fmt
    assert result["counts"]["seen"] == 0


def test_emits_parent_count_metadata():
    raw_items = [
        _parent(item_id="p_ok", station_x=11.0, include_prov=True),
        _parent(item_id="p_bad", station_x=None, include_prov=False),
    ]
    inventory = {
        PARTITION_KEY: [],
        "MAXIMA/xrd_raw": raw_items,
    }
    girder = MagicMock()
    config = CoordEnrichmentConfig(dry_run=True)
    snap = _snapshot()
    ctx = build_asset_context(partition_key=PARTITION_KEY)
    ctx.add_output_metadata = MagicMock()

    enriched_maxima_derived(ctx, config, inventory, snap, girder)

    ctx.add_output_metadata.assert_called_once()
    metadata = ctx.add_output_metadata.call_args[0][0]
    assert metadata["parents_total"].value == 2
    assert metadata["parents_enriched"].value == 1
