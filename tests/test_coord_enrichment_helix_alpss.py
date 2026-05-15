"""Integration-style tests for enriched_helix_alpss asset."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import CoordTransformSnapshot
from helix_dagster.coord_enrichment.check_support import (
    evaluate_coord_failures,
    evaluate_success_rate,
)
from helix_dagster.coord_enrichment.helix_alpss_leaf import enriched_helix_alpss


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

EXAMPLE_TS = datetime(2026, 2, 18, 18, 45, 56, tzinfo=timezone.utc)

ALL_PARTITIONS = [
    "HELIX/pdv_alpss_output",
    "HELIX/pdv_alpss_result",
    "HELIX/pdv_alpss_results",
]


def _snapshot(yaml_sha="abc123", version="0.3.0"):
    return CoordTransformSnapshot(
        yaml_path="test.yaml",
        yaml_sha256=yaml_sha,
        transformer_version=version,
    )


def _parent(
    item_id="parent_1",
    data_type="pdv_trace",
    station_x=8.0,
    station_y=8.0,
    source_timestamp="2026-02-18T18:45:56+00:00",
    transform_version="HELIX/v1",
    include_prov=True,
):
    meta = {
        "Station_X": station_x,
        "Station_Y": station_y,
        "data_type": data_type,
    }
    if include_prov:
        meta["coord_provenance"] = {
            "instrument": "HELIX",
            "transform_version": transform_version,
            "source_timestamp": source_timestamp,
        }
    return {"_id": item_id, "meta": meta}


def _alpss_item(
    item_id="alpss1",
    name="shot15_ch1-iq.png",
    data_type="pdv_alpss_output",
    parent_id="parent_1",
    *,
    coord_prov=None,
):
    meta = {
        "data_type": data_type,
        "igsn": "JHAMAL00018-005",
        "prov": {"wasDerivedFrom": parent_id},
    }
    if coord_prov is not None:
        meta["coord_provenance"] = coord_prov
    return {"_id": item_id, "name": name, "meta": meta}


def _empty_inventory():
    return {pk: [] for pk in ALL_PARTITIONS}


def _run_asset(
    items,
    *,
    partition_key="HELIX/pdv_alpss_output",
    dry_run=True,
    snapshot=None,
    parent=None,
    transform_fn=None,
):
    """Run enriched_helix_alpss with full mocking."""
    inventory = _empty_inventory()
    inventory[partition_key] = items

    girder = MagicMock()
    parent_item = parent or _parent()
    girder.get.return_value = parent_item

    config = CoordEnrichmentConfig(dry_run=dry_run)
    snap = snapshot or _snapshot()
    ctx = build_asset_context(partition_key=partition_key)

    tfn = transform_fn or (lambda inst, ver, sx, sy: (32.0, 8.0))

    with patch(
        "helix_dagster.coord_enrichment.helix_alpss_leaf.transform_with_named_version",
        side_effect=tfn,
    ):
        result = enriched_helix_alpss(ctx, config, inventory, snap, girder)

    return result, girder


# ---- Core tests ----


def test_single_alpss_item_live():
    item = _alpss_item()
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    call_args = girder.addMetadataToItem.call_args
    item_id_arg = call_args[0][0]
    payload = call_args[0][1]

    assert item_id_arg == "alpss1"
    assert payload["Station_X"] == 8.0
    assert payload["Station_Y"] == 8.0
    assert payload["Sample_X"] == 32.0
    assert payload["Sample_Y"] == 8.0
    assert "coord_provenance" in payload
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "HELIX"
    assert prov["transform_version"] == "HELIX/v1"
    assert prov["source_timestamp_origin"] == "inherited_from_parent"
    assert prov["station_coord_source"]["kind"] == "inherited"
    assert prov["station_coord_source"]["parent_item_id"] == "parent_1"
    assert prov["station_coord_source"]["parent_data_type"] == "pdv_trace"
    assert result["counts"]["written"] == 1


def test_single_alpss_item_dry_run():
    item = _alpss_item()
    result, girder = _run_asset([item], dry_run=True)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["seen"] == 1
    assert result["counts"]["simulated_dry_run"] == 1
    assert result["counts"]["written"] == 0
    assert "HELIX/v1" in result["version_counter"]


def test_alpss_inherits_v2_transform_when_parent_v2():
    parent = _parent(transform_version="HELIX/v2")

    def v2_transform(inst, ver, sx, sy):
        assert ver == "HELIX/v2"
        return (8.0, 8.0)

    item = _alpss_item()
    result, girder = _run_asset(
        [item], dry_run=False, parent=parent, transform_fn=v2_transform,
    )

    girder.addMetadataToItem.assert_called_once()
    payload = girder.addMetadataToItem.call_args[0][1]
    assert payload["Sample_X"] == 8.0
    assert payload["Sample_Y"] == 8.0
    prov = payload["coord_provenance"]
    assert prov["transform_version"] == "HELIX/v2"
    assert result["counts"]["written"] == 1
    assert "HELIX/v2" in result["version_counter"]


def test_alpss_skips_when_parent_not_enriched():
    parent = _parent(station_x=None, include_prov=True)
    # Parent missing Station_X → inherit_from_parent raises ResolutionError
    parent["meta"]["Station_X"] = None

    item = _alpss_item()
    result, girder = _run_asset([item], dry_run=False, parent=parent)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["resolution_errors"] == 1
    assert len(result["resolution_errors"]) == 1
    assert result["resolution_errors"][0]["stage"] == "inherit_from_parent"


def test_alpss_skips_when_stored_prov_identical():
    from helix_dagster.provenance import build_coord_provenance
    from helix_dagster import __version__

    stored_prov = build_coord_provenance(
        instrument="HELIX",
        transform_version="HELIX/v1",
        transform_yaml_sha256="abc123",
        transformer_version="0.3.0",
        pipeline_version=__version__,
        source_timestamp=EXAMPLE_TS,
        source_timestamp_origin="inherited_from_parent",
        station_coord_source={
            "kind": "inherited",
            "parent_item_id": "parent_1",
            "parent_data_type": "pdv_trace",
        },
        dagster_run_id="old-run",
    )

    item = _alpss_item(coord_prov=stored_prov)
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["skipped_no_change"] == 1


def test_alpss_overwrites_when_yaml_sha_differs():
    stored_prov = {
        "instrument": "HELIX",
        "transform_version": "HELIX/v1",
        "transform_yaml_sha256": "OLD_SHA",
        "transformer_version": "0.3.0",
        "station_coord_source": {
            "kind": "inherited",
            "parent_item_id": "parent_1",
            "parent_data_type": "pdv_trace",
        },
    }
    item = _alpss_item(coord_prov=stored_prov)
    result, girder = _run_asset([item], dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    assert result["counts"]["written"] == 1


def test_alpss_partition_filters_items():
    output_item = _alpss_item("out1", data_type="pdv_alpss_output")
    result_item = _alpss_item("res1", data_type="pdv_alpss_result")

    inventory = _empty_inventory()
    inventory["HELIX/pdv_alpss_output"] = [output_item]
    inventory["HELIX/pdv_alpss_result"] = [result_item]

    girder = MagicMock()
    girder.get.return_value = _parent()
    config = CoordEnrichmentConfig(dry_run=False)
    snap = _snapshot()
    ctx = build_asset_context(partition_key="HELIX/pdv_alpss_output")

    with patch(
        "helix_dagster.coord_enrichment.helix_alpss_leaf.transform_with_named_version",
        return_value=(32.0, 8.0),
    ):
        result = enriched_helix_alpss(ctx, config, inventory, snap, girder)

    assert result["counts"]["seen"] == 1
    assert result["partition_key"] == "HELIX/pdv_alpss_output"
    girder.addMetadataToItem.assert_called_once()
    written_id = girder.addMetadataToItem.call_args[0][0]
    assert written_id == "out1"


@pytest.mark.parametrize("partition_key", ALL_PARTITIONS)
def test_alpss_all_three_partitions_work_identically(partition_key):
    dt = partition_key.split("/", 1)[1]
    item = _alpss_item(data_type=dt)
    result, girder = _run_asset([item], partition_key=partition_key, dry_run=False)

    girder.addMetadataToItem.assert_called_once()
    assert result["counts"]["written"] == 1
    assert result["partition_key"] == partition_key


def test_coord_failure_counted():
    def fail_transform(inst, ver, sx, sy):
        return (None, None)

    item = _alpss_item()
    result, girder = _run_asset([item], dry_run=False, transform_fn=fail_transform)

    girder.addMetadataToItem.assert_not_called()
    assert result["counts"]["coord_failures"] == 1


# ---- Check tests ----


def test_enrichment_success_rate_check_passes():
    item = _alpss_item()
    result, _ = _run_asset([item], dry_run=False)
    assert _success_rate(result).passed is True


def test_enrichment_success_rate_check_fails_on_all_errors():
    parent = _parent(station_x=None)
    item = _alpss_item()
    result, _ = _run_asset([item], dry_run=False, parent=parent)
    assert _success_rate(result).passed is False


def test_enrichment_success_rate_check_empty_partition():
    result, _ = _run_asset([], dry_run=False)
    assert _success_rate(result).passed is True


def test_no_coord_transform_failures_check_passes():
    item = _alpss_item()
    result, _ = _run_asset([item], dry_run=False)
    assert evaluate_coord_failures(result["counts"]["coord_failures"]).passed is True


def test_no_coord_transform_failures_check_fails():
    def fail_transform(inst, ver, sx, sy):
        return (None, None)

    item = _alpss_item()
    result, _ = _run_asset([item], dry_run=False, transform_fn=fail_transform)
    assert evaluate_coord_failures(result["counts"]["coord_failures"]).passed is False
