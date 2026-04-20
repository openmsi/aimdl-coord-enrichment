"""enrichable_items_inventory asset + partition definitions."""

from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)

from helix_dagster.girder_io import fetch_all_aimdl_datafiles
from helix_dagster.instruments import (
    all_in_scope_data_types,
    instrument_for_data_type,
)
from helix_dagster.resources import GirderConnection

MAXIMA_RAW_PARTITIONS = StaticPartitionsDefinition(
    ["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"]
)


def _partition_key(instrument: str, data_type: str) -> str:
    return f"{instrument}/{data_type}"


def _is_in_scope(item: dict) -> bool:
    """Apply the §7.5 scope gate to a single item."""
    meta = item.get("meta") or {}
    igsn = meta.get("igsn")
    if not igsn:
        return False
    dt = meta.get("data_type")
    if dt not in all_in_scope_data_types():
        return False
    return True


@asset
def enrichable_items_inventory(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> dict[str, list[dict[str, Any]]]:
    """Return all in-scope items for the coord_enrichment DAG,
    grouped by partition key "<INSTRUMENT>/<data_type>".

    Every in-scope data_type appears as a key, even when its item
    list is empty, so downstream consumers can dereference by key
    without defensive `.get`.
    """
    inventory: dict[str, list[dict[str, Any]]] = {}
    data_types = sorted(all_in_scope_data_types())
    context.log.info("Fetching inventory for data types: %s", data_types)

    for dt in data_types:
        items = fetch_all_aimdl_datafiles(girder, dt)
        in_scope = [it for it in items if _is_in_scope(it)]
        instrument = instrument_for_data_type(dt)
        key = _partition_key(instrument, dt)
        inventory[key] = in_scope
        context.log.info(
            "  %s: %d returned, %d in-scope", dt, len(items), len(in_scope)
        )

    context.add_output_metadata(
        {
            "partition_counts": MetadataValue.text(
                "\n".join(f"{k}: {len(v)}" for k, v in sorted(inventory.items()))
            ),
            "total_items": MetadataValue.int(
                sum(len(v) for v in inventory.values())
            ),
        }
    )
    return inventory


@asset_check(asset="enrichable_items_inventory")
def inventory_nonempty_per_instrument(context, enrichable_items_inventory):
    """WARN if any (instrument, data_type) partition returned 0 items."""
    empties = [k for k, v in enrichable_items_inventory.items() if len(v) == 0]
    passed = len(empties) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "empty_partitions": MetadataValue.text(
                ", ".join(sorted(empties)) or "none"
            ),
            "partition_count": MetadataValue.int(len(enrichable_items_inventory)),
        },
        description=(
            "All in-scope partitions have at least one item."
            if passed
            else f"{len(empties)} partition(s) empty: {', '.join(sorted(empties))}"
        ),
    )
