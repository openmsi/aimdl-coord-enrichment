"""enrichable_items_inventory asset + partition definitions."""

import logging
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    DynamicPartitionsDefinition,
    MetadataValue,
    asset,
    asset_check,
)

from aimdl_coord_enrichment.girder_io import (
    fetch_all_aimdl_datafiles,
    fetch_items_by_partition,
)
from aimdl_coord_enrichment.instruments import (
    all_in_scope_data_types,
    instrument_for_data_type,
)
from aimdl_coord_enrichment.resources import GirderConnection

logger = logging.getLogger(__name__)

# Data types served by the partition-aware endpoints
# (/aimdl/partition + /aimdl/partition/details). These return items
# with full meta (experiment_date, prov, checksum, ...) intact,
# unlike /aimdl/datafiles which strips meta to data_type and igsn.
PARTITION_AWARE_DATA_TYPES = frozenset(
    {"xrd_raw", "xrf_raw", "xrd_derived", "xrd_visualization", "xrd_metadata"}
)

# One partition per AIMD-L run, keyed "<igsn>//<experiment_date>".
#
# A run is the unit of work: one instructions.txt supplies the coordinates for
# every file the run produced, raw measurements and derived products alike.
# Storage nests raw/ inside the run folder, but lineage runs the other way —
# the derived products are made FROM the raw measurements. Neither is a
# separate partition; they materialize together.
#
# Replaced MultiPartitionsDefinition({data_type, run}): splitting a run by
# data_type meant fetching and parsing the same instructions.txt once per
# data_type, and it forced enriched_maxima_derived to depend on the raw
# partitions through AllPartitionMapping.
MAXIMA_RUN_PARTITIONS = DynamicPartitionsDefinition(name="maxima_run")


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


@asset(group_name="coord_enrichment_core")
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
        if dt in PARTITION_AWARE_DATA_TYPES:
            items = fetch_items_by_partition(girder, dt)
        else:
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
