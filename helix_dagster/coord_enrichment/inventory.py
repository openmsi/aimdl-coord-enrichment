"""enrichable_items_inventory asset + partition definitions."""

import logging
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

from helix_dagster.girder_io import (
    fetch_all_aimdl_datafiles,
    fetch_items_by_partition,
)
from helix_dagster.instruments import (
    all_in_scope_data_types,
    instrument_for_data_type,
)
from helix_dagster.resources import GirderConnection

logger = logging.getLogger(__name__)

# Data types served by the partition-aware endpoints
# (/aimdl/partition + /aimdl/partition/details). These return items
# with full meta (experiment_date, prov, checksum, ...) intact,
# unlike /aimdl/datafiles which strips meta to data_type and igsn.
PARTITION_AWARE_DATA_TYPES = frozenset(
    {"xrd_raw", "xrf_raw", "xrd_derived", "xrd_metadata"}
)

MAXIMA_RAW_PARTITIONS = StaticPartitionsDefinition(
    ["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"]
)


def _partition_key(instrument: str, data_type: str) -> str:
    return f"{instrument}/{data_type}"


def filter_to_raw_subfolder(
    items: list[dict[str, Any]], girder: GirderConnection,
) -> list[dict[str, Any]]:
    """Keep only items whose immediate Girder folder is named ``raw``.

    Used by the inventory to implement the §7.1 / §7.5 scope gate for
    ``xrd_derived`` items: root-level TIFFs live outside ``raw/`` and
    are excluded.  Folder names are batch-fetched (one request per
    unique ``folderId``) to avoid per-item round trips.
    """
    folder_ids = {it.get("folderId") for it in items if it.get("folderId")}
    name_by_id: dict[str, str] = {}
    for fid in folder_ids:
        try:
            folder = girder.get(f"folder/{fid}")
            name_by_id[fid] = folder.get("name", "")
        except Exception as exc:
            logger.warning(
                "Could not fetch folder %s while filtering xrd_derived: %s",
                fid, exc,
            )
            name_by_id[fid] = ""

    kept = [it for it in items if name_by_id.get(it.get("folderId")) == "raw"]
    dropped_count = len(items) - len(kept)
    if dropped_count:
        logger.info(
            "xrd_derived filter: kept %d in-raw items, dropped %d non-raw "
            "(e.g. root TIFFs)",
            len(kept), dropped_count,
        )
    return kept


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
        if dt in PARTITION_AWARE_DATA_TYPES:
            items = fetch_items_by_partition(girder, dt)
        else:
            items = fetch_all_aimdl_datafiles(girder, dt)
        in_scope = [it for it in items if _is_in_scope(it)]
        if dt == "xrd_derived":
            in_scope = filter_to_raw_subfolder(in_scope, girder)
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
