"""coord_enrichment_report asset.

Aggregates per-partition leaf state from the Dagster event log so the
report can run on a fresh instance without requiring leaf outputs to
be passed in. Per-run detail (write_errors, resolution_errors) lives
on each leaf's run page in the UI; this asset is for global state.
"""

from typing import Any

from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    AssetRecordsFilter,
    MetadataValue,
    asset,
)

from aimdl_coord_enrichment.coord_enrichment.helix_alpss_leaf import HELIX_ALPSS_PARTITIONS
from aimdl_coord_enrichment.coord_enrichment.inventory import MAXIMA_RUN_PARTITIONS


_COUNT_KEYS = (
    "seen",
    "written",
    "simulated_dry_run",
    "skipped_no_change",
    "coord_failures",
    "resolution_errors",
)


def _materialization_metadata(
    instance, asset_key: AssetKey, partition_key_str: str
) -> dict[str, Any] | None:
    """Return the latest materialization's metadata dict for a partition.

    Returns None if no materialization has been recorded for that
    (asset, partition) pair. The returned dict has Dagster's
    MetadataValue wrappers as values; callers extract `.value`.

    Reads from `instance.fetch_materializations`, scoped to the single
    partition via AssetRecordsFilter(asset_partitions=[...]). The
    leaf assets currently surface their counts (and
    `transform_versions_used`) through `context.add_output_metadata`;
    if a future leaf change adds new keys, this helper will need to
    learn about them.
    """
    result = instance.fetch_materializations(
        AssetRecordsFilter(
            asset_key=asset_key,
            asset_partitions=[partition_key_str],
        ),
        limit=1,
    )
    if not result.records:
        return None
    mat = (
        result.records[0]
        .event_log_entry.dagster_event.event_specific_data.materialization
    )
    return dict(mat.metadata) if mat.metadata else {}


def _extract_leaf_counts(metadata: dict[str, Any]) -> dict[str, int]:
    """Pull the count keys out of a leaf's materialization metadata.

    Missing keys default to 0. Each metadata value is a Dagster
    MetadataValue; we read `.value` off it.
    """
    out: dict[str, int] = {}
    for key in _COUNT_KEYS:
        meta_val = metadata.get(key)
        out[key] = int(meta_val.value) if meta_val is not None else 0
    return out


def _read_leaf_partitions(
    instance,
    asset_name: str,
    partition_keys: list[str],
) -> tuple[dict[str, dict[str, Any]], int]:
    """Read every partition of one leaf asset.

    Returns (per_partition_state, unmaterialized_count). A partition
    with no materialization event contributes to the count and is
    omitted from the dict.
    """
    asset_key = AssetKey(asset_name)
    by_partition: dict[str, dict[str, Any]] = {}
    unmaterialized = 0
    for pk in partition_keys:
        metadata = _materialization_metadata(instance, asset_key, pk)
        if metadata is None:
            unmaterialized += 1
            continue
        tvu_meta = metadata.get("transform_versions_used")
        by_partition[pk] = {
            "counts": _extract_leaf_counts(metadata),
            "transform_versions_used": (
                tvu_meta.value if tvu_meta is not None else "none"
            ),
        }
    return by_partition, unmaterialized


@asset(
    group_name="coord_enrichment_reporting",
    deps=[
        AssetDep("enriched_maxima_run", partition_mapping=AllPartitionMapping()),
        AssetDep("enriched_helix_alpss", partition_mapping=AllPartitionMapping()),
    ],
)
def coord_enrichment_report(
    context: AssetExecutionContext,
    helix_alpss_provenance_tagged: dict[str, Any],
    helix_pdv_coverage_observer: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate leaf state across all partitions, plus tagger and observer.

    Reads each leaf's latest materialization metadata from the
    instance event log rather than receiving leaf outputs directly,
    so the asset is loadable on a fresh instance with no prior leaf
    materializations.

    Per-run detail (write_errors, resolution_errors lists) is NOT in
    this report by design — that detail belongs on each leaf's
    Dagster UI run page. This asset reports global state only.
    """
    instance = context.instance

    maxima_run_keys = [
        str(k) for k in MAXIMA_RUN_PARTITIONS.get_partition_keys(
            dynamic_partitions_store=instance,
        )
    ]
    helix_alpss_keys = list(HELIX_ALPSS_PARTITIONS.get_partition_keys())

    maxima_state, maxima_unmat = _read_leaf_partitions(
        instance, "enriched_maxima_run", maxima_run_keys,
    )
    alpss_state, alpss_unmat = _read_leaf_partitions(
        instance, "enriched_helix_alpss", helix_alpss_keys,
    )

    leaves_by_partition: dict[str, dict[str, Any]] = {}
    leaves_by_partition.update(maxima_state)
    leaves_by_partition.update(alpss_state)

    leaves_unmaterialized = {
        "enriched_maxima_run": maxima_unmat,
        "enriched_helix_alpss": alpss_unmat,
    }

    agg = {k: 0 for k in _COUNT_KEYS}
    for leaf in leaves_by_partition.values():
        for k in _COUNT_KEYS:
            agg[k] += leaf["counts"].get(k, 0)

    report = {
        "leaves": leaves_by_partition,
        "leaves_unmaterialized": leaves_unmaterialized,
        "tagging": {
            "counters": helix_alpss_provenance_tagged["counters"],
            "unresolved_count": len(helix_alpss_provenance_tagged["unresolved"]),
            "write_ops_count": len(helix_alpss_provenance_tagged["write_ops"]),
            "dry_run": helix_alpss_provenance_tagged["dry_run"],
        },
        "coverage": {
            "pdv_trace": helix_pdv_coverage_observer,
        },
        "summary": {
            "total_items_seen": agg["seen"],
            "total_writes": agg["written"],
            "total_simulated": agg["simulated_dry_run"],
            "total_skipped_no_change": agg["skipped_no_change"],
            "total_resolution_errors": agg["resolution_errors"],
            "total_coord_failures": agg["coord_failures"],
            "leaf_partitions_covered": len(leaves_by_partition),
            "leaf_partitions_unmaterialized": sum(leaves_unmaterialized.values()),
        },
    }

    context.add_output_metadata({
        "total_writes": MetadataValue.int(agg["written"]),
        "total_simulated": MetadataValue.int(agg["simulated_dry_run"]),
        "leaf_partitions": MetadataValue.int(len(leaves_by_partition)),
        "leaf_partitions_unmaterialized": MetadataValue.int(
            sum(leaves_unmaterialized.values())
        ),
        "pdv_fully_enriched": MetadataValue.int(
            helix_pdv_coverage_observer["fully_enriched"]
        ),
        "pdv_coverage_rate": MetadataValue.float(
            round(helix_pdv_coverage_observer["coverage_rate"], 3)
        ),
    })
    return report
