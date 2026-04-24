"""coord_enrichment_report asset."""

from typing import Any

from dagster import AssetExecutionContext, MetadataValue, asset


@asset
def coord_enrichment_report(
    context: AssetExecutionContext,
    enriched_maxima_raw: dict[str, Any],
    enriched_helix_alpss: dict[str, Any],
    enriched_maxima_derived: dict[str, Any],
    helix_alpss_provenance_tagged: dict[str, Any],
    helix_pdv_coverage_observer: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate counts across all enrichment leaves and the PDV observer."""
    leaves_by_partition: dict[str, dict[str, Any]] = {}

    def _flatten_leaf(leaf):
        if leaf is None:
            return
        pk = leaf.get("partition_key")
        if pk is None:
            return
        leaves_by_partition[pk] = {
            "counts": leaf.get("counts", {}),
            "write_errors": leaf.get("write_errors", []),
            "resolution_errors": leaf.get("resolution_errors", []),
            "version_counter": leaf.get("version_counter", {}),
            "dry_run": leaf.get("dry_run", None),
        }

    _flatten_leaf(enriched_maxima_raw)
    _flatten_leaf(enriched_helix_alpss)
    _flatten_leaf(enriched_maxima_derived)

    agg = {
        "seen": 0,
        "written": 0,
        "simulated_dry_run": 0,
        "skipped_no_change": 0,
        "resolution_errors": 0,
        "coord_failures": 0,
    }
    for leaf in leaves_by_partition.values():
        counts = leaf["counts"]
        for k in agg:
            agg[k] += counts.get(k, 0)

    report = {
        "leaves": leaves_by_partition,
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
        },
    }

    context.add_output_metadata({
        "total_writes": MetadataValue.int(agg["written"]),
        "total_simulated": MetadataValue.int(agg["simulated_dry_run"]),
        "leaf_partitions": MetadataValue.int(len(leaves_by_partition)),
        "pdv_fully_enriched": MetadataValue.int(
            helix_pdv_coverage_observer["fully_enriched"]
        ),
        "pdv_coverage_rate": MetadataValue.float(
            round(helix_pdv_coverage_observer["coverage_rate"], 3)
        ),
        "dry_run_aggregate": MetadataValue.bool(
            any(leaf["dry_run"] for leaf in leaves_by_partition.values())
            or bool(helix_alpss_provenance_tagged["dry_run"])
        ),
    })
    return report
