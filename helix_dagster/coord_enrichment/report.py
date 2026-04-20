"""coord_enrichment_report asset."""

from typing import Any

from dagster import AssetExecutionContext, MetadataValue, asset


@asset
def coord_enrichment_report(
    context: AssetExecutionContext,
    enriched_maxima_raw: dict[str, Any],
    provenance_tagged_items: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate counts across the Phase 3 enrichment surface.

    In Phase 3 only one enrichment leaf exists (enriched_maxima_raw,
    split across two partitions). Phase 4 will add HELIX ALPSS and
    MAXIMA derived leaves and this asset will aggregate across all
    of them.

    The asset's runtime behavior: it receives the two upstream
    dicts and flattens them into a single structured report. It
    writes nothing to Girder.
    """
    leaf = enriched_maxima_raw
    tagging = provenance_tagged_items

    report = {
        "leaves": {
            leaf["partition_key"]: {
                "counts": leaf["counts"],
                "write_errors": leaf["write_errors"],
                "resolution_errors": leaf["resolution_errors"],
                "version_counter": leaf["version_counter"],
                "dry_run": leaf["dry_run"],
            }
        },
        "tagging": {
            "counters": tagging["counters"],
            "unresolved_count": len(tagging["unresolved"]),
            "write_ops_count": len(tagging["write_ops"]),
            "dry_run": tagging["dry_run"],
        },
        "summary": {
            "total_items_seen": leaf["counts"]["seen"],
            "total_writes": leaf["counts"]["written"],
            "total_simulated": leaf["counts"]["simulated_dry_run"],
            "total_skipped_no_change": leaf["counts"]["skipped_no_change"],
            "total_resolution_errors": leaf["counts"]["resolution_errors"],
            "total_coord_failures": leaf["counts"]["coord_failures"],
        },
    }
    context.add_output_metadata(
        {
            "total_writes": MetadataValue.int(report["summary"]["total_writes"]),
            "total_simulated": MetadataValue.int(report["summary"]["total_simulated"]),
            "dry_run_aggregate": MetadataValue.bool(
                bool(leaf["dry_run"]) or bool(tagging["dry_run"])
            ),
        }
    )
    return report
