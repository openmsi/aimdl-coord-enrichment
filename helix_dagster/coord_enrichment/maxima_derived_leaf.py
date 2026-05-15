"""enriched_maxima_derived leaf asset + checks.

Partitioned across MAXIMA/xrd_derived. For each in-scope xrd_derived
item, inherit coordinates from the parent xrd_raw master.h5 (whose
prov was written upstream by the amdee_xrd Girder plugin), re-apply
the parent's recorded MAXIMA transform version, and write
coord_provenance with station_coord_source.kind == "inherited".
"""

from typing import Any

from dagster import (
    AllPartitionMapping,
    AssetCheckSeverity,
    AssetDep,
    AssetExecutionContext,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)

from helix_dagster import __version__ as PIPELINE_VERSION
from helix_dagster.coord_enrichment.check_support import (
    evaluate_coord_failures,
    evaluate_provenance_valid,
    evaluate_success_rate,
    latest_partition_metadata,
    no_materialization_result,
)
from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.inheritance import (
    inherit_from_parent,
    inherited_station_coord_source,
)
from helix_dagster.coord_enrichment.overwrite import should_write
from helix_dagster.coordinates import transform_with_named_version
from helix_dagster.instruments import INSTRUMENT_MAXIMA
from helix_dagster.instruments.types import ResolutionError
from helix_dagster.provenance import build_coord_provenance
from helix_dagster.resources import GirderConnection


MAXIMA_DERIVED_PARTITIONS = StaticPartitionsDefinition(
    ["MAXIMA/xrd_derived"]
)


@asset(
    partitions_def=MAXIMA_DERIVED_PARTITIONS,
    deps=[
        AssetDep(
            "enriched_maxima_raw",
            partition_mapping=AllPartitionMapping(),
        ),
    ],
)
def enriched_maxima_derived(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    enrichable_items_inventory: dict[str, list[dict[str, Any]]],
    coord_transform_config_snapshot,
    girder: GirderConnection,
) -> dict[str, Any]:
    """Enrich MAXIMA xrd_derived items by inheriting coords from their parent xrd_raw."""
    partition_key = context.partition_key
    items = enrichable_items_inventory.get(partition_key, [])
    context.log.info(
        "enriched_maxima_derived partition %s: %d items to consider",
        partition_key, len(items),
    )

    try:
        run_id = context.run.run_id
    except Exception:
        run_id = None

    counts = {
        "seen": len(items),
        "written": 0,
        "simulated_dry_run": 0,
        "skipped_no_change": 0,
        "coord_failures": 0,
        "resolution_errors": 0,
    }
    write_errors: list[dict[str, Any]] = []
    resolution_errors: list[dict[str, Any]] = []
    version_counter: dict[str, int] = {}

    for item in items:
        item_id = item.get("_id")
        name = item.get("name", "")

        try:
            inherited = inherit_from_parent(item, girder)
        except ResolutionError as exc:
            resolution_errors.append(
                {"item_id": item_id, "name": name,
                 "stage": "inherit_from_parent", "error": str(exc)}
            )
            counts["resolution_errors"] += 1
            continue

        sample_x, sample_y = transform_with_named_version(
            INSTRUMENT_MAXIMA, inherited.parent_transform_version,
            inherited.station_x, inherited.station_y,
        )
        if sample_x is None or sample_y is None:
            counts["coord_failures"] += 1
            continue
        sample_x = round(sample_x, 4)
        sample_y = round(sample_y, 4)
        version_counter[inherited.parent_transform_version] = (
            version_counter.get(inherited.parent_transform_version, 0) + 1
        )

        new_prov = build_coord_provenance(
            instrument=INSTRUMENT_MAXIMA,
            transform_version=inherited.parent_transform_version,
            transform_yaml_sha256=coord_transform_config_snapshot.yaml_sha256 or "",
            transformer_version=coord_transform_config_snapshot.transformer_version,
            pipeline_version=PIPELINE_VERSION,
            source_timestamp=inherited.parent_source_timestamp,
            source_timestamp_origin="inherited_from_parent",
            station_coord_source=inherited_station_coord_source(inherited),
            dagster_run_id=run_id,
        )

        stored_prov = (item.get("meta") or {}).get("coord_provenance")
        write, reason = should_write(new_prov, stored_prov)

        if not write:
            counts["skipped_no_change"] += 1
            continue

        payload = {
            "Station_X": float(inherited.station_x),
            "Station_Y": float(inherited.station_y),
            "Sample_X": sample_x,
            "Sample_Y": sample_y,
            "coord_provenance": new_prov,
        }

        if config.dry_run:
            counts["simulated_dry_run"] += 1
            continue

        try:
            girder.addMetadataToItem(item_id, payload)
            counts["written"] += 1
        except Exception as exc:
            context.log.error(
                "enriched_maxima_derived write failed for %s: %s", item_id, exc
            )
            write_errors.append({"item_id": item_id, "error": str(exc)})

    inherit_errors = [
        e for e in resolution_errors
        if e.get("stage") == "inherit_from_parent"
    ]
    inherit_examples = ", ".join(
        f"{e.get('item_id', '?')}: {e.get('error', '')}"
        for e in inherit_errors[:3]
    ) or "none"

    context.add_output_metadata(
        {
            "partition": MetadataValue.text(partition_key),
            "seen": MetadataValue.int(counts["seen"]),
            "written": MetadataValue.int(counts["written"]),
            "simulated_dry_run": MetadataValue.int(counts["simulated_dry_run"]),
            "skipped_no_change": MetadataValue.int(counts["skipped_no_change"]),
            "coord_failures": MetadataValue.int(counts["coord_failures"]),
            "resolution_errors": MetadataValue.int(counts["resolution_errors"]),
            "write_errors": MetadataValue.int(len(write_errors)),
            "inherit_from_parent_errors": MetadataValue.int(len(inherit_errors)),
            "inherit_from_parent_examples": MetadataValue.text(inherit_examples),
            "transform_versions_used": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
                or "none"
            ),
        }
    )

    return {
        "partition_key": partition_key,
        "counts": counts,
        "write_errors": write_errors,
        "resolution_errors": resolution_errors,
        "version_counter": version_counter,
        "dry_run": config.dry_run,
    }


@asset_check(asset="enriched_maxima_derived")
def enrichment_success_rate_maxima_derived(context):
    """WARN if <90% of items in this partition ended in a successful decision.

    Reads partition materialization metadata from the event log (see
    check_support module docstring) instead of taking the asset as an
    input.
    """
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_derived", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return evaluate_success_rate(
        seen=int(md.get("seen", 0)),
        written=int(md.get("written", 0)),
        simulated_dry_run=int(md.get("simulated_dry_run", 0)),
        skipped_no_change=int(md.get("skipped_no_change", 0)),
        resolution_errors=int(md.get("resolution_errors", 0)),
        write_errors_count=int(md.get("write_errors", 0)),
        partition_label=str(md.get("partition", context.partition_key)),
    )


@asset_check(asset="enriched_maxima_derived")
def no_coord_transform_failures_maxima_derived(context):
    """WARN if any coordinate transform returned None."""
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_derived", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return evaluate_coord_failures(int(md.get("coord_failures", 0)))


@asset_check(asset="enriched_maxima_derived")
def maxima_xrd_derived_provenance_valid(context):
    """ERROR if any xrd_derived item failed parent resolution.

    Parent resolution for xrd_derived reads meta.prov.wasDerivedFrom
    or meta.prov.isPartOf, written upstream by the amdee_xrd Girder
    plugin, and looks the parent up in the inventory. Failures at
    this stage indicate either a missing prov link on the item (a
    data-hygiene problem in Girder) or a parent not present in the
    current inventory slice (an ingest lag).

    Both conditions should be zero in a healthy pipeline. Reads the
    partition's materialization metadata from the event log (see
    check_support module docstring) instead of taking the asset as an
    input.
    """
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_derived", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result(severity=AssetCheckSeverity.ERROR)
    return evaluate_provenance_valid(
        int(md.get("inherit_from_parent_errors", 0)),
        str(md.get("inherit_from_parent_examples", "none")),
    )
