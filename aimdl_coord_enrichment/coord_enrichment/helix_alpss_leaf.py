"""enriched_helix_alpss leaf asset + checks.

Partitioned across the three ALPSS data-type variants. For each
in-scope ALPSS item, inherit coordinates from the parent PDV trace
(tagged by helix_alpss_provenance_tagged), re-apply the parent's
recorded HELIX transform version, and write coord_provenance with
station_coord_source.kind == "inherited".
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)

from aimdl_coord_enrichment import __version__ as PIPELINE_VERSION
from aimdl_coord_enrichment.coord_enrichment.check_support import (
    evaluate_coord_failures,
    evaluate_success_rate,
    latest_partition_metadata,
    no_materialization_result,
)
from aimdl_coord_enrichment.coord_enrichment.config import CoordEnrichmentConfig
from aimdl_coord_enrichment.coord_enrichment.inheritance import (
    inherit_from_parent,
    inherited_station_coord_source,
)
from aimdl_coord_enrichment.coord_enrichment.overwrite import should_write
from aimdl_coord_enrichment.coordinates import transform_with_named_version
from aimdl_coord_enrichment.instruments import INSTRUMENT_HELIX
from aimdl_coord_enrichment.instruments.types import ResolutionError
from aimdl_coord_enrichment.provenance import build_coord_provenance
from aimdl_coord_enrichment.resources import GirderConnection


HELIX_ALPSS_PARTITIONS = StaticPartitionsDefinition(
    [
        "HELIX/pdv_alpss_output",
        "HELIX/pdv_alpss_result",
        "HELIX/pdv_alpss_results",
    ]
)


@asset(
    partitions_def=HELIX_ALPSS_PARTITIONS,
    deps=["helix_alpss_provenance_tagged"],
)
def enriched_helix_alpss(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    enrichable_items_inventory: dict[str, list[dict[str, Any]]],
    coord_transform_config_snapshot,
    girder: GirderConnection,
) -> dict[str, Any]:
    """Enrich HELIX ALPSS items by inheriting coords from their parent PDV trace."""
    partition_key = context.partition_key
    items = enrichable_items_inventory.get(partition_key, [])
    context.log.info(
        "enriched_helix_alpss partition %s: %d items to consider",
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
            INSTRUMENT_HELIX, inherited.parent_transform_version,
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
            instrument=INSTRUMENT_HELIX,
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
                "enriched_helix_alpss write failed for %s: %s", item_id, exc
            )
            write_errors.append({"item_id": item_id, "error": str(exc)})

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


@asset_check(asset="enriched_helix_alpss")
def enrichment_success_rate_helix_alpss(context):
    """WARN if <90% of items in this partition ended in a successful decision.

    Reads partition materialization metadata from the event log (see
    check_support module docstring) instead of taking the asset as an
    input.
    """
    md = latest_partition_metadata(
        context.instance, "enriched_helix_alpss", str(context.partition_key)
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


@asset_check(asset="enriched_helix_alpss")
def no_coord_transform_failures_helix_alpss(context):
    """WARN if any coordinate transform returned None."""
    md = latest_partition_metadata(
        context.instance, "enriched_helix_alpss", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return evaluate_coord_failures(int(md.get("coord_failures", 0)))
