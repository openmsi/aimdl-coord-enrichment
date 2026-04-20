"""enriched_maxima_derived leaf asset + checks.

Partitioned across MAXIMA/xrd_derived. For each in-scope xrd_derived
item, inherit coordinates from the parent xrd_raw master.h5 (whose
prov was healed in provenance_tagged_items), re-apply the parent's
recorded MAXIMA transform version, and write coord_provenance with
station_coord_source.kind == "inherited".
"""

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

from helix_dagster import __version__ as PIPELINE_VERSION
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
    deps=["provenance_tagged_items"],
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

    context.add_output_metadata(
        {
            "partition": MetadataValue.text(partition_key),
            "seen": MetadataValue.int(counts["seen"]),
            "written": MetadataValue.int(counts["written"]),
            "simulated_dry_run": MetadataValue.int(counts["simulated_dry_run"]),
            "skipped_no_change": MetadataValue.int(counts["skipped_no_change"]),
            "coord_failures": MetadataValue.int(counts["coord_failures"]),
            "resolution_errors": MetadataValue.int(counts["resolution_errors"]),
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
def enrichment_success_rate_maxima_derived(context, enriched_maxima_derived):
    """WARN if <90% of items in this partition ended in a successful decision."""
    c = enriched_maxima_derived["counts"]
    write_errors = enriched_maxima_derived.get("write_errors", [])
    total = c["seen"]
    if total == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"note": MetadataValue.text("partition empty")},
            description="Partition empty; no items to check.",
        )
    success = c["written"] + c["simulated_dry_run"] + c["skipped_no_change"]
    rate = success / total
    passed = rate >= 0.9
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "success_rate": MetadataValue.float(round(rate, 3)),
            "write_errors": MetadataValue.int(len(write_errors)),
            "resolution_errors": MetadataValue.int(c["resolution_errors"]),
            "partition": MetadataValue.text(enriched_maxima_derived["partition_key"]),
        },
        description=f"Success rate: {rate:.1%} ({success}/{total})",
    )


@asset_check(asset="enriched_maxima_derived")
def no_coord_transform_failures_maxima_derived(context, enriched_maxima_derived):
    """WARN if any coordinate transform returned None."""
    failures = enriched_maxima_derived["counts"]["coord_failures"]
    passed = failures == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"coord_failures": MetadataValue.int(failures)},
        description=(
            "No coordinate transform failures."
            if passed else f"{failures} coordinate transform failure(s)."
        ),
    )
