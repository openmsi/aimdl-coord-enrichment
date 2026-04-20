"""Coordinate enrichment leaves. Phase 3 delivers MAXIMA raw only."""

from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from helix_dagster import __version__ as PIPELINE_VERSION
from helix_dagster.coord_enrichment.cache import InstructionsCache
from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.inventory import MAXIMA_RAW_PARTITIONS
from helix_dagster.coord_enrichment.overwrite import should_write
from helix_dagster.coordinates import transform_station_to_sample
from helix_dagster.instruments import INSTRUMENT_MAXIMA
from helix_dagster.instruments.maxima import (
    _experiment_date,
    parse_scan_point_index,
    scan_point_coords,
)
from helix_dagster.instruments.types import ResolutionError
from helix_dagster.provenance import build_coord_provenance
from helix_dagster.resources import GirderConnection


@asset(
    partitions_def=MAXIMA_RAW_PARTITIONS,
    deps=["provenance_tagged_items"],
)
def enriched_maxima_raw(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    enrichable_items_inventory: dict[str, list[dict[str, Any]]],
    coord_transform_config_snapshot,
    girder: GirderConnection,
) -> dict[str, Any]:
    """Write Sample_X/Y and coord_provenance to MAXIMA xrd_raw or xrf_raw items.

    Partitioned by "MAXIMA/xrd_raw" and "MAXIMA/xrf_raw". Each
    partition run processes only its subset of the inventory.
    """
    partition_key = context.partition_key
    items = enrichable_items_inventory.get(partition_key, [])
    context.log.info(
        "enriched_maxima_raw partition %s: %d items to consider",
        partition_key, len(items),
    )

    cache = InstructionsCache()

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
            run_folder_id, instr_item, parsed = cache.get_for_item(item, girder)
        except ResolutionError as exc:
            resolution_errors.append(
                {"item_id": item_id, "name": name, "stage": "run_folder_or_instructions", "error": str(exc)}
            )
            counts["resolution_errors"] += 1
            continue

        try:
            index = parse_scan_point_index(name)
            if index is None:
                raise ResolutionError(f"cannot parse scan_point index from {name!r}")
            station_x, station_y = scan_point_coords(parsed, index)
        except ResolutionError as exc:
            resolution_errors.append(
                {"item_id": item_id, "name": name, "stage": "scan_point_lookup", "error": str(exc)}
            )
            counts["resolution_errors"] += 1
            continue

        try:
            shot_ts = _experiment_date(item)
        except ResolutionError as exc:
            resolution_errors.append(
                {"item_id": item_id, "name": name, "stage": "experiment_date", "error": str(exc)}
            )
            counts["resolution_errors"] += 1
            continue

        sample_x, sample_y, transform_name = transform_station_to_sample(
            station_x, station_y, instrument=INSTRUMENT_MAXIMA, timestamp=shot_ts,
        )
        if sample_x is None or sample_y is None:
            counts["coord_failures"] += 1
            continue
        sample_x = round(sample_x, 4)
        sample_y = round(sample_y, 4)
        if transform_name is not None:
            version_counter[transform_name] = version_counter.get(transform_name, 0) + 1

        station_coord_source = {
            "kind": "maxima_instructions",
            "instructions_item_id": instr_item["_id"],
            "scan_point_index": index,
        }

        new_prov = build_coord_provenance(
            instrument=INSTRUMENT_MAXIMA,
            transform_version=transform_name,
            transform_yaml_sha256=coord_transform_config_snapshot.yaml_sha256 or "",
            transformer_version=coord_transform_config_snapshot.transformer_version,
            pipeline_version=PIPELINE_VERSION,
            source_timestamp=shot_ts,
            source_timestamp_origin="meta.experiment_date",
            station_coord_source=station_coord_source,
            dagster_run_id=run_id,
        )

        stored_prov = (item.get("meta") or {}).get("coord_provenance")
        write, reason = should_write(new_prov, stored_prov)

        if not write:
            counts["skipped_no_change"] += 1
            continue

        payload = {
            "Station_X": float(station_x),
            "Station_Y": float(station_y),
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
                "enriched_maxima_raw write failed for %s: %s", item_id, exc
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
            "cache_size": MetadataValue.int(cache.cache_size()),
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


@asset_check(asset="enriched_maxima_raw")
def enrichment_success_rate_maxima_raw(context, enriched_maxima_raw):
    """WARN if <90% of items in this partition ended in a successful decision."""
    c = enriched_maxima_raw["counts"]
    write_errors = enriched_maxima_raw.get("write_errors", [])
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
            "partition": MetadataValue.text(enriched_maxima_raw["partition_key"]),
        },
        description=f"Success rate: {rate:.1%} ({success}/{total})",
    )


@asset_check(asset="enriched_maxima_raw")
def no_coord_transform_failures_maxima_raw(context, enriched_maxima_raw):
    """WARN if any coordinate transform returned (None, None, None)."""
    failures = enriched_maxima_raw["counts"]["coord_failures"]
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
