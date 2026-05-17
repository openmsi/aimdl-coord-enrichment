"""Coordinate enrichment leaves.

``enriched_maxima_raw`` is partitioned on
``MultiPartitionsDefinition({data_type, run})`` where ``run`` is a
dynamic dimension keyed on the AIMD-L partition string
``"<igsn>//<experiment_date>"``. Each partition fetches its own
items and the matching ``instructions.txt`` via the
``/aimdl/partition/details`` endpoint.
"""

import io
from typing import Any

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from helix_dagster import __version__ as PIPELINE_VERSION
from helix_dagster.coord_enrichment.check_support import (
    evaluate_coord_failures,
    evaluate_success_rate,
    latest_partition_metadata,
    no_materialization_result,
)
from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.inventory import MAXIMA_RAW_PARTITIONS
from helix_dagster.coord_enrichment.overwrite import should_write
from helix_dagster.coordinates import transform_station_to_sample
from helix_dagster.girder_io import fetch_partition_details
from helix_dagster.instruments import INSTRUMENT_MAXIMA
from helix_dagster.instruments.maxima import (
    _experiment_date,
    parse_instructions_json,
    parse_scan_point_index,
    scan_point_coords,
)
from helix_dagster.instruments.types import ResolutionError
from helix_dagster.provenance import build_coord_provenance
from helix_dagster.resources import GirderConnection


def _fetch_instructions_for_run(
    girder: GirderConnection,
    aimdl_key: str,
    context: AssetExecutionContext,
) -> tuple[dict | None, dict | None, list[dict]]:
    """Fetch and parse the instructions.txt for a single AIMD-L run.

    Calls the scoped partition-details endpoint for xrd_metadata
    keyed by ``aimdl_key``, filters to instructions.txt items,
    downloads and parses the first one.

    Returns ``(instr_item, parsed, errors)``:
      - ``instr_item``: the Girder item dict for the instructions.txt
        used, or ``None`` if none was found or parseable.
      - ``parsed``: the parsed JSON dict, or ``None`` on failure.
      - ``errors``: list of per-run error dicts (empty on happy path).

    If multiple ``instructions.txt`` items are present for the same
    run, the first is used and the rest are recorded as warnings in
    ``errors``.
    """
    errors: list[dict] = []
    metadata_items = fetch_partition_details(girder, "xrd_metadata", aimdl_key)
    instr_items = [
        it for it in metadata_items if it.get("name") == "instructions.txt"
    ]
    if len(instr_items) == 0:
        errors.append(
            {
                "stage": "instructions_missing",
                "error": f"no instructions.txt in xrd_metadata for {aimdl_key}",
            }
        )
        return None, None, errors

    instr_item = instr_items[0]
    if len(instr_items) > 1:
        for extra in instr_items[1:]:
            errors.append(
                {
                    "stage": "instructions_duplicate",
                    "error": (
                        f"multiple instructions.txt for {aimdl_key}; "
                        f"using {instr_item.get('_id')}, ignoring {extra.get('_id')}"
                    ),
                }
            )

    try:
        files = girder.get(f"item/{instr_item['_id']}/files")
    except Exception as exc:
        errors.append(
            {
                "stage": "instructions_fetch",
                "error": f"failed to list files for {instr_item.get('_id')}: {exc}",
            }
        )
        return None, None, errors
    if not files:
        errors.append(
            {
                "stage": "instructions_fetch",
                "error": f"instructions.txt item {instr_item.get('_id')} has no files",
            }
        )
        return None, None, errors

    buf = io.BytesIO()
    try:
        girder.downloadFile(files[0]["_id"], buf)
    except Exception as exc:
        errors.append(
            {
                "stage": "instructions_fetch",
                "error": f"failed to download {instr_item.get('_id')}: {exc}",
            }
        )
        return None, None, errors
    buf.seek(0)

    try:
        parsed = parse_instructions_json(buf.read())
    except ResolutionError as exc:
        errors.append(
            {
                "stage": "instructions_parse",
                "error": str(exc),
            }
        )
        return instr_item, None, errors

    return instr_item, parsed, errors


@asset(
    partitions_def=MAXIMA_RAW_PARTITIONS,
)
def enriched_maxima_raw(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    coord_transform_config_snapshot,
    girder: GirderConnection,
) -> dict[str, Any]:
    """Write Sample_X/Y and coord_provenance to MAXIMA xrd_raw or xrf_raw items.

    Partitioned on ``MultiPartitionsDefinition({data_type, run})``.
    Each partition fetches its own items and the matching
    ``instructions.txt`` via the ``/aimdl/partition/details``
    endpoint, keyed on ``aimdl_key = "<igsn>//<experiment_date>"``.
    """
    keys = context.partition_key.keys_by_dimension
    data_type = keys["data_type"]
    aimdl_key = keys["run"]
    partition_key_str = str(context.partition_key)

    items = fetch_partition_details(girder, data_type, aimdl_key)
    context.log.info(
        "enriched_maxima_raw (%s, %s): %d items to consider",
        data_type, aimdl_key, len(items),
    )

    instr_item, parsed, instructions_errors = _fetch_instructions_for_run(
        girder, aimdl_key, context,
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

    instructions_missing_error = (
        instructions_errors[0]["error"] if instr_item is None or parsed is None
        else None
    )

    for item in items:
        item_id = item.get("_id")
        name = item.get("name", "")

        if instructions_missing_error is not None:
            resolution_errors.append(
                {
                    "item_id": item_id,
                    "name": name,
                    "stage": "instructions",
                    "error": instructions_missing_error,
                }
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
            "partition": MetadataValue.text(partition_key_str),
            "data_type": MetadataValue.text(data_type),
            "aimdl_key": MetadataValue.text(aimdl_key),
            "seen": MetadataValue.int(counts["seen"]),
            "written": MetadataValue.int(counts["written"]),
            "simulated_dry_run": MetadataValue.int(counts["simulated_dry_run"]),
            "skipped_no_change": MetadataValue.int(counts["skipped_no_change"]),
            "coord_failures": MetadataValue.int(counts["coord_failures"]),
            "resolution_errors": MetadataValue.int(counts["resolution_errors"]),
            "write_errors": MetadataValue.int(len(write_errors)),
            "instructions_errors": MetadataValue.int(len(instructions_errors)),
            "transform_versions_used": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
                or "none"
            ),
        }
    )

    return {
        "partition_key": partition_key_str,
        "data_type": data_type,
        "aimdl_key": aimdl_key,
        "counts": counts,
        "write_errors": write_errors,
        "resolution_errors": resolution_errors,
        "instructions_errors": instructions_errors,
        "version_counter": version_counter,
        "dry_run": config.dry_run,
    }


@asset_check(asset="enriched_maxima_raw")
def enrichment_success_rate_maxima_raw(context):
    """WARN if <90% of items in this partition ended in a successful decision.

    Reads the partition's materialization metadata from the event log
    rather than taking ``enriched_maxima_raw`` as an input, so the
    check does not force an IOManager load across the partition
    cross-product (see check_support module docstring).
    """
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_raw", str(context.partition_key)
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


@asset_check(asset="enriched_maxima_raw")
def no_coord_transform_failures_maxima_raw(context):
    """WARN if any coordinate transform returned (None, None, None)."""
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_raw", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return evaluate_coord_failures(int(md.get("coord_failures", 0)))
