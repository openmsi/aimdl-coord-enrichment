"""Coordinate enrichment leaves.

``enriched_maxima_run`` is partitioned on a single dynamic dimension
keyed on the AIMD-L partition string ``"<igsn>//<experiment_date>"``.
One partition == one run == one ``instructions.txt``, which supplies
the station coordinates for every file the run produced: raw
measurements and derived products alike, each mapped by the
``scan_point_<i>`` index in its own filename.
"""

import io
from typing import Any

from dagster import (
    AssetExecutionContext,
    MetadataValue,
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
from aimdl_coord_enrichment.coord_enrichment.exclusions import (
    MALFORMED_INSTRUCTIONS,
    NO_EXPERIMENT_DATE,
    NO_INSTRUCTIONS,
    SCAN_POINT_OUT_OF_RANGE,
    UNPARSEABLE_NAME,
    ExclusionLog,
)
from aimdl_coord_enrichment.coord_enrichment.inventory import MAXIMA_RUN_PARTITIONS
from aimdl_coord_enrichment.coord_enrichment.overwrite import should_write
from aimdl_coord_enrichment.coordinates import transform_station_to_sample
from aimdl_coord_enrichment.girder_io import fetch_partition_details
from aimdl_coord_enrichment.instruments import (
    INSTRUMENT_MAXIMA,
    MAXIMA_LEAF_DATA_TYPES,
)
from aimdl_coord_enrichment.instruments.maxima import (
    _experiment_date,
    parse_instructions_json,
    parse_scan_point_index,
    scan_point_coords,
)
from aimdl_coord_enrichment.instruments.types import ResolutionError
from aimdl_coord_enrichment.provenance import build_coord_provenance
from aimdl_coord_enrichment.resources import GirderConnection


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
    group_name="maxima",
    partitions_def=MAXIMA_RUN_PARTITIONS,
)
def enriched_maxima_run(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    coord_transform_config_snapshot,
    girder: GirderConnection,
) -> dict[str, Any]:
    """Write Station/Sample coordinates + coord_provenance to every in-scope
    MAXIMA item produced by one run.

    The partition key is the AIMD-L run key ``"<igsn>//<experiment_date>"``.
    The run's ``instructions.txt`` is fetched once and supplies coordinates
    for all of its files; each file selects its scan point by the
    ``scan_point_<i>`` index in its own name.
    """
    aimdl_key = str(context.partition_key)

    instr_item, parsed, instructions_errors = _fetch_instructions_for_run(
        girder, aimdl_key, context,
    )

    try:
        run_id = context.run.run_id
    except Exception:
        run_id = None

    counts = {
        "seen": 0,
        "written": 0,
        "simulated_dry_run": 0,
        "skipped_no_change": 0,
        "coord_failures": 0,
        "resolution_errors": 0,
    }
    write_errors: list[dict[str, Any]] = []
    resolution_errors: list[dict[str, Any]] = []
    excluded = ExclusionLog()
    version_counter: dict[str, int] = {}
    per_data_type: dict[str, int] = {}

    # No usable instructions.txt for this run: nothing in it can be enriched.
    # That is a property of the run as produced, not a failure — see
    # coord_enrichment/exclusions.py.
    if instr_item is None or parsed is None:
        run_exclusion_reason = (
            MALFORMED_INSTRUCTIONS if instr_item is not None else NO_INSTRUCTIONS
        )
    else:
        run_exclusion_reason = None

    for data_type in sorted(MAXIMA_LEAF_DATA_TYPES):
        items = fetch_partition_details(girder, data_type, aimdl_key)
        per_data_type[data_type] = len(items)
        counts["seen"] += len(items)

        for item in items:
            item_id = item.get("_id")
            name = item.get("name", "")

            if run_exclusion_reason is not None:
                excluded.add(run_exclusion_reason, name, item_id)
                continue

            index = parse_scan_point_index(name)
            if index is None:
                excluded.add(UNPARSEABLE_NAME, name, item_id)
                continue
            try:
                station_x, station_y = scan_point_coords(parsed, index)
            except ResolutionError:
                excluded.add(SCAN_POINT_OUT_OF_RANGE, name, item_id)
                continue

            try:
                shot_ts = _experiment_date(item)
            except ResolutionError:
                excluded.add(NO_EXPERIMENT_DATE, name, item_id)
                continue

            sample_x, sample_y, transform_name = transform_station_to_sample(
                station_x, station_y,
                instrument=INSTRUMENT_MAXIMA, timestamp=shot_ts,
            )
            if sample_x is None or sample_y is None:
                counts["coord_failures"] += 1
                continue
            sample_x = round(sample_x, 4)
            sample_y = round(sample_y, 4)
            if transform_name is not None:
                version_counter[transform_name] = (
                    version_counter.get(transform_name, 0) + 1
                )

            station_coord_source = {
                "kind": "maxima_instructions",
                "instructions_item_id": instr_item["_id"],
                "scan_point_index": index,
            }
            # Lineage, recorded but not load-bearing: the coordinate's origin
            # is the instructions.txt above, not the parent. Kept so the
            # provenance carries the scientific parent link where one exists.
            parent_id = ((item.get("meta") or {}).get("prov") or {}).get(
                "wasDerivedFrom"
            )
            if parent_id:
                station_coord_source["parent_item_id"] = parent_id

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
                    "enriched_maxima_run write failed for %s: %s", item_id, exc
                )
                write_errors.append({"item_id": item_id, "error": str(exc)})

    context.log.info(
        "enriched_maxima_run %s: %d items across %s | %d in scope, "
        "%d excluded (%s)",
        aimdl_key, counts["seen"],
        ", ".join(f"{k}={v}" for k, v in sorted(per_data_type.items())),
        counts["seen"] - excluded.total, excluded.total, excluded.summary_text(),
    )

    context.add_output_metadata(
        {
            "partition": MetadataValue.text(aimdl_key),
            "aimdl_key": MetadataValue.text(aimdl_key),
            "seen": MetadataValue.int(counts["seen"]),
            "written": MetadataValue.int(counts["written"]),
            "simulated_dry_run": MetadataValue.int(counts["simulated_dry_run"]),
            "skipped_no_change": MetadataValue.int(counts["skipped_no_change"]),
            "coord_failures": MetadataValue.int(counts["coord_failures"]),
            "resolution_errors": MetadataValue.int(counts["resolution_errors"]),
            "write_errors": MetadataValue.int(len(write_errors)),
            "instructions_errors": MetadataValue.int(len(instructions_errors)),
            "in_scope": MetadataValue.int(counts["seen"] - excluded.total),
            "excluded": MetadataValue.int(excluded.total),
            "excluded_by_reason": MetadataValue.text(excluded.summary_text()),
            "excluded_examples": MetadataValue.text(excluded.examples_text()),
            "items_by_data_type": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(per_data_type.items()))
                or "none"
            ),
            "transform_versions_used": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
                or "none"
            ),
        }
    )

    return {
        "partition_key": aimdl_key,
        "aimdl_key": aimdl_key,
        "counts": counts,
        "per_data_type": per_data_type,
        "write_errors": write_errors,
        "resolution_errors": resolution_errors,
        "excluded": excluded.as_dict(),
        "instructions_errors": instructions_errors,
        "version_counter": version_counter,
        "dry_run": config.dry_run,
    }


@asset_check(asset="enriched_maxima_run")
def enrichment_success_rate_maxima(context):
    """WARN if <90% of the run's items ended in a successful decision.

    Reads the partition's materialization metadata from the event log rather
    than taking the asset as an input (see check_support module docstring).
    """
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_run", str(context.partition_key)
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
        excluded=int(md.get("excluded", 0)),
    )


@asset_check(asset="enriched_maxima_run")
def no_coord_transform_failures_maxima(context):
    """WARN if any coordinate transform returned (None, None, None)."""
    md = latest_partition_metadata(
        context.instance, "enriched_maxima_run", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return evaluate_coord_failures(int(md.get("coord_failures", 0)))
