import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Config,
    MetadataValue,
    asset,
    define_asset_job,
)

from aimdl_coord_enrichment.coordinates import _COORD_YAML
from aimdl_coord_enrichment.girder_io import (
    download_and_read,
    fetch_partition_details,
)
from aimdl_coord_enrichment.partitions import (
    HELIX_EXPERIMENT_LOG_DATA_TYPE,
    HELIX_TRACE_DATA_TYPE,
    HELIX_TRACE_PARTITIONS,
)
from aimdl_coord_enrichment.provenance import compute_yaml_sha256, get_transformer_version
from aimdl_coord_enrichment.resources import GirderConnection
from aimdl_coord_enrichment.spreadsheet import (
    classify_shots,
    count_rows_with_pdv,
    normalize_experiment_log,
    pair_traces_to_rows,
    summarize_pdv_processing,
    validate_log_rows,
    write_pdv_metadata,
    write_processing_manifest,
)

# Design principle for this module: assets model durable external-state
# transitions, helpers (spreadsheet.py) do the computation, checks
# (checks.py) provide validation visibility, and partitions give each shot
# session its own history/retries/backfills. The flow is three partitioned
# assets — pdv_log -> pdv_data -> pdv_processing_manifest — partitioned by
# the AIMD-L logical key "<igsn>//<experiment_date>" of the PDV *traces*.
#
# The trace is the unit of work. Every annotated trace has an IGSN and an
# experiment date, and those resolve to the log holding the row that gives its
# flyer position. Driving the flow from the traces (rather than iterating log
# rows and hunting for files) means a trace can only ever be enriched from a
# row belonging to its own sample, and a log row with no ingested trace is a
# non-event rather than a reported gap.


class HelixSpreadsheetConfig(Config):
    """Run-time configuration for the helix_spreadsheet writing assets.

    dry_run — if True (default), pdv_data and pdv_processing_manifest
              perform all reads and compute the would-be Girder writes
              but skip the actual PUTs. Mirrors the coord_enrichment
              dry_run convention so a sweep can be rehearsed against
              production data without mutating it. Set False for a live
              run.
    """

    dry_run: bool = True


@asset(
    group_name="helix_spreadsheet",
    partitions_def=HELIX_TRACE_PARTITIONS,
)
def pdv_log(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> dict:
    """Durable boundary: read the experiment log(s) for one shot session.

    The partition key is the AIMD-L logical key ``"<igsn>//<experiment_date>"``
    taken from the PDV *trace* index; the same key selects the experiment
    log(s) for that session. A key may resolve to one log item, several, or
    none — traces exist for sessions whose log was never tagged upstream, and
    those partitions simply have nothing to read here.

    All resolved logs are concatenated and every source item id recorded (each
    row tagged with its origin via the ``_source_item_id`` column).

    Pure computation lives in spreadsheet.py; this asset owns only the
    Girder reads.
    """
    key = context.partition_key
    items = fetch_partition_details(girder, HELIX_EXPERIMENT_LOG_DATA_TYPE, key)

    frames = []
    source_item_ids = []
    for item in items:
        item_id = item["_id"]
        df = download_and_read(girder, item_id, item["name"])
        df = normalize_experiment_log(df)
        df["_source_item_id"] = item_id
        frames.append(df)
        source_item_ids.append(item_id)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame()

    combined, igsn_issues = validate_log_rows(combined)

    valid_igsn_count = (
        int(combined["valid_igsn"].notna().sum())
        if "valid_igsn" in combined.columns
        else 0
    )
    # Row-side context only. A log rows every *candidate* shot and the station
    # decides at fire time; rows for shots that never fired name no file and
    # have no trace. They explain why a log can hold more rows than the
    # session has traces, but they are not part of any denominator.
    shots = classify_shots(combined) if len(combined) else {
        "fired": 0, "not_fired": 0, "not_fired_by_reason": {}, "fired_but_unnamed": 0,
    }

    if not items:
        context.log.warning(
            "No experiment log tagged for partition %s — its traces cannot be "
            "enriched until the log is registered upstream.", key,
        )

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(key),
            "source_item_count": MetadataValue.int(len(source_item_ids)),
            "row_count": MetadataValue.int(len(combined)),
            "rows_naming_a_file": MetadataValue.int(count_rows_with_pdv(combined))
            if len(combined) else MetadataValue.int(0),
            "valid_igsn_count": MetadataValue.int(valid_igsn_count),
            "igsn_issue_count": MetadataValue.int(len(igsn_issues)),
            "shots_not_fired": MetadataValue.int(shots["not_fired"]),
            "not_fired_by_reason": MetadataValue.text(
                ", ".join(f"{k} ({v})" for k, v in shots["not_fired_by_reason"].items())
                or "none"
            ),
        }
    )
    return {
        "dataframe": combined,
        "igsn_issues": igsn_issues,
        "source_item_ids": source_item_ids,
        "partition_key": key,
    }


@asset(
    group_name="helix_spreadsheet",
    partitions_def=HELIX_TRACE_PARTITIONS,
)
def pdv_data(
    context: AssetExecutionContext,
    config: HelixSpreadsheetConfig,
    pdv_log: dict,
    girder: GirderConnection,
) -> dict:
    """Durable boundary: pair this session's traces with their log rows and
    write coordinate metadata to each trace.

    Fetches the PDV traces for this partition — not the whole collection —
    and, for each, finds the single log row naming it. Matching is scoped to
    the session, so a trace can only take coordinates from a row describing
    its own sample. Traces that resolve to no row, to more than one, or to a
    row declaring a different IGSN are reported and left untouched.

    With ``config.dry_run`` True (the default) the writes are computed and
    tallied but not performed.
    """
    key = context.partition_key
    df = pdv_log["dataframe"]
    source_item_ids = pdv_log["source_item_ids"]

    traces = fetch_partition_details(girder, HELIX_TRACE_DATA_TYPE, key)

    pairs, pair_issues = pair_traces_to_rows(traces, df)

    yaml_sha256 = compute_yaml_sha256(_COORD_YAML)
    transformer_version = get_transformer_version()

    try:
        run_id = context.run.run_id
    except Exception:
        run_id = None

    write_summary = write_pdv_metadata(
        girder,
        df,
        pairs,
        run_id=run_id,
        source_item_id=source_item_ids[0] if source_item_ids else None,
        yaml_sha256=yaml_sha256,
        transformer_version=transformer_version,
        dry_run=config.dry_run,
    )

    if write_summary["naive_timestamps_count"] > 0:
        context.log.warning(
            "Spreadsheet contained %d naive Timestamp values; "
            "interpreted as UTC. Set an explicit timezone in the "
            "station export to remove this ambiguity.",
            write_summary["naive_timestamps_count"],
        )

    if config.dry_run:
        context.log.info(
            "DRY RUN — would have written coordinate metadata to %d "
            "paired pdv_trace item(s); no Girder writes performed.",
            write_summary["simulated_count"],
        )

    version_counter = write_summary["version_counter"]
    by_type = {}
    for i in pair_issues:
        by_type[i["type"]] = by_type.get(i["type"], 0) + 1

    context.log.info(
        "pdv_data %s: %d trace(s), %d paired, %d unpaired (%s)",
        key, len(traces), len(pairs), len(pair_issues),
        ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())) or "none",
    )

    context.add_output_metadata(
        {
            "dry_run": MetadataValue.bool(config.dry_run),
            "traces_in_partition": MetadataValue.int(len(traces)),
            "paired_count": MetadataValue.int(len(pairs)),
            "unpaired_count": MetadataValue.int(len(pair_issues)),
            "unpaired_by_reason": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())) or "none"
            ),
            "log_items": MetadataValue.int(len(source_item_ids)),
            "items_enriched": MetadataValue.int(write_summary["written_count"]),
            "items_simulated": MetadataValue.int(write_summary["simulated_count"]),
            "no_station_coords": MetadataValue.int(write_summary["no_station_coords"]),
            "paired_by_shot_identity": MetadataValue.int(
                write_summary["paired_by_shot_identity"]
            ),
            "write_errors_count": MetadataValue.int(len(write_summary["write_errors"])),
            "igsn_mismatch_count": MetadataValue.int(by_type.get("igsn_mismatch", 0)),
            "coordinate_transform_failures": MetadataValue.int(
                write_summary["coord_failures"]
            ),
            "transform_version_count": MetadataValue.int(len(version_counter)),
            "yaml_sha256_present": MetadataValue.bool(yaml_sha256 is not None),
            "transform_versions_used": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
                or "none"
            ),
        }
    )
    return {
        "dry_run": config.dry_run,
        "traces_in_partition": len(traces),
        "paired_count": len(pairs),
        "pair_issues": pair_issues,
        "written_count": write_summary["written_count"],
        "simulated_count": write_summary["simulated_count"],
        "no_station_coords": write_summary["no_station_coords"],
        "paired_by_shot_identity": write_summary["paired_by_shot_identity"],
        "write_errors": write_summary["write_errors"],
        "coord_failures": write_summary["coord_failures"],
        "version_counter": version_counter,
        "yaml_sha256": yaml_sha256,
        "naive_timestamps_count": write_summary["naive_timestamps_count"],
    }


@asset(
    group_name="helix_spreadsheet",
    partitions_def=HELIX_TRACE_PARTITIONS,
)
def pdv_processing_manifest(
    context: AssetExecutionContext,
    config: HelixSpreadsheetConfig,
    pdv_log: dict,
    pdv_data: dict,
    girder: GirderConnection,
) -> dict:
    """Durable boundary: write meta.processing_status to source log item(s).

    Summarizes the run's issues and writes a processing manifest back to
    each source ``pdv_experiment_log`` Girder item, giving an audit
    trail, sensor idempotency, and cross-system visibility. With
    ``config.dry_run`` True (the default) the manifest is computed but
    not written. ``manifest_written`` is False only if a real per-item
    write failed.
    """
    summary = summarize_pdv_processing(pdv_log, pdv_data)

    try:
        run_id = context.run.run_id
    except Exception:
        run_id = "direct-invocation"

    source_item_ids = pdv_log["source_item_ids"]
    write_failed = False
    for item_id in source_item_ids:
        manifest = write_processing_manifest(
            girder, item_id, summary, run_id=run_id, dry_run=config.dry_run
        )
        if manifest.get("write_failed"):
            write_failed = True

    # A partition whose log was never tagged upstream has nothing to write a
    # manifest to. That is an upstream gap, not a write failure, so it does
    # not fail the check — `has_log` carries the distinction.
    manifest_written = not write_failed

    if config.dry_run:
        context.log.info(
            "DRY RUN — would have written meta.processing_status to %d "
            "source log item(s); no Girder writes performed.",
            len(source_item_ids),
        )

    context.add_output_metadata(
        {
            "dry_run": MetadataValue.bool(config.dry_run),
            "status": MetadataValue.text(summary["status"]),
            "manifest_written": MetadataValue.bool(manifest_written),
            "has_log": MetadataValue.bool(bool(source_item_ids)),
            "source_item_count": MetadataValue.int(len(source_item_ids)),
            "traces_enriched": MetadataValue.int(summary["traces_enriched"]),
        }
    )
    return {
        "dry_run": config.dry_run,
        "status": summary["status"],
        "manifest_written": manifest_written,
        "has_log": bool(source_item_ids),
        "issues_summary": summary["issues_summary"],
        "total_rows": summary["total_rows"],
        "rows_valid_igsn": summary["rows_valid_igsn"],
        "traces_in_partition": summary["traces_in_partition"],
        "traces_paired": summary["traces_paired"],
        "traces_enriched": summary["traces_enriched"],
    }


process_helix_assets_job = define_asset_job(
    name="process_helix_assets_job",
    selection=AssetSelection.assets(
        pdv_log,
        pdv_data,
        pdv_processing_manifest,
    ),
)
