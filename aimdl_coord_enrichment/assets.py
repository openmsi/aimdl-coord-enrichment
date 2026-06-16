import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    MetadataValue,
    asset,
    define_asset_job,
)

from aimdl_coord_enrichment.constants import PDV_TRACE_DATA_TYPE
from aimdl_coord_enrichment.coordinates import _COORD_YAML
from aimdl_coord_enrichment.girder_io import (
    download_and_read,
    fetch_all_aimdl_datafiles,
    fetch_partition_details,
)
from aimdl_coord_enrichment.partitions import (
    HELIX_EXPERIMENT_LOG_DATA_TYPE,
    HELIX_EXPERIMENT_LOG_PARTITIONS,
)
from aimdl_coord_enrichment.provenance import compute_yaml_sha256, get_transformer_version
from aimdl_coord_enrichment.resources import GirderConnection
from aimdl_coord_enrichment.spreadsheet import (
    count_rows_with_pdv,
    match_pdv_rows,
    normalize_experiment_log,
    summarize_pdv_processing,
    validate_log_rows,
    write_pdv_metadata,
    write_processing_manifest,
)

# Design principle for this module: assets model durable external-state
# transitions, helpers (spreadsheet.py) do the computation, checks
# (checks.py) provide validation visibility, and partitions give each
# spreadsheet its own history/retries/backfills. The flow is three
# partitioned assets — pdv_log -> pdv_data -> pdv_processing_manifest —
# partitioned by the AIMD-L logical key "<igsn>//<experiment_date>".


@asset(
    group_name="helix_spreadsheet",
    partitions_def=HELIX_EXPERIMENT_LOG_PARTITIONS,
)
def pdv_log(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> dict:
    """Durable boundary: read the experiment log(s) for one partition.

    The partition key is the AIMD-L logical key
    ``"<igsn>//<experiment_date>"``. Fetches the matching
    ``pdv_experiment_log`` items, downloads + normalizes each into a
    DataFrame, concatenates them, and validates IGSNs. A partition key
    may resolve to one or more log items; all are concatenated and every
    source item id recorded (each row tagged with its origin via the
    ``_source_item_id`` column).

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

    context.add_output_metadata(
        {
            "partition_key": MetadataValue.text(key),
            "source_item_count": MetadataValue.int(len(source_item_ids)),
            "row_count": MetadataValue.int(len(combined)),
            "igsn_issue_count": MetadataValue.int(len(igsn_issues)),
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
    partitions_def=HELIX_EXPERIMENT_LOG_PARTITIONS,
)
def pdv_data(
    context: AssetExecutionContext,
    pdv_log: dict,
    girder: GirderConnection,
) -> dict:
    """Durable boundary: match PDV traces and write coordinate metadata.

    Fetches the full ``pdv_trace`` inventory (indexed /aimdl/datafiles
    query), matches each log row by PDV_FileName prefix, then writes
    Station/Sample coordinates + coord_provenance to each matched Girder
    item. Returns one dict bundling everything the five attached checks
    need.
    """
    df = pdv_log["dataframe"]
    source_item_ids = pdv_log["source_item_ids"]

    inventory = fetch_all_aimdl_datafiles(girder, PDV_TRACE_DATA_TYPE)
    matches, pdv_issues = match_pdv_rows(df, inventory)

    try:
        yaml_sha256 = compute_yaml_sha256(_COORD_YAML)
    except FileNotFoundError:
        context.log.error(
            "Coordinate transforms YAML not found at %s; "
            "coord_provenance.transform_yaml_sha256 will be null.",
            _COORD_YAML,
        )
        yaml_sha256 = None
    transformer_version = get_transformer_version()

    try:
        run_id = context.run.run_id
    except Exception:
        run_id = None

    write_summary = write_pdv_metadata(
        girder,
        df,
        matches,
        run_id=run_id,
        source_item_id=source_item_ids[0] if source_item_ids else None,
        yaml_sha256=yaml_sha256,
        transformer_version=transformer_version,
    )

    if write_summary["naive_timestamps_count"] > 0:
        context.log.warning(
            "Spreadsheet contained %d naive Timestamp values; "
            "interpreted as UTC. Set an explicit timezone in the "
            "station export to remove this ambiguity.",
            write_summary["naive_timestamps_count"],
        )

    version_counter = write_summary["version_counter"]
    context.add_output_metadata(
        {
            "inventory_count": MetadataValue.int(len(inventory)),
            "matched_count": MetadataValue.int(len(matches)),
            "items_enriched": MetadataValue.int(write_summary["written_count"]),
            "coordinate_transform_failures": MetadataValue.int(
                write_summary["coord_failures"]
            ),
            "transform_versions_used": MetadataValue.text(
                ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
                or "none"
            ),
        }
    )
    return {
        "inventory_count": len(inventory),
        "matched_count": len(matches),
        "rows_with_pdv": count_rows_with_pdv(df),
        "pdv_issues": pdv_issues,
        "written_count": write_summary["written_count"],
        "write_errors": write_summary["write_errors"],
        "coord_failures": write_summary["coord_failures"],
        "version_counter": version_counter,
        "yaml_sha256": yaml_sha256,
        "naive_timestamps_count": write_summary["naive_timestamps_count"],
    }


@asset(
    group_name="helix_spreadsheet",
    partitions_def=HELIX_EXPERIMENT_LOG_PARTITIONS,
)
def pdv_processing_manifest(
    context: AssetExecutionContext,
    pdv_log: dict,
    pdv_data: dict,
    girder: GirderConnection,
) -> dict:
    """Durable boundary: write meta.processing_status to source log item(s).

    Summarizes the run's issues and writes a processing manifest back to
    each source ``pdv_experiment_log`` Girder item, giving an audit
    trail, sensor idempotency, and cross-system visibility.
    ``manifest_written`` is False if any per-item write failed.
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
            girder, item_id, summary, run_id=run_id
        )
        if manifest.get("write_failed"):
            write_failed = True

    manifest_written = bool(source_item_ids) and not write_failed

    context.add_output_metadata(
        {
            "status": MetadataValue.text(summary["status"]),
            "manifest_written": MetadataValue.bool(manifest_written),
            "source_item_count": MetadataValue.int(len(source_item_ids)),
            "rows_enriched": MetadataValue.int(summary["rows_enriched"]),
        }
    )
    return {
        "status": summary["status"],
        "manifest_written": manifest_written,
        "issues_summary": summary["issues_summary"],
        "total_rows": summary["total_rows"],
        "rows_valid_igsn": summary["rows_valid_igsn"],
        "rows_matched_pdv": summary["rows_matched_pdv"],
        "rows_enriched": summary["rows_enriched"],
    }


process_helix_assets_job = define_asset_job(
    name="process_helix_assets_job",
    selection=AssetSelection.assets(
        pdv_log,
        pdv_data,
        pdv_processing_manifest,
    ),
)
