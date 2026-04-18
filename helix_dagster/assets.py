import json
from datetime import datetime, timezone

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Config,
    MetadataValue,
    asset,
    define_asset_job,
)

from helix_dagster import __version__ as PIPELINE_VERSION
from helix_dagster.constants import ALPSS_RESULT_DATA_TYPE, COLUMN_MAP, PDV_TRACE_DATA_TYPE
from helix_dagster.coordinates import transform_station_to_sample
from helix_dagster.girder_io import download_and_read, fetch_all_aimdl_datafiles, nan_to_none
from helix_dagster.matching import match_pdv_file
from helix_dagster.resources import GirderConnection
from helix_dagster.validation import NpEncoder, validate_igsn


class ExperimentLogConfig(Config):
    item_id: str
    filename: str


@asset
def raw_experiment_log(
    context: AssetExecutionContext,
    config: ExperimentLogConfig,
    girder: GirderConnection,
) -> pd.DataFrame:
    """Download a spreadsheet from Girder and apply COLUMN_MAP rename."""
    df = download_and_read(girder, config.item_id, config.filename)
    df = df.rename(columns=COLUMN_MAP)
    context.add_output_metadata(
        {
            "row_count": MetadataValue.int(len(df)),
            "filename": MetadataValue.text(config.filename),
            "source_item_id": MetadataValue.text(config.item_id),
        }
    )
    return df


@asset
def pdv_trace_inventory(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> list:
    """Fetch PDV trace items via the /aimdl/datafiles endpoint.

    Uses an indexed MongoDB query filtered by meta.data_type='pdv_trace'
    instead of crawling the PDV folder tree. Items must have meta.igsn
    set to appear in results.
    """
    items = fetch_all_aimdl_datafiles(girder, PDV_TRACE_DATA_TYPE)

    igsns = set()
    for item in items:
        igsn = item.get("meta", {}).get("igsn")
        if igsn:
            igsns.add(igsn)

    context.add_output_metadata({
        "item_count": MetadataValue.int(len(items)),
        "unique_igsns": MetadataValue.int(len(igsns)),
        "data_type": MetadataValue.text(PDV_TRACE_DATA_TYPE),
    })

    if len(items) == 0:
        context.log.warning(
            "pdv_trace_inventory returned 0 items. This may indicate that "
            "meta.igsn has not been tagged on PDV files yet."
        )

    return items


@asset
def validated_rows(
    context: AssetExecutionContext,
    raw_experiment_log: pd.DataFrame,
) -> dict:
    """Validate IGSNs for each row. Pure transformation, no network calls."""
    df = raw_experiment_log.copy()
    igsn_issues = []
    valid_igsns = []

    for idx, row in df.iterrows():
        valid_igsn, issue = validate_igsn(row.get("Sample_IGSN"))
        valid_igsns.append(valid_igsn)
        if issue is not None:
            issue["row"] = idx
            igsn_issues.append(issue)

    df["valid_igsn"] = valid_igsns

    valid_count = sum(1 for v in valid_igsns if v is not None)
    missing_count = sum(1 for i in igsn_issues if i["issue"] == "missing")
    invalid_count = sum(1 for i in igsn_issues if i["issue"] == "invalid_format")

    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(len(df)),
            "valid_igsn_count": MetadataValue.int(valid_count),
            "invalid_igsn_count": MetadataValue.int(invalid_count),
            "missing_igsn_count": MetadataValue.int(missing_count),
        }
    )
    return {"dataframe": df, "igsn_issues": igsn_issues}


@asset
def pdv_cross_references(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_trace_inventory: list,
) -> dict:
    """Match PDV filenames to inventory items. Pure matching, no network calls."""
    df = validated_rows["dataframe"]
    matches = {}
    pdv_issues = []

    for idx, row in df.iterrows():
        pdv_filename = row.get("PDV_FileName")
        pdv_item, issue = match_pdv_file(pdv_trace_inventory, pdv_filename)
        if pdv_item is not None:
            matches[idx] = pdv_item
            # Cross-check IGSN consistency
            row_igsn = row.get("valid_igsn")
            item_igsn = pdv_item.get("meta", {}).get("igsn")
            if row_igsn and item_igsn and row_igsn != item_igsn:
                pdv_issues.append({
                    "pdv_filename": pdv_filename,
                    "type": "igsn_mismatch",
                    "row": idx,
                    "spreadsheet_igsn": row_igsn,
                    "item_igsn": item_igsn,
                })
        if issue is not None:
            issue["row"] = idx
            pdv_issues.append(issue)

    matched_count = len(matches)
    not_found_count = sum(1 for i in pdv_issues if i["type"] == "not_found")
    ambiguous_count = sum(1 for i in pdv_issues if i["type"] == "ambiguous")

    mismatch_count = sum(1 for i in pdv_issues if i["type"] == "igsn_mismatch")

    context.add_output_metadata(
        {
            "matched_count": MetadataValue.int(matched_count),
            "not_found_count": MetadataValue.int(not_found_count),
            "ambiguous_count": MetadataValue.int(ambiguous_count),
            "igsn_mismatch_count": MetadataValue.int(mismatch_count),
        }
    )
    return {"matches": matches, "pdv_issues": pdv_issues}


@asset
def enriched_pdv_metadata(
    context: AssetExecutionContext,
    pdv_cross_references: dict,
    validated_rows: dict,
    girder: GirderConnection,
) -> dict:
    """Write coordinate and flyer position metadata to matched Girder PDV items.

    For each matched PDV item, writes: Flyer_Row, Flyer_Column,
    Station_X, Station_Y (instrument coordinates), and Sample_X,
    Sample_Y (transformed sample-frame coordinates).
    """
    df = validated_rows["dataframe"]
    matches = pdv_cross_references["matches"]

    written_count = 0
    write_errors = []
    coord_failures = 0

    for row_idx, pdv_item in matches.items():
        row = df.loc[row_idx]

        station_x = nan_to_none(row.get("Flyer_X_Position_Final_mm"))
        station_y = nan_to_none(row.get("Flyer_Y_Position_Final_mm"))
        sample_x, sample_y = transform_station_to_sample(station_x, station_y)
        # ensure that sample_x,y have only 4 meaningful digits to avoid bogus precision
        if sample_x is not None:
            sample_x = round(sample_x, 4)
        if sample_y is not None:
            sample_y = round(sample_y, 4)

        if station_x is not None and station_y is not None and sample_x is None:
            coord_failures += 1

        metadata = {
            "Flyer_Row": nan_to_none(row.get("Flyer_Row")),
            "Flyer_Column": nan_to_none(row.get("Flyer_Column")),
            "Station_X": station_x,
            "Station_Y": station_y,
            "Sample_X": sample_x,
            "Sample_Y": sample_y,
        }
        # Ensure all values are JSON-serializable
        metadata = json.loads(json.dumps(metadata, cls=NpEncoder))

        try:
            girder.addMetadataToItem(pdv_item["_id"], metadata)
            written_count += 1
        except Exception as exc:
            context.log.error(
                f"Failed to write metadata for row {row_idx}, item {pdv_item['_id']}: {exc}"
            )
            write_errors.append({"row": row_idx, "error": str(exc)})

    context.add_output_metadata(
        {
            "items_enriched": MetadataValue.int(written_count),
            "coordinate_transform_failures": MetadataValue.int(coord_failures),
        }
    )
    return {
        "written_count": written_count,
        "write_errors": write_errors,
        "coord_failures": coord_failures,
    }


@asset
def alpss_results_inventory(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> list:
    """Fetch ALPSS result items via the /aimdl/datafiles endpoint.

    Returns items with meta.data_type='pdv_alpss_result'. Used for
    quality reporting on ALPSS processing completeness.
    """
    items = fetch_all_aimdl_datafiles(girder, ALPSS_RESULT_DATA_TYPE)

    igsns = set()
    for item in items:
        igsn = item.get("meta", {}).get("igsn")
        if igsn:
            igsns.add(igsn)

    context.add_output_metadata({
        "item_count": MetadataValue.int(len(items)),
        "unique_igsns": MetadataValue.int(len(igsns)),
        "data_type": MetadataValue.text(ALPSS_RESULT_DATA_TYPE),
    })
    return items


@asset
def quality_report(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_cross_references: dict,
    enriched_pdv_metadata: dict,
    alpss_results_inventory: list,
) -> dict:
    """Aggregate all issues and ALPSS completeness metrics."""
    igsn_issues = validated_rows["igsn_issues"]
    pdv_issues = pdv_cross_references["pdv_issues"]
    write_errors = enriched_pdv_metadata["write_errors"]
    matches = pdv_cross_references["matches"]

    # ALPSS completeness: which matched PDV traces have ALPSS results?
    alpss_igsns = set()
    for item in alpss_results_inventory:
        igsn = item.get("meta", {}).get("igsn")
        if igsn:
            alpss_igsns.add(igsn)

    matched_igsns = set()
    df = validated_rows["dataframe"]
    for row_idx in matches:
        row_igsn = df.loc[row_idx].get("valid_igsn")
        if row_igsn:
            matched_igsns.add(row_igsn)

    igsns_with_alpss = matched_igsns & alpss_igsns
    igsns_without_alpss = matched_igsns - alpss_igsns

    report = {
        "igsn_issues": igsn_issues,
        "pdv_issues": pdv_issues,
        "write_errors": write_errors,
        "alpss_completeness": {
            "matched_igsns": len(matched_igsns),
            "igsns_with_alpss_results": len(igsns_with_alpss),
            "igsns_without_alpss_results": len(igsns_without_alpss),
            "missing_igsns": sorted(igsns_without_alpss),
        },
        "summary": {
            "total_igsn_issues": len(igsn_issues),
            "total_pdv_issues": len(pdv_issues),
            "total_write_errors": len(write_errors),
            "alpss_coverage_pct": (
                round(100 * len(igsns_with_alpss) / len(matched_igsns), 1)
                if matched_igsns else 0.0
            ),
        },
    }

    context.add_output_metadata({
        "total_igsn_issues": MetadataValue.int(len(igsn_issues)),
        "total_pdv_issues": MetadataValue.int(len(pdv_issues)),
        "total_write_errors": MetadataValue.int(len(write_errors)),
        "alpss_coverage_pct": MetadataValue.float(
            report["summary"]["alpss_coverage_pct"]
        ),
        "igsns_without_alpss": MetadataValue.int(len(igsns_without_alpss)),
    })
    return report


@asset
def processing_manifest(
    context: AssetExecutionContext,
    config: ExperimentLogConfig,
    quality_report: dict,
    validated_rows: dict,
    pdv_cross_references: dict,
    enriched_pdv_metadata: dict,
    girder: GirderConnection,
) -> dict:
    """Write a processing status record to the source spreadsheet's Girder item.

    This provides:
    - Audit trail: persistent record of what the pipeline did
    - Idempotency: sensor can check before triggering reruns
    - Cross-system visibility: Girder UI shows processing status
    """
    df = validated_rows["dataframe"]
    total_rows = len(df)
    valid_igsn_count = int(df["valid_igsn"].notna().sum())
    matched_count = len(pdv_cross_references["matches"])
    written_count = enriched_pdv_metadata["written_count"]
    coord_failures = enriched_pdv_metadata.get("coord_failures", 0)

    igsn_issues = validated_rows["igsn_issues"]
    pdv_issues = pdv_cross_references["pdv_issues"]
    write_errors = enriched_pdv_metadata["write_errors"]

    issues_summary = {
        "igsn_invalid": sum(1 for i in igsn_issues if i.get("issue") == "invalid_format"),
        "igsn_missing": sum(1 for i in igsn_issues if i.get("issue") == "missing"),
        "pdv_not_found": sum(1 for i in pdv_issues if i.get("type") == "not_found"),
        "pdv_ambiguous": sum(1 for i in pdv_issues if i.get("type") == "ambiguous"),
        "igsn_mismatch": sum(1 for i in pdv_issues if i.get("type") == "igsn_mismatch"),
        "write_errors": len(write_errors),
        "coord_failures": coord_failures,
    }

    has_issues = any(v > 0 for v in issues_summary.values())
    status = "completed_with_warnings" if has_issues else "completed_clean"

    try:
        run_id = context.run.run_id
    except Exception:
        run_id = "direct-invocation"

    manifest = {
        "last_processed": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": run_id,
        "pipeline_version": PIPELINE_VERSION,
        "total_rows": total_rows,
        "rows_valid_igsn": valid_igsn_count,
        "rows_matched_pdv": matched_count,
        "rows_enriched": written_count,
        "status": status,
        "issues_summary": issues_summary,
    }

    # Write to the source spreadsheet's Girder item
    try:
        girder.addMetadataToItem(config.item_id, {"processing_status": manifest})
        context.log.info(
            "Wrote processing manifest to Girder item %s: status=%s",
            config.item_id,
            status,
        )
    except Exception as exc:
        context.log.error(
            "Failed to write processing manifest to Girder item %s: %s",
            config.item_id,
            exc,
        )
        manifest["write_failed"] = True

    context.add_output_metadata({
        "status": MetadataValue.text(status),
        "total_rows": MetadataValue.int(total_rows),
        "rows_enriched": MetadataValue.int(written_count),
        "has_issues": MetadataValue.bool(has_issues),
        "source_item_id": MetadataValue.text(config.item_id),
    })

    return manifest


process_helix_assets_job = define_asset_job(
    name="process_helix_assets_job",
    selection=AssetSelection.all(),
)
