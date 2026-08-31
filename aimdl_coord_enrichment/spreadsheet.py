"""Pure helper functions for the HELIX spreadsheet flow.

Context-free, unit-testable computation extracted from the former
nine-asset spreadsheet DAG. Each function reuses an existing domain
helper (``validate_igsn``, ``match_pdv_file``, ``transform_station_to_sample``,
``build_coord_provenance``) rather than reimplementing it. No ``dagster``
import lives here — the three partitioned assets in ``assets.py`` call
these and own all durable external-state transitions.
"""

import json
import math
from datetime import datetime, timezone

import pandas as pd

from aimdl_coord_enrichment import __version__ as PIPELINE_VERSION
from aimdl_coord_enrichment.constants import COLUMN_MAP
from aimdl_coord_enrichment.coordinates import transform_station_to_sample
from aimdl_coord_enrichment.girder_io import nan_to_none
from aimdl_coord_enrichment.matching import match_pdv_file
from aimdl_coord_enrichment.provenance import build_coord_provenance
from aimdl_coord_enrichment.validation import NpEncoder, validate_igsn


def normalize_experiment_log(df):
    """Apply COLUMN_MAP rename to a raw experiment-log DataFrame."""
    return df.rename(columns=COLUMN_MAP)


def validate_log_rows(df):
    """Validate IGSNs for each row.

    Returns ``(df_with_valid_igsn, igsn_issues)`` where the returned
    DataFrame has a ``valid_igsn`` column added and ``igsn_issues`` is a
    list of structured issue dicts (each carrying its ``row`` index).
    """
    df = df.copy()
    igsn_issues = []
    valid_igsns = []

    for idx, row in df.iterrows():
        valid_igsn, issue = validate_igsn(row.get("Sample_IGSN"))
        valid_igsns.append(valid_igsn)
        if issue is not None:
            issue["row"] = idx
            igsn_issues.append(issue)

    df["valid_igsn"] = valid_igsns
    return df, igsn_issues


def match_pdv_rows(df, pdv_items):
    """Match each row's PDV_FileName to the inventory and cross-check IGSNs.

    Returns ``(matches, pdv_issues)`` where ``matches`` is
    ``{row_idx: pdv_item}`` and ``pdv_issues`` carries not_found /
    ambiguous / igsn_mismatch issues (each with its ``row`` index).
    """
    matches = {}
    pdv_issues = []

    for idx, row in df.iterrows():
        pdv_filename = row.get("PDV_FileName")
        pdv_item, issue = match_pdv_file(pdv_items, pdv_filename)
        if pdv_item is not None:
            matches[idx] = pdv_item
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

    return matches, pdv_issues


# A HELIX experiment log records one row per *candidate* shot. Before firing,
# the station measures PDV return power and decides whether to proceed; when it
# declines, the row still carries a real flyer position but no shot is taken and
# no PDV trace is produced. Such a row is not a coverage gap — there is nothing
# to enrich, by design.
#
# The log states the outcome in its Notes column. Measured across all 214
# production logs (2026-08-31): every one of the 3,584 rows with a PDV_FileName
# reads exactly "Laser triggered", and all 1,506 rows without one carry a skip
# reason instead ("Laser skipped due to invalid ...", "Laser skipped due to
# detection ...", "Skipped due to failed flyer ...", and list-valued variants).
#
# Test for the positive rather than enumerating skip reasons, so a new skip
# string added upstream is still read as not-fired. Do NOT key on
# Laser_Target_Energy_mJ: 623 of the 1,506 skipped rows carry a non-zero value
# (the intended energy, recorded before the decision), so energy misclassifies
# 41% of them as fired.
FIRED_NOTE = "Laser triggered"


def _cell_is_blank(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return str(value).strip() == ""


def shot_fired(row) -> bool:
    """True if this row's candidate shot actually fired.

    A row with a PDV_FileName always fired (empirically 3,584/3,584). Otherwise
    the Notes column decides.
    """
    if not _cell_is_blank(row.get("PDV_FileName")):
        return True
    return str(row.get("Notes", "")).strip() == FIRED_NOTE


def skip_reason(row) -> str:
    """Short reason a candidate shot did not fire, for grouped reporting."""
    note = str(row.get("Notes", "")).strip()
    if not note or note.lower() == "nan":
        return "unrecorded"
    return note[:60]


def classify_shots(df):
    """Split a log into fired / not-fired, with skip reasons grouped.

    Returns ``{"fired": int, "not_fired": int, "not_fired_by_reason": {...},
    "fired_but_unnamed": int}``. ``fired_but_unnamed`` counts rows that claim to
    have fired yet name no PDV file — zero across the current corpus, surfaced
    so it does not pass unnoticed if upstream behaviour changes.
    """
    fired = not_fired = fired_but_unnamed = 0
    reasons: dict[str, int] = {}
    for _, row in df.iterrows():
        named = not _cell_is_blank(row.get("PDV_FileName"))
        if shot_fired(row):
            fired += 1
            if not named:
                fired_but_unnamed += 1
        else:
            not_fired += 1
            r = skip_reason(row)
            reasons[r] = reasons.get(r, 0) + 1
    return {
        "fired": fired,
        "not_fired": not_fired,
        "not_fired_by_reason": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
        "fired_but_unnamed": fired_but_unnamed,
    }


def count_rows_with_pdv(df):
    """Count rows whose PDV_FileName is non-null, non-NaN, non-empty."""
    return sum(
        1
        for _, row in df.iterrows()
        if row.get("PDV_FileName") is not None
        and not (
            isinstance(row.get("PDV_FileName"), float)
            and math.isnan(row.get("PDV_FileName"))
        )
        and str(row.get("PDV_FileName")).strip() != ""
    )


def _parse_row_timestamp(raw):
    """Return (tz-aware datetime or None, was_naive: bool, origin: str)."""
    if raw is None:
        return None, False, "missing"
    try:
        ts = pd.to_datetime(raw)
    except (ValueError, TypeError):
        return None, False, "unparseable"
    if pd.isna(ts):
        return None, False, "missing"
    py_ts = ts.to_pydatetime()
    if py_ts.tzinfo is None:
        py_ts = py_ts.replace(tzinfo=timezone.utc)
        return py_ts, True, "spreadsheet_timestamp_col_assumed_utc"
    return py_ts, False, "spreadsheet_timestamp_col"


def write_pdv_metadata(
    girder,
    df,
    matches,
    *,
    run_id,
    source_item_id,
    yaml_sha256,
    transformer_version,
    dry_run=False,
):
    """Write coordinate + provenance metadata to each matched PDV item.

    For each matched row: parse the shot timestamp, transform station to
    sample coordinates (version selected by timestamp), build the
    coord_provenance block, and write to the Girder item. Each row's
    provenance is attributed to its originating source-log item via the
    ``_source_item_id`` column when present, else ``source_item_id``.

    When ``dry_run`` is True the coordinate metadata is fully computed but
    the Girder PUT is skipped; the would-be write is tallied in
    ``simulated_count`` instead of ``written_count``.

    Returns a summary dict with ``written_count``, ``simulated_count``,
    ``write_errors``, ``coord_failures``, ``version_counter``,
    ``naive_timestamps_count``.
    """
    naive_timestamps_count = 0
    version_counter = {}
    written_count = 0
    simulated_count = 0
    write_errors = []
    coord_failures = 0

    for row_idx, pdv_item in matches.items():
        row = df.loc[row_idx]

        station_x = nan_to_none(row.get("Flyer_X_Position_Final_mm"))
        station_y = nan_to_none(row.get("Flyer_Y_Position_Final_mm"))
        shot_ts, was_naive, ts_origin = _parse_row_timestamp(row.get("Timestamp"))
        if was_naive:
            naive_timestamps_count += 1
        sample_x, sample_y, transform_name = transform_station_to_sample(
            station_x, station_y, timestamp=shot_ts
        )
        if transform_name is not None:
            version_counter[transform_name] = version_counter.get(transform_name, 0) + 1
        # ensure that sample_x,y have only 4 meaningful digits to avoid bogus precision
        if sample_x is not None:
            sample_x = round(sample_x, 4)
        if sample_y is not None:
            sample_y = round(sample_y, 4)

        if station_x is not None and station_y is not None and sample_x is None:
            coord_failures += 1

        row_source_item_id = row.get("_source_item_id")
        if row_source_item_id is None or (
            isinstance(row_source_item_id, float) and math.isnan(row_source_item_id)
        ):
            row_source_item_id = source_item_id

        coord_prov = build_coord_provenance(
            instrument="HELIX",
            transform_version=transform_name,
            transform_yaml_sha256=yaml_sha256 or "",
            transformer_version=transformer_version,
            pipeline_version=PIPELINE_VERSION,
            source_timestamp=shot_ts,
            source_timestamp_origin=ts_origin,
            station_coord_source={
                "kind": "helix_experiment_log",
                "spreadsheet_item_id": row_source_item_id,
                "spreadsheet_row_index": int(row_idx),
                "spreadsheet_pdv_filename": row.get("PDV_FileName"),
            },
            dagster_run_id=run_id,
        )

        metadata = {
            "Flyer_Row": nan_to_none(row.get("Flyer_Row")),
            "Flyer_Column": nan_to_none(row.get("Flyer_Column")),
            "Station_X": station_x,
            "Station_Y": station_y,
            "Sample_X": sample_x,
            "Sample_Y": sample_y,
            "coord_provenance": coord_prov,
        }
        # Ensure all values are JSON-serializable
        metadata = json.loads(json.dumps(metadata, cls=NpEncoder))

        if dry_run:
            simulated_count += 1
            continue

        try:
            girder.addMetadataToItem(pdv_item["_id"], metadata)
            written_count += 1
        except Exception as exc:
            write_errors.append({"row": row_idx, "error": str(exc)})

    return {
        "written_count": written_count,
        "simulated_count": simulated_count,
        "write_errors": write_errors,
        "coord_failures": coord_failures,
        "version_counter": version_counter,
        "naive_timestamps_count": naive_timestamps_count,
    }


def summarize_pdv_processing(pdv_log, pdv_data):
    """Aggregate issues into a processing summary (status + issues_summary).

    Replaces the old quality_report + processing_manifest aggregation,
    minus ALPSS completeness (coverage now lives in the read-only
    helix_pdv_coverage_observer).
    """
    df = pdv_log["dataframe"]
    igsn_issues = pdv_log["igsn_issues"]
    pdv_issues = pdv_data["pdv_issues"]
    write_errors = pdv_data["write_errors"]

    total_rows = len(df)
    valid_igsn_count = (
        int(df["valid_igsn"].notna().sum()) if "valid_igsn" in df.columns else 0
    )

    issues_summary = {
        "igsn_invalid": sum(1 for i in igsn_issues if i.get("issue") == "invalid_format"),
        "igsn_missing": sum(1 for i in igsn_issues if i.get("issue") == "missing"),
        "pdv_not_found": sum(1 for i in pdv_issues if i.get("type") == "not_found"),
        "pdv_ambiguous": sum(1 for i in pdv_issues if i.get("type") == "ambiguous"),
        "igsn_mismatch": sum(1 for i in pdv_issues if i.get("type") == "igsn_mismatch"),
        "write_errors": len(write_errors),
        "coord_failures": pdv_data.get("coord_failures", 0),
    }

    has_issues = any(v > 0 for v in issues_summary.values())
    status = "completed_with_warnings" if has_issues else "completed_clean"

    return {
        "status": status,
        "has_issues": has_issues,
        "issues_summary": issues_summary,
        "total_rows": total_rows,
        "rows_valid_igsn": valid_igsn_count,
        "rows_matched_pdv": pdv_data["matched_count"],
        "rows_enriched": pdv_data["written_count"],
    }


def write_processing_manifest(girder, item_id, summary, *, run_id, dry_run=False):
    """Write meta.processing_status to one source-log Girder item.

    When ``dry_run`` is True the manifest is built but the Girder PUT is
    skipped (and ``write_failed`` is never set — the would-be write is
    treated as a success for reporting).

    Returns the manifest dict, with ``write_failed`` set True if a real
    Girder write raised.
    """
    manifest = {
        "last_processed": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": run_id,
        "pipeline_version": PIPELINE_VERSION,
        "total_rows": summary["total_rows"],
        "rows_valid_igsn": summary["rows_valid_igsn"],
        "rows_matched_pdv": summary["rows_matched_pdv"],
        "rows_enriched": summary["rows_enriched"],
        "status": summary["status"],
        "issues_summary": summary["issues_summary"],
    }

    if dry_run:
        return manifest

    try:
        girder.addMetadataToItem(item_id, {"processing_status": manifest})
    except Exception:
        manifest["write_failed"] = True

    return manifest
