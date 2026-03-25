import io
import math

import pandas as pd
from coordinate_transformer import CoordinateTransformer

_COORD_TRANSFORMER = CoordinateTransformer.from_yaml(
    "/Users/alirachidi/dev/work/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml"
)

from helix_dagster.constants import (
    COLUMN_MAP,
    FORM_ID,
    FORM_SCHEMA_FIELDS,
    IGSN_PATTERN,
    PDV_FOLDER_ID,
)


def list_all_spreadsheet_items(client, folder_id):
    """Recursively list all CSV/XLSX items in a Girder folder."""
    items = client.get(
        "item",
        parameters={"folderId": folder_id, "limit": 100000},
    )
    result = [i for i in items if i["name"].endswith((".csv", ".xlsx", ".xls"))]
    subfolders = client.get(
        "folder",
        parameters={"parentType": "folder", "parentId": folder_id, "limit": 100000},
    )
    for subfolder in subfolders:
        result.extend(list_all_spreadsheet_items(client, subfolder["_id"]))
    return result


def download_and_read(client, item_id, filename):
    """Download a Girder item and return as a DataFrame."""
    files = client.get(f"item/{item_id}/files")
    if not files:
        raise ValueError(f"No files found for item {item_id}")
    file_id = files[0]["_id"]
    buf = io.BytesIO()
    client.downloadFile(file_id, buf)
    buf.seek(0)
    if filename.endswith(".csv"):
        return pd.read_csv(buf)
    else:
        return pd.read_excel(buf)


def _fetch_all_pdv_items(client):
    """Fetch all items from the PDV folder."""
    return client.get(
        "item",
        parameters={"folderId": PDV_FOLDER_ID, "limit": 100000},
    )


def find_pdv_matches(pdv_items, pdv_filename):
    """Return items whose name starts with pdv_filename."""
    return [i for i in pdv_items if i["name"].startswith(pdv_filename)]


def _nan_to_none(val):
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def _build_entry(row):
    """Map spreadsheet row to form entry dict, converting NaN to None."""
    entry = {}
    for col, field in COLUMN_MAP.items():
        if col in row.index:
            entry[field] = _nan_to_none(row[col])
    return entry


def process_row(client, pdv_items, source_filename, row_index, row, logger):
    """
    Process a single spreadsheet row. Returns a dict of issues found.
    """
    issues = {
        "igsn_issues": [],
        "pdv_issues": [],
        "form_issues": [],
    }

    sample_id = row.get("Sample_IGSN")
    pdv_filename = row.get("PDV_FileName")

    # --- IGSN validation ---
    valid_igsn = None
    if sample_id is None or (isinstance(sample_id, float) and math.isnan(sample_id)):
        msg = f"row {row_index}: Sample_ID is missing"
        logger.warning(f"[IGSN_MISSING] {source_filename} {msg}")
        issues["igsn_issues"].append(
            {"row": row_index, "value": None, "issue": "missing"}
        )
    else:
        match = IGSN_PATTERN.search(str(sample_id))
        if not match:
            msg = f"row {row_index}: '{sample_id}' does not match IGSN pattern"
            logger.warning(f"[IGSN_INVALID] {source_filename} {msg}")
            issues["igsn_issues"].append(
                {"row": row_index, "value": str(sample_id), "issue": "invalid_format"}
            )
        else:
            valid_igsn = match.group(0)

    # --- PDV file lookup ---
    pdv_item = None
    if pdv_filename and not (
        isinstance(pdv_filename, float) and math.isnan(pdv_filename)
    ):
        matches = find_pdv_matches(pdv_items, str(pdv_filename))
        if len(matches) == 0:
            logger.warning(
                f"[PDV_NOT_FOUND] {source_filename} row {row_index}: '{pdv_filename}'"
            )
            issues["pdv_issues"].append(
                {"row": row_index, "pdv_filename": pdv_filename, "type": "not_found"}
            )
        elif len(matches) > 1:
            names = [m["name"] for m in matches]
            logger.warning(
                f"[PDV_AMBIGUOUS] {source_filename} row {row_index}: '{pdv_filename}' matched {names}"
            )
            issues["pdv_issues"].append(
                {
                    "row": row_index,
                    "pdv_filename": pdv_filename,
                    "type": "ambiguous",
                    "matches": names,
                }
            )
        else:
            pdv_item = matches[0]

    # --- PDV metadata: add Flyer_Row / Flyer_Column regardless of IGSN ---
    if pdv_item is not None:
        flyer_row = _nan_to_none(row.get("Flyer_Row"))
        flyer_col = _nan_to_none(row.get("Flyer_Column"))
        item_meta = pdv_item.get("meta", {})

        station_x = _nan_to_none(row.get("Flyer_X_Position_Corrected"))
        station_y = _nan_to_none(row.get("Flyer_Y_Position_Corrected"))
        sample_x, sample_y = None, None
        if station_x is not None and station_y is not None:
            sample_x, sample_y = _COORD_TRANSFORMER.transform(
                "HELIX", station_x, station_y
            )
        client.addMetadataToItem(
            pdv_item["_id"],
            {
                "Flyer_Row": flyer_row,
                "Flyer_Column": flyer_col,
                "Station_X": station_x,
                "Station_Y": station_y,
                "Sample_X": sample_x,
                "Sample_Y": sample_y,
            },
        )

        # --- IGSN metadata checks (only if IGSN is valid) ---
        if valid_igsn is not None:
            if "igsn" not in item_meta:
                logger.warning(
                    f"[PDV_NO_IGSN_METADATA] {source_filename} row {row_index}: '{pdv_filename}'"
                )
                issues["pdv_issues"].append(
                    {
                        "row": row_index,
                        "pdv_filename": pdv_filename,
                        "type": "no_igsn_metadata",
                    }
                )
            elif item_meta["igsn"] != valid_igsn:
                logger.warning(
                    f"[PDV_IGSN_MISMATCH] {source_filename} row {row_index}: "
                    f"expected '{valid_igsn}', got '{item_meta['igsn']}'"
                )
                issues["pdv_issues"].append(
                    {
                        "row": row_index,
                        "pdv_filename": pdv_filename,
                        "type": "igsn_mismatch",
                        "expected": valid_igsn,
                        "got": item_meta["igsn"],
                    }
                )

    return issues


def process_file(client, item_id, filename, logger):
    """
    Download and process a spreadsheet file. Returns aggregated issues dict.
    """
    logger.info(f"Processing {filename} (item {item_id})")

    df = download_and_read(client, item_id, filename)
    pdv_items = _fetch_all_pdv_items(client)

    aggregated = {
        "igsn_issues": [],
        "pdv_issues": [],
        "form_issues": [],
    }

    for row_index, row in df.iterrows():
        row_issues = process_row(client, pdv_items, filename, row_index, row, logger)
        aggregated["igsn_issues"].extend(row_issues["igsn_issues"])
        aggregated["pdv_issues"].extend(row_issues["pdv_issues"])
        aggregated["form_issues"].extend(row_issues["form_issues"])

    logger.info(
        f"Done {filename}: {len(df)} rows, "
        f"{len(aggregated['igsn_issues'])} IGSN issues, "
        f"{len(aggregated['pdv_issues'])} PDV issues, "
        f"{len(aggregated['form_issues'])} form issues"
    )
    return aggregated
