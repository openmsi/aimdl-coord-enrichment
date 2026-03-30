import io
import math

import pandas as pd

from helix_dagster.constants import PDV_FOLDER_ID
from helix_dagster.coordinates import transform_station_to_sample
from helix_dagster.matching import match_pdv_file
from helix_dagster.validation import validate_igsn


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
    valid_igsn, igsn_issue = validate_igsn(sample_id)
    if igsn_issue is not None:
        igsn_issue["row"] = row_index
        tag = "IGSN_MISSING" if igsn_issue["issue"] == "missing" else "IGSN_INVALID"
        logger.warning(f"[{tag}] {source_filename} row {row_index}: {sample_id}")
        issues["igsn_issues"].append(igsn_issue)

    # --- PDV file lookup ---
    pdv_item, pdv_issue = match_pdv_file(pdv_items, pdv_filename)
    if pdv_issue is not None:
        pdv_issue["row"] = row_index
        tag = "PDV_AMBIGUOUS" if pdv_issue["type"] == "ambiguous" else "PDV_NOT_FOUND"
        logger.warning(f"[{tag}] {source_filename} row {row_index}: '{pdv_filename}'")
        issues["pdv_issues"].append(pdv_issue)

    # --- PDV metadata: add Flyer_Row / Flyer_Column regardless of IGSN ---
    if pdv_item is not None:
        flyer_row = _nan_to_none(row.get("Flyer_Row"))
        flyer_col = _nan_to_none(row.get("Flyer_Column"))
        item_meta = pdv_item.get("meta", {})

        station_x = _nan_to_none(row.get("Flyer_X_Position_Corrected (mm)"))
        station_y = _nan_to_none(row.get("Flyer_Y_Position_Corrected (mm)"))
        sample_x, sample_y = transform_station_to_sample(station_x, station_y)
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
