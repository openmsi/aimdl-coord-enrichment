"""Girder I/O utilities for downloading spreadsheets and listing items."""

import io
import math

import pandas as pd

from helix_dagster.constants import PDV_FOLDER_ID


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


def nan_to_none(val):
    """Convert NaN floats to None, pass through everything else."""
    if isinstance(val, float) and math.isnan(val):
        return None
    return val
