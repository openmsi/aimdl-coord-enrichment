"""Girder I/O utilities for downloading spreadsheets and listing items."""

import io
import math

import pandas as pd

from aimdl_coord_enrichment.constants import AIMDL_PAGE_LIMIT


def list_all_spreadsheet_items(client, folder_id):
    """Recursively list all CSV/XLSX items in a Girder folder.

    .. deprecated::
        Use list_recent_spreadsheets() for sensor polling. This function
        performs a recursive folder crawl that scales poorly.
    """
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


def list_recent_spreadsheets(client, folder_id, limit=100):
    """List recently created spreadsheet items in a Girder folder.

    Unlike list_all_spreadsheet_items(), this does NOT recursively walk
    subfolders. It queries items sorted by creation date (newest first)
    and filters for CSV/XLSX extensions.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    folder_id : str
        The Girder folder ID to search.
    limit : int
        Maximum number of items to return.

    Returns
    -------
    list[dict]
        Girder item dicts for spreadsheet files, newest first.
    """
    items = client.get(
        "item",
        parameters={
            "folderId": folder_id,
            "sort": "created",
            "sortdir": -1,
            "limit": limit,
        },
    )
    return [i for i in items if i["name"].endswith((".csv", ".xlsx", ".xls"))]


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


def fetch_aimdl_datatypes(client):
    """Fetch the list of available meta.data_type values from /aimdl/datatype.

    Returns a list of strings, e.g. ["pdv_trace", "xrd_raw", ...].
    """
    return client.get("aimdl/datatype")


def fetch_aimdl_datafiles(client, data_type, limit=100, offset=0):
    """Fetch a single page of items from /aimdl/datafiles.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    data_type : str
        The meta.data_type value to filter by (e.g., "pdv_trace").
    limit : int
        Max items per page (capped at 100 by endpoint).
    offset : int
        Pagination offset.

    Returns
    -------
    list[dict]
        List of Girder item dicts with _id, name, meta.igsn, meta.data_type,
        size, created, folderId, etc.
    """
    return client.get(
        "aimdl/datafiles",
        parameters={
            "dataType": data_type,
            "limit": min(limit, AIMDL_PAGE_LIMIT),
            "offset": offset,
        },
    )


def fetch_all_aimdl_datafiles(client, data_type):
    """Paginate through all items of a given data type via /aimdl/datafiles.

    The endpoint has a hard limit of 100 per page. This function fetches all
    pages and returns the concatenated result.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    data_type : str
        The meta.data_type value to filter by.

    Returns
    -------
    list[dict]
        All matching Girder item dicts.
    """
    all_items = []
    offset = 0
    while True:
        batch = fetch_aimdl_datafiles(client, data_type, offset=offset)
        if not batch:
            break
        all_items.extend(batch)
        if len(batch) < AIMDL_PAGE_LIMIT:
            break
        offset += AIMDL_PAGE_LIMIT
    return all_items


def fetch_partition_index(
    client, data_type: str, since: str | None = None
) -> dict[str, str]:
    """Return the partition index for a partition-aware data_type.

    Calls ``GET /aimdl/partition?dataType=<data_type>[&since=<since>]``
    and returns the response dict, keyed by
    ``"<igsn>//<experiment_date>"`` with content-hash values.

    The ``since`` parameter is accepted for future incremental-
    discovery use (e.g. sensor cursors). No caller in this codebase
    wires it up today; pass None to get the full index.
    """
    parameters: dict[str, str] = {"dataType": data_type}
    if since is not None:
        parameters["since"] = since
    return client.get("aimdl/partition", parameters=parameters)


def fetch_partition_details(
    client, data_type: str, key: str
) -> list[dict]:
    """Fetch items for one (data_type, partition-key) pair with full meta.

    Calls ``GET /aimdl/partition/details?dataType=<data_type>&key=<key>``.
    This is the scoped helper used by partition-bound assets. To
    enumerate all partitions of a data_type, prefer
    ``fetch_partition_index`` plus this per key, or the flattening
    ``fetch_items_by_partition`` (inventory/reporting only).

    ``key`` is the literal AIMD-L partition key — the string
    ``"<igsn>//<experiment_date>"`` as emitted by the Girder plugin
    and returned by ``fetch_partition_index``.
    """
    return client.get(
        "aimdl/partition/details",
        parameters={"dataType": data_type, "key": key},
    )


def fetch_items_by_partition(client, data_type):
    """Fetch all items of a partition-aware data_type with FULL meta.

    Inventory/reporting helper only — flattens every partition of the
    given data_type into a single list. Per-partition asset work
    should use ``fetch_partition_index`` + ``fetch_partition_details``
    directly so each partition stays scoped to its own run.

    Calls ``fetch_partition_index`` once to enumerate keys, then
    ``fetch_partition_details`` once per key. Returns a flat list of
    Girder item dicts with full meta (``experiment_date``, ``prov``,
    ``checksum``, etc.) preserved — unlike ``/aimdl/datafiles`` which
    strips meta down to ``data_type`` and ``igsn``.

    Partition keys with empty details are silently skipped.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    data_type : str
        A partition-aware ``meta.data_type`` (e.g. ``xrd_raw``).

    Returns
    -------
    list[dict]
        All items for the data_type with full meta.
    """
    keys = fetch_partition_index(client, data_type)
    all_items = []
    for key in keys:
        details = fetch_partition_details(client, data_type, key)
        if not details:
            continue
        all_items.extend(details)
    return all_items
