"""MAXIMA adapter for coordinate enrichment.

MAXIMA leaf items (xrd_raw, xrf_raw) derive their station-frame
coordinates from a per-run-folder ``instructions.txt`` file whose
JSON payload contains ``sample.scan_points`` as a list of [x, y]
pairs. The scan-point index is encoded in the filename as
``scan_point_<i>``.

MAXIMA derived items (xrd_derived in the raw/ subfolder) inherit
their coordinates by pointing ``meta.prov.wasDerivedFrom`` at the
matching ``scan_point_<i>_master.h5`` item.

This module has two halves:
  - pure helpers (this commit): filename parsing, JSON parsing,
    scan-point lookup
  - Girder-backed helpers (next commit): run-folder discovery,
    instructions.txt fetch, master.h5 lookup
"""

from __future__ import annotations

import io
import json
import re
from datetime import datetime
from typing import Any

from aimdl_coord_enrichment.instruments.types import LeafResolution, ResolutionError

_SCAN_POINT_INDEX_RE = re.compile(r"^scan_point_(\d+)(?:[._]|$)")


def parse_scan_point_index(filename: str) -> int | None:
    """Return the scan-point index encoded in a MAXIMA filename, or
    None if the filename does not match the scan_point_<i> pattern.

    Accepts all observed MAXIMA filename shapes:
      scan_point_0.xrf            → 0
      scan_point_0.tiff           → 0
      scan_point_0_master.h5      → 0
      scan_point_0_data_000001.h5 → 0
      scan_point_0_scan.png       → 0
      scan_point_24_xrd.csv       → 24
    """
    if not filename:
        return None
    m = _SCAN_POINT_INDEX_RE.match(filename)
    if not m:
        return None
    return int(m.group(1))


def parse_instructions_json(content: str | bytes) -> dict[str, Any]:
    """Parse an instructions.txt payload into a dict.

    Validates that ``sample.scan_points`` exists and is a list of
    [x, y] pairs (two numeric elements each). Does not validate
    other fields.

    Raises ResolutionError if the content is not JSON or if
    sample.scan_points is missing or malformed.
    """
    if isinstance(content, bytes):
        try:
            content = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ResolutionError(
                f"instructions.txt is not UTF-8 decodable: {exc}"
            ) from exc
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ResolutionError(
            f"instructions.txt is not valid JSON: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ResolutionError(
            f"instructions.txt top-level must be a JSON object, got {type(data).__name__}"
        )
    sample = data.get("sample")
    if not isinstance(sample, dict):
        raise ResolutionError(
            "instructions.txt missing 'sample' object"
        )
    sp = sample.get("scan_points")
    if not isinstance(sp, list) or not sp:
        raise ResolutionError(
            "instructions.txt 'sample.scan_points' must be a non-empty list"
        )
    for idx, pt in enumerate(sp):
        if (
            not isinstance(pt, list)
            or len(pt) != 2
            or not all(isinstance(c, (int, float)) for c in pt)
        ):
            raise ResolutionError(
                f"scan_points[{idx}] malformed: expected [x, y] numeric pair, got {pt!r}"
            )
    return data


def scan_point_coords(
    parsed_instructions: dict[str, Any], index: int
) -> tuple[float, float]:
    """Return (x, y) in millimeters for the given scan-point index.

    parsed_instructions must be the output of parse_instructions_json.
    Raises ResolutionError if index is out of range.
    """
    sp = parsed_instructions["sample"]["scan_points"]
    if index < 0 or index >= len(sp):
        raise ResolutionError(
            f"scan_point index {index} out of range (0..{len(sp) - 1})"
        )
    x, y = sp[index]
    return float(x), float(y)


# ---------------------------------------------------------------------------
# Girder-backed helpers
# ---------------------------------------------------------------------------


def find_run_folder_id(item: dict, girder) -> str:
    """Return the Girder folder id of the run folder containing *item*.

    Raw items live in ``<run_folder>/raw/``; derived items may live at
    the run-folder root. If the item's immediate folder is named
    ``"raw"``, step up one level; otherwise return the folder as-is.
    """
    folder_id = item.get("folderId")
    if not folder_id:
        raise ResolutionError(
            f"item {item.get('_id')} has no folderId"
        )
    folder = girder.get(f"folder/{folder_id}")
    if folder.get("name") == "raw":
        parent_id = folder.get("parentId")
        if not parent_id:
            raise ResolutionError(
                f"raw folder {folder_id} has no parentId"
            )
        return parent_id
    return folder_id


def fetch_instructions_for_run(
    run_folder_id: str, girder
) -> tuple[dict, dict]:
    """Find and fetch instructions.txt inside a run folder.

    Returns ``(instructions_item, parsed_json)``.
    """
    items = girder.get(
        "item", parameters={"folderId": run_folder_id, "limit": 1000}
    )
    instr_items = [it for it in items if it.get("name") == "instructions.txt"]
    if len(instr_items) == 0:
        raise ResolutionError(
            f"run folder {run_folder_id} has no instructions.txt item"
        )
    if len(instr_items) > 1:
        raise ResolutionError(
            f"run folder {run_folder_id} has multiple instructions.txt items "
            f"({len(instr_items)}); ambiguous."
        )
    instr_item = instr_items[0]
    files = girder.get(f"item/{instr_item['_id']}/files")
    if not files:
        raise ResolutionError(
            f"instructions.txt item {instr_item['_id']} has no files"
        )
    buf = io.BytesIO()
    girder.downloadFile(files[0]["_id"], buf)
    buf.seek(0)
    parsed = parse_instructions_json(buf.read())
    return instr_item, parsed


def _experiment_date(item: dict) -> datetime:
    """Return item's ``meta.experiment_date`` as a tz-aware datetime."""
    raw = (item.get("meta") or {}).get("experiment_date")
    if raw is None:
        raise ResolutionError(
            f"item {item.get('_id')} missing meta.experiment_date; "
            "cannot select transform version"
        )
    try:
        s = raw[:-1] + "+00:00" if isinstance(raw, str) and raw.endswith("Z") else raw
        ts = datetime.fromisoformat(s) if isinstance(s, str) else s
    except (TypeError, ValueError) as exc:
        raise ResolutionError(
            f"item {item.get('_id')} meta.experiment_date={raw!r} unparseable: {exc}"
        ) from exc
    if ts.tzinfo is None:
        raise ResolutionError(
            f"item {item.get('_id')} meta.experiment_date={raw!r} is naive; must include timezone"
        )
    return ts


def resolve_leaf_coords(item: dict, girder) -> LeafResolution:
    """End-to-end leaf coordinate resolver for MAXIMA xrd_raw / xrf_raw."""
    name = item.get("name", "")
    index = parse_scan_point_index(name)
    if index is None:
        raise ResolutionError(
            f"item {item.get('_id')} name {name!r} does not encode a scan_point index"
        )
    run_folder_id = find_run_folder_id(item, girder)
    instr_item, parsed = fetch_instructions_for_run(run_folder_id, girder)
    x, y = scan_point_coords(parsed, index)
    ts = _experiment_date(item)
    return LeafResolution(
        station_x=x,
        station_y=y,
        source_timestamp=ts,
        source_timestamp_origin="meta.experiment_date",
        station_coord_source={
            "kind": "maxima_instructions",
            "instructions_item_id": instr_item["_id"],
            "scan_point_index": index,
        },
    )


def find_master_h5_item_id(
    run_folder_id: str, index: int, girder
) -> str | None:
    """Return the ``_id`` of ``scan_point_<index>_master.h5`` in the run
    folder's ``raw/`` subfolder, or ``None`` if missing.
    """
    subfolders = girder.get(
        "folder",
        parameters={"parentId": run_folder_id, "parentType": "folder", "limit": 1000},
    )
    raw_folders = [f for f in subfolders if f.get("name") == "raw"]
    if len(raw_folders) != 1:
        return None
    raw_id = raw_folders[0]["_id"]
    target = f"scan_point_{index}_master.h5"
    items = girder.get(
        "item", parameters={"folderId": raw_id, "limit": 1000}
    )
    for it in items:
        if it.get("name") == target:
            return it.get("_id")
    return None


def heal_maxima_derived_parent(
    derived_item: dict, girder
) -> str | None:
    """Determine the correct parent item id for a MAXIMA xrd_derived item.

    Returns the ``scan_point_<i>_master.h5`` item id if found, else
    ``None``. Does NOT consult or mutate prov metadata.
    """
    name = derived_item.get("name", "")
    index = parse_scan_point_index(name)
    if index is None:
        return None
    try:
        run_folder_id = find_run_folder_id(derived_item, girder)
    except ResolutionError:
        return None
    return find_master_h5_item_id(run_folder_id, index, girder)
