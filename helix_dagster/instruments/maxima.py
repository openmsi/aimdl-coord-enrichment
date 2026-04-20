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

import json
import re
from typing import Any

from helix_dagster.instruments.types import ResolutionError

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
