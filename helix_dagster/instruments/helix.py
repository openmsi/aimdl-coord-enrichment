"""HELIX adapter for coordinate enrichment.

The new DAG does not enrich pdv_trace items — they arrive already
enriched by the existing spreadsheet-driven DAG. HELIX's only
contribution here is parent discovery for ALPSS items: given an
ALPSS output/result filename, find the PDV trace item whose name
matches the shot stem.

All functions in this module are pure — no Girder or network
dependencies.
"""

from __future__ import annotations

import re
from typing import Any

_ALPSS_SUFFIX_RE = re.compile(r"-[A-Za-z_]+(?:--[A-Za-z_]+)*\.[A-Za-z0-9]+$")
_SHOT_STEM_TAIL_RE = re.compile(r"_ch\d+$")


def alpss_shot_stem(filename: str) -> str | None:
    """Return the shot stem for an ALPSS item's filename.

    Example:
      "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png"
      → "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1"

    Returns None if ``filename`` does not match the expected
    `<stem>_ch<N>-<output>.ext` shape.
    """
    if not filename:
        return None
    m = _ALPSS_SUFFIX_RE.search(filename)
    if not m:
        return None
    stem = filename[: m.start()]
    if not _SHOT_STEM_TAIL_RE.search(stem):
        return None
    return stem


def find_parent_pdv_item_id(
    alpss_item: dict[str, Any], pdv_inventory: list[dict[str, Any]]
) -> str | None:
    """Return the _id of the PDV trace whose filename matches the
    ALPSS item's shot stem, or None if no unique match.

    Matching rule: the PDV trace's ``name`` must equal
    ``<shot_stem>.csv`` (exact match). ``.csv`` is the observed
    extension for PDV trace items in the production collection.

    If multiple inventory items share the same name, returns None —
    a caller-visible ambiguity rather than a silent pick.
    """
    stem = alpss_shot_stem(alpss_item.get("name", ""))
    if stem is None:
        return None
    target = f"{stem}.csv"
    matches = [it for it in pdv_inventory if it.get("name") == target]
    if len(matches) == 1:
        return matches[0].get("_id")
    return None
