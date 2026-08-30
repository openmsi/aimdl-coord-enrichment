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

_ALPSS_SUFFIX_RE = re.compile(r"[-_][A-Za-z_]+(?:--[A-Za-z_]+)*\.[A-Za-z0-9]+$")


def alpss_shot_stem(filename: str) -> str | None:
    """Return the shot stem for an ALPSS item's filename.

    Two filename conventions are in production use, both with the
    same `<stem><sep><output>.ext` shape (``sep`` is ``-`` or ``_``):

      "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1-iq.png"
      → "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1"
      "C1--20250807--00001-results.csv" → "C1--20250807--00001"

    The stem shape is deliberately NOT constrained further: whether a
    stem names a real shot is settled authoritatively by
    ``find_parent_pdv_item_id`` requiring a unique ``<stem>.csv`` in
    the pdv_trace pool. An earlier ``_ch<N>`` tail requirement encoded
    the IGSN convention only and silently dropped every C-named item.

    Returns None if ``filename`` has no ``<sep><output>.ext`` suffix —
    which is what excludes the pdv_trace files themselves, since their
    stems end in digits.
    """
    if not filename:
        return None
    m = _ALPSS_SUFFIX_RE.search(filename)
    if not m:
        return None
    return filename[: m.start()]


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
