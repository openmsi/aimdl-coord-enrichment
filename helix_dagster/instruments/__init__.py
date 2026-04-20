"""Per-instrument knowledge and dispatch for coordinate enrichment.

This package contains the per-instrument rules used by the new
coordinate-enrichment DAG. Each instrument's filename conventions,
provenance-healing logic, and station-side authoritative sources live
in the instrument-specific module (`helix.py`, `maxima.py`). This
file provides the registry that maps a Girder item's `meta.data_type`
to (a) the owning instrument and (b) whether the item is a leaf
(station-side source of coordinates) or a derived item (inherits
from a parent).

The leaf/derived distinction drives downstream behavior:
  - leaf:    station coords come from an authoritative source artifact
             (HELIX experiment-log CSV, MAXIMA instructions.txt, ...)
  - derived: station coords are inherited from the parent item linked
             by `meta.prov.wasDerivedFrom`.

The existing HELIX DAG handles `pdv_trace` leaf enrichment from the
spreadsheet, so the new DAG treats pdv_trace as a pre-enriched input,
not as a data type it writes to. pdv_trace therefore does not appear
in the leaf_data_types set for HELIX here — it is listed as an
"external" data type that other data types inherit from.
"""

from __future__ import annotations

from helix_dagster.instruments.types import (
    InstrumentName,
    ItemRole,
    LeafResolution,
    ResolutionError,
)

# Canonical instrument names — must match the `name` field in the
# coordinate-transformer YAML.
INSTRUMENT_HELIX: InstrumentName = "HELIX"
INSTRUMENT_MAXIMA: InstrumentName = "MAXIMA"

# Per-instrument data_type membership.
HELIX_DERIVED_DATA_TYPES: frozenset[str] = frozenset(
    {"pdv_alpss_output", "pdv_alpss_result", "pdv_alpss_results"}
)
HELIX_LEAF_DATA_TYPES: frozenset[str] = frozenset()  # pdv_trace is external

MAXIMA_LEAF_DATA_TYPES: frozenset[str] = frozenset({"xrd_raw", "xrf_raw"})
MAXIMA_DERIVED_DATA_TYPES: frozenset[str] = frozenset({"xrd_derived"})

# External pre-enriched data types that derived items may inherit from.
# Not written to by the new DAG.
EXTERNAL_LEAF_DATA_TYPES: frozenset[str] = frozenset({"pdv_trace"})

# Data types explicitly out of scope for the new DAG. Listed here so
# tests and audits can distinguish "unknown data_type" from
# "intentionally excluded data_type."
OUT_OF_SCOPE_DATA_TYPES: frozenset[str] = frozenset(
    {"xrd_metadata", "pdv_experiment_log", "xrd_calibrant_raw",
     "xrd_calibrant_derived", "unclassified"}
)


# Assembled once; used for O(1) lookups below.
_DATA_TYPE_REGISTRY: dict[str, tuple[InstrumentName, ItemRole]] = {}
for _dt in HELIX_LEAF_DATA_TYPES:
    _DATA_TYPE_REGISTRY[_dt] = (INSTRUMENT_HELIX, "leaf")
for _dt in HELIX_DERIVED_DATA_TYPES:
    _DATA_TYPE_REGISTRY[_dt] = (INSTRUMENT_HELIX, "derived")
for _dt in MAXIMA_LEAF_DATA_TYPES:
    _DATA_TYPE_REGISTRY[_dt] = (INSTRUMENT_MAXIMA, "leaf")
for _dt in MAXIMA_DERIVED_DATA_TYPES:
    _DATA_TYPE_REGISTRY[_dt] = (INSTRUMENT_MAXIMA, "derived")


def instrument_for_data_type(data_type: str) -> InstrumentName | None:
    """Return the instrument that owns a data_type, or None if not in scope."""
    entry = _DATA_TYPE_REGISTRY.get(data_type)
    return entry[0] if entry else None


def role_for_data_type(data_type: str) -> ItemRole | None:
    """Return 'leaf' or 'derived' for a data_type, or None if not in scope."""
    entry = _DATA_TYPE_REGISTRY.get(data_type)
    return entry[1] if entry else None


def is_in_scope(data_type: str | None) -> bool:
    """True iff data_type is one the new DAG enriches (leaf or derived)."""
    return data_type in _DATA_TYPE_REGISTRY


def all_in_scope_data_types() -> frozenset[str]:
    """Return the frozenset of every data_type the new DAG acts on."""
    return frozenset(_DATA_TYPE_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Dispatch helpers
# ---------------------------------------------------------------------------

from helix_dagster.instruments import helix as _helix
from helix_dagster.instruments import maxima as _maxima


def resolve_parent_item_id(item: dict, **context) -> str | None:
    """Resolve the parent item id for a derived item.

    Dispatch based on the item's data_type:
      - HELIX ALPSS items → helix.find_parent_pdv_item_id, requires
        ``pdv_inventory=`` in ``context``.
      - MAXIMA xrd_derived items → maxima.heal_maxima_derived_parent,
        requires ``girder=`` in ``context``.

    Returns None if the item's data_type is not in scope, is not a
    derived type, or no parent can be resolved.
    """
    dt = (item.get("meta") or {}).get("data_type")
    entry = _DATA_TYPE_REGISTRY.get(dt)
    if entry is None or entry[1] != "derived":
        return None
    instrument = entry[0]
    if instrument == INSTRUMENT_HELIX:
        inventory = context.get("pdv_inventory")
        if inventory is None:
            raise TypeError(
                "resolve_parent_item_id for HELIX requires pdv_inventory="
            )
        return _helix.find_parent_pdv_item_id(item, inventory)
    if instrument == INSTRUMENT_MAXIMA:
        girder = context.get("girder")
        if girder is None:
            raise TypeError(
                "resolve_parent_item_id for MAXIMA requires girder="
            )
        return _maxima.heal_maxima_derived_parent(item, girder)
    return None


def resolve_leaf(item: dict, **context) -> LeafResolution:
    """Resolve station coords and timestamp for a leaf item.

    Currently only MAXIMA (xrd_raw, xrf_raw) have leaves in the new
    DAG's scope. Raises TypeError if called for a non-leaf data_type.
    Requires ``girder=`` in ``context``.
    """
    dt = (item.get("meta") or {}).get("data_type")
    entry = _DATA_TYPE_REGISTRY.get(dt)
    if entry is None or entry[1] != "leaf":
        raise TypeError(
            f"resolve_leaf called for non-leaf or out-of-scope data_type {dt!r}"
        )
    instrument = entry[0]
    if instrument == INSTRUMENT_MAXIMA:
        girder = context.get("girder")
        if girder is None:
            raise TypeError(
                "resolve_leaf for MAXIMA requires girder="
            )
        return _maxima.resolve_leaf_coords(item, girder)
    raise TypeError(
        f"No leaf resolver registered for instrument {instrument!r}"
    )


__all__ = [
    "INSTRUMENT_HELIX",
    "INSTRUMENT_MAXIMA",
    "HELIX_LEAF_DATA_TYPES",
    "HELIX_DERIVED_DATA_TYPES",
    "MAXIMA_LEAF_DATA_TYPES",
    "MAXIMA_DERIVED_DATA_TYPES",
    "EXTERNAL_LEAF_DATA_TYPES",
    "OUT_OF_SCOPE_DATA_TYPES",
    "InstrumentName",
    "ItemRole",
    "LeafResolution",
    "ResolutionError",
    "instrument_for_data_type",
    "role_for_data_type",
    "is_in_scope",
    "all_in_scope_data_types",
    "resolve_parent_item_id",
    "resolve_leaf",
]
