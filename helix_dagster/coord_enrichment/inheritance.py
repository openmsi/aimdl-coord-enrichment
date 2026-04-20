"""Parent-inheritance helper for Phase 4 derived-item leaves.

Given a derived Girder item that has `meta.prov.wasDerivedFrom`
set by `provenance_tagged_items`, fetch the parent and return
everything the enrichment leaf needs to produce a fresh
coord_provenance for the derived item.

This module is Dagster-adjacent (called from inside asset
bodies), so it avoids PEP 563 deferred annotations.
See docs/developer_notes/annotations.md.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from helix_dagster.instruments.types import ResolutionError


@dataclass(frozen=True)
class InheritedCoords:
    """Everything needed to enrich a derived item by inheritance."""

    station_x: float
    station_y: float
    parent_source_timestamp: datetime
    parent_transform_version: str
    parent_item_id: str
    parent_data_type: str


def _parse_iso_tz(value: Any, *, derived_item_id: str, field: str) -> datetime:
    """Parse an ISO-8601 string to a tz-aware datetime."""
    if value is None:
        raise ResolutionError(
            f"derived item {derived_item_id} parent provenance missing '{field}'"
        )
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        s = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            ts = datetime.fromisoformat(s)
        except ValueError as exc:
            raise ResolutionError(
                f"derived item {derived_item_id} parent provenance "
                f"'{field}'={value!r} unparseable: {exc}"
            ) from exc
    else:
        raise ResolutionError(
            f"derived item {derived_item_id} parent provenance "
            f"'{field}'={value!r} has unexpected type {type(value).__name__}"
        )
    if ts.tzinfo is None:
        raise ResolutionError(
            f"derived item {derived_item_id} parent provenance "
            f"'{field}'={value!r} is naive; must include timezone"
        )
    return ts


def inherit_from_parent(
    derived_item: dict, girder, *, fetch_item=None
) -> InheritedCoords:
    """Resolve a derived item's coordinates by reading from its parent.

    Parameters
    ----------
    derived_item : dict
        Must have ``meta.prov.wasDerivedFrom`` set to a parent item id.
    girder : GirderConnection
        Used to fetch the parent item when *fetch_item* is None.
    fetch_item : callable, optional
        Test seam: ``fetch_item(item_id) -> dict``.
    """
    derived_id = derived_item.get("_id", "<unknown>")
    prov = (derived_item.get("meta") or {}).get("prov") or {}
    parent_id = prov.get("wasDerivedFrom")
    if not parent_id:
        raise ResolutionError(
            f"derived item {derived_id} has no prov.wasDerivedFrom"
        )

    if fetch_item is None:
        fetch_item = lambda item_id: girder.get(f"item/{item_id}")

    try:
        parent = fetch_item(parent_id)
    except Exception as exc:
        raise ResolutionError(
            f"derived item {derived_id} parent fetch failed for "
            f"{parent_id!r}: {exc}"
        ) from exc

    if not isinstance(parent, dict) or not parent.get("_id"):
        raise ResolutionError(
            f"derived item {derived_id} parent fetch returned unusable value "
            f"for id {parent_id!r}"
        )

    p_meta = parent.get("meta") or {}
    station_x = p_meta.get("Station_X")
    station_y = p_meta.get("Station_Y")
    if station_x is None or station_y is None:
        raise ResolutionError(
            f"derived item {derived_id} parent {parent_id} missing Station_X/Station_Y "
            "(parent not yet enriched)"
        )

    parent_prov = p_meta.get("coord_provenance")
    if not isinstance(parent_prov, dict):
        raise ResolutionError(
            f"derived item {derived_id} parent {parent_id} missing coord_provenance "
            "(parent not yet enriched)"
        )

    parent_ts = _parse_iso_tz(
        parent_prov.get("source_timestamp"),
        derived_item_id=derived_id,
        field="source_timestamp",
    )

    parent_transform_version = parent_prov.get("transform_version")
    if not isinstance(parent_transform_version, str) or not parent_transform_version:
        raise ResolutionError(
            f"derived item {derived_id} parent {parent_id} "
            "coord_provenance.transform_version absent"
        )

    parent_data_type = p_meta.get("data_type")
    if not isinstance(parent_data_type, str) or not parent_data_type:
        raise ResolutionError(
            f"derived item {derived_id} parent {parent_id} missing meta.data_type"
        )

    return InheritedCoords(
        station_x=float(station_x),
        station_y=float(station_y),
        parent_source_timestamp=parent_ts,
        parent_transform_version=parent_transform_version,
        parent_item_id=parent["_id"],
        parent_data_type=parent_data_type,
    )


def inherited_station_coord_source(
    inherited: InheritedCoords,
) -> dict:
    """Return the station_coord_source sub-dict for inherited coords."""
    return {
        "kind": "inherited",
        "parent_item_id": inherited.parent_item_id,
        "parent_data_type": inherited.parent_data_type,
    }
