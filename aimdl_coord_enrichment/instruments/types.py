"""Shared types for the instruments subpackage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

InstrumentName = Literal["HELIX", "MAXIMA"]
ItemRole = Literal["leaf", "derived"]


class ResolutionError(Exception):
    """Raised when an adapter cannot resolve the requested attribute
    from an item — e.g., a scan-point index is out of bounds, an
    instructions.txt is malformed, or a run folder has no parent.

    Distinguishes a deterministic-failure case (that should be
    surfaced to the DAG as a failed partition item) from unexpected
    exceptions (which should still propagate as bugs).
    """


@dataclass(frozen=True)
class LeafResolution:
    """Result of resolving station coordinates for a leaf item.

    station_x, station_y  — instrument-frame coordinates, in mm.
    source_timestamp      — timezone-aware datetime used for selecting
                            the transform version.
    source_timestamp_origin — e.g., "meta.experiment_date",
                              "hdf5_header".
    station_coord_source  — dict matching the station_coord_source
                            shape in coord_provenance §6.2, to be
                            written verbatim.
    """

    station_x: float
    station_y: float
    source_timestamp: datetime
    source_timestamp_origin: str
    station_coord_source: dict[str, Any]
