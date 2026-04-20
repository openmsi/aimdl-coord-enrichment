import logging
import os
from datetime import datetime

from coordinate_transformer import CoordinateTransformer

logger = logging.getLogger(__name__)

_COORD_YAML = os.environ.get(
    "COORD_TRANSFORMS_YAML",
    "instrument_coordinate_transforms.yaml",
)
try:
    _COORD_TRANSFORMER = CoordinateTransformer.from_yaml(_COORD_YAML)
except FileNotFoundError:
    logger.warning(
        f"Coordinate transforms YAML file not found: {_COORD_YAML}."
        "Coordinate transformations will be unavailable.",
    )
    _COORD_TRANSFORMER = None


def transform_station_to_sample(
    station_x,
    station_y,
    instrument: str = "HELIX",
    timestamp: datetime | None = None,
) -> tuple[float | None, float | None, str | None]:
    """Transform instrument station coordinates to sample-frame coordinates.

    If ``timestamp`` is provided it must be timezone-aware; it selects the
    coordinate transform version valid at that instant. When omitted the
    currently-valid version is used.

    Returns ``(sample_x, sample_y, transform_name)`` where ``transform_name``
    is the resolved ``InstrumentTransform.name`` (e.g. ``"HELIX/v2"``).
    Returns ``(None, None, None)`` if inputs are missing, no transformer is
    configured, or the transform fails.
    """
    if timestamp is not None and timestamp.tzinfo is None:
        raise ValueError(
            f"timestamp must be timezone-aware; got naive datetime {timestamp!r}"
        )

    if station_x is None or station_y is None:
        return None, None, None

    if _COORD_TRANSFORMER is None:
        return None, None, None

    try:
        transform = _COORD_TRANSFORMER.get_transform(instrument, timestamp=timestamp)
        sample_x, sample_y = transform.transform_point(station_x, station_y)
        return sample_x, sample_y, transform.name
    except Exception:
        logger.warning(
            "Coordinate transform failed for (%s, %s) on instrument %s",
            station_x,
            station_y,
            instrument,
            exc_info=True,
        )
        return None, None, None


def transform_with_named_version(
    instrument: str,
    version_label: str,
    station_x,
    station_y,
) -> tuple[float | None, float | None]:
    """Transform using a specific registered version label (e.g., 'HELIX/v1').

    Looks up the InstrumentTransform whose name matches version_label
    directly, bypassing valid_from/valid_until resolution. The caller
    has already decided which version to use (typically inherited from
    a parent item's coord_provenance).

    Returns (sample_x, sample_y) or (None, None) on failure.

    TODO(aimdl_coordinate_systems): upstream a public
    get_transform_by_version() method; this reaches into _transforms
    because Phase 4 needs deterministic parent-version reuse.
    """
    if station_x is None or station_y is None:
        return None, None
    if _COORD_TRANSFORMER is None:
        return None, None
    versions = _COORD_TRANSFORMER._transforms.get(instrument, [])
    for v in versions:
        if v.transform.name == version_label:
            try:
                return v.transform.transform_point(station_x, station_y)
            except Exception:
                logger.warning(
                    "transform_with_named_version failed for %s %s (%s, %s)",
                    instrument, version_label, station_x, station_y,
                    exc_info=True,
                )
                return None, None
    logger.warning(
        "No registered version %r for instrument %r", version_label, instrument
    )
    return None, None
