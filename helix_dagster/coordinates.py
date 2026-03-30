import logging
import os

from coordinate_transformer import CoordinateTransformer

logger = logging.getLogger(__name__)

_COORD_YAML = os.environ.get(
    "COORD_TRANSFORMS_YAML",
    "instrument_coordinate_transforms.yaml",
)
try:
    _COORD_TRANSFORMER = CoordinateTransformer.from_yaml(_COORD_YAML)
except FileNotFoundError:
    _COORD_TRANSFORMER = None


def transform_station_to_sample(station_x, station_y, instrument="HELIX"):
    """Transform instrument station coordinates to sample-frame coordinates.

    Returns (None, None) if either input is None or if the transformation fails.
    """
    if station_x is None or station_y is None:
        return None, None

    if _COORD_TRANSFORMER is None:
        return None, None

    try:
        return _COORD_TRANSFORMER.transform(instrument, station_x, station_y)
    except Exception:
        logger.warning(
            "Coordinate transform failed for (%s, %s) on instrument %s",
            station_x, station_y, instrument,
            exc_info=True,
        )
        return None, None
