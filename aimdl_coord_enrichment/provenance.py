"""Provenance payload construction for coordinate-enriched Girder items.

Pure functions; no Dagster, Girder, or network dependencies. Called
by enrichment assets to build the coord_provenance dict that goes
alongside Station_X/Y and Sample_X/Y on every enriched item.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def compute_yaml_sha256(yaml_path: str | Path) -> str:
    """Return the hex sha256 digest of the file at yaml_path.

    Raises FileNotFoundError if the file does not exist.
    """
    return hashlib.sha256(Path(yaml_path).read_bytes()).hexdigest()


def get_transformer_version() -> str:
    """Return the installed coordinate-transformer version string.

    Falls back to 'unknown' if the package does not expose a version.
    Does not raise.
    """
    try:
        import coordinate_transformer as ct

        version = getattr(ct, "__version__", None)
        if isinstance(version, str) and version:
            return version
    except Exception:
        pass

    try:
        from importlib.metadata import version as _metadata_version

        return _metadata_version("coordinate-transformer")
    except Exception:
        logger.warning(
            "Could not resolve coordinate-transformer version; using 'unknown'."
        )
        return "unknown"


def build_coord_provenance(
    *,
    instrument: str,
    transform_version: str | None,
    transform_yaml_sha256: str,
    transformer_version: str,
    pipeline_version: str,
    source_timestamp: datetime | None,
    source_timestamp_origin: str,
    station_coord_source: dict[str, Any],
    enriched_at: datetime | None = None,
    dagster_run_id: str | None = None,
) -> dict[str, Any]:
    """Build a coord_provenance dict suitable for writing to Girder.

    All parameters are keyword-only. source_timestamp may be None when
    the caller could not resolve one; in that case the output's
    source_timestamp field is null.

    enriched_at defaults to datetime.now(timezone.utc) when None.
    dagster_run_id may be None; when so it is omitted from the dict
    rather than written as null.

    Returns a JSON-serializable dict that matches §6.1 of
    docs/coordinate_enrichment_dag.md.
    """
    if source_timestamp is not None and source_timestamp.tzinfo is None:
        raise ValueError(
            f"source_timestamp must be timezone-aware; got naive datetime {source_timestamp!r}"
        )

    if enriched_at is None:
        enriched_at = datetime.now(timezone.utc)
    elif enriched_at.tzinfo is None:
        raise ValueError(
            f"enriched_at must be timezone-aware; got naive datetime {enriched_at!r}"
        )

    payload: dict[str, Any] = {
        "instrument": instrument,
        "transform_version": transform_version,
        "transform_yaml_sha256": transform_yaml_sha256,
        "transformer_version": transformer_version,
        "pipeline_version": pipeline_version,
        "source_timestamp": source_timestamp.isoformat() if source_timestamp else None,
        "source_timestamp_origin": source_timestamp_origin,
        "station_coord_source": station_coord_source,
        "enriched_at": enriched_at.isoformat(),
    }
    if dagster_run_id is not None:
        payload["dagster_run_id"] = dagster_run_id
    return payload
