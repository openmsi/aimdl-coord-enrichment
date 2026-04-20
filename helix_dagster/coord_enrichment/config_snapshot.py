"""coord_transform_config_snapshot asset.

Captures a point-in-time snapshot of the coordinate-transformer
configuration so every enriched item's provenance can reference a
specific, reproducible set of transform parameters.
"""

from dataclasses import dataclass, field
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, asset

from helix_dagster.coordinates import _COORD_TRANSFORMER, _COORD_YAML
from helix_dagster.provenance import compute_yaml_sha256, get_transformer_version


@dataclass(frozen=True)
class CoordTransformSnapshot:
    """A frozen view of the coordinate-transformer config at a point in time."""

    yaml_path: str
    yaml_sha256: str | None
    transformer_version: str
    versions_by_instrument: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


@asset
def coord_transform_config_snapshot(
    context: AssetExecutionContext,
) -> CoordTransformSnapshot:
    """Snapshot the current coordinate-transformer configuration."""
    if _COORD_TRANSFORMER is None:
        raise RuntimeError(
            f"CoordinateTransformer not loaded (check COORD_TRANSFORMS_YAML={_COORD_YAML})."
        )

    try:
        yaml_sha256 = compute_yaml_sha256(_COORD_YAML)
    except FileNotFoundError:
        context.log.warning(
            "YAML file %s disappeared since module import; proceeding with None sha256.",
            _COORD_YAML,
        )
        yaml_sha256 = None

    transformer_version = get_transformer_version()

    versions_by_instrument: dict[str, list[dict[str, Any]]] = {}
    for name in _COORD_TRANSFORMER.instruments():
        versions_by_instrument[name] = _COORD_TRANSFORMER.list_versions(name)

    snap = CoordTransformSnapshot(
        yaml_path=_COORD_YAML,
        yaml_sha256=yaml_sha256,
        transformer_version=transformer_version,
        versions_by_instrument=versions_by_instrument,
    )

    context.add_output_metadata(
        {
            "yaml_path": MetadataValue.text(_COORD_YAML),
            "yaml_sha256": MetadataValue.text(yaml_sha256 or "<missing>"),
            "transformer_version": MetadataValue.text(transformer_version),
            "instruments": MetadataValue.text(
                ", ".join(sorted(versions_by_instrument))
            ),
        }
    )

    return snap
