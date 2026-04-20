"""Tests for coord_transform_config_snapshot asset."""

from unittest.mock import patch

import pytest
from dagster import build_asset_context

from helix_dagster.coord_enrichment.config_snapshot import (
    CoordTransformSnapshot,
    coord_transform_config_snapshot,
)
from helix_dagster.coordinates import _COORD_TRANSFORMER


@pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="CoordinateTransformer not loaded (YAML not found)",
)
def test_snapshot_success():
    context = build_asset_context()
    snap = coord_transform_config_snapshot(context)

    assert isinstance(snap, CoordTransformSnapshot)
    assert snap.yaml_sha256 is not None
    assert len(snap.yaml_sha256) == 64
    assert all(c in "0123456789abcdef" for c in snap.yaml_sha256)
    assert isinstance(snap.transformer_version, str)
    assert len(snap.transformer_version) > 0
    assert "HELIX" in snap.versions_by_instrument
    assert "MAXIMA" in snap.versions_by_instrument
    assert len(snap.versions_by_instrument["HELIX"]) >= 2


def test_snapshot_missing_transformer_raises():
    with patch(
        "helix_dagster.coord_enrichment.config_snapshot._COORD_TRANSFORMER", None
    ):
        context = build_asset_context()
        with pytest.raises(RuntimeError, match="CoordinateTransformer not loaded"):
            coord_transform_config_snapshot(context)


@pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="CoordinateTransformer not loaded (YAML not found)",
)
def test_snapshot_missing_yaml_leaves_sha_none():
    with patch(
        "helix_dagster.coord_enrichment.config_snapshot._COORD_YAML",
        "/nonexistent/path/to/transforms.yaml",
    ):
        context = build_asset_context()
        snap = coord_transform_config_snapshot(context)

        assert snap.yaml_sha256 is None
        assert snap.yaml_path == "/nonexistent/path/to/transforms.yaml"
        assert len(snap.versions_by_instrument) > 0


def test_snapshot_dataclass_is_frozen():
    snap = CoordTransformSnapshot(
        yaml_path="/tmp/test.yaml",
        yaml_sha256="a" * 64,
        transformer_version="1.0.0",
        versions_by_instrument={},
    )
    with pytest.raises(AttributeError):
        snap.yaml_path = "/other/path"  # type: ignore[misc]
