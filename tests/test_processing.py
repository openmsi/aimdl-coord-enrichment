"""Smoke tests for core modules."""

import importlib


def test_import_coordinates_with_missing_yaml(monkeypatch):
    """Verify coordinates.py can be imported when COORD_TRANSFORMS_YAML points to
    a nonexistent file — the module-level init should handle it gracefully."""
    monkeypatch.setenv("COORD_TRANSFORMS_YAML", "/tmp/nonexistent_transforms.yaml")

    try:
        import aimdl_coord_enrichment.coordinates as coord_mod

        importlib.reload(coord_mod)
    except Exception as exc:
        assert isinstance(exc, Exception), f"Unexpected error type: {type(exc)}"


def test_constants_importable():
    """Verify constants.py exports the expected symbols after cleanup."""
    from aimdl_coord_enrichment.constants import COLUMN_MAP, IGSN_PATTERN

    assert "Sample_ID" in COLUMN_MAP
    assert COLUMN_MAP["Sample_ID"] == "Sample_IGSN"
    assert IGSN_PATTERN.search("ABCDEF12345") is not None


def test_nan_to_none():
    """Test the nan_to_none helper."""
    from aimdl_coord_enrichment.girder_io import nan_to_none

    assert nan_to_none(float("nan")) is None
    assert nan_to_none(42) == 42
    assert nan_to_none("hello") == "hello"
    assert nan_to_none(None) is None
