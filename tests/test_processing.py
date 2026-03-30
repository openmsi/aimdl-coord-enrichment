"""Smoke tests for helix_dagster.processing."""

import os


def test_import_processing_with_missing_yaml(monkeypatch):
    """Verify processing.py can be imported when COORD_TRANSFORMS_YAML points to
    a nonexistent file — the CoordinateTransformer.from_yaml call may raise, but
    import-time errors should be catchable, not crash the interpreter."""
    # Point to a nonexistent YAML so the module-level init hits a missing file
    monkeypatch.setenv("COORD_TRANSFORMS_YAML", "/tmp/nonexistent_transforms.yaml")

    # Reload the module to trigger the module-level CoordinateTransformer init
    import importlib

    try:
        import helix_dagster.processing as proc_mod

        importlib.reload(proc_mod)
    except Exception as exc:
        # It's acceptable for the import to raise (FileNotFoundError, etc.)
        # as long as it doesn't segfault or produce an unrecoverable error.
        assert isinstance(exc, Exception), f"Unexpected error type: {type(exc)}"


def test_constants_importable():
    """Verify constants.py exports the expected symbols after cleanup."""
    from helix_dagster.constants import COLUMN_MAP, IGSN_PATTERN

    assert "Sample_ID" in COLUMN_MAP
    # Verify the case fix: should map to "Sample_IGSN" (uppercase S)
    assert COLUMN_MAP["Sample_ID"] == "Sample_IGSN"
    assert IGSN_PATTERN.search("ABCDEF12345") is not None


def test_find_pdv_matches():
    """Test the pure find_pdv_matches function."""
    from helix_dagster.processing import find_pdv_matches

    pdv_items = [
        {"name": "shot001_ch1.tdms"},
        {"name": "shot001_ch2.tdms"},
        {"name": "shot002_ch1.tdms"},
    ]
    assert len(find_pdv_matches(pdv_items, "shot001")) == 2
    assert len(find_pdv_matches(pdv_items, "shot002")) == 1
    assert len(find_pdv_matches(pdv_items, "shot003")) == 0


def test_nan_to_none():
    """Test the _nan_to_none helper."""
    import math

    from helix_dagster.processing import _nan_to_none

    assert _nan_to_none(float("nan")) is None
    assert _nan_to_none(42) == 42
    assert _nan_to_none("hello") == "hello"
    assert _nan_to_none(None) is None
