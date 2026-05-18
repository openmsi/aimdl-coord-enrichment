from datetime import datetime, timezone

import pytest

from aimdl_coord_enrichment.coordinates import (
    transform_station_to_sample,
    transform_with_named_version,
    _COORD_TRANSFORMER,
)


def test_valid_transform():
    """Test coordinate transform with real YAML config (skip if unavailable)."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    sx, sy, name = transform_station_to_sample(10.0, 20.0)
    assert sx is not None
    assert sy is not None
    assert name is not None


def test_none_input():
    sx, sy, name = transform_station_to_sample(None, 5.0)
    assert sx is None
    assert sy is None
    assert name is None


def test_both_none():
    sx, sy, name = transform_station_to_sample(None, None)
    assert sx is None
    assert sy is None
    assert name is None


def test_missing_transformer(monkeypatch):
    """When _COORD_TRANSFORMER is None, returns (None, None, None)."""
    import aimdl_coord_enrichment.coordinates as coord_mod
    monkeypatch.setattr(coord_mod, "_COORD_TRANSFORMER", None)
    sx, sy, name = transform_station_to_sample(10.0, 20.0)
    assert sx is None
    assert sy is None
    assert name is None


def test_naive_timestamp_raises():
    """Naive (tz-unaware) datetimes must be rejected."""
    naive = datetime(2026, 1, 1, 12, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        transform_station_to_sample(10.0, 20.0, timestamp=naive)


def test_historical_timestamp_selects_v1():
    """A timestamp before HELIX v2's valid_from should resolve to v1."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    ts = datetime(2025, 6, 1, tzinfo=timezone.utc)
    sx, sy, name = transform_station_to_sample(10.0, 20.0, timestamp=ts)
    assert sx is not None
    assert sy is not None
    assert name is not None
    assert "v1" in name


def test_current_timestamp_selects_v2():
    """A current timestamp should resolve to HELIX v2."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    ts = datetime.now(timezone.utc)
    sx, sy, name = transform_station_to_sample(10.0, 20.0, timestamp=ts)
    assert sx is not None
    assert sy is not None
    assert name is not None
    assert "v2" in name


def test_no_timestamp_returns_current_version():
    """Omitting timestamp should resolve to the currently-valid version (v2)."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    sx, sy, name = transform_station_to_sample(10.0, 20.0)
    assert name is not None
    assert "v2" in name


# --- transform_with_named_version tests ---


def test_transform_with_named_version_happy_path_v1():
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    sx, sy = transform_with_named_version("HELIX", "HELIX/v1", 8.0, 8.0)
    assert sx is not None and sy is not None
    assert sx == pytest.approx(32.0, abs=0.01)
    assert sy == pytest.approx(8.0, abs=0.01)


def test_transform_with_named_version_happy_path_v2():
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    sx, sy = transform_with_named_version("HELIX", "HELIX/v2", 8.0, 8.0)
    assert sx is not None and sy is not None
    assert sx == pytest.approx(8.0, abs=0.01)
    assert sy == pytest.approx(8.0, abs=0.01)


def test_transform_with_named_version_unknown_version_returns_none():
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    sx, sy = transform_with_named_version("HELIX", "HELIX/v99", 8.0, 8.0)
    assert sx is None
    assert sy is None


def test_transform_with_named_version_none_input_returns_none():
    sx, sy = transform_with_named_version("HELIX", "HELIX/v1", None, 8.0)
    assert sx is None
    assert sy is None


def test_transform_with_named_version_missing_transformer_returns_none():
    import aimdl_coord_enrichment.coordinates as coord_mod
    original = coord_mod._COORD_TRANSFORMER
    try:
        coord_mod._COORD_TRANSFORMER = None
        sx, sy = transform_with_named_version("HELIX", "HELIX/v1", 8.0, 8.0)
        assert sx is None
        assert sy is None
    finally:
        coord_mod._COORD_TRANSFORMER = original
