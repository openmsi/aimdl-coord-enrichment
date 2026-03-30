import pytest

from helix_dagster.coordinates import transform_station_to_sample, _COORD_TRANSFORMER


def test_valid_transform():
    """Test coordinate transform with real YAML config (skip if unavailable)."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    sx, sy = transform_station_to_sample(10.0, 20.0)
    assert sx is not None
    assert sy is not None


def test_none_input():
    sx, sy = transform_station_to_sample(None, 5.0)
    assert sx is None
    assert sy is None


def test_both_none():
    sx, sy = transform_station_to_sample(None, None)
    assert sx is None
    assert sy is None


def test_missing_transformer(monkeypatch):
    """When _COORD_TRANSFORMER is None, returns (None, None)."""
    import helix_dagster.coordinates as coord_mod
    monkeypatch.setattr(coord_mod, "_COORD_TRANSFORMER", None)
    sx, sy = transform_station_to_sample(10.0, 20.0)
    assert sx is None
    assert sy is None
