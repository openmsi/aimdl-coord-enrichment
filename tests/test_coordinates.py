from datetime import datetime, timedelta, timezone

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


# --- HELIX version-selection boundary tests ---
#
# HELIX recalibrated on 2026-04-01T00:00:00-04:00: the instrument's station
# frame was physically realigned to match the sample frame, so from that
# instant on Station == Sample (v2 = identity). Earlier shots use v1, a
# horizontal flip about x=20 (x' = 40 - x, y' = y). These tests lock the
# cutover instant and the value-level behavior through the timestamp-driven
# selection path, so a future YAML edit can't silently move the boundary or
# break the realignment. valid_from is inclusive, valid_until exclusive.

HELIX_V2_CUTOVER_UTC = datetime(2026, 4, 1, 4, 0, 0, tzinfo=timezone.utc)
# 2026-04-01T04:00:00Z == 2026-04-01T00:00:00-04:00 (the YAML boundary)


def test_helix_v1_flip_before_cutover():
    """A pre-cutover shot resolves to v1 and applies the horizontal flip."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    ts = HELIX_V2_CUTOVER_UTC - timedelta(days=30)
    sx, sy, name = transform_station_to_sample(8.0, 8.0, instrument="HELIX", timestamp=ts)
    assert name == "HELIX/v1"
    assert sx == pytest.approx(32.0, abs=1e-6)  # 40 - 8
    assert sy == pytest.approx(8.0, abs=1e-6)


def test_helix_v2_identity_after_cutover():
    """A post-cutover shot resolves to v2 (identity: Station == Sample)."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    ts = HELIX_V2_CUTOVER_UTC + timedelta(days=30)
    sx, sy, name = transform_station_to_sample(8.0, 8.0, instrument="HELIX", timestamp=ts)
    assert name == "HELIX/v2"
    assert sx == pytest.approx(8.0, abs=1e-6)
    assert sy == pytest.approx(8.0, abs=1e-6)


def test_helix_boundary_just_before_is_v1():
    """One second before the cutover still resolves to v1 (valid_until exclusive)."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    ts = HELIX_V2_CUTOVER_UTC - timedelta(seconds=1)
    _, _, name = transform_station_to_sample(8.0, 8.0, instrument="HELIX", timestamp=ts)
    assert name == "HELIX/v1"


def test_helix_boundary_at_cutover_is_v2():
    """Exactly at the cutover instant resolves to v2 (valid_from inclusive)."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    _, _, name = transform_station_to_sample(
        8.0, 8.0, instrument="HELIX", timestamp=HELIX_V2_CUTOVER_UTC
    )
    assert name == "HELIX/v2"


def test_helix_version_changes_result_across_cutover():
    """The same station point yields different sample coords on either side
    of the cutover, proving version selection actually drives the math."""
    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")
    before = transform_station_to_sample(
        8.0, 8.0, instrument="HELIX", timestamp=HELIX_V2_CUTOVER_UTC - timedelta(seconds=1)
    )
    at = transform_station_to_sample(
        8.0, 8.0, instrument="HELIX", timestamp=HELIX_V2_CUTOVER_UTC
    )
    assert before[2] == "HELIX/v1"
    assert at[2] == "HELIX/v2"
    assert before[:2] != at[:2]
