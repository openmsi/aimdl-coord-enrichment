import hashlib
from datetime import datetime, timezone

import pytest

from aimdl_coord_enrichment.provenance import (
    build_coord_provenance,
    compute_yaml_sha256,
    get_transformer_version,
)


@pytest.fixture
def sample_yaml_bytes() -> bytes:
    return b"instruments:\n  HELIX:\n    - name: HELIX/v1\n"


@pytest.fixture
def yaml_file(tmp_path, sample_yaml_bytes):
    path = tmp_path / "coords.yaml"
    path.write_bytes(sample_yaml_bytes)
    return path


@pytest.fixture
def helix_station_source() -> dict:
    return {
        "kind": "helix_experiment_log",
        "spreadsheet_item_id": "abc123",
        "spreadsheet_row_index": 0,
        "spreadsheet_pdv_filename": "JHAMAC00003-S1R4C3_shot01_ch1.csv",
    }


@pytest.fixture
def provenance_kwargs(helix_station_source):
    return {
        "instrument": "HELIX",
        "transform_version": "HELIX/v2",
        "transform_yaml_sha256": "deadbeef" * 8,
        "transformer_version": "0.3.0",
        "pipeline_version": "0.2.0",
        "source_timestamp": datetime(2026, 4, 16, 17, 12, tzinfo=timezone.utc),
        "source_timestamp_origin": "spreadsheet_timestamp_col",
        "station_coord_source": helix_station_source,
    }


def test_compute_yaml_sha256_stable(yaml_file, sample_yaml_bytes):
    expected = hashlib.sha256(sample_yaml_bytes).hexdigest()
    assert compute_yaml_sha256(yaml_file) == expected


def test_compute_yaml_sha256_differs_on_change(tmp_path):
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_bytes(b"version: 1\n")
    b.write_bytes(b"version: 2\n")
    assert compute_yaml_sha256(a) != compute_yaml_sha256(b)


def test_compute_yaml_sha256_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        compute_yaml_sha256(tmp_path / "does_not_exist.yaml")


def test_get_transformer_version_returns_string():
    v = get_transformer_version()
    assert isinstance(v, str)
    assert v  # non-empty


def test_build_coord_provenance_minimal_shape(provenance_kwargs, helix_station_source):
    result = build_coord_provenance(**provenance_kwargs)

    expected_keys = {
        "instrument",
        "transform_version",
        "transform_yaml_sha256",
        "transformer_version",
        "pipeline_version",
        "source_timestamp",
        "source_timestamp_origin",
        "station_coord_source",
        "enriched_at",
    }
    assert set(result.keys()) == expected_keys
    assert result["station_coord_source"] == helix_station_source
    assert result["instrument"] == "HELIX"
    assert result["transform_version"] == "HELIX/v2"


def test_build_coord_provenance_omits_run_id_when_none(provenance_kwargs):
    result = build_coord_provenance(**provenance_kwargs, dagster_run_id=None)
    assert "dagster_run_id" not in result

    result_with = build_coord_provenance(**provenance_kwargs, dagster_run_id="run-42")
    assert result_with["dagster_run_id"] == "run-42"


def test_build_coord_provenance_naive_timestamp_raises(provenance_kwargs):
    provenance_kwargs["source_timestamp"] = datetime(2026, 4, 16, 17, 12)
    with pytest.raises(ValueError):
        build_coord_provenance(**provenance_kwargs)


def test_build_coord_provenance_iso_format(provenance_kwargs):
    result = build_coord_provenance(**provenance_kwargs)

    source_ts = result["source_timestamp"]
    enriched_at = result["enriched_at"]

    assert isinstance(source_ts, str)
    assert isinstance(enriched_at, str)
    assert source_ts.endswith("+00:00") or source_ts.endswith("Z")
    assert enriched_at.endswith("+00:00") or enriched_at.endswith("Z")

    # Round-trip parse to confirm valid ISO-8601
    assert datetime.fromisoformat(source_ts).tzinfo is not None
    assert datetime.fromisoformat(enriched_at).tzinfo is not None


def test_build_coord_provenance_none_source_timestamp(provenance_kwargs):
    provenance_kwargs["source_timestamp"] = None
    result = build_coord_provenance(**provenance_kwargs)
    assert result["source_timestamp"] is None
