"""Tests for the overwrite-policy evaluator (should_write)."""

import pytest

from aimdl_coord_enrichment.coord_enrichment.overwrite import should_write


def _base_prov(**overrides):
    prov = {
        "instrument": "MAXIMA",
        "transform_version": "MAXIMA/v1",
        "transform_yaml_sha256": "abc123",
        "transformer_version": "0.3.0",
        "pipeline_version": "0.2.0",
        "source_timestamp": "2026-04-16T16:56:16+00:00",
        "source_timestamp_origin": "meta.experiment_date",
        "station_coord_source": {
            "kind": "maxima_instructions",
            "instructions_item_id": "instr1",
            "scan_point_index": 0,
        },
        "enriched_at": "2026-04-19T12:00:00+00:00",
        "dagster_run_id": "run-abc",
    }
    prov.update(overrides)
    return prov


def test_first_write_when_no_stored_prov():
    write, reason = should_write(_base_prov(), None)
    assert write is True
    assert reason == "first_write"


def test_first_write_when_stored_prov_is_not_dict():
    write, reason = should_write(_base_prov(), "not-a-dict")
    assert write is True
    assert reason == "first_write"


def test_no_change_when_identical():
    prov = _base_prov()
    write, reason = should_write(prov, dict(prov))
    assert write is False
    assert reason == "no_change"


def test_write_when_yaml_sha_differs():
    new = _base_prov()
    stored = _base_prov(transform_yaml_sha256="different_sha")
    write, reason = should_write(new, stored)
    assert write is True
    assert reason == "yaml_sha256_changed"


def test_write_when_transformer_version_differs():
    new = _base_prov()
    stored = _base_prov(transformer_version="0.2.9")
    write, reason = should_write(new, stored)
    assert write is True
    assert reason == "transformer_version_changed"


def test_write_when_transform_version_differs():
    new = _base_prov()
    stored = _base_prov(transform_version="MAXIMA/v0")
    write, reason = should_write(new, stored)
    assert write is True
    assert reason == "transform_version_changed"


def test_write_when_station_coord_source_differs():
    new = _base_prov()
    stored = _base_prov(station_coord_source={
        "kind": "maxima_instructions",
        "instructions_item_id": "different_instr",
        "scan_point_index": 0,
    })
    write, reason = should_write(new, stored)
    assert write is True
    assert reason == "station_coord_source_changed"


def test_no_change_ignores_enriched_at():
    new = _base_prov(enriched_at="2026-04-20T00:00:00+00:00")
    stored = _base_prov(enriched_at="2026-04-19T00:00:00+00:00")
    write, reason = should_write(new, stored)
    assert write is False
    assert reason == "no_change"


def test_no_change_ignores_source_timestamp():
    new = _base_prov(source_timestamp="2026-04-20T00:00:00+00:00")
    stored = _base_prov(source_timestamp="2026-04-19T00:00:00+00:00")
    write, reason = should_write(new, stored)
    assert write is False
    assert reason == "no_change"


def test_no_change_ignores_dagster_run_id():
    new = _base_prov(dagster_run_id="run-new")
    stored = _base_prov(dagster_run_id="run-old")
    write, reason = should_write(new, stored)
    assert write is False
    assert reason == "no_change"
