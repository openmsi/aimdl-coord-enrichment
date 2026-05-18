"""Tests for maxima_raw_discovery_sensor — dedup key construction,
dynamic partition add requests, metadata-hash fallback."""

from unittest.mock import MagicMock

from dagster import (
    DagsterInstance,
    MultiPartitionKey,
    SensorResult,
    build_sensor_context,
)

from aimdl_coord_enrichment.sensors import maxima_raw_discovery_sensor


def _make_indexes(xrd_raw=None, xrf_raw=None, xrd_metadata=None):
    return {
        "xrd_raw": xrd_raw or {},
        "xrf_raw": xrf_raw or {},
        "xrd_metadata": xrd_metadata or {},
    }


def _mock_girder_with(indexes: dict[str, dict[str, str]]) -> MagicMock:
    """A MagicMock client whose .get('aimdl/partition', parameters=...) returns
    the matching index, simulating fetch_partition_index's HTTP shape."""
    client = MagicMock()

    def fake_get(path, parameters=None):
        if path == "aimdl/partition":
            return indexes.get(parameters["dataType"], {})
        raise AssertionError(f"unexpected client call: {path} {parameters}")

    client.get.side_effect = fake_get
    return client


def _run_sensor(girder):
    ctx = build_sensor_context(
        instance=DagsterInstance.ephemeral(),
        resources={"girder": girder},
    )
    return maxima_raw_discovery_sensor(ctx)


def test_sensor_empty_indexes_emits_no_requests():
    girder = _mock_girder_with(_make_indexes())
    result = _run_sensor(girder)
    assert isinstance(result, SensorResult)
    assert result.run_requests == []
    assert len(result.dynamic_partitions_requests) == 1
    assert result.dynamic_partitions_requests[0].partition_keys == []


def test_sensor_emits_run_request_per_data_type_per_key():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawhashA", "K2//T2": "rawhashB"},
        xrf_raw={"K1//T1": "rawhashC"},  # same aimdl_key as xrd_raw K1
        xrd_metadata={"K1//T1": "metaA", "K2//T2": "metaB"},
    )
    girder = _mock_girder_with(indexes)
    result = _run_sensor(girder)

    # 3 RunRequests: xrd_raw×2 + xrf_raw×1
    assert len(result.run_requests) == 3

    # Partition add request has the union (2 unique keys) — not 3
    adds = result.dynamic_partitions_requests[0].partition_keys
    assert sorted(adds) == ["K1//T1", "K2//T2"]


def test_sensor_dedup_key_includes_both_hashes():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawA"},
        xrd_metadata={"K1//T1": "metaA"},
    )
    girder = _mock_girder_with(indexes)
    result = _run_sensor(girder)
    rr = result.run_requests[0]
    assert rr.run_key == (
        "coord-enrichment|xrd_raw|K1//T1|raw=rawA|xrd_metadata=metaA"
    )


def test_sensor_metadata_hash_fallback():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawA"},
        xrd_metadata={},  # no xrd_metadata entries
    )
    girder = _mock_girder_with(indexes)
    result = _run_sensor(girder)
    rr = result.run_requests[0]
    assert "xrd_metadata=no-xrd-metadata" in rr.run_key


def test_sensor_partition_key_is_multi_dim():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawA"},
        xrd_metadata={"K1//T1": "metaA"},
    )
    girder = _mock_girder_with(indexes)
    result = _run_sensor(girder)
    rr = result.run_requests[0]
    pk = rr.partition_key
    if isinstance(pk, MultiPartitionKey):
        assert pk.keys_by_dimension == {"data_type": "xrd_raw", "run": "K1//T1"}
    else:
        assert "xrd_raw" in str(pk) and "K1//T1" in str(pk)
