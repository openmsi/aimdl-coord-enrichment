"""Tests for helix_trace_discovery_sensor — partition add requests and
partitioned RunRequest construction with content-hash run keys."""

from unittest.mock import MagicMock

from dagster import (
    DagsterInstance,
    SensorResult,
    build_sensor_context,
)

from aimdl_coord_enrichment.sensors import helix_trace_discovery_sensor


def _mock_girder_with(index: dict[str, str]) -> MagicMock:
    """MagicMock client whose .get('aimdl/partition', parameters=...) returns
    the pdv_trace index, simulating fetch_partition_index's HTTP shape."""
    client = MagicMock()

    def fake_get(path, parameters=None):
        if path == "aimdl/partition":
            assert parameters["dataType"] == "pdv_trace"
            return index
        raise AssertionError(f"unexpected client call: {path} {parameters}")

    client.get.side_effect = fake_get
    return client


def _run_sensor(girder):
    ctx = build_sensor_context(
        instance=DagsterInstance.ephemeral(),
        resources={"girder": girder},
    )
    return helix_trace_discovery_sensor(ctx)


def test_sensor_empty_index_emits_no_requests():
    result = _run_sensor(_mock_girder_with({}))
    assert isinstance(result, SensorResult)
    assert result.run_requests == []
    assert len(result.dynamic_partitions_requests) == 1
    assert result.dynamic_partitions_requests[0].partition_keys == []


def test_sensor_adds_sorted_partitions_and_one_request_per_key():
    index = {"K2//T2": "hashB", "K1//T1": "hashA"}
    result = _run_sensor(_mock_girder_with(index))

    adds = result.dynamic_partitions_requests[0].partition_keys
    assert adds == ["K1//T1", "K2//T2"]  # sorted union

    assert len(result.run_requests) == 2
    by_pk = {rr.partition_key: rr for rr in result.run_requests}
    assert set(by_pk) == {"K1//T1", "K2//T2"}


def test_sensor_run_key_embeds_content_hash():
    index = {"K1//T1": "hashA"}
    result = _run_sensor(_mock_girder_with(index))
    rr = result.run_requests[0]
    assert rr.run_key == "helix-pdv-trace|K1//T1|hash=hashA"
    assert rr.partition_key == "K1//T1"
    assert rr.tags["data_type"] == "pdv_trace"
    assert rr.tags["content_hash"] == "hashA"
