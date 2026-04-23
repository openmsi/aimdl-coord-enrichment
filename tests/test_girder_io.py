"""Tests for the /aimdl endpoint helper functions."""

from unittest.mock import MagicMock

from helix_dagster.girder_io import (
    fetch_aimdl_datatypes,
    fetch_aimdl_datafiles,
    fetch_all_aimdl_datafiles,
    fetch_items_by_partition,
    fetch_partition_keys,
    list_recent_spreadsheets,
)


def _make_item(name, igsn, data_type, item_id=None):
    """Helper to create a mock Girder item dict."""
    return {
        "_id": item_id or f"id_{name}",
        "name": name,
        "meta": {"igsn": igsn, "data_type": data_type},
        "size": 1024,
        "created": "2025-01-01T00:00:00Z",
        "folderId": "folder123",
        "lowerName": name.lower(),
    }


def test_list_recent_spreadsheets():
    client = MagicMock()
    client.get.return_value = [
        {"name": "log_2025.csv", "_id": "c1", "created": "2025-03-01T00:00:00Z"},
        {"name": "data.tdms", "_id": "c2", "created": "2025-03-01T00:00:00Z"},
        {"name": "results.xlsx", "_id": "c3", "created": "2025-02-28T00:00:00Z"},
    ]
    result = list_recent_spreadsheets(client, "folder123", limit=50)
    assert len(result) == 2  # only .csv and .xlsx, not .tdms
    assert result[0]["name"] == "log_2025.csv"
    assert result[1]["name"] == "results.xlsx"
    client.get.assert_called_once_with(
        "item",
        parameters={
            "folderId": "folder123",
            "sort": "created",
            "sortdir": -1,
            "limit": 50,
        },
    )


def test_fetch_datatypes():
    client = MagicMock()
    client.get.return_value = ["pdv_trace", "xrd_raw", "pdv_alpss_result"]
    result = fetch_aimdl_datatypes(client)
    client.get.assert_called_once_with("aimdl/datatype")
    assert result == ["pdv_trace", "xrd_raw", "pdv_alpss_result"]


def test_fetch_datafiles_single_page():
    items = [_make_item(f"file{i}.tdms", "ABCDEF12345", "pdv_trace") for i in range(5)]
    client = MagicMock()
    client.get.return_value = items
    result = fetch_aimdl_datafiles(client, "pdv_trace", limit=100, offset=0)
    client.get.assert_called_once_with(
        "aimdl/datafiles",
        parameters={"dataType": "pdv_trace", "limit": 100, "offset": 0},
    )
    assert len(result) == 5


def test_fetch_datafiles_respects_limit_cap():
    """Verify that limit is capped at 100 even if caller requests more."""
    client = MagicMock()
    client.get.return_value = []
    fetch_aimdl_datafiles(client, "pdv_trace", limit=500, offset=0)
    call_params = client.get.call_args[1]["parameters"]
    assert call_params["limit"] == 100


def test_fetch_all_paginates():
    """fetch_all should paginate until a short page is returned."""
    page1 = [_make_item(f"f{i}", "IGSN1", "pdv_trace") for i in range(100)]
    page2 = [_make_item(f"f{i}", "IGSN1", "pdv_trace") for i in range(100, 130)]

    client = MagicMock()
    client.get.side_effect = [page1, page2]
    result = fetch_all_aimdl_datafiles(client, "pdv_trace")
    assert len(result) == 130
    assert client.get.call_count == 2


def test_fetch_all_empty():
    client = MagicMock()
    client.get.return_value = []
    result = fetch_all_aimdl_datafiles(client, "pdv_trace")
    assert result == []


def test_fetch_all_single_page():
    items = [_make_item(f"f{i}", "IGSN1", "pdv_trace") for i in range(50)]
    client = MagicMock()
    client.get.return_value = items
    result = fetch_all_aimdl_datafiles(client, "pdv_trace")
    assert len(result) == 50
    assert client.get.call_count == 1  # No second page needed


# ── partition-aware helpers ─────────────────────────────────────────


def test_fetch_partition_keys():
    """fetch_partition_keys returns the raw dict from /aimdl/partition."""
    client = MagicMock()
    client.get.return_value = {
        "JHAMAB00001//2026-04-16": "hash-a",
        "JHAMAB00002//2026-04-17": "hash-b",
    }
    result = fetch_partition_keys(client, "xrd_raw")
    client.get.assert_called_once_with(
        "aimdl/partition",
        parameters={"dataType": "xrd_raw"},
    )
    assert result == {
        "JHAMAB00001//2026-04-16": "hash-a",
        "JHAMAB00002//2026-04-17": "hash-b",
    }


def test_fetch_items_by_partition_paginates_over_keys():
    """Each partition key is fetched via /aimdl/partition/details and
    the results are concatenated in key-iteration order."""
    partition_index = {
        "JHAMAB00001//2026-04-16": "hash-a",
        "JHAMAB00002//2026-04-17": "hash-b",
    }
    items_a = [
        _make_item("a1.tif", "JHAMAB00001", "xrd_raw"),
        _make_item("a2.tif", "JHAMAB00001", "xrd_raw"),
    ]
    items_b = [
        _make_item("b1.tif", "JHAMAB00002", "xrd_raw"),
        _make_item("b2.tif", "JHAMAB00002", "xrd_raw"),
    ]

    client = MagicMock()

    def fake_get(path, parameters=None):
        if path == "aimdl/partition":
            assert parameters == {"dataType": "xrd_raw"}
            return partition_index
        if path == "aimdl/partition/details":
            assert parameters["dataType"] == "xrd_raw"
            if parameters["key"] == "JHAMAB00001//2026-04-16":
                return items_a
            if parameters["key"] == "JHAMAB00002//2026-04-17":
                return items_b
        raise AssertionError(f"unexpected call: {path} {parameters}")

    client.get.side_effect = fake_get

    result = fetch_items_by_partition(client, "xrd_raw")
    assert len(result) == 4
    assert [it["name"] for it in result] == ["a1.tif", "a2.tif", "b1.tif", "b2.tif"]


def test_fetch_items_by_partition_handles_empty_partition():
    """A key whose details call returns [] is skipped silently."""
    partition_index = {
        "JHAMAB00001//2026-04-16": "hash-a",
        "JHAMAB00002//2026-04-17": "hash-b",
    }
    client = MagicMock()

    def fake_get(path, parameters=None):
        if path == "aimdl/partition":
            return partition_index
        if path == "aimdl/partition/details":
            if parameters["key"] == "JHAMAB00001//2026-04-16":
                return []  # empty — should be skipped, not raised
            return [_make_item("only.tif", "JHAMAB00002", "xrd_raw")]
        raise AssertionError(f"unexpected call: {path}")

    client.get.side_effect = fake_get

    result = fetch_items_by_partition(client, "xrd_raw")
    assert len(result) == 1
    assert result[0]["name"] == "only.tif"
