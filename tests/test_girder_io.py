"""Tests for the /aimdl endpoint helper functions."""

from unittest.mock import MagicMock

from helix_dagster.girder_io import (
    fetch_aimdl_datatypes,
    fetch_aimdl_datafiles,
    fetch_all_aimdl_datafiles,
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
