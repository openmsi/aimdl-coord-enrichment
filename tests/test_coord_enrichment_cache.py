"""Tests for the InstructionsCache."""

from unittest.mock import MagicMock, patch

from aimdl_coord_enrichment.coord_enrichment.cache import InstructionsCache


def _item(folder_id):
    return {"_id": f"item-in-{folder_id}", "folderId": folder_id}


@patch("aimdl_coord_enrichment.coord_enrichment.cache.fetch_instructions_for_run")
@patch("aimdl_coord_enrichment.coord_enrichment.cache.find_run_folder_id")
def test_cache_fetches_once_per_run_folder(mock_find, mock_fetch):
    mock_find.return_value = "run-folder-A"
    instr_item = {"_id": "instr1"}
    parsed = {"sample": {"scan_points": [[1.0, 2.0]]}}
    mock_fetch.return_value = (instr_item, parsed)

    girder = MagicMock()
    cache = InstructionsCache()

    r1 = cache.get_for_item(_item("raw1"), girder)
    r2 = cache.get_for_item(_item("raw2"), girder)

    assert r1 == ("run-folder-A", instr_item, parsed)
    assert r2 == ("run-folder-A", instr_item, parsed)
    assert mock_fetch.call_count == 1
    assert mock_find.call_count == 2


@patch("aimdl_coord_enrichment.coord_enrichment.cache.fetch_instructions_for_run")
@patch("aimdl_coord_enrichment.coord_enrichment.cache.find_run_folder_id")
def test_cache_independent_folders(mock_find, mock_fetch):
    mock_find.side_effect = lambda item, g: f"run-{item['folderId']}"
    mock_fetch.side_effect = lambda rf_id, g: (
        {"_id": f"instr-{rf_id}"},
        {"sample": {"scan_points": [[0.0, 0.0]]}},
    )

    girder = MagicMock()
    cache = InstructionsCache()

    cache.get_for_item(_item("folderA"), girder)
    cache.get_for_item(_item("folderB"), girder)

    assert mock_fetch.call_count == 2


@patch("aimdl_coord_enrichment.coord_enrichment.cache.fetch_instructions_for_run")
@patch("aimdl_coord_enrichment.coord_enrichment.cache.find_run_folder_id")
def test_cache_size_reports_entry_count(mock_find, mock_fetch):
    mock_find.side_effect = lambda item, g: f"run-{item['folderId']}"
    mock_fetch.side_effect = lambda rf_id, g: ({"_id": "x"}, {"sample": {"scan_points": [[0, 0]]}})

    girder = MagicMock()
    cache = InstructionsCache()

    assert cache.cache_size() == 0
    cache.get_for_item(_item("f1"), girder)
    assert cache.cache_size() == 1
    cache.get_for_item(_item("f2"), girder)
    assert cache.cache_size() == 2
    cache.get_for_item(_item("f1"), girder)
    assert cache.cache_size() == 2
