from aimdl_coord_enrichment.matching import match_pdv_file


PDV_ITEMS = [
    {"name": "shot001_ch1.tdms", "_id": "a1"},
    {"name": "shot001_ch2.tdms", "_id": "a2"},
    {"name": "shot002_ch1.tdms", "_id": "b1"},
]


def test_exact_match():
    item, issue = match_pdv_file(PDV_ITEMS, "shot002")
    assert item == {"name": "shot002_ch1.tdms", "_id": "b1"}
    assert issue is None


def test_no_match():
    item, issue = match_pdv_file(PDV_ITEMS, "shot999")
    assert item is None
    assert issue["type"] == "not_found"


def test_ambiguous_match():
    item, issue = match_pdv_file(PDV_ITEMS, "shot001")
    assert item is None
    assert issue["type"] == "ambiguous"
    assert len(issue["matches"]) == 2


def test_nan_filename():
    item, issue = match_pdv_file(PDV_ITEMS, float("nan"))
    assert item is None
    assert issue is None


def test_none_filename():
    item, issue = match_pdv_file(PDV_ITEMS, None)
    assert item is None
    assert issue is None
