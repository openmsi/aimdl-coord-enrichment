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


# --- channel-prefix fallback (measured 2026-08-31) --------------------------
# A PDV trace is stored under the digitizer channel that recorded it, so its
# Girder name may carry a leading "C<n>--" that the log's PDV_FileName omits.
# Across all 214 tagged log partitions this accounted for 1,156 fired shots,
# 32% of every fired shot on record, with zero ambiguities.

SHEET = "JHAMAC00003-S1R5C3_68efdf9ebe3476695206a18e_0_1503_2026-07-14_13-21-58_shot01"
ITEM = f"C1--{SHEET}--00000.csv"


def test_matches_across_channel_prefix():
    items = [{"_id": "a", "name": ITEM}]
    match, issue = match_pdv_file(items, SHEET)
    assert issue is None
    assert match["_id"] == "a"


def test_exact_prefix_match_wins_over_the_fallback():
    """The fallback must never change the outcome for a name that already
    matched, so it only runs when the exact pass finds nothing."""
    items = [
        {"_id": "exact", "name": f"{SHEET}--00000.csv"},
        {"_id": "prefixed", "name": ITEM},
    ]
    match, issue = match_pdv_file(items, SHEET)
    assert issue is None
    assert match["_id"] == "exact"


def test_two_channels_for_one_row_is_still_ambiguous():
    """INV-5 holds on the relaxed path: never an arbitrary pick. Zero such
    cases in the current corpus, but the guard must not be dropped."""
    items = [
        {"_id": "c1", "name": f"C1--{SHEET}--00000.csv"},
        {"_id": "c3", "name": f"C3--{SHEET}--00000.csv"},
    ]
    match, issue = match_pdv_file(items, SHEET)
    assert match is None
    assert issue["type"] == "ambiguous"
    assert issue["via"] == "channel_prefix"
    assert len(issue["matches"]) == 2


def test_unrelated_names_still_not_found():
    items = [{"_id": "x", "name": "C1--SOMETHINGELSE_2026-01-01_shot01--00000.csv"}]
    match, issue = match_pdv_file(items, SHEET)
    assert match is None
    assert issue["type"] == "not_found"


def test_only_a_channel_prefix_is_stripped():
    """Guard the rule stays narrow: an arbitrary leading token is not ignored."""
    items = [{"_id": "x", "name": f"XX--{SHEET}--00000.csv"}]
    match, issue = match_pdv_file(items, SHEET)
    assert match is None
    assert issue["type"] == "not_found"
