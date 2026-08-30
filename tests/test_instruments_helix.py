"""Tests for aimdl_coord_enrichment.instruments.helix — ALPSS parent discovery."""

import pytest

from aimdl_coord_enrichment.instruments.helix import alpss_shot_stem, find_parent_pdv_item_id

STEM = "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1"

ALPSS_FILENAMES = [
    f"{STEM}-inputs.csv",
    f"{STEM}-iq.png",
    f"{STEM}-noisefrac.csv",
    f"{STEM}-plots.png",
    f"{STEM}-results.csv",
    f"{STEM}-velocity.csv",
    f"{STEM}-velocity--smooth.csv",
    f"{STEM}-veluncert.csv",
    f"{STEM}-voltage.csv",
]


@pytest.mark.parametrize("filename", ALPSS_FILENAMES, ids=[
    "inputs", "iq", "noisefrac", "plots", "results",
    "velocity", "velocity--smooth", "veluncert", "voltage",
])
def test_alpss_shot_stem_for_each_suffix(filename):
    assert alpss_shot_stem(filename) == STEM


def test_alpss_shot_stem_returns_none_on_pdv_trace_name():
    assert alpss_shot_stem(f"{STEM}.csv") is None


@pytest.mark.parametrize("filename", ["", "foo.txt", "shot01_ch1-9000.csv"])
def test_alpss_shot_stem_returns_none_on_empty_or_unrelated(filename):
    assert alpss_shot_stem(filename) is None


def test_alpss_shot_stem_handles_ch10():
    name = "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch10-iq.png"
    assert alpss_shot_stem(name) == (
        "JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch10"
    )


def test_find_parent_pdv_item_id_success():
    alpss_item = {"name": f"{STEM}-results.csv"}
    pdv_inventory = [
        {"_id": "aaa", "name": "other_file.csv"},
        {"_id": "bbb", "name": f"{STEM}.csv"},
    ]
    assert find_parent_pdv_item_id(alpss_item, pdv_inventory) == "bbb"


def test_find_parent_pdv_item_id_none_on_ambiguous():
    alpss_item = {"name": f"{STEM}-iq.png"}
    pdv_inventory = [
        {"_id": "aaa", "name": f"{STEM}.csv"},
        {"_id": "bbb", "name": f"{STEM}.csv"},
    ]
    assert find_parent_pdv_item_id(alpss_item, pdv_inventory) is None


def test_find_parent_pdv_item_id_none_on_missing_stem():
    alpss_item = {"name": "not-a-real-file.txt"}
    pdv_inventory = [{"_id": "aaa", "name": f"{STEM}.csv"}]
    assert find_parent_pdv_item_id(alpss_item, pdv_inventory) is None


def test_find_parent_pdv_item_id_none_on_no_match():
    alpss_item = {"name": f"{STEM}-iq.png"}
    pdv_inventory = [{"_id": "aaa", "name": "completely_different.csv"}]
    assert find_parent_pdv_item_id(alpss_item, pdv_inventory) is None


# --- C-named convention (probe 2026-08-30) ---------------------------------
# Production carries two concurrent HELIX naming conventions. The C-named one
# ("C1--<date>--<seq>") is ~70% of pdv_trace and is the current, growing one.
# An earlier `_ch<N>` stem-tail requirement encoded the IGSN convention only,
# so all 47,408 C-named ALPSS items resolved to no parent.

C_STEM = "C1--20250807--00001"

C_ALPSS_FILENAMES = [
    f"{C_STEM}-inputs.csv",
    f"{C_STEM}-noisefrac.csv",
    f"{C_STEM}-plots.png",
    f"{C_STEM}-results.csv",
    f"{C_STEM}-velocity--smooth.csv",
    f"{C_STEM}_iq.png",
]


@pytest.mark.parametrize("filename", C_ALPSS_FILENAMES, ids=[
    "inputs", "noisefrac", "plots", "results", "velocity--smooth", "iq_underscore",
])
def test_alpss_shot_stem_c_named(filename):
    assert alpss_shot_stem(filename) == C_STEM


def test_alpss_shot_stem_underscore_separator_igsn_named():
    """`_iq.png` uses an underscore separator; 2,160 IGSN-named items in prod."""
    assert alpss_shot_stem(f"{STEM}_iq.png") == STEM


def test_alpss_shot_stem_returns_none_on_c_named_pdv_trace():
    """The C-named trace itself must stay excluded — its stem ends in digits."""
    assert alpss_shot_stem(f"{C_STEM}.csv") is None


def test_find_parent_pdv_item_id_c_named():
    alpss_item = {"name": f"{C_STEM}-results.csv"}
    pdv_inventory = [
        {"_id": "aaa", "name": "C1--20250807--00002.csv"},
        {"_id": "bbb", "name": f"{C_STEM}.csv"},
    ]
    assert find_parent_pdv_item_id(alpss_item, pdv_inventory) == "bbb"


def test_find_parent_pdv_item_id_c_named_ambiguity_still_blocked():
    """INV-5: relaxing the stem rule must not weaken ambiguity handling."""
    alpss_item = {"name": f"{C_STEM}_iq.png"}
    pdv_inventory = [
        {"_id": "aaa", "name": f"{C_STEM}.csv"},
        {"_id": "bbb", "name": f"{C_STEM}.csv"},
    ]
    assert find_parent_pdv_item_id(alpss_item, pdv_inventory) is None


def test_both_conventions_resolve_against_one_mixed_inventory():
    """Both families coexist in prod and must resolve from the same pool."""
    pdv_inventory = [
        {"_id": "c", "name": f"{C_STEM}.csv"},
        {"_id": "i", "name": f"{STEM}.csv"},
    ]
    assert find_parent_pdv_item_id({"name": f"{C_STEM}-plots.png"}, pdv_inventory) == "c"
    assert find_parent_pdv_item_id({"name": f"{STEM}-plots.png"}, pdv_inventory) == "i"
