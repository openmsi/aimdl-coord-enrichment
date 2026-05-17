"""Tests for helix_dagster.instruments.helix — ALPSS parent discovery."""

import pytest

from helix_dagster.instruments.helix import alpss_shot_stem, find_parent_pdv_item_id

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
