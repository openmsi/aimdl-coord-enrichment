"""Tests for the pure MAXIMA helpers (filename + instructions.txt parsing)."""

from pathlib import Path

import pytest

from helix_dagster.instruments.maxima import (
    parse_instructions_json,
    parse_scan_point_index,
    scan_point_coords,
)
from helix_dagster.instruments.types import ResolutionError

FIXTURE = Path(__file__).parent / "fixtures" / "instructions_example.json"


# -- parse_scan_point_index --------------------------------------------------

@pytest.mark.parametrize(
    "filename, expected",
    [
        ("scan_point_0.xrf", 0),
        ("scan_point_0.tiff", 0),
        ("scan_point_0_master.h5", 0),
        ("scan_point_0_data_000001.h5", 0),
        ("scan_point_0_scan.png", 0),
        ("scan_point_0_xrd.csv", 0),
        ("scan_point_24_xrd.csv", 24),
    ],
)
def test_parse_scan_point_index_across_filename_shapes(filename, expected):
    assert parse_scan_point_index(filename) == expected


@pytest.mark.parametrize(
    "filename",
    [
        "instructions.txt",
        "",
        "scan_other_0.csv",
        "scan_point_.xrf",
    ],
)
def test_parse_scan_point_index_returns_none_on_unrelated(filename):
    assert parse_scan_point_index(filename) is None


# -- parse_instructions_json --------------------------------------------------

def test_parse_instructions_json_roundtrip_from_fixture():
    data = parse_instructions_json(FIXTURE.read_text())
    sp = data["sample"]["scan_points"]
    assert len(sp) == 25
    assert sp[0] == [-4.0, -10.0]


def test_parse_instructions_json_invalid_json():
    with pytest.raises(ResolutionError, match="not valid JSON"):
        parse_instructions_json("not json")


def test_parse_instructions_json_missing_sample():
    with pytest.raises(ResolutionError, match="missing 'sample'"):
        parse_instructions_json('{"foo": {}}')


def test_parse_instructions_json_missing_scan_points():
    with pytest.raises(ResolutionError, match="scan_points"):
        parse_instructions_json('{"sample": {}}')


def test_parse_instructions_json_malformed_point():
    with pytest.raises(ResolutionError, match=r"scan_points\[0\]"):
        parse_instructions_json('{"sample": {"scan_points": [[1, 2, 3]]}}')


# -- scan_point_coords --------------------------------------------------------

def test_scan_point_coords_valid():
    data = parse_instructions_json(FIXTURE.read_text())
    assert scan_point_coords(data, 17) == (11.0, 0.0)


def test_scan_point_coords_out_of_range_low():
    data = parse_instructions_json(FIXTURE.read_text())
    with pytest.raises(ResolutionError, match="out of range"):
        scan_point_coords(data, -1)


def test_scan_point_coords_out_of_range_high():
    data = parse_instructions_json(FIXTURE.read_text())
    with pytest.raises(ResolutionError, match="out of range"):
        scan_point_coords(data, 25)
