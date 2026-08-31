import pytest

from aimdl_coord_enrichment.validation import validate_igsn


def test_valid_igsn():
    igsn, issue = validate_igsn("HTMXYZ00123")
    assert igsn == "HTMXYZ00123"
    assert issue is None


def test_valid_igsn_with_suffix():
    igsn, issue = validate_igsn("HTMXYZ00123-A")
    assert igsn == "HTMXYZ00123-A"
    assert issue is None


def test_invalid_igsn_format():
    igsn, issue = validate_igsn("not-an-igsn")
    assert igsn is None
    assert issue["issue"] == "invalid_format"
    assert issue["value"] == "not-an-igsn"


def test_missing_igsn_none():
    igsn, issue = validate_igsn(None)
    assert igsn is None
    assert issue["issue"] == "missing"
    assert issue["value"] is None


def test_missing_igsn_nan():
    igsn, issue = validate_igsn(float("nan"))
    assert igsn is None
    assert issue["issue"] == "missing"


def test_igsn_embedded_in_string():
    # The IGSN pattern's suffix group captures "-suffix" as part of the match
    igsn, issue = validate_igsn("prefix-HTMXYZ00123-suffix")
    assert igsn == "HTMXYZ00123-suffix"
    assert issue is None


# --- multi-segment suffixes (dry run 2026-08-30) ---------------------------
# Production IGSNs carry a variable number of hyphen-delimited segments. The
# pattern originally allowed at most one, silently truncating two-segment
# IGSNs and manufacturing a false igsn_consistency ERROR (an ERROR-severity
# check, so a NO-GO in the readiness rubric) on 2 of 214 HELIX partitions.

@pytest.mark.parametrize("value", [
    "JHAMAL00018",              # no suffix
    "JHAMAL00018-005",          # one segment
    "NWXMAB00010-002-001",      # two segments — the regression
    "JHAMAC00003-S1R4C3",       # alphanumeric segment
    "APLMAL00006-001",
])
def test_validate_igsn_roundtrips_production_shapes(value):
    """Every real IGSN shape must survive extraction unchanged."""
    igsn, issue = validate_igsn(value)
    assert igsn == value
    assert issue is None


def test_validate_igsn_extracts_multi_segment_from_filename():
    """The C-named hybrid convention embeds a two-segment IGSN in a filename."""
    name = "C1--NWXMAB00010-002-001_2026-07-06_21-45-25_shot01--00000.csv"
    igsn, issue = validate_igsn(name)
    assert igsn == "NWXMAB00010-002-001"
    assert issue is None


def test_validate_igsn_ignores_trailing_hyphen():
    """A bare trailing hyphen is not a segment and must not be captured."""
    igsn, _ = validate_igsn("NWXMAB00010-002-001-")
    assert igsn == "NWXMAB00010-002-001"


def test_validate_igsn_matches_girder_meta_igsn_exactly():
    """SPEC-HELIX-05: the extracted value is compared against the item's
    meta.igsn, so truncation manufactures a mismatch that is not real."""
    spreadsheet_value = "NWXMAB00010-002-001"
    girder_meta_igsn = "NWXMAB00010-002-001"
    extracted, _ = validate_igsn(spreadsheet_value)
    assert extracted == girder_meta_igsn
