from helix_dagster.validation import validate_igsn


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
    # The IGSN pattern's optional suffix group captures "-suffix" as part of the match
    igsn, issue = validate_igsn("prefix-HTMXYZ00123-suffix")
    assert igsn == "HTMXYZ00123-suffix"
    assert issue is None
