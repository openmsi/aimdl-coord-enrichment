# Claude Code Prompt — Stage 1: Add `/aimdl` endpoint helpers

**Read CLAUDE.md first.** Then read `ROADMAP_aimdl_refactor.md` for full context.
Then read `issues/01-add-aimdl-helpers.md`.

## GitHub Issue

```bash
gh issue create \
  --title "feat: add /aimdl endpoint helper functions to girder_io.py" \
  --body-file issues/01-add-aimdl-helpers.md \
  --label "enhancement"
```

Note the issue number returned.

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b feat/aimdl-helpers
```

## Context

The Girder instance at `data.htmdec.org` has an `/aimdl` REST endpoint with two
routes (see `ROADMAP_aimdl_refactor.md` for full API details):

- `GET /aimdl/datatype` → list of distinct `meta.data_type` strings
- `GET /aimdl/datafiles?dataType=...&limit=N&offset=N` → paginated Girder items

The endpoint has a hard limit of 100 items per page. Items must have `meta.igsn`
set to appear in results.

Available `meta.data_type` values: `pdv_trace`, `pdv_alpss_result`,
`pdv_alpss_output`, `xrd_raw`, `xrd_derived`, `xrd_metadata`,
`xrd_calibrant_raw`, `xrd_calibrant_derived`, `xrf_raw`.

## Changes

### Step 1: Add data type constants to constants.py

Add to `helix_dagster/constants.py`:

```python
# /aimdl endpoint data types
# These correspond to meta.data_type values set on Girder items
AIMDL_DATA_TYPES = {
    "pdv_trace": "pdv_trace",
    "pdv_alpss_result": "pdv_alpss_result",
    "pdv_alpss_output": "pdv_alpss_output",
    "xrd_raw": "xrd_raw",
    "xrd_derived": "xrd_derived",
    "xrd_metadata": "xrd_metadata",
    "xrd_calibrant_raw": "xrd_calibrant_raw",
    "xrd_calibrant_derived": "xrd_calibrant_derived",
    "xrf_raw": "xrf_raw",
}

# Default data type for PDV trace matching (used by pdv_trace_inventory asset)
PDV_TRACE_DATA_TYPE = os.environ.get("PDV_TRACE_DATA_TYPE", "pdv_trace")
ALPSS_RESULT_DATA_TYPE = os.environ.get("ALPSS_RESULT_DATA_TYPE", "pdv_alpss_result")

# Hard limit imposed by the /aimdl/datafiles endpoint
AIMDL_PAGE_LIMIT = 100
```

Keep `PDV_FOLDER_ID` and `HELIX_FOLDER_ID` for now — they're still used by
other code. They'll be removed in Stage 2.

### Step 2: Add helper functions to girder_io.py

Add to `helix_dagster/girder_io.py`:

```python
from helix_dagster.constants import AIMDL_PAGE_LIMIT


def fetch_aimdl_datatypes(client):
    """Fetch the list of available meta.data_type values from /aimdl/datatype.

    Returns a list of strings, e.g. ["pdv_trace", "xrd_raw", ...].
    """
    return client.get("aimdl/datatype")


def fetch_aimdl_datafiles(client, data_type, limit=100, offset=0):
    """Fetch a single page of items from /aimdl/datafiles.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    data_type : str
        The meta.data_type value to filter by (e.g., "pdv_trace").
    limit : int
        Max items per page (capped at 100 by endpoint).
    offset : int
        Pagination offset.

    Returns
    -------
    list[dict]
        List of Girder item dicts with _id, name, meta.igsn, meta.data_type,
        size, created, folderId, etc.
    """
    return client.get(
        "aimdl/datafiles",
        parameters={
            "dataType": data_type,
            "limit": min(limit, AIMDL_PAGE_LIMIT),
            "offset": offset,
        },
    )


def fetch_all_aimdl_datafiles(client, data_type):
    """Paginate through all items of a given data type via /aimdl/datafiles.

    The endpoint has a hard limit of 100 per page. This function fetches all
    pages and returns the concatenated result.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    data_type : str
        The meta.data_type value to filter by.

    Returns
    -------
    list[dict]
        All matching Girder item dicts.
    """
    all_items = []
    offset = 0
    while True:
        batch = fetch_aimdl_datafiles(client, data_type, offset=offset)
        if not batch:
            break
        all_items.extend(batch)
        if len(batch) < AIMDL_PAGE_LIMIT:
            break
        offset += AIMDL_PAGE_LIMIT
    return all_items
```

Do NOT remove any existing functions. The existing `list_all_spreadsheet_items`,
`download_and_read`, and `nan_to_none` functions are still used.

### Step 3: Add tests

Create `tests/test_girder_io.py`:

```python
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
```

### Step 4: Verify

```bash
poetry run pytest tests/test_girder_io.py -v
poetry run pytest tests/ -v  # ensure no regressions
```

### Step 5: Commit, push, create PR

```bash
git add -A
git commit -m "feat: add /aimdl endpoint helper functions to girder_io.py

Added fetch_aimdl_datatypes(), fetch_aimdl_datafiles(), and
fetch_all_aimdl_datafiles() to girder_io.py for querying the indexed
/aimdl/datafiles endpoint on the Girder data portal.

Added AIMDL_DATA_TYPES constants and per-page limit to constants.py.
Added unit tests with mocked Girder responses.

No changes to existing asset behavior.

Closes #ISSUE_NUMBER"
git push -u origin feat/aimdl-helpers

gh pr create \
  --title "feat: add /aimdl endpoint helpers to girder_io.py" \
  --body "## Summary

Adds helper functions for the \`/aimdl/datafiles\` Girder endpoint, which
performs indexed MongoDB queries by \`meta.data_type\` instead of crawling
folder trees. This is Stage 1 of the refactoring described in
\`ROADMAP_aimdl_refactor.md\`.

## Changes

- \`girder_io.py\`: Added \`fetch_aimdl_datatypes()\`, \`fetch_aimdl_datafiles()\`,
  and \`fetch_all_aimdl_datafiles()\` with pagination support
- \`constants.py\`: Added \`AIMDL_DATA_TYPES\`, \`PDV_TRACE_DATA_TYPE\`,
  \`ALPSS_RESULT_DATA_TYPE\`, \`AIMDL_PAGE_LIMIT\`
- \`tests/test_girder_io.py\`: 6 unit tests with mocked Girder responses

No behavior changes to existing assets.

Closes #ISSUE_NUMBER" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] GitHub issue created
- [ ] `fetch_aimdl_datatypes` function added
- [ ] `fetch_aimdl_datafiles` function added with limit cap
- [ ] `fetch_all_aimdl_datafiles` function paginates correctly
- [ ] Constants added to `constants.py`
- [ ] All 6 new tests pass
- [ ] Existing tests still pass (no regressions)
- [ ] No changes to existing asset behavior
- [ ] PR created against `refactor/asset-dag` branch
