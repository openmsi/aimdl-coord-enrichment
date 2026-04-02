# Claude Code Prompt — Stage 6: Optimize sensor to avoid recursive folder crawl

**Read CLAUDE.md first.** Then read `ROADMAP_aimdl_refactor.md`.
Then read `issues/04-optimize-sensor.md`.

**Prerequisite:** Stages 1–5 must be merged. The processing manifest (Stage 4)
must exist so the sensor can optionally check it.

## GitHub Issue

```bash
gh issue create \
  --title "feat: optimize sensor to avoid recursive folder crawl" \
  --body-file issues/04-optimize-sensor.md \
  --label "enhancement"
```

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b feat/optimize-sensor
```

## Overview

The `helix_folder_sensor` currently calls `list_all_spreadsheet_items()`, which
recursively walks the HELIX folder tree listing items in every subfolder. Replace
this with a targeted query that fetches only recent items, sorted by creation date.

Optionally integrate with the processing manifest: if a spreadsheet already has
`meta.processing_status.status == "completed_clean"` and hasn't been modified
since `last_processed`, skip it.

## Changes

### Step 1: Add a non-recursive recent-items helper to girder_io.py

Add to `helix_dagster/girder_io.py`:

```python
def list_recent_spreadsheets(client, folder_id, limit=100):
    """List recently created spreadsheet items in a Girder folder.

    Unlike list_all_spreadsheet_items(), this does NOT recursively walk
    subfolders. It queries items sorted by creation date (newest first)
    and filters for CSV/XLSX extensions.

    Parameters
    ----------
    client : GirderClient
        Authenticated Girder client.
    folder_id : str
        The Girder folder ID to search.
    limit : int
        Maximum number of items to return.

    Returns
    -------
    list[dict]
        Girder item dicts for spreadsheet files, newest first.
    """
    items = client.get(
        "item",
        parameters={
            "folderId": folder_id,
            "sort": "created",
            "sortdir": -1,
            "limit": limit,
        },
    )
    return [i for i in items if i["name"].endswith((".csv", ".xlsx", ".xls"))]
```

Mark `list_all_spreadsheet_items` with a deprecation comment:

```python
def list_all_spreadsheet_items(client, folder_id):
    """Recursively list all CSV/XLSX items in a Girder folder.

    .. deprecated::
        Use list_recent_spreadsheets() for sensor polling. This function
        performs a recursive folder crawl that scales poorly.
    """
    # ... existing implementation unchanged
```

### Step 2: Update the sensor

In `helix_dagster/sensors.py`:

```python
import json

from dagster import RunRequest, SensorEvaluationContext, sensor

from helix_dagster.constants import HELIX_FOLDER_ID
from helix_dagster.assets import process_helix_assets_job
from helix_dagster.girder_io import list_recent_spreadsheets
from helix_dagster.resources import GirderConnection


@sensor(job=process_helix_assets_job, minimum_interval_seconds=3600)
def helix_folder_sensor(context: SensorEvaluationContext, girder: GirderConnection):
    """Poll for new experiment log spreadsheets in the HELIX folder.

    Uses a sorted recent-items query instead of recursive folder crawling.
    Tracks seen item IDs in the cursor to avoid reprocessing.
    """
    cursor_data = json.loads(context.cursor or '{"seen": []}')
    seen = set(cursor_data["seen"])

    items = list_recent_spreadsheets(girder, HELIX_FOLDER_ID, limit=100)

    new_seen = set(seen)
    requests = []
    for item in items:
        item_id = item["_id"]
        if item_id not in seen:
            # Optionally check processing manifest
            existing_meta = item.get("meta", {})
            processing_status = existing_meta.get("processing_status", {})
            if processing_status.get("status") == "completed_clean":
                context.log.info(
                    "Skipping %s — already processed cleanly on %s",
                    item["name"],
                    processing_status.get("last_processed", "unknown"),
                )
                new_seen.add(item_id)
                continue

            requests.append(
                RunRequest(
                    run_key=item_id,
                    run_config={
                        "ops": {
                            "raw_experiment_log": {
                                "config": {
                                    "item_id": item_id,
                                    "filename": item["name"],
                                }
                            }
                        }
                    },
                )
            )
            new_seen.add(item_id)

    context.update_cursor(json.dumps({"seen": list(new_seen)}))
    return requests
```

**Key changes:**
- `list_all_spreadsheet_items` → `list_recent_spreadsheets`
- Checks `meta.processing_status` before triggering a run
- Items processed with `completed_clean` status are added to `seen` without reprocessing
- Items with `completed_with_warnings` are allowed to reprocess (operator might have fixed the data)

**Note:** The `meta` field may not be present in the item listing response
depending on the Girder endpoint. If `item.get("meta")` is always empty from
the listing endpoint, you'll need to fetch each item individually with
`girder.get(f"item/{item_id}")` to check the manifest. Only do this for
items not already in `seen` to avoid excessive API calls.

### Step 3: Add tests

Add to `tests/test_girder_io.py`:

```python
from helix_dagster.girder_io import list_recent_spreadsheets

def test_list_recent_spreadsheets():
    client = MagicMock()
    client.get.return_value = [
        {"name": "log_2025.csv", "_id": "c1", "created": "2025-03-01T00:00:00Z"},
        {"name": "data.tdms", "_id": "c2", "created": "2025-03-01T00:00:00Z"},
        {"name": "results.xlsx", "_id": "c3", "created": "2025-02-28T00:00:00Z"},
    ]
    result = list_recent_spreadsheets(client, "folder123", limit=50)
    assert len(result) == 2  # only .csv and .xlsx, not .tdms
    assert result[0]["name"] == "log_2025.csv"
    assert result[1]["name"] == "results.xlsx"
    client.get.assert_called_once_with(
        "item",
        parameters={
            "folderId": "folder123",
            "sort": "created",
            "sortdir": -1,
            "limit": 50,
        },
    )
```

### Step 4: Update CLAUDE.md

Remove `list_all_spreadsheet_items` from the workflow description. Add a note
about the sensor's manifest-aware behavior.

### Step 5: Verify, commit, push, create PR

```bash
poetry run pytest tests/ -v

git add -A
git commit -m "feat: optimize sensor to avoid recursive folder crawl

Replaced list_all_spreadsheet_items() recursive folder walk in the sensor
with list_recent_spreadsheets() which queries the 100 most recent items
sorted by creation date. Also integrated with the processing manifest:
spreadsheets already processed cleanly are skipped.

Closes #ISSUE_NUMBER"
git push -u origin feat/optimize-sensor

gh pr create \
  --title "feat: optimize sensor to avoid recursive folder crawl" \
  --body "## Summary

Replaces the recursive folder crawl in \`helix_folder_sensor\` with a targeted
recent-items query, and integrates with the processing manifest to skip
already-processed spreadsheets. Stage 6 of the refactoring.

## Changes

- **girder_io.py**: Added \`list_recent_spreadsheets()\`, deprecated
  \`list_all_spreadsheet_items()\`
- **sensors.py**: Updated to use \`list_recent_spreadsheets()\` and check
  \`meta.processing_status\` before triggering reruns
- **tests**: Added test for new function
- **CLAUDE.md**: Updated workflow description

Closes #ISSUE_NUMBER" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] GitHub issue created
- [ ] `list_recent_spreadsheets` function added
- [ ] `list_all_spreadsheet_items` marked deprecated
- [ ] Sensor updated to use new function and check manifest
- [ ] Test added and passing
- [ ] All existing tests still pass
- [ ] CLAUDE.md updated
- [ ] PR created against `refactor/asset-dag`
