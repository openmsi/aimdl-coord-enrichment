# Claude Code Prompt — Stage 2: Replace `pdv_inventory` with `/aimdl` endpoint

**Read CLAUDE.md first.** Then read `ROADMAP_aimdl_refactor.md` for full context.
Then read `issues/02-replace-pdv-inventory.md`.

**Prerequisite:** Stage 1 must be merged. The `fetch_all_aimdl_datafiles` function
must exist in `girder_io.py` and `PDV_TRACE_DATA_TYPE` must exist in `constants.py`.

## GitHub Issue

```bash
gh issue create \
  --title "feat: replace pdv_inventory with /aimdl/datafiles endpoint" \
  --body-file issues/02-replace-pdv-inventory.md \
  --label "enhancement"
```

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b feat/replace-pdv-inventory
```

## Overview

Replace the `pdv_inventory` asset (which fetches 100,000+ items from a folder) with
`pdv_trace_inventory` (which queries the indexed `/aimdl/datafiles?dataType=pdv_trace`
endpoint). Update all downstream dependencies and tests.

## Changes

### Step 1: Replace `pdv_inventory` asset in assets.py

In `helix_dagster/assets.py`:

1. Remove the `pdv_inventory` asset function entirely.

2. Add the import:
   ```python
   from helix_dagster.girder_io import fetch_all_aimdl_datafiles
   from helix_dagster.constants import PDV_TRACE_DATA_TYPE
   ```

3. Add the new asset:
   ```python
   @asset
   def pdv_trace_inventory(
       context: AssetExecutionContext,
       girder: GirderConnection,
   ) -> list:
       """Fetch PDV trace items via the /aimdl/datafiles endpoint.

       Uses an indexed MongoDB query filtered by meta.data_type='pdv_trace'
       instead of crawling the PDV folder tree. Items must have meta.igsn
       set to appear in results.
       """
       items = fetch_all_aimdl_datafiles(girder, PDV_TRACE_DATA_TYPE)

       # Extract unique IGSNs from the inventory
       igsns = set()
       for item in items:
           igsn = item.get("meta", {}).get("igsn")
           if igsn:
               igsns.add(igsn)

       context.add_output_metadata({
           "item_count": MetadataValue.int(len(items)),
           "unique_igsns": MetadataValue.int(len(igsns)),
           "data_type": MetadataValue.text(PDV_TRACE_DATA_TYPE),
       })

       if len(items) == 0:
           context.log.warning(
               "pdv_trace_inventory returned 0 items. This may indicate that "
               "meta.igsn has not been tagged on PDV files yet."
           )

       return items
   ```

4. Remove the `PDV_FOLDER_ID` import from the top of assets.py (it was used
   by the old `pdv_inventory`). Keep other imports from constants.

### Step 2: Update `pdv_cross_references` upstream dependency

In `helix_dagster/assets.py`, change the `pdv_cross_references` asset:

**Before:**
```python
@asset
def pdv_cross_references(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_inventory: list,
) -> dict:
```

**After:**
```python
@asset
def pdv_cross_references(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_trace_inventory: list,
) -> dict:
```

Update the body to use `pdv_trace_inventory` instead of `pdv_inventory`:

**Before:**
```python
    pdv_item, issue = match_pdv_file(pdv_inventory, pdv_filename)
```

**After:**
```python
    pdv_item, issue = match_pdv_file(pdv_trace_inventory, pdv_filename)
```

### Step 3: Add IGSN consistency checking to `pdv_cross_references`

After a successful match, cross-check the IGSN from the spreadsheet row against
the `meta.igsn` on the matched Girder item. This catches data provenance errors
where a PDV file is tagged with a different IGSN than the experiment log expects.

Add after the match succeeds:
```python
    if pdv_item is not None:
        matches[idx] = pdv_item
        # Cross-check IGSN consistency
        row_igsn = row.get("valid_igsn")
        item_igsn = pdv_item.get("meta", {}).get("igsn")
        if row_igsn and item_igsn and row_igsn != item_igsn:
            pdv_issues.append({
                "pdv_filename": pdv_filename,
                "type": "igsn_mismatch",
                "row": idx,
                "spreadsheet_igsn": row_igsn,
                "item_igsn": item_igsn,
            })
```

Add the mismatch count to output metadata:
```python
    mismatch_count = sum(1 for i in pdv_issues if i["type"] == "igsn_mismatch")

    context.add_output_metadata({
        "matched_count": MetadataValue.int(matched_count),
        "not_found_count": MetadataValue.int(not_found_count),
        "ambiguous_count": MetadataValue.int(ambiguous_count),
        "igsn_mismatch_count": MetadataValue.int(mismatch_count),
    })
```

### Step 4: Remove `PDV_FOLDER_ID` from constants.py

In `helix_dagster/constants.py`, remove the line:
```python
PDV_FOLDER_ID = os.environ.get("PDV_FOLDER_ID")
```

It is no longer used by any code.

### Step 5: Update `__init__.py` Definitions

In `helix_dagster/__init__.py`:

1. Change the import:
   **Before:** `pdv_inventory,`
   **After:** `pdv_trace_inventory,`

2. Update the assets list:
   **Before:** `pdv_inventory,`
   **After:** `pdv_trace_inventory,`

### Step 6: Update tests

**`tests/test_assets.py`:**

Update `test_pdv_cross_references_pure`:
- Change the parameter name from `pdv_inventory=pdv_items` to
  `pdv_trace_inventory=pdv_items`
- Add `meta.igsn` to mock items to test IGSN consistency:
  ```python
  pdv_items = [
      {"name": "shot001_ch1.tdms", "_id": "a1", "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
      {"name": "shot002_ch1.tdms", "_id": "b1", "meta": {"igsn": "ABCDEF12346", "data_type": "pdv_trace"}},
  ]
  ```

Update `test_asset_dag_loads`:
- Change `"pdv_inventory"` to `"pdv_trace_inventory"` in the expected set

Add a new test `test_igsn_mismatch_detection`:
```python
def test_igsn_mismatch_detection():
    """Verify that IGSN mismatches between spreadsheet and Girder item are flagged."""
    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001",
         "valid_igsn": "ABCDEF12345"},
    ])
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "a1",
         "meta": {"igsn": "XXXXXX99999", "data_type": "pdv_trace"}},
    ]
    validated = {"dataframe": df, "igsn_issues": []}
    ctx = build_asset_context()
    result = pdv_cross_references_fn(
        context=ctx,
        validated_rows=validated,
        pdv_trace_inventory=pdv_items,
    )
    issues = result["pdv_issues"]
    mismatch_issues = [i for i in issues if i["type"] == "igsn_mismatch"]
    assert len(mismatch_issues) == 1
    assert mismatch_issues[0]["spreadsheet_igsn"] == "ABCDEF12345"
    assert mismatch_issues[0]["item_igsn"] == "XXXXXX99999"
```

**`tests/test_matching.py`:**

The existing tests use items without `meta` field, which is fine — the matching
function only looks at `name`. No changes needed here unless the match function
changes.

### Step 7: Update CLAUDE.md

Update the Workflow section to describe the new inventory approach:
```
## Workflow
1. Sensor polls a Girder folder for new experiment log spreadsheets (CSV/XLSX)
2. Pipeline downloads, parses, validates IGSNs
3. PDV trace inventory fetched via /aimdl/datafiles?dataType=pdv_trace
   (indexed MongoDB query, no directory crawling)
4. Cross-references PDV filenames and checks IGSN consistency
5. Writes enriched metadata to matched Girder items
6. Quality issues reported as structured metadata on the Dagster run
```

Update the Environment variables section to remove `PDV_FOLDER_ID` and add
`PDV_TRACE_DATA_TYPE`.

### Step 8: Verify

```bash
poetry run pytest tests/ -v
```

All tests should pass, including the new IGSN mismatch test.

### Step 9: Commit, push, create PR

```bash
git add -A
git commit -m "feat: replace pdv_inventory with /aimdl/datafiles endpoint

Replaced the pdv_inventory asset (which fetched 100,000+ items from the PDV
folder) with pdv_trace_inventory (which queries the indexed /aimdl/datafiles
endpoint for items with meta.data_type=pdv_trace).

Added IGSN consistency checking: the pipeline now cross-checks the IGSN from
the experiment log spreadsheet against meta.igsn on matched Girder items,
flagging mismatches as a new quality issue type.

Removed PDV_FOLDER_ID environment variable dependency.

Closes #ISSUE_NUMBER"
git push -u origin feat/replace-pdv-inventory

gh pr create \
  --title "feat: replace pdv_inventory with /aimdl/datafiles endpoint" \
  --body "## Summary

Replaces the \`pdv_inventory\` asset with \`pdv_trace_inventory\`, switching from
a 100,000-item folder crawl to an indexed MongoDB query via the \`/aimdl/datafiles\`
endpoint. Stage 2 of the refactoring in \`ROADMAP_aimdl_refactor.md\`.

## Changes

- **assets.py**: Replaced \`pdv_inventory\` with \`pdv_trace_inventory\` using
  \`fetch_all_aimdl_datafiles()\`. Updated \`pdv_cross_references\` dependency.
  Added IGSN consistency checking.
- **constants.py**: Removed \`PDV_FOLDER_ID\`
- **__init__.py**: Updated Definitions asset list
- **tests**: Updated all references, added IGSN mismatch test
- **CLAUDE.md**: Updated workflow and env var documentation

## Migration Note

The \`/aimdl/datafiles\` endpoint requires \`meta.igsn\` to exist on items.
Until IGSN tagging is complete, some PDV files may not appear in the inventory.
The asset logs a warning if the inventory is empty.

Closes #ISSUE_NUMBER" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] GitHub issue created
- [ ] `pdv_inventory` asset removed from assets.py
- [ ] `pdv_trace_inventory` asset added using `/aimdl/datafiles`
- [ ] `pdv_cross_references` depends on `pdv_trace_inventory`
- [ ] IGSN consistency checking added
- [ ] `PDV_FOLDER_ID` removed from constants.py
- [ ] `__init__.py` Definitions updated
- [ ] `test_pdv_cross_references_pure` updated with new param name
- [ ] `test_asset_dag_loads` updated with new asset name
- [ ] `test_igsn_mismatch_detection` added and passing
- [ ] CLAUDE.md updated
- [ ] All tests pass
- [ ] PR created against `refactor/asset-dag`
