# Claude Code Prompt — Fix: Complete Stage 2 rename (pdv_inventory → pdv_trace_inventory)

**Read CLAUDE.md first.**

## Context

A code review found that Stage 2 of the refactoring (the core switchover from
`pdv_inventory` to `pdv_trace_inventory` using the `/aimdl/datafiles` endpoint)
was never executed. Stages 3–6 were all built on top of the old `pdv_inventory`
asset. This fix completes Stage 2 and updates all downstream references.

**Do NOT create a GitHub issue for this.** This is a fix to an incomplete
prior stage, not a new feature.

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b fix/complete-stage2-rename
```

## Changes — read each one carefully, then execute ALL of them

### 1. `helix_dagster/assets.py`

**Replace the `pdv_inventory` asset** with `pdv_trace_inventory`. Find this exact function:

```python
@asset
def pdv_inventory(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> list:
    """Fetch all items from the PDV folder via Girder API."""
    items = girder.get(
        "item",
        parameters={"folderId": PDV_FOLDER_ID, "limit": 100000},
    )
    context.add_output_metadata(
        {
            "item_count": MetadataValue.int(len(items)),
        }
    )
    return items
```

Replace it entirely with:

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

**Update the `pdv_cross_references` asset signature and body.** Change the
parameter name and add IGSN consistency checking:

Find this function signature:
```python
def pdv_cross_references(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_inventory: list,
) -> dict:
```

Change to:
```python
def pdv_cross_references(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_trace_inventory: list,
) -> dict:
```

In the function body, change:
```python
        pdv_item, issue = match_pdv_file(pdv_inventory, pdv_filename)
```
to:
```python
        pdv_item, issue = match_pdv_file(pdv_trace_inventory, pdv_filename)
```

And add IGSN consistency checking. After the existing `if pdv_item is not None:` block,
add a cross-check. The full loop body should become:

```python
    for idx, row in df.iterrows():
        pdv_filename = row.get("PDV_FileName")
        pdv_item, issue = match_pdv_file(pdv_trace_inventory, pdv_filename)
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
        if issue is not None:
            issue["row"] = idx
            pdv_issues.append(issue)
```

And update the metadata to include mismatch count:
```python
    mismatch_count = sum(1 for i in pdv_issues if i["type"] == "igsn_mismatch")

    context.add_output_metadata(
        {
            "matched_count": MetadataValue.int(matched_count),
            "not_found_count": MetadataValue.int(not_found_count),
            "ambiguous_count": MetadataValue.int(ambiguous_count),
            "igsn_mismatch_count": MetadataValue.int(mismatch_count),
        }
    )
```

**Update the imports** at the top of `assets.py`:
- Change `from helix_dagster.constants import ALPSS_RESULT_DATA_TYPE, COLUMN_MAP, PDV_FOLDER_ID`
  to `from helix_dagster.constants import ALPSS_RESULT_DATA_TYPE, COLUMN_MAP, PDV_TRACE_DATA_TYPE`

The `fetch_all_aimdl_datafiles` import should already be there from the existing code.

### 2. `helix_dagster/constants.py`

Remove this line:
```python
PDV_FOLDER_ID = os.environ.get("PDV_FOLDER_ID")
```

Keep everything else including `PDV_TRACE_DATA_TYPE` and `HELIX_FOLDER_ID`.

### 3. `helix_dagster/checks.py`

Change the `zero_inventory` check. Find:
```python
@asset_check(asset="pdv_inventory")
def zero_inventory(context, pdv_inventory):
    """ERROR if the PDV inventory returned zero items.

    This typically means the PDV folder is empty or misconfigured.
    """
    count = len(pdv_inventory)
```

Replace with:
```python
@asset_check(asset="pdv_trace_inventory")
def zero_inventory(context, pdv_trace_inventory):
    """ERROR if the PDV trace inventory returned zero items.

    This typically means meta.igsn has not been tagged on PDV files yet,
    so the /aimdl/datafiles endpoint returns nothing.
    """
    count = len(pdv_trace_inventory)
```

And change the error description from:
```python
            else "Inventory is EMPTY. Check PDV folder configuration."
```
to:
```python
            else "Inventory is EMPTY. Check that meta.igsn is tagged on PDV files."
```

### 4. `helix_dagster/__init__.py`

Change the import:
```python
    pdv_inventory,
```
to:
```python
    pdv_trace_inventory,
```

Change in the assets list:
```python
        pdv_inventory,
```
to:
```python
        pdv_trace_inventory,
```

### 5. `tests/test_assets.py`

In `test_pdv_cross_references_pure`, change the function call parameter:
```python
        pdv_inventory=pdv_items,
```
to:
```python
        pdv_trace_inventory=pdv_items,
```

Also add `meta` to the mock PDV items in that test so IGSN consistency checking can work:
```python
    pdv_items = [
        {"name": "shot001_ch1.tdms", "_id": "a1", "meta": {"igsn": "ABCDEF12345", "data_type": "pdv_trace"}},
        {"name": "shot002_ch1.tdms", "_id": "b1", "meta": {"igsn": "ABCDEF12346", "data_type": "pdv_trace"}},
    ]
```

In `test_asset_dag_loads`, change:
```python
        "pdv_inventory",
```
to:
```python
        "pdv_trace_inventory",
```

Add a new test for IGSN mismatch detection:

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

### 6. `tests/test_checks.py`

Change both `zero_inventory` tests. Find:
```python
    result = zero_inventory(ctx, pdv_inventory=[])
```
Replace with:
```python
    result = zero_inventory(ctx, pdv_trace_inventory=[])
```

And:
```python
    result = zero_inventory(ctx, pdv_inventory=[{"_id": "a"}])
```
Replace with:
```python
    result = zero_inventory(ctx, pdv_trace_inventory=[{"_id": "a"}])
```

## Verification

Run all tests:
```bash
pytest tests/ -v
```

Every test must pass. If any test fails, read the error message carefully and fix it.
The most likely failure mode is a missed rename — search the entire `helix_dagster/`
and `tests/` directories for any remaining occurrence of `pdv_inventory` (the old name):

```bash
grep -rn "pdv_inventory" helix_dagster/ tests/ --include="*.py"
```

This should return ZERO results. If it finds any, fix them.

Also verify no remaining reference to `PDV_FOLDER_ID`:
```bash
grep -rn "PDV_FOLDER_ID" helix_dagster/ tests/ --include="*.py"
```

This should also return ZERO results.

## Commit, push, create PR

```bash
git add -A
git commit -m "fix: complete Stage 2 rename — pdv_inventory → pdv_trace_inventory

The Stage 2 refactoring (replacing the 100,000-item folder crawl with the
indexed /aimdl/datafiles endpoint) was skipped during the initial execution.
Stages 3-6 were built on top of the old pdv_inventory asset.

This commit completes Stage 2:
- Replaced pdv_inventory with pdv_trace_inventory using /aimdl/datafiles
- Added IGSN consistency checking to pdv_cross_references
- Removed PDV_FOLDER_ID dependency
- Updated checks.py zero_inventory to target pdv_trace_inventory
- Updated all tests and __init__.py Definitions

Resolves the critical finding from the code review."
git push -u origin fix/complete-stage2-rename

gh pr create \
  --title "fix: complete Stage 2 — pdv_inventory → pdv_trace_inventory" \
  --body "## Problem

The Stage 2 refactoring was skipped during initial execution. The pipeline
still used the old \`pdv_inventory\` asset which fetches 100,000+ items from
the PDV folder. All of Stages 3-6 were built on top of this old asset.

## Fix

- Replaced \`pdv_inventory\` with \`pdv_trace_inventory\` using
  \`/aimdl/datafiles?dataType=pdv_trace\` (indexed MongoDB query)
- Added IGSN consistency checking to \`pdv_cross_references\`
- Removed \`PDV_FOLDER_ID\` environment variable dependency
- Updated \`checks.py\` \`zero_inventory\` to target \`pdv_trace_inventory\`
- Updated all tests and Definitions

## Files changed

- \`helix_dagster/assets.py\` — core rename + IGSN consistency
- \`helix_dagster/constants.py\` — removed \`PDV_FOLDER_ID\`
- \`helix_dagster/checks.py\` — retargeted \`zero_inventory\`
- \`helix_dagster/__init__.py\` — updated imports and Definitions
- \`tests/test_assets.py\` — updated params, added IGSN mismatch test
- \`tests/test_checks.py\` — updated params" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] `pdv_inventory` asset removed from assets.py
- [ ] `pdv_trace_inventory` asset added using `/aimdl/datafiles`
- [ ] `pdv_cross_references` depends on `pdv_trace_inventory`
- [ ] IGSN consistency checking added to `pdv_cross_references`
- [ ] `PDV_FOLDER_ID` removed from constants.py
- [ ] `checks.py` zero_inventory targets `pdv_trace_inventory`
- [ ] `__init__.py` imports and registers `pdv_trace_inventory`
- [ ] `grep -rn "pdv_inventory" helix_dagster/ tests/` returns ZERO results
- [ ] `grep -rn "PDV_FOLDER_ID" helix_dagster/ tests/` returns ZERO results
- [ ] All tests pass
- [ ] IGSN mismatch test added and passing
- [ ] PR created against `refactor/asset-dag`
