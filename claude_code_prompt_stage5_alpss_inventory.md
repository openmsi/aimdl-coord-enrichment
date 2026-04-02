# Claude Code Prompt — Stage 5: Add ALPSS results inventory for quality reporting

**Read CLAUDE.md first.** Then read `ROADMAP_aimdl_refactor.md`.
Then read `issues/03-alpss-results-inventory.md`.

**Prerequisite:** Stages 1–4 must be merged. The `pdv_trace_inventory` asset,
asset checks, and processing manifest must already exist.

## GitHub Issue

```bash
gh issue create \
  --title "feat: add ALPSS results inventory for quality reporting" \
  --body-file issues/03-alpss-results-inventory.md \
  --label "enhancement"
```

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b feat/alpss-results-inventory
```

## Overview

Add an `alpss_results_inventory` asset that fetches `pdv_alpss_result` items
from the `/aimdl/datafiles` endpoint. Enhance `quality_report` to include
ALPSS processing completeness metrics.

## Changes

### Step 1: Add `alpss_results_inventory` asset

In `helix_dagster/assets.py`, add a new asset:

```python
from helix_dagster.constants import ALPSS_RESULT_DATA_TYPE

@asset
def alpss_results_inventory(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> list:
    """Fetch ALPSS result items via the /aimdl/datafiles endpoint.

    Returns items with meta.data_type='pdv_alpss_result'. Used for
    quality reporting on ALPSS processing completeness.
    """
    items = fetch_all_aimdl_datafiles(girder, ALPSS_RESULT_DATA_TYPE)

    igsns = set()
    for item in items:
        igsn = item.get("meta", {}).get("igsn")
        if igsn:
            igsns.add(igsn)

    context.add_output_metadata({
        "item_count": MetadataValue.int(len(items)),
        "unique_igsns": MetadataValue.int(len(igsns)),
        "data_type": MetadataValue.text(ALPSS_RESULT_DATA_TYPE),
    })
    return items
```

### Step 2: Enhance `quality_report` to include ALPSS completeness

Update the `quality_report` asset to accept `alpss_results_inventory` as an
additional upstream dependency:

```python
@asset
def quality_report(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_cross_references: dict,
    enriched_pdv_metadata: dict,
    alpss_results_inventory: list,
) -> dict:
    """Aggregate all issues and ALPSS completeness metrics."""
    igsn_issues = validated_rows["igsn_issues"]
    pdv_issues = pdv_cross_references["pdv_issues"]
    write_errors = enriched_pdv_metadata["write_errors"]
    matches = pdv_cross_references["matches"]

    # ALPSS completeness: which matched PDV traces have ALPSS results?
    alpss_igsns = set()
    for item in alpss_results_inventory:
        igsn = item.get("meta", {}).get("igsn")
        if igsn:
            alpss_igsns.add(igsn)

    matched_igsns = set()
    df = validated_rows["dataframe"]
    for row_idx in matches:
        row_igsn = df.loc[row_idx].get("valid_igsn")
        if row_igsn:
            matched_igsns.add(row_igsn)

    igsns_with_alpss = matched_igsns & alpss_igsns
    igsns_without_alpss = matched_igsns - alpss_igsns

    report = {
        "igsn_issues": igsn_issues,
        "pdv_issues": pdv_issues,
        "write_errors": write_errors,
        "alpss_completeness": {
            "matched_igsns": len(matched_igsns),
            "igsns_with_alpss_results": len(igsns_with_alpss),
            "igsns_without_alpss_results": len(igsns_without_alpss),
            "missing_igsns": sorted(igsns_without_alpss),
        },
        "summary": {
            "total_igsn_issues": len(igsn_issues),
            "total_pdv_issues": len(pdv_issues),
            "total_write_errors": len(write_errors),
            "alpss_coverage_pct": (
                round(100 * len(igsns_with_alpss) / len(matched_igsns), 1)
                if matched_igsns else 0.0
            ),
        },
    }

    context.add_output_metadata({
        "total_igsn_issues": MetadataValue.int(len(igsn_issues)),
        "total_pdv_issues": MetadataValue.int(len(pdv_issues)),
        "total_write_errors": MetadataValue.int(len(write_errors)),
        "alpss_coverage_pct": MetadataValue.float(
            report["summary"]["alpss_coverage_pct"]
        ),
        "igsns_without_alpss": MetadataValue.int(len(igsns_without_alpss)),
    })
    return report
```

### Step 3: Update `__init__.py`

Add `alpss_results_inventory` to the imports and Definitions assets list.
Make sure `processing_manifest` (from Stage 4) is also in the list.

### Step 4: Update tests

Update `test_asset_dag_loads` to include `"alpss_results_inventory"` in the
expected asset set.

Add a test for the quality report's ALPSS completeness section:

```python
def test_quality_report_alpss_completeness():
    """Verify quality_report includes ALPSS completeness metrics."""
    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001",
         "valid_igsn": "ABCDEF12345"},
        {"Sample_IGSN": "ABCDEF12346", "PDV_FileName": "shot002",
         "valid_igsn": "ABCDEF12346"},
    ])
    validated = {"dataframe": df, "igsn_issues": []}
    pdv_xrefs = {
        "matches": {0: {"_id": "a1"}, 1: {"_id": "b1"}},
        "pdv_issues": [],
    }
    enriched = {"written_count": 2, "write_errors": [], "coord_failures": 0}
    alpss_items = [
        {"meta": {"igsn": "ABCDEF12345", "data_type": "pdv_alpss_result"}},
        # ABCDEF12346 has no ALPSS result
    ]

    ctx = build_asset_context()
    from helix_dagster.assets import quality_report as quality_report_fn
    report = quality_report_fn(
        context=ctx,
        validated_rows=validated,
        pdv_cross_references=pdv_xrefs,
        enriched_pdv_metadata=enriched,
        alpss_results_inventory=alpss_items,
    )
    assert report["alpss_completeness"]["igsns_with_alpss_results"] == 1
    assert report["alpss_completeness"]["igsns_without_alpss_results"] == 1
    assert "ABCDEF12346" in report["alpss_completeness"]["missing_igsns"]
    assert report["summary"]["alpss_coverage_pct"] == 50.0
```

### Step 5: Verify, commit, push, create PR

```bash
poetry run pytest tests/ -v

git add -A
git commit -m "feat: add ALPSS results inventory for quality reporting

Added alpss_results_inventory asset that fetches pdv_alpss_result items
via /aimdl/datafiles. Enhanced quality_report to include ALPSS processing
completeness metrics (coverage percentage, IGSNs missing ALPSS results).

Closes #ISSUE_NUMBER"
git push -u origin feat/alpss-results-inventory

gh pr create \
  --title "feat: add ALPSS results inventory for quality reporting" \
  --body "## Summary

Adds an \`alpss_results_inventory\` asset and enhances the quality report with
ALPSS processing completeness metrics. Stage 5 of the refactoring.

## Changes

- **assets.py**: Added \`alpss_results_inventory\` asset. Enhanced \`quality_report\`
  with ALPSS coverage percentage and list of IGSNs missing results.
- **__init__.py**: Added new asset to Definitions
- **tests**: Updated \`test_asset_dag_loads\`, added ALPSS completeness test

Closes #ISSUE_NUMBER" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] GitHub issue created
- [ ] `alpss_results_inventory` asset added
- [ ] `quality_report` enhanced with ALPSS completeness
- [ ] `__init__.py` updated
- [ ] `test_asset_dag_loads` updated
- [ ] ALPSS completeness test added and passing
- [ ] All tests pass
- [ ] PR created against `refactor/asset-dag`
