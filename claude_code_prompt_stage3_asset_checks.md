# Claude Code Prompt — Stage 3: Add Dagster asset checks for error surfacing

**Read CLAUDE.md first.** Then read `ROADMAP_aimdl_refactor.md` for full context.
Then read `issues/05-asset-checks.md`.

**Prerequisite:** Stages 1 and 2 must be merged. The assets `pdv_trace_inventory`,
`validated_rows`, `pdv_cross_references`, and `enriched_pdv_metadata` must exist.

## GitHub Issue

```bash
gh issue create \
  --title "feat: add Dagster asset checks for data quality surfacing" \
  --body-file issues/05-asset-checks.md \
  --label "enhancement"
```

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b feat/asset-checks
```

## Overview

The pipeline currently treats all data quality issues as data — invalid IGSNs
become list entries, unmatched PDV files become dict entries, write exceptions
are caught. Dagster always shows runs as "Success."

Dagster's `@asset_check` mechanism evaluates data quality after each asset
materializes and displays colored pass/warn/fail indicators in the UI. This
stage adds checks for each critical asset without changing pipeline flow.

## Changes

### Step 1: Create `helix_dagster/checks.py`

Create a new file `helix_dagster/checks.py` with all asset check functions:

```python
"""Dagster asset checks for data quality surfacing.

These checks evaluate the output of each critical asset and produce
pass/warn/fail indicators visible in the Dagster UI. They do NOT block
pipeline execution — they are advisory.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)


@asset_check(asset="pdv_trace_inventory")
def zero_inventory(context, pdv_trace_inventory):
    """ERROR if the PDV trace inventory returned zero items.

    This typically means meta.igsn has not been tagged on PDV files yet,
    so the /aimdl/datafiles endpoint returns nothing.
    """
    count = len(pdv_trace_inventory)
    passed = count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"item_count": count},
        description=(
            f"Inventory contains {count} items."
            if passed
            else "Inventory is EMPTY. Check that meta.igsn is tagged on PDV files."
        ),
    )


@asset_check(asset="validated_rows")
def igsn_validity_rate(context, validated_rows):
    """WARN if fewer than 80% of rows have valid IGSNs."""
    df = validated_rows["dataframe"]
    total = len(df)
    if total == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"total_rows": 0, "validity_rate": 0.0},
            description="No rows to validate.",
        )
    valid_count = df["valid_igsn"].notna().sum()
    rate = valid_count / total
    passed = rate >= 0.8
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "total_rows": total,
            "valid_count": int(valid_count),
            "validity_rate": round(float(rate), 3),
        },
        description=f"IGSN validity rate: {rate:.1%} ({valid_count}/{total})",
    )


@asset_check(asset="pdv_cross_references")
def pdv_match_rate(context, pdv_cross_references, validated_rows):
    """WARN if fewer than 50% of rows with PDV filenames were matched."""
    df = validated_rows["dataframe"]
    # Count rows that have a non-empty, non-NaN PDV filename
    import math
    rows_with_pdv = sum(
        1
        for _, row in df.iterrows()
        if row.get("PDV_FileName") is not None
        and not (isinstance(row.get("PDV_FileName"), float) and math.isnan(row.get("PDV_FileName")))
        and str(row.get("PDV_FileName")).strip() != ""
    )
    matched_count = len(pdv_cross_references["matches"])
    rate = matched_count / rows_with_pdv if rows_with_pdv > 0 else 0.0
    passed = rate >= 0.5
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "rows_with_pdv_filename": rows_with_pdv,
            "matched_count": matched_count,
            "match_rate": round(rate, 3),
        },
        description=f"PDV match rate: {rate:.1%} ({matched_count}/{rows_with_pdv})",
    )


@asset_check(asset="pdv_cross_references")
def igsn_consistency(context, pdv_cross_references):
    """ERROR if any matched PDV item has a different IGSN than the spreadsheet row."""
    issues = pdv_cross_references["pdv_issues"]
    mismatches = [i for i in issues if i.get("type") == "igsn_mismatch"]
    passed = len(mismatches) == 0
    metadata = {"mismatch_count": len(mismatches)}
    if mismatches:
        # Include first few mismatches for quick diagnosis
        examples = mismatches[:3]
        metadata["examples"] = str(examples)
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata=metadata,
        description=(
            "No IGSN mismatches."
            if passed
            else f"{len(mismatches)} IGSN mismatch(es) between spreadsheet and Girder items."
        ),
    )


@asset_check(asset="enriched_pdv_metadata")
def enrichment_success_rate(context, enriched_pdv_metadata, pdv_cross_references):
    """WARN if fewer than 90% of matched items were successfully enriched."""
    matched_count = len(pdv_cross_references["matches"])
    written_count = enriched_pdv_metadata["written_count"]
    rate = written_count / matched_count if matched_count > 0 else 0.0
    error_count = len(enriched_pdv_metadata["write_errors"])
    passed = rate >= 0.9
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "matched_count": matched_count,
            "written_count": written_count,
            "error_count": error_count,
            "success_rate": round(rate, 3),
        },
        description=f"Enrichment success rate: {rate:.1%} ({written_count}/{matched_count})",
    )


@asset_check(asset="enriched_pdv_metadata")
def coord_transform_check(context, enriched_pdv_metadata):
    """WARN if any coordinate transformations failed."""
    # The enriched_pdv_metadata asset currently doesn't expose coord_failures
    # in its return dict, only as output metadata. We check write_errors as a proxy.
    # TODO: expose coord_failures in the return dict in a future refactor.
    errors = enriched_pdv_metadata["write_errors"]
    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"write_error_count": len(errors)},
        description=(
            "No write errors."
            if passed
            else f"{len(errors)} metadata write error(s)."
        ),
    )
```

**Important:** The `pdv_match_rate` check needs `validated_rows` as an additional
input to count how many rows had PDV filenames. Dagster allows asset checks to
accept additional asset dependencies.

### Step 2: Expose `coord_failures` in `enriched_pdv_metadata` return dict

In `helix_dagster/assets.py`, update the `enriched_pdv_metadata` asset's return
statement to include `coord_failures`:

**Before:**
```python
    return {"written_count": written_count, "write_errors": write_errors}
```

**After:**
```python
    return {
        "written_count": written_count,
        "write_errors": write_errors,
        "coord_failures": coord_failures,
    }
```

Then update the `coord_transform_check` in `checks.py` to use this:

```python
@asset_check(asset="enriched_pdv_metadata")
def coord_transform_check(context, enriched_pdv_metadata):
    """WARN if any coordinate transformations failed."""
    failures = enriched_pdv_metadata.get("coord_failures", 0)
    passed = failures == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"coord_failures": failures},
        description=(
            "All coordinate transforms succeeded."
            if passed
            else f"{failures} coordinate transform failure(s). Check COORD_TRANSFORMS_YAML."
        ),
    )
```

### Step 3: Register checks in `__init__.py`

In `helix_dagster/__init__.py`, import and register all checks:

```python
from helix_dagster.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    pdv_match_rate,
    zero_inventory,
)

defs = Definitions(
    assets=[
        raw_experiment_log,
        pdv_trace_inventory,
        validated_rows,
        pdv_cross_references,
        enriched_pdv_metadata,
        quality_report,
    ],
    asset_checks=[
        zero_inventory,
        igsn_validity_rate,
        pdv_match_rate,
        igsn_consistency,
        enrichment_success_rate,
        coord_transform_check,
    ],
    jobs=[process_helix_assets_job],
    sensors=[helix_folder_sensor],
    resources={...},  # unchanged
)
```

### Step 4: Add tests

Create `tests/test_checks.py`:

```python
"""Tests for Dagster asset checks."""

import pandas as pd
from dagster import build_asset_context

from helix_dagster.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    pdv_match_rate,
    zero_inventory,
)


def test_zero_inventory_fails_on_empty():
    ctx = build_asset_context()
    result = zero_inventory(ctx, pdv_trace_inventory=[])
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_zero_inventory_passes_with_items():
    ctx = build_asset_context()
    result = zero_inventory(ctx, pdv_trace_inventory=[{"_id": "a"}])
    assert result.passed


def test_igsn_validity_rate_passes_above_threshold():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"valid_igsn": ["A", "B", "C", "D", None]}),
        "igsn_issues": [],
    }
    result = igsn_validity_rate(ctx, validated_rows=validated)
    assert result.passed  # 4/5 = 80%


def test_igsn_validity_rate_warns_below_threshold():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"valid_igsn": ["A", None, None, None, None]}),
        "igsn_issues": [],
    }
    result = igsn_validity_rate(ctx, validated_rows=validated)
    assert not result.passed  # 1/5 = 20%
    assert result.severity.value == "WARN"


def test_pdv_match_rate_passes():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"PDV_FileName": ["shot1", "shot2"]}),
        "igsn_issues": [],
    }
    xrefs = {"matches": {0: {}, 1: {}}, "pdv_issues": []}
    result = pdv_match_rate(ctx, pdv_cross_references=xrefs, validated_rows=validated)
    assert result.passed  # 2/2 = 100%


def test_pdv_match_rate_warns():
    ctx = build_asset_context()
    validated = {
        "dataframe": pd.DataFrame({"PDV_FileName": ["shot1", "shot2", "shot3", "shot4"]}),
        "igsn_issues": [],
    }
    xrefs = {"matches": {0: {}}, "pdv_issues": []}
    result = pdv_match_rate(ctx, pdv_cross_references=xrefs, validated_rows=validated)
    assert not result.passed  # 1/4 = 25%


def test_igsn_consistency_passes():
    ctx = build_asset_context()
    xrefs = {"matches": {}, "pdv_issues": []}
    result = igsn_consistency(ctx, pdv_cross_references=xrefs)
    assert result.passed


def test_igsn_consistency_errors_on_mismatch():
    ctx = build_asset_context()
    xrefs = {
        "matches": {},
        "pdv_issues": [
            {"type": "igsn_mismatch", "spreadsheet_igsn": "A", "item_igsn": "B"},
        ],
    }
    result = igsn_consistency(ctx, pdv_cross_references=xrefs)
    assert not result.passed
    assert result.severity.value == "ERROR"


def test_enrichment_success_rate_passes():
    ctx = build_asset_context()
    enriched = {"written_count": 9, "write_errors": [], "coord_failures": 0}
    xrefs = {"matches": {i: {} for i in range(10)}, "pdv_issues": []}
    result = enrichment_success_rate(
        ctx, enriched_pdv_metadata=enriched, pdv_cross_references=xrefs
    )
    assert result.passed  # 9/10 = 90%


def test_enrichment_success_rate_warns():
    ctx = build_asset_context()
    enriched = {"written_count": 5, "write_errors": [{}] * 5, "coord_failures": 0}
    xrefs = {"matches": {i: {} for i in range(10)}, "pdv_issues": []}
    result = enrichment_success_rate(
        ctx, enriched_pdv_metadata=enriched, pdv_cross_references=xrefs
    )
    assert not result.passed  # 5/10 = 50%


def test_coord_transform_check_passes():
    ctx = build_asset_context()
    enriched = {"written_count": 10, "write_errors": [], "coord_failures": 0}
    result = coord_transform_check(ctx, enriched_pdv_metadata=enriched)
    assert result.passed


def test_coord_transform_check_warns():
    ctx = build_asset_context()
    enriched = {"written_count": 10, "write_errors": [], "coord_failures": 3}
    result = coord_transform_check(ctx, enriched_pdv_metadata=enriched)
    assert not result.passed
```

### Step 5: Update `test_asset_dag_loads`

In `tests/test_assets.py`, update to verify checks are registered:

```python
def test_asset_dag_loads():
    """Verify the Dagster Definitions object loads with all assets and checks."""
    from helix_dagster import defs

    repo = defs.get_repository_def()
    asset_keys = {ak.to_user_string() for ak in repo.asset_graph.get_all_asset_keys()}

    expected_assets = {
        "raw_experiment_log",
        "pdv_trace_inventory",
        "validated_rows",
        "pdv_cross_references",
        "enriched_pdv_metadata",
        "quality_report",
    }
    for name in expected_assets:
        assert name in asset_keys, f"Missing asset: {name}"

    # Verify asset checks are registered
    check_keys = {
        str(ck) for ck in repo.asset_graph.asset_check_keys
    }
    assert len(check_keys) >= 5, f"Expected at least 5 asset checks, got {len(check_keys)}"
```

### Step 6: Verify

```bash
poetry run pytest tests/test_checks.py -v
poetry run pytest tests/ -v
```

### Step 7: Commit, push, create PR

```bash
git add -A
git commit -m "feat: add Dagster asset checks for data quality surfacing

Added @asset_check functions for each critical asset that produce
pass/warn/fail indicators visible in the Dagster UI:

- zero_inventory: ERROR if pdv_trace_inventory returns 0 items
- igsn_validity_rate: WARN if <80% of rows have valid IGSNs
- pdv_match_rate: WARN if <50% of PDV filenames matched
- igsn_consistency: ERROR if any IGSN mismatch between spreadsheet and Girder
- enrichment_success_rate: WARN if <90% of matched items enriched
- coord_transform_check: WARN if any coordinate transforms failed

Checks are advisory and do not block pipeline execution.

Also exposed coord_failures in enriched_pdv_metadata return dict.

Closes #ISSUE_NUMBER"
git push -u origin feat/asset-checks

gh pr create \
  --title "feat: add Dagster asset checks for data quality surfacing" \
  --body "## Summary

Adds \`@asset_check\` functions that evaluate data quality after each critical
asset materializes, displaying colored pass/warn/fail indicators in the Dagster
UI. Stage 3 of the refactoring.

## Checks Added

| Check | Asset | Severity | Threshold |
|-------|-------|----------|-----------|
| \`zero_inventory\` | \`pdv_trace_inventory\` | ERROR | 0 items |
| \`igsn_validity_rate\` | \`validated_rows\` | WARN | <80% |
| \`pdv_match_rate\` | \`pdv_cross_references\` | WARN | <50% |
| \`igsn_consistency\` | \`pdv_cross_references\` | ERROR | any mismatch |
| \`enrichment_success_rate\` | \`enriched_pdv_metadata\` | WARN | <90% |
| \`coord_transform_check\` | \`enriched_pdv_metadata\` | WARN | any failure |

## Changes

- New file: \`helix_dagster/checks.py\` with 6 asset checks
- \`assets.py\`: Exposed \`coord_failures\` in \`enriched_pdv_metadata\` return dict
- \`__init__.py\`: Registered all checks in Definitions
- \`tests/test_checks.py\`: 12 unit tests

Closes #ISSUE_NUMBER" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] GitHub issue created
- [ ] `checks.py` created with 6 asset check functions
- [ ] `enriched_pdv_metadata` returns `coord_failures`
- [ ] All checks registered in `__init__.py` Definitions
- [ ] 12 check tests pass
- [ ] Existing tests still pass
- [ ] PR created against `refactor/asset-dag`
