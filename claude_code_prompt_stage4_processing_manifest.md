# Claude Code Prompt — Stage 4: Add processing manifest with Girder write-back

**Read CLAUDE.md first.** Then read `ROADMAP_aimdl_refactor.md` for full context.
Then read `issues/06-processing-manifest.md`.

**Prerequisite:** Stages 1–3 must be merged.

## GitHub Issue

```bash
gh issue create \
  --title "feat: add processing manifest with Girder write-back" \
  --body-file issues/06-processing-manifest.md \
  --label "enhancement"
```

## Branch

```bash
git checkout refactor/asset-dag
git pull
git checkout -b feat/processing-manifest
```

## Overview

Add a `processing_manifest` asset at the end of the DAG that writes a structured
processing record to the source spreadsheet's Girder item as `meta.processing_status`.
This provides an audit trail, enables idempotency (sensor can check before
reprocessing), and gives Girder-side visibility into pipeline status.

## Changes

### Step 1: Add `processing_manifest` asset to assets.py

Add a new asset that depends on `quality_report` and all upstream data:

```python
from datetime import datetime, timezone
from helix_dagster import __version__ as PIPELINE_VERSION
```

(If `__version__` doesn't exist in `__init__.py`, add `__version__ = "0.2.0"` there.)

```python
@asset
def processing_manifest(
    context: AssetExecutionContext,
    config: ExperimentLogConfig,
    quality_report: dict,
    validated_rows: dict,
    pdv_cross_references: dict,
    enriched_pdv_metadata: dict,
    girder: GirderConnection,
) -> dict:
    """Write a processing status record to the source spreadsheet's Girder item.

    This provides:
    - Audit trail: persistent record of what the pipeline did
    - Idempotency: sensor can check before triggering reruns
    - Cross-system visibility: Girder UI shows processing status
    """
    df = validated_rows["dataframe"]
    total_rows = len(df)
    valid_igsn_count = int(df["valid_igsn"].notna().sum())
    matched_count = len(pdv_cross_references["matches"])
    written_count = enriched_pdv_metadata["written_count"]
    coord_failures = enriched_pdv_metadata.get("coord_failures", 0)

    igsn_issues = validated_rows["igsn_issues"]
    pdv_issues = pdv_cross_references["pdv_issues"]
    write_errors = enriched_pdv_metadata["write_errors"]

    issues_summary = {
        "igsn_invalid": sum(1 for i in igsn_issues if i.get("issue") == "invalid_format"),
        "igsn_missing": sum(1 for i in igsn_issues if i.get("issue") == "missing"),
        "pdv_not_found": sum(1 for i in pdv_issues if i.get("type") == "not_found"),
        "pdv_ambiguous": sum(1 for i in pdv_issues if i.get("type") == "ambiguous"),
        "igsn_mismatch": sum(1 for i in pdv_issues if i.get("type") == "igsn_mismatch"),
        "write_errors": len(write_errors),
        "coord_failures": coord_failures,
    }

    has_issues = any(v > 0 for v in issues_summary.values())
    status = "completed_with_warnings" if has_issues else "completed_clean"

    manifest = {
        "last_processed": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": context.run_id,
        "pipeline_version": PIPELINE_VERSION,
        "total_rows": total_rows,
        "rows_valid_igsn": valid_igsn_count,
        "rows_matched_pdv": matched_count,
        "rows_enriched": written_count,
        "status": status,
        "issues_summary": issues_summary,
    }

    # Write to the source spreadsheet's Girder item
    try:
        girder.addMetadataToItem(config.item_id, {"processing_status": manifest})
        context.log.info(
            "Wrote processing manifest to Girder item %s: status=%s",
            config.item_id,
            status,
        )
    except Exception as exc:
        context.log.error(
            "Failed to write processing manifest to Girder item %s: %s",
            config.item_id,
            exc,
        )
        manifest["write_failed"] = True

    context.add_output_metadata({
        "status": MetadataValue.text(status),
        "total_rows": MetadataValue.int(total_rows),
        "rows_enriched": MetadataValue.int(written_count),
        "has_issues": MetadataValue.bool(has_issues),
        "source_item_id": MetadataValue.text(config.item_id),
    })

    return manifest
```

**Important:** This asset needs `config: ExperimentLogConfig` to know which
Girder item to write to. The `ExperimentLogConfig` is already defined and used
by `raw_experiment_log`. In Dagster, all assets in a job share the same run
config, so `processing_manifest` can access the same config.

### Step 2: Update `__init__.py` to register the new asset

```python
from helix_dagster.assets import (
    alpss_results_inventory,    # if Stage 5 is merged, otherwise omit
    enriched_pdv_metadata,
    pdv_cross_references,
    pdv_trace_inventory,
    process_helix_assets_job,
    processing_manifest,
    quality_report,
    raw_experiment_log,
    validated_rows,
)
```

Add `processing_manifest` to the assets list in `Definitions`.

### Step 3: Update `process_helix_assets_job` selection

If the job uses `AssetSelection.all()`, no change is needed — the new asset
is automatically included. If it uses an explicit list, add `processing_manifest`.

### Step 4: Add a `__version__` to the package if it doesn't exist

In `helix_dagster/__init__.py`, if there's no `__version__`, add near the top:

```python
__version__ = "0.2.0"
```

This is referenced by the manifest for provenance tracking.

### Step 5: Add tests

Add to `tests/test_assets.py`:

```python
def test_processing_manifest_clean():
    """Test manifest for a run with no issues."""
    from unittest.mock import MagicMock
    from helix_dagster.assets import processing_manifest as manifest_fn, ExperimentLogConfig

    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345", "PDV_FileName": "shot001",
         "valid_igsn": "ABCDEF12345"},
    ])
    validated = {"dataframe": df, "igsn_issues": []}
    xrefs = {"matches": {0: {"_id": "a1"}}, "pdv_issues": []}
    enriched = {"written_count": 1, "write_errors": [], "coord_failures": 0}
    report = {"igsn_issues": [], "pdv_issues": [], "write_errors": [],
              "summary": {"total_igsn_issues": 0, "total_pdv_issues": 0,
                          "total_write_errors": 0}}

    config = ExperimentLogConfig(item_id="test_item_123", filename="test.csv")
    mock_girder = MagicMock()

    ctx = build_asset_context()
    result = manifest_fn(
        context=ctx,
        config=config,
        quality_report=report,
        validated_rows=validated,
        pdv_cross_references=xrefs,
        enriched_pdv_metadata=enriched,
        girder=mock_girder,
    )

    assert result["status"] == "completed_clean"
    assert result["total_rows"] == 1
    assert result["rows_enriched"] == 1
    assert result["issues_summary"]["igsn_invalid"] == 0
    mock_girder.addMetadataToItem.assert_called_once()


def test_processing_manifest_with_warnings():
    """Test manifest for a run with issues."""
    from unittest.mock import MagicMock
    from helix_dagster.assets import processing_manifest as manifest_fn, ExperimentLogConfig

    df = pd.DataFrame([
        {"Sample_IGSN": "INVALID", "PDV_FileName": "shot001",
         "valid_igsn": None},
    ])
    validated = {
        "dataframe": df,
        "igsn_issues": [{"issue": "invalid_format", "value": "INVALID", "row": 0}],
    }
    xrefs = {"matches": {}, "pdv_issues": [{"type": "not_found", "row": 0}]}
    enriched = {"written_count": 0, "write_errors": [], "coord_failures": 0}
    report = {"igsn_issues": validated["igsn_issues"],
              "pdv_issues": xrefs["pdv_issues"], "write_errors": [],
              "summary": {}}

    config = ExperimentLogConfig(item_id="test_item_456", filename="test.csv")
    mock_girder = MagicMock()

    ctx = build_asset_context()
    result = manifest_fn(
        context=ctx,
        config=config,
        quality_report=report,
        validated_rows=validated,
        pdv_cross_references=xrefs,
        enriched_pdv_metadata=enriched,
        girder=mock_girder,
    )

    assert result["status"] == "completed_with_warnings"
    assert result["issues_summary"]["igsn_invalid"] == 1
    assert result["issues_summary"]["pdv_not_found"] == 1
```

Update `test_asset_dag_loads` to include `"processing_manifest"` in expected assets.

### Step 6: Verify

```bash
poetry run pytest tests/ -v
```

### Step 7: Commit, push, create PR

```bash
git add -A
git commit -m "feat: add processing manifest with Girder write-back

Added processing_manifest asset that writes a structured processing record
to the source spreadsheet's Girder item as meta.processing_status. This
provides an audit trail, enables idempotency (sensor can check before
reprocessing), and gives Girder-side visibility into pipeline status.

Manifest includes: timestamp, Dagster run ID, pipeline version, row counts,
enrichment counts, status (completed_clean/completed_with_warnings), and
a detailed issues_summary breakdown.

Closes #ISSUE_NUMBER"
git push -u origin feat/processing-manifest

gh pr create \
  --title "feat: add processing manifest with Girder write-back" \
  --body "## Summary

Adds a \`processing_manifest\` asset that writes a structured processing
record to the source spreadsheet's Girder item. Stage 4 of the refactoring.

## What it writes to Girder

\`meta.processing_status\` on the source spreadsheet item:
\`\`\`json
{
  \"status\": \"completed_with_warnings\",
  \"total_rows\": 45,
  \"rows_valid_igsn\": 42,
  \"rows_matched_pdv\": 40,
  \"rows_enriched\": 38,
  \"dagster_run_id\": \"abc123\",
  \"issues_summary\": { ... }
}
\`\`\`

## Why

- **Audit trail:** Persistent record linked to source data in Girder
- **Idempotency:** Sensor can check status before triggering reruns
- **Cross-system visibility:** Girder web UI shows processing status

## Changes

- \`assets.py\`: Added \`processing_manifest\` asset
- \`__init__.py\`: Registered new asset, added \`__version__\`
- \`tests/test_assets.py\`: 2 new tests (clean run, warnings run)

Closes #ISSUE_NUMBER" \
  --base refactor/asset-dag
```

## Verification Checklist

- [ ] GitHub issue created
- [ ] `processing_manifest` asset added to assets.py
- [ ] Writes `meta.processing_status` to source Girder item
- [ ] Status is `completed_clean` or `completed_with_warnings`
- [ ] `__version__` added to package
- [ ] Registered in `__init__.py` Definitions
- [ ] Both tests pass (clean + warnings scenarios)
- [ ] All existing tests still pass
- [ ] PR created against `refactor/asset-dag`
