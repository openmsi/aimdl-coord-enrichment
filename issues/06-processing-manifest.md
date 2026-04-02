# Issue: Add processing manifest (write-back to Girder)

## Problem

There is no durable record of what was processed, when, or what the outcome was.
The sensor cursor (a list of seen item IDs) lives in Dagster's storage and gets
lost on reset. There's no way to look at a spreadsheet in Girder and know whether
it was successfully processed, or what the outcome was.

This creates three problems:
1. **No idempotency**: Resetting Dagster reprocesses everything.
2. **No audit trail**: No cross-system visibility into processing history.
3. **No verification**: Cannot compare expected vs. actual processing outcomes.

## Proposed Change

Add a `processing_manifest` asset that runs after `quality_report` and writes
`meta.processing_status` to the source spreadsheet Girder item. The manifest
records:

```json
{
  "processing_status": {
    "last_processed": "2026-04-02T14:30:00Z",
    "dagster_run_id": "abc123...",
    "status": "completed_with_warnings",
    "total_rows": 45,
    "rows_valid_igsn": 42,
    "rows_matched_pdv": 40,
    "rows_enriched": 38,
    "issues_summary": {
      "igsn_issues": 3,
      "pdv_issues": 2,
      "write_errors": 0,
      "igsn_mismatches": 0
    }
  }
}
```

Update the sensor to check the manifest before triggering reprocessing:
- If the spreadsheet has `meta.processing_status.status == "completed"` and
  hasn't been modified since `last_processed`, skip it.

## Labels

`enhancement`, `backend`, `stage-4`
