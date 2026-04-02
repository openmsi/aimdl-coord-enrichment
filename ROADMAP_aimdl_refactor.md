# Refactoring Roadmap: Replace Directory Crawling with `/aimdl` Endpoint

## Background

The HELIX metadata extraction Dagster pipeline currently discovers PDV data files
by crawling Girder folder trees — fetching up to 100,000 items to match a handful
of PDV filenames from an experiment log spreadsheet. The Girder instance at
`data.htmdec.org` now has an `/aimdl/datafiles` REST endpoint (implemented in the
`girder-jsonforms` plugin, branch `igsn`) that performs an indexed MongoDB query
filtered by `meta.data_type` and `meta.igsn`, returning only the relevant items
without any directory traversal.

## The `/aimdl` Endpoint (from `girder-jsonforms` plugin)

**Source:** `Xarthisius/girder-jsonforms` on the `igsn` branch, file
`girder_jsonforms/rest/aimdl.py`

**Two routes:**

| Route | Method | Description |
|-------|--------|-------------|
| `/aimdl/datatype` | GET | Returns distinct `meta.data_type` values across all items |
| `/aimdl/datafiles` | GET | Returns Girder items filtered by `dataType`, paginated |

**`/aimdl/datafiles` behavior:**
- Requires `dataType` query parameter (string, required)
- Supports `limit` (max 100, hard cap), `offset`, and `sort` (default: `lowerName`)
- Queries MongoDB for items where:
  - `meta.igsn` exists
  - `meta.data_type` equals the requested type
  - Item is in the AIMD-L collection (`665de536bcc722774ce53754`)
- Returns standard Girder item objects with fields: `_id`, `name`, `meta.igsn`,
  `meta.data_type`, `size`, `created`, `folderId`, `lowerName`
- Requires user-level authentication (the existing API key auth works)

**Available `meta.data_type` values:**
- `pdv_trace` — raw PDV oscilloscope traces (what the pipeline matches against)
- `pdv_alpss_result` — ALPSS processed result files
- `pdv_alpss_output` — ALPSS output files
- `xrd_raw`, `xrd_derived`, `xrd_metadata` — XRD data
- `xrd_calibrant_raw`, `xrd_calibrant_derived` — XRD calibrant data
- `xrf_raw` — XRF data

## Current Pipeline Problems

### Problem 1: Expensive inventory
`pdv_inventory` fetches ALL items from PDV folder with `limit=100000`.
This is the primary performance bottleneck.

### Problem 2: Errors treated as data
Invalid IGSNs become `igsn_issues`, unmatched PDV files become `pdv_issues`,
write exceptions are swallowed into `write_errors`. Dagster always shows
green unless an exception escapes the asset body. An operator looking at
the Dagster UI sees a successful run even when 50% of rows failed to match.

### Problem 3: No processing log
There is no record of what was processed, when, or what the outcome was.
The sensor cursor (a list of seen item IDs) lives in Dagster's storage
and gets lost on reset. There's no way to look at a spreadsheet in Girder
and know whether it was successfully processed.

### Problem 4: No ALPSS completeness visibility
The pipeline doesn't track whether ALPSS processing has completed for
matched PDV traces. Operators have no visibility into the processing
pipeline's coverage.

## Target Architecture

```
Assets:
  raw_experiment_log             ◄── unchanged
       │
  validated_rows                 ◄── unchanged
       │  ✓ igsn_validity_rate check (WARN if <80%)
       │
  pdv_trace_inventory            ◄── NEW: /aimdl/datafiles?dataType=pdv_trace
       │  ✓ zero_inventory check (ERROR if empty)
       │
  pdv_cross_references           ◄── UPDATED: IGSN consistency checking
       │  ✓ pdv_match_rate check (WARN if <50%)
       │  ✓ igsn_consistency check (ERROR on any mismatch)
       │
  enriched_pdv_metadata          ◄── unchanged
       │  ✓ enrichment_success_rate check (WARN if <90%)
       │  ✓ no_write_errors check (ERROR on any exception)
       │
  alpss_results_inventory        ◄── NEW: /aimdl/datafiles?dataType=pdv_alpss_result
       │
  quality_report                 ◄── UPDATED: ALPSS completeness, comprehensive
       │
  processing_manifest            ◄── NEW: writes meta.processing_status to Girder
```

## Critical Prerequisite

The `/aimdl/datafiles` endpoint **requires `meta.igsn` to exist** on items.
Items without `meta.igsn` will not appear in results. Until IGSN metadata is
tagged on all PDV files, the endpoint will return an incomplete inventory.

## Stages

### Stage 1: Add `/aimdl` endpoint helpers to girder_io.py
- Add `fetch_aimdl_datafiles()` and `fetch_all_aimdl_datafiles()` functions
- Add `fetch_aimdl_datatypes()` function
- Add `AIMDL_DATA_TYPES` constants
- Add tests for the helpers (mocked Girder responses)
- **No behavior changes to existing assets**
- **Branch:** `feat/aimdl-helpers`
- **Issue file:** `issues/01-add-aimdl-helpers.md`

### Stage 2: Replace `pdv_inventory` with `pdv_trace_inventory`
- Add new `pdv_trace_inventory` asset using `/aimdl/datafiles?dataType=pdv_trace`
- Update `pdv_cross_references` to use `pdv_trace_inventory` upstream
- Add IGSN consistency checking (cross-check spreadsheet IGSN vs item `meta.igsn`)
- Update tests, remove `PDV_FOLDER_ID`, update `__init__.py`
- **Branch:** `feat/replace-pdv-inventory`
- **Issue file:** `issues/02-replace-pdv-inventory.md`

### Stage 3: Add Dagster asset checks for error surfacing
- Add `@asset_check` functions that produce colored pass/warn/fail in Dagster UI
- Six checks covering IGSN validity, PDV matching, IGSN consistency,
  enrichment success, write errors, and inventory emptiness
- Register checks in `__init__.py` Definitions
- Operators can now see at a glance whether a run had quality issues
- **Branch:** `feat/asset-checks`
- **Issue file:** `issues/05-asset-checks.md`

### Stage 4: Add processing manifest (write-back to Girder)
- New `processing_manifest` asset that writes `meta.processing_status`
  to the source spreadsheet Girder item after processing completes
- Records: timestamp, dagster_run_id, status, row counts, issue summary
- Update sensor to check manifest before triggering reprocessing
- Enables idempotency, audit trail, and cross-system visibility
- **Branch:** `feat/processing-manifest`
- **Issue file:** `issues/06-processing-manifest.md`

### Stage 5: Add ALPSS results inventory for quality reporting
- Add `alpss_results_inventory` asset fetching `pdv_alpss_result` items
- Enhance `quality_report` with ALPSS completeness metrics
- **Branch:** `feat/alpss-results-inventory`
- **Issue file:** `issues/03-alpss-results-inventory.md`

### Stage 6: Optimize the sensor
- Replace recursive folder crawl with sorted recent-items query
- Integrate with processing manifest for smarter reprocessing decisions
- **Branch:** `feat/optimize-sensor`
- **Issue file:** `issues/04-optimize-sensor.md`

## Environment Variable Changes

| Variable | Status | Description |
|----------|--------|-------------|
| `GIRDER_API_URL` | Keep | API URL for data.htmdec.org |
| `GIRDER_API_KEY` | Keep | Authentication |
| `HELIX_FOLDER_ID` | Keep (for sensor) | Folder to watch for new spreadsheets |
| `PDV_FOLDER_ID` | Remove (Stage 2) | No longer needed — replaced by endpoint |
| `PDV_TRACE_DATA_TYPE` | Add (Stage 1) | Default: `pdv_trace` |
| `ALPSS_RESULT_DATA_TYPE` | Add (Stage 1) | Default: `pdv_alpss_result` |

## Files Changed by Stage

| File | S1 | S2 | S3 | S4 | S5 | S6 |
|------|----|----|----|----|----|----|
| `girder_io.py` | ✅ add | | | ✅ add | | ✅ rm |
| `constants.py` | ✅ add | ✅ rm | | | | |
| `assets.py` | | ✅ replace | | ✅ add | ✅ add | |
| `checks.py` | | | ✅ new | | | |
| `matching.py` | | ✅ update | | | | |
| `sensors.py` | | | | ✅ update | | ✅ update |
| `__init__.py` | | ✅ update | ✅ update | ✅ update | ✅ update | |
| `tests/test_girder_io.py` | ✅ new | | | | | |
| `tests/test_checks.py` | | | ✅ new | | | |
| `tests/test_assets.py` | | ✅ update | | ✅ add | ✅ add | |
| `tests/test_matching.py` | | ✅ update | | | | |
| `CLAUDE.md` | | ✅ update | ✅ update | ✅ update | | ✅ update |

## Design Decision: Asset Checks vs. Blocking

The checks are designed in two tiers:

**WARN checks** (yellow in UI, non-blocking):
- `igsn_validity_rate` — Bad IGSNs may be correctable
- `pdv_match_rate` — Low match rate may be expected during initial setup
- `enrichment_success_rate` — Partial success is still useful

**ERROR checks** (red in UI, optionally blocking):
- `zero_inventory` — Empty inventory means the endpoint isn't returning data
- `igsn_consistency` — IGSN mismatch is a data integrity problem
- `no_write_errors` — Girder write failures need immediate attention

ERROR checks can be set to `blocking=True` to prevent downstream assets
from materializing when they fail. Start non-blocking, then tighten once
the pipeline is stable.

## Design Decision: Processing Manifest Location

The manifest is written to Girder as `meta.processing_status` on the
source spreadsheet item. This was chosen over alternatives:

- **Dagster IO manager**: Would store in Dagster's own storage, invisible
  from Girder. Loses the cross-system visibility benefit.
- **Separate Girder item**: Would create a separate "receipt" file.
  More complex, harder to discover.
- **On the source spreadsheet item**: Natural place. Anyone browsing the
  data portal can see if/when processing happened. The sensor can read
  it directly.
