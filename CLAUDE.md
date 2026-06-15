# CLAUDE.md — aimdl-coord-enrichment

## What this project does
This is a Dagster pipeline that processes laser shock experiment log spreadsheets
from the HELIX station in the AIMD-L programmable cloud laboratory at Johns Hopkins.
It extracts metadata, validates sample identifiers (IGSNs), cross-references PDV
(Photon Doppler Velocimetry) data files in a Girder data management server, transforms
instrument coordinates into sample-frame coordinates, and writes enriched metadata
back to Girder items.

## Project conventions

Short-form rules that bind both human and agentic contributors
live under `docs/developer_notes/`. The current set:

- `docs/developer_notes/annotations.md` — **do not** use
  `from __future__ import annotations` in Dagster-adjacent
  modules (assets, sensors, resources, `Config` subclasses, or
  the Definitions registry). A CI test enforces this.

Any new repo-wide convention worth enforcing should go in that
directory, be named by the rule it codifies, and be referenced
from this list.

## Operations

Scheduled and manual sweeps of the coord_enrichment DAG live in:

- `docs/runbooks/readiness_dry_run.md` — read-only
  production-readiness dry run (GO/NO-GO rubric); run before a
  live sweep. Driven by `operations/dry_run_readiness.py` or the
  Dagster UI.
- `docs/runbooks/coord_enrichment_production_sweep.md` — the
  operator-facing runbook for live sweeps.
- `docs/runbooks/first_sweep_expected_values.md` — reference
  values for what each asset check should report on a sweep
  against the test collection.
- `operations/run_live_sweep.sh` — one-shot script invoked by
  the runbook.

Schedules in `aimdl_coord_enrichment/schedules.py` all ship STOPPED.
An operator opts in via the Dagster UI.

## Architecture
- **Girder** is the data management platform (REST API at data.htmdec.org)
- **Dagster** orchestrates the metadata extraction pipeline
- **coordinate-transformer** (`aimdl_coordinate_systems` package) handles
  instrument-to-sample coordinate transformations with timestamp-based
  version selection (v0.3.0+). `coordinates.py` passes the shot timestamp,
  so historical shots resolve to the version valid at the time. HELIX `v1`
  applies before the 2026-04-01 recalibration and `v2` (identity — the
  station frame was realigned to the sample frame) on/after it.
- **IGSN** persistent identifiers link measurements to physical samples
- **`/aimdl/datafiles`** Girder endpoint provides indexed queries by
  `meta.data_type` (no folder crawling)

## Development environment
- **Python ≥3.12** — use the `.venv` virtual environment in the repo root
- Activate with: `source .venv/bin/activate`
- Install with: `pip install -e ".[dev]"`
- **Do NOT use the conda `short_course` environment** (Python 3.9) — it lacks
  support for modern type syntax and will cause import/test failures
- Run tests with: `pytest tests/ -v`
- Run Dagster dev server with: `dagster dev`

## Key conventions
- Environment variables: `GIRDER_API_URL`, `GIRDER_API_KEY`, `HELIX_FOLDER_ID`,
  `PDV_TRACE_DATA_TYPE`, `ALPSS_RESULT_DATA_TYPE`, `COORD_TRANSFORMS_YAML`
- `PDV_FOLDER_ID` is no longer used — replaced by the `/aimdl/datafiles` endpoint
- Use `pytest` for testing; tests go in `tests/`

## Workflow
1. Sensor polls a Girder folder for recent experiment log spreadsheets (CSV/XLSX)
   using a sorted recent-items query (not recursive folder crawl). Spreadsheets
   already processed cleanly (per `meta.processing_status`) are skipped.
2. Pipeline downloads, parses, validates IGSNs
3. PDV trace inventory fetched via `/aimdl/datafiles?dataType=pdv_trace`
   (indexed MongoDB query, no directory crawling)
4. Cross-references PDV filenames and checks IGSN consistency between
   spreadsheet rows and matched Girder items
5. Writes enriched coordinate metadata to matched Girder items
6. ALPSS results inventory fetched via `/aimdl/datafiles?dataType=pdv_alpss_result`
   for completeness reporting
7. Asset checks produce colored pass/warn/fail indicators in the Dagster UI
8. Processing manifest writes `meta.processing_status` to the source Girder item

## Important domain context
- Each spreadsheet row represents one laser shock experiment (one "shot")
- Sample_ID / sample_IGSN is the persistent identifier linking to physical samples
- Flyer_X/Y positions are in the HELIX instrument coordinate frame (mm)
- The coordinate transformer converts these to sample-frame coordinates
- PDV files contain raw oscilloscope traces that ALPSS processes for velocity extraction

## Prerequisite for full operation
The `/aimdl/datafiles` endpoint requires `meta.igsn` and `meta.data_type` to
be set on Girder items. Until these are tagged on all PDV files, the
`pdv_trace_inventory` asset will return incomplete results and the
`zero_inventory` asset check will flag this as an ERROR.
