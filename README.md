# aimdl-coord-enrichment

## Dagster Jobs Overview

This repo runs **two DAGs** in one `Definitions` registry, spread across
**six jobs**. One DAG is spreadsheet-driven (HELIX PDV traces); the other is
the partitioned `coord_enrichment` DAG that fans out across HELIX ALPSS and
MAXIMA data types. Every coordinate-writing asset writes the same core fields
to Girder items: `Station_X`, `Station_Y`, `Sample_X`, `Sample_Y`, and a
`coord_provenance` dict.

The transform itself is `Station → Sample` via the `coordinate-transformer`
package, version-selected by shot timestamp. Leaf assets compute `Station_X/Y`
from a primary source (spreadsheet row or instructions file); derived assets
**inherit** `Station_X/Y` from a parent item and re-apply the parent's recorded
transform version.

--
## Coordinate-supply summary

Which job/asset supplies `Station_X/Y` + `Sample_X/Y` + `coord_provenance` to
each HELIX and MAXIMA file type, and where those coordinates come from:

| Instrument | `data_type` (Girder) | Role | Writing asset | Job | Coordinate source |
|---|---|---|---|---|---|
| HELIX | `pdv_trace` | leaf | `enriched_pdv_metadata` | `process_helix_assets_job` | Spreadsheet row `Flyer_X/Y_Position_Corrected` |
| HELIX | `pdv_alpss_output` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| HELIX | `pdv_alpss_result` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| HELIX | `pdv_alpss_results` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| MAXIMA | `xrd_raw` | leaf | `enriched_maxima_raw` | `coord_enrichment_maxima_raw_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrf_raw` | leaf | `enriched_maxima_raw` | `coord_enrichment_maxima_raw_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrd_derived` | derived | `enriched_maxima_derived` | `coord_enrichment_maxima_derived_job` | Inherited from parent `xrd_raw` |

**Provenance-only (no coordinates):** `helix_alpss_provenance_tagged` writes
`meta.prov.wasDerivedFrom` on the three HELIX ALPSS types so
`enriched_helix_alpss` can find each item's parent `pdv_trace`. It runs in both
`coord_enrichment_job` and `coord_enrichment_helix_alpss_job`.

--

## Quick Start

### Installation

```bash
# Create a virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install the package
pip install -e .

# For development (includes pytest and dagster test utilities):
pip install -e ".[dev]"
```

### Environment variables

| Variable | Description | Required |
|---|---|---|
| `GIRDER_API_URL` | Girder REST API URL (e.g. `https://data.htmdec.org/api/v1`) | Yes |
| `GIRDER_API_KEY` | Girder API key for authentication | Yes |
| `HELIX_FOLDER_ID` | Girder folder ID containing experiment log spreadsheets | Yes |
| `PDV_TRACE_DATA_TYPE` | `meta.data_type` for PDV traces (default: `pdv_trace`) | No |
| `ALPSS_RESULT_DATA_TYPE` | `meta.data_type` for ALPSS results (default: `pdv_alpss_result`) | No |
| `COORD_TRANSFORMS_YAML` | Path to coordinate transform YAML config | No |

### Running

```bash
dagster dev
```

This starts the Dagster webserver. The asset DAG and asset checks will be
visible in the UI, and the sensor will automatically trigger runs when new
spreadsheets appear in the HELIX folder.

### Prerequisite: IGSN tagging

The `pdv_trace_inventory` and `alpss_results_inventory` assets use the
`/aimdl/datafiles` Girder endpoint, which requires `meta.igsn` and
`meta.data_type` to be set on items. Until these metadata fields are tagged
on PDV files in Girder, the inventories will return incomplete results.
The `zero_inventory` asset check will flag this as an ERROR in the Dagster UI.

--

## Job-by-job details

### 1. `process_helix_assets_job` — HELIX spreadsheet DAG
- **Trigger:** `helix_folder_sensor` (polls the HELIX folder for new experiment-log spreadsheets).
- **Partitioning:** none — one run per spreadsheet item.
- **What it does:** Downloads a HELIX experiment-log spreadsheet, validates IGSNs, fetches the PDV trace inventory, matches PDV files to spreadsheet rows by filename, and **writes coordinate metadata to `pdv_trace` items**. Aggregates a quality report and stamps `meta.processing_status` back onto the source spreadsheet item.
- **Coordinate source:** each spreadsheet row's `Flyer_X/Y_Position_Corrected (mm)` becomes `Station_X/Y`; the row `Timestamp` selects the HELIX transform version.
- **Writing asset:** `enriched_pdv_metadata` → `pdv_trace`.

### 2. `coord_enrichment_job` — state report (read-only)
- **Trigger:** `coord_enrichment_state_report_schedule` (nightly 03:00 ET, ships STOPPED).
- **Partitioning:** none.
- **What it does:** Inventories all in-scope items, **tags HELIX ALPSS provenance** (`helix_alpss_provenance_tagged` writes `meta.prov.wasDerivedFrom` so ALPSS items point at their parent PDV trace), observes PDV coverage, and emits a status report. Writes provenance links only — **no coordinate writes**.

### 3. `coord_enrichment_maxima_raw_job` — MAXIMA raw, weekly reconciliation
- **Trigger:** `coord_enrichment_maxima_raw_weekly_schedule` (Sunday 04:00 ET, dry-run, STOPPED).
- **Partitioning:** `MAXIMA_RAW_PARTITIONS` = `MultiPartitionsDefinition({data_type: [xrd_raw, xrf_raw], run: dynamic "maxima_raw_run"})`.
- **What it does:** Gap-filling sweep — enumerates all registered `(data_type, run)` partitions and materializes only those lacking a successful run. **Writes coordinates to `xrd_raw` and `xrf_raw` items.**
- **Coordinate source:** `Station_X/Y` parsed from the MAXIMA `instructions.txt` scan-point table for the run; timestamp from `meta.experiment_date`.
- **Writing asset:** `enriched_maxima_raw`.

### 4. `coord_enrichment_maxima_raw_partition_job` — MAXIMA raw, event-driven
- **Trigger:** `maxima_raw_discovery_sensor` (polls `/aimdl/partition`, registers new run keys, emits deduped single-partition RunRequests; STOPPED).
- **Partitioning / writing asset:** identical selection to job 3 (`enriched_maxima_raw` → `xrd_raw`, `xrf_raw`). Kept as a separate job purely so sensor-driven runs are filterable from the weekly reconciliation runs in the UI; the partition key — not the job — decides what materializes.

### 5. `coord_enrichment_helix_alpss_job` — HELIX ALPSS derived
- **Trigger:** `coord_enrichment_helix_alpss_weekly_schedule` (Sunday 04:30 ET, STOPPED).
- **Partitioning:** `HELIX_ALPSS_PARTITIONS` = static `["HELIX/pdv_alpss_output", "HELIX/pdv_alpss_result", "HELIX/pdv_alpss_results"]`.
- **What it does:** Ensures ALPSS items are provenance-tagged, then **enriches `pdv_alpss_output`, `pdv_alpss_result`, and `pdv_alpss_results` items** by inheriting `Station_X/Y` from their parent `pdv_trace` (via `prov.wasDerivedFrom`) and re-applying the parent's recorded transform version.
- **Writing asset:** `enriched_helix_alpss`.

### 6. `coord_enrichment_maxima_derived_job` — MAXIMA derived
- **Trigger:** `coord_enrichment_maxima_derived_weekly_schedule` (Sunday 04:30 ET, STOPPED).
- **Partitioning:** `MAXIMA_DERIVED_PARTITIONS` = static `["MAXIMA/xrd_derived"]`.
- **What it does:** **Enriches `xrd_derived` items** by inheriting `Station_X/Y` from the parent `xrd_raw` master.h5 (linked via `prov.wasDerivedFrom`, written upstream by the `amdee_xrd` Girder plugin) and re-applying the parent's recorded transform version.
- **Writing asset:** `enriched_maxima_derived`.

> All `coord_enrichment` schedules and both `coord_enrichment` sensors ship
> **STOPPED** and dry-run by default; an operator opts in via the Dagster UI.

--

### Assets

| Asset | Description |
|---|---|
| `raw_experiment_log` | Downloads a spreadsheet from Girder and applies column renaming |
| `pdv_trace_inventory` | Fetches PDV trace items via `/aimdl/datafiles` (indexed query, no folder crawl) |
| `validated_rows` | Validates IGSN identifiers on each row (pure transformation) |
| `pdv_cross_references` | Matches PDV filenames to inventory items, checks IGSN consistency |
| `enriched_pdv_metadata` | Writes coordinate and flyer position metadata to matched Girder items |
| `alpss_results_inventory` | Fetches ALPSS result items via `/aimdl/datafiles` for completeness reporting |
| `quality_report` | Aggregates all issues and ALPSS completeness metrics |
| `processing_manifest` | Writes a structured processing record to the source Girder item |

### Asset checks

Data quality is surfaced via Dagster asset checks that display colored
pass/warn/fail indicators in the Dagster UI:

| Check | Asset | Severity | Triggers when |
|---|---|---|---|
| `zero_inventory` | `pdv_trace_inventory` | ERROR | Inventory returned 0 items |
| `igsn_validity_rate` | `validated_rows` | WARN | <80% of rows have valid IGSNs |
| `pdv_match_rate` | `pdv_cross_references` | WARN | <50% of PDV filenames matched |
| `igsn_consistency` | `pdv_cross_references` | ERROR | IGSN mismatch between spreadsheet and Girder |
| `enrichment_success_rate` | `enriched_pdv_metadata` | WARN | <90% of matched items enriched |
| `coord_transform_check` | `enriched_pdv_metadata` | WARN | Any coordinate transform failures |

### Processing manifest

After each run, the `processing_manifest` asset writes `meta.processing_status`
to the source spreadsheet's Girder item:

```json
{
  "status": "completed_with_warnings",
  "dagster_run_id": "abc123...",
  "pipeline_version": "0.2.0",
  "total_rows": 45,
  "rows_valid_igsn": 42,
  "rows_matched_pdv": 40,
  "rows_enriched": 38,
  "issues_summary": {
    "igsn_invalid": 1,
    "igsn_missing": 2,
    "pdv_not_found": 5,
    "pdv_ambiguous": 0,
    "igsn_mismatch": 0,
    "write_errors": 0,
    "coord_failures": 0
  }
}
```

This provides an audit trail visible in the Girder web UI and enables the
sensor to skip already-processed spreadsheets.

### Sensor

The `helix_folder_sensor` polls the HELIX Girder folder for new spreadsheets
using a sorted recent-items query (not a recursive folder crawl). Spreadsheets
already processed cleanly (per `meta.processing_status`) are automatically
skipped.

## Setup

### Prerequisites

- Python >= 3.12
- Access to the HTMDEC Girder server (data.htmdec.org)
- The `coordinate-transformer` package (from `aimdl_coordinate_systems`)


## Development

### Running tests

```bash
pytest tests/ -v
```

### Project structure

```
aimdl_coord_enrichment/
├── __init__.py          # Dagster Definitions, version, asset/check registration
├── assets.py            # Eight-asset DAG definitions
├── checks.py            # Six asset checks for data quality surfacing
├── constants.py         # Column mappings, data types, IGSN pattern
├── coordinates.py       # Coordinate transformation wrapper
├── girder_io.py         # Girder download/listing and /aimdl endpoint helpers
├── matching.py          # PDV file matching logic
├── resources.py         # GirderConnection (Dagster resource)
├── sensors.py           # Folder sensor with manifest-aware skip logic
└── validation.py        # IGSN validation logic
tests/
├── conftest.py          # Shared fixtures
├── test_assets.py       # Asset integration tests (including manifest, IGSN mismatch)
├── test_checks.py       # Asset check tests (12 tests)
├── test_coordinates.py  # Coordinate transform tests
├── test_girder_io.py    # /aimdl endpoint helper tests (7 tests)
├── test_matching.py     # PDV matching tests
├── test_processing.py   # Smoke tests for core modules
└── test_validation.py   # IGSN validation tests
```

## License

See [LICENSE](LICENSE).
