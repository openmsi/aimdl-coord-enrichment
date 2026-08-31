# aimdl-coord-enrichment

## Dagster Jobs Overview

This repo runs **two DAGs** in one `Definitions` registry, spread across
**five jobs**. One DAG is spreadsheet-driven (HELIX PDV traces); the other is
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
| HELIX | `pdv_trace` | leaf | `pdv_data` | `process_helix_assets_job` | Spreadsheet row `Flyer_X/Y_Position_Corrected` |
| HELIX | `pdv_alpss_output` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| HELIX | `pdv_alpss_result` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| HELIX | `pdv_alpss_results` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| MAXIMA | `xrd_raw` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrf_raw` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrd_derived` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrd_visualization` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |

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

The `pdv_log` asset partitions on `pdv_experiment_log` via the
`/aimdl/partition` Girder endpoint, and `pdv_data` fetches the PDV trace
inventory via `/aimdl/datafiles`. Both require `meta.igsn` and
`meta.data_type` to be set on items. Until these metadata fields are tagged
on experiment-log and PDV files in Girder, the flow is inert / returns
incomplete results. The `zero_pdv_inventory` asset check will flag an empty
PDV inventory as an ERROR in the Dagster UI.

--

## Job-by-job details

### 1. `process_helix_assets_job` — HELIX spreadsheet flow
- **Trigger:** `helix_experiment_log_discovery_sensor` (polls `/aimdl/partition` for `pdv_experiment_log`, registers partitions, emits one partitioned RunRequest per changed log).
- **Partitioning:** `HELIX_EXPERIMENT_LOG_PARTITIONS` = `DynamicPartitionsDefinition("helix_experiment_log")`, keyed on the AIMD-L logical key `<igsn>//<experiment_date>`.
- **What it does:** Three durable partitioned assets — `pdv_log` (read + normalize + validate the experiment log for the partition), `pdv_data` (fetch the PDV trace inventory, match rows by filename, **write coordinate metadata to `pdv_trace` items**), and `pdv_processing_manifest` (stamp `meta.processing_status` back onto each source log item).
- **Coordinate source:** each spreadsheet row's `Flyer_X/Y_Position_Corrected (mm)` becomes `Station_X/Y`; the row `Timestamp` selects the HELIX transform version.
- **Writing asset:** `pdv_data` → `pdv_trace`.
- **Dry run:** `pdv_data` and `pdv_processing_manifest` take a `dry_run` config (`HelixSpreadsheetConfig`, **default `True`**). When dry, all reads/matching/transforms run but the Girder writes are simulated — checks count the would-be writes so a rehearsal reads as a live run. Set `dry_run: false` on both assets in the launchpad for a live sweep. (Because the default is safe, even an accidentally-enabled sensor run writes nothing.)

### 2. `coord_enrichment_job` — state report (read-only)
- **Trigger:** `coord_enrichment_state_report_schedule` (nightly 03:00 ET, ships STOPPED).
- **Partitioning:** none.
- **What it does:** Inventories all in-scope items, **tags HELIX ALPSS provenance** (`helix_alpss_provenance_tagged` writes `meta.prov.wasDerivedFrom` so ALPSS items point at their parent PDV trace), observes PDV coverage, and emits a status report. Writes provenance links only — **no coordinate writes**.

### 3. `coord_enrichment_maxima_job` — MAXIMA, weekly reconciliation
- **Trigger:** `coord_enrichment_maxima_weekly_schedule` (Sunday 04:00 ET, dry-run, STOPPED).
- **Partitioning:** `MAXIMA_RUN_PARTITIONS` = `DynamicPartitionsDefinition("maxima_run")`, one partition per AIMD-L run (`<igsn>//<experiment_date>`).
- **What it does:** Gap-filling sweep — enumerates all registered run partitions and materializes only those lacking a successful run. **Writes coordinates to every in-scope MAXIMA item the run produced: `xrd_raw`, `xrf_raw`, `xrd_derived`, `xrd_visualization`.**
- **Coordinate source:** `Station_X/Y` parsed from the run's `instructions.txt` scan-point table, selected by the `scan_point_<i>` prefix in each filename; timestamp from `meta.experiment_date`.
- **Writing asset:** `enriched_maxima_run`.

### 4. `coord_enrichment_maxima_partition_job` — MAXIMA, event-driven
- **Trigger:** `maxima_run_discovery_sensor` (polls `/aimdl/partition`, registers new run keys, emits one deduped RunRequest per run; STOPPED).
- **Partitioning / writing asset:** identical selection to job 3. Kept as a separate job purely so sensor-driven runs are filterable from the weekly reconciliation runs in the UI; the partition key — not the job — decides what materializes.

### 5. `coord_enrichment_helix_alpss_job` — HELIX ALPSS derived
- **Trigger:** `coord_enrichment_helix_alpss_weekly_schedule` (Sunday 04:30 ET, STOPPED).
- **Partitioning:** `HELIX_ALPSS_PARTITIONS` = static `["HELIX/pdv_alpss_output", "HELIX/pdv_alpss_result", "HELIX/pdv_alpss_results"]`.
- **What it does:** Ensures ALPSS items are provenance-tagged, then **enriches `pdv_alpss_output`, `pdv_alpss_result`, and `pdv_alpss_results` items** by inheriting `Station_X/Y` from their parent `pdv_trace` (via `prov.wasDerivedFrom`) and re-applying the parent's recorded transform version.
- **Writing asset:** `enriched_helix_alpss`.

> **The run is the unit of work.** One `instructions.txt` per run supplies the
> coordinates for everything the run produced. Storage nests `raw/` inside the
> run folder while lineage runs the other way — the derived products are made
> *from* the raw measurements — but neither shape is a partition boundary, so
> they enrich together. `xrd_derived` no longer inherits from its parent
> `master.h5`: the parent read the same `instructions.txt`, so inheritance
> recorded a path rather than an origin. Any `prov.wasDerivedFrom` link is still
> recorded in `coord_provenance` as a cross-reference.

> All `coord_enrichment` schedules and both `coord_enrichment` sensors ship
> **STOPPED** and dry-run by default; an operator opts in via the Dagster UI.

--

### Assets

The `helix_spreadsheet` flow is three partitioned assets (assets model
durable external-state transitions; pure computation lives in
`spreadsheet.py` helpers):

| Asset | Description |
|---|---|
| `pdv_log` | Reads the experiment log(s) for the partition, applies column renaming, and validates IGSN identifiers per row |
| `pdv_data` | Fetches the PDV trace inventory via `/aimdl/datafiles`, matches PDV filenames to rows (checking IGSN consistency), and writes coordinate + provenance metadata to matched `pdv_trace` items |
| `pdv_processing_manifest` | Writes a structured processing record to each source experiment-log item |

### Asset checks

Data quality is surfaced via Dagster asset checks that display colored
pass/warn/fail indicators in the Dagster UI:

| Check | Asset | Severity | Triggers when |
|---|---|---|---|
| `igsn_validity_rate` | `pdv_log` | WARN | <80% of rows have valid IGSNs |
| `zero_pdv_inventory` | `pdv_data` | ERROR | PDV trace inventory returned 0 items |
| `pdv_match_rate` | `pdv_data` | WARN | <50% of PDV filenames matched |
| `igsn_consistency` | `pdv_data` | ERROR | IGSN mismatch between spreadsheet and Girder |
| `enrichment_success_rate` | `pdv_data` | WARN | <90% of matched items enriched |
| `coord_transform_check` | `pdv_data` | WARN | Any coordinate transform failures |
| `manifest_written` | `pdv_processing_manifest` | ERROR | Processing manifest write failed for any source item |

### Processing manifest

After each run, the `pdv_processing_manifest` asset writes
`meta.processing_status` to each source experiment-log Girder item:

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

The `helix_experiment_log_discovery_sensor` polls `/aimdl/partition` for
`pdv_experiment_log` items, registers each AIMD-L key
(`<igsn>//<experiment_date>`) on the `helix_experiment_log` dynamic
partition dimension, and emits one partitioned RunRequest per key. The
run_key embeds the partition's content hash, so unchanged logs are
suppressed and a changed log re-triggers. Ships **STOPPED**.

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
