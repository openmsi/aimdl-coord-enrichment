# aimdl-coord-enrichment

A [Dagster](https://dagster.io) pipeline that processes laser shock experiment log
spreadsheets from the HELIX station in the
[AIMD-L](https://hemi.jhu.edu/aimd-l/) programmable cloud laboratory at
Johns Hopkins University. It extracts metadata, validates sample identifiers
(IGSNs), cross-references PDV (Photon Doppler Velocimetry) data files stored in
a [Girder](https://girder.readthedocs.io/) data management server, transforms
instrument coordinates into sample-frame coordinates, and writes enriched
metadata back to Girder items.

> **Note:** The `coordinate-transformer` package is available on
> [PyPI](https://pypi.org/project/coordinate-transformer/). For development
> against the latest changes, install from a local clone of
> [`aimdl_coordinate_systems`](https://github.com/htmdec/aimdl_coordinate_systems)
> with `pip install -e /path/to/aimdl_coordinate_systems`.

## Architecture

The pipeline is structured as an eight-asset Dagster DAG with six asset checks
and a processing manifest that writes back to Girder:

```
raw_experiment_log   pdv_trace_inventory   alpss_results_inventory
        │                    │                    │
        ▼                    │                    │
  validated_rows             │                    │
   ✓ igsn_validity_rate      │                    │
        │                    │                    │
        ▼                    ▼                    │
  pdv_cross_references ◄─────┘                    │
   ✓ pdv_match_rate                               │
   ✓ igsn_consistency                             │
        │                                         │
        ▼                                         │
  enriched_pdv_metadata                           │
   ✓ enrichment_success_rate                      │
   ✓ coord_transform_check                        │
        │                                         │
        ▼                                         ▼
    quality_report ◄──────────────────────────────┘
        │
        ▼
  processing_manifest  ──► writes meta.processing_status to Girder
```

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
