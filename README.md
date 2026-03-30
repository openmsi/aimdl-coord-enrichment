# helix_metadata_extraction_dagster

A Dagster pipeline that processes laser shock experiment log spreadsheets from the
HELIX station in the AIMD-L programmable cloud laboratory at Johns Hopkins. It
extracts metadata, validates sample identifiers (IGSNs), cross-references PDV
(Photon Doppler Velocimetry) data files in a Girder data management server,
transforms instrument coordinates into sample-frame coordinates, and writes
enriched metadata back to Girder items.

## Architecture

The pipeline is structured as a six-asset Dagster DAG:

```
raw_experiment_log     pdv_inventory
        │                    │
        ▼                    │
  validated_rows             │
        │                    │
        ▼                    ▼
  pdv_cross_references ◄─────┘
        │
        ▼
  enriched_pdv_metadata
        │
        ▼
    quality_report
```

| Asset | Description |
|---|---|
| `raw_experiment_log` | Downloads a spreadsheet from Girder and applies column renaming |
| `pdv_inventory` | Fetches all PDV items from Girder (independent, can be materialized separately) |
| `validated_rows` | Validates IGSN identifiers on each row (pure transformation) |
| `pdv_cross_references` | Matches PDV filenames to inventory items (pure matching) |
| `enriched_pdv_metadata` | Writes coordinate and IGSN metadata to Girder PDV items |
| `quality_report` | Aggregates all issues into a structured report |

A sensor (`helix_folder_sensor`) polls the HELIX Girder folder for new spreadsheets
and triggers the asset job automatically.

## Setup

### Prerequisites

- Python >= 3.9
- Access to the HTMDEC Girder server (data.htmdec.org)
- The `coordinate-transformer` package (aimdl_coordinate_systems)

### Installation

```bash
pip install -e .

# For development (includes pytest and dagster test utilities):
pip install -e ".[dev]"
```

### Environment variables

| Variable | Description | Required |
|---|---|---|
| `GIRDER_API_URL` | Girder REST API URL (e.g. `https://data.htmdec.org/api/v1`) | Yes |
| `GIRDER_TOKEN` | Girder API key for authentication | Yes |
| `HELIX_FOLDER_ID` | Girder folder ID containing experiment log spreadsheets | Yes |
| `PDV_FOLDER_ID` | Girder folder ID containing PDV data files | Yes |
| `COORD_TRANSFORMS_YAML` | Path to coordinate transform YAML config | No (defaults to `instrument_coordinate_transforms.yaml`) |

### Running

```bash
dagster dev
```

This starts the Dagster webserver. The asset DAG will be visible in the UI, and
the sensor will automatically trigger runs when new spreadsheets appear in the
HELIX folder.

## Development

### Running tests

```bash
pytest tests/ -v
```

### Adding a new validation check

Add your validation logic as a pure function in `helix_dagster/validation.py`,
then call it from the `validated_rows` asset in `helix_dagster/assets.py`.
Write unit tests in `tests/test_validation.py`.

### Adding a new metadata field to the enrichment step

1. Add the field extraction logic in the `enriched_pdv_metadata` asset
   in `helix_dagster/assets.py`
2. Include the new field in the metadata dict passed to
   `client.addMetadataToItem()`
3. If the field comes from a new spreadsheet column, add the column mapping
   to `COLUMN_MAP` in `helix_dagster/constants.py`

## Project structure

```
helix_dagster/
├── __init__.py          # Dagster Definitions entry point
├── assets.py            # Six-asset DAG definitions
├── constants.py         # Column mappings, folder IDs, IGSN pattern
├── coordinates.py       # Coordinate transformation wrapper
├── girder_io.py         # Girder download/listing utilities
├── matching.py          # PDV file matching logic
├── resources.py         # GirderResource (Dagster resource)
├── sensors.py           # Folder sensor for new spreadsheets
└── validation.py        # IGSN validation logic
tests/
├── conftest.py          # Shared fixtures
├── test_assets.py       # Asset integration tests
├── test_coordinates.py  # Coordinate transform tests
├── test_matching.py     # PDV matching tests
├── test_processing.py   # Smoke tests for core modules
└── test_validation.py   # IGSN validation tests
```
