# CLAUDE.md — helix_metadata_extraction_dagster

## What this project does
This is a Dagster pipeline that processes laser shock experiment log spreadsheets
from the HELIX station in the AIMD-L programmable cloud laboratory at Johns Hopkins.
It extracts metadata, validates sample identifiers (IGSNs), cross-references PDV
(Photon Doppler Velocimetry) data files in a Girder data management server, transforms
instrument coordinates into sample-frame coordinates, and writes enriched metadata
back to Girder items.

## Architecture
- **Girder** is the data management platform (REST API at data.htmdec.org)
- **Dagster** orchestrates the metadata extraction pipeline
- **coordinate-transformer** (`aimdl_coordinate_systems` package) handles
  instrument-to-sample coordinate transformations
- **IGSN** persistent identifiers link measurements to physical samples

## Key conventions
- Run `dagster dev` from the repo root to start the Dagster webserver
- Environment variables: GIRDER_API_URL, GIRDER_TOKEN, HELIX_FOLDER_ID, PDV_TRACE_DATA_TYPE
- The coordinate transform YAML path should be set via COORD_TRANSFORMS_YAML env var
- Use `pytest` for testing; tests go in `tests/`
- Python ≥3.9

## Workflow
1. Sensor polls a Girder folder for new experiment log spreadsheets (CSV/XLSX)
2. Pipeline downloads, parses, validates IGSNs
3. PDV trace inventory fetched via /aimdl/datafiles?dataType=pdv_trace
   (indexed MongoDB query, no directory crawling)
4. Cross-references PDV filenames and checks IGSN consistency
5. Writes enriched metadata to matched Girder items
6. Quality issues reported as structured metadata on the Dagster run

## Important domain context
- Each spreadsheet row represents one laser shock experiment (one "shot")
- Sample_ID / sample_IGSN is the persistent identifier linking to physical samples
- Flyer_X/Y positions are in the HELIX instrument coordinate frame (mm)
- The coordinate transformer converts these to sample-frame coordinates
- PDV files contain raw oscilloscope traces that ALPSS processes for velocity extraction
