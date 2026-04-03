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
- Environment variables: GIRDER_API_URL, GIRDER_TOKEN, HELIX_FOLDER_ID, PDV_FOLDER_ID
- The coordinate transform YAML path should be set via COORD_TRANSFORMS_YAML env var
- Use `pytest` for testing; tests go in `tests/`
- Python ≥3.9

## Workflow
1. Sensor polls a Girder folder for recent experiment log spreadsheets (CSV/XLSX)
   using a sorted recent-items query (not recursive folder crawl). Spreadsheets
   already processed cleanly (per `meta.processing_status`) are skipped.
2. Pipeline downloads, parses, validates IGSNs, cross-references PDV files,
   transforms coordinates, writes enriched metadata to Girder
3. Quality issues (missing IGSNs, unmatched PDV files, IGSN mismatches) are
   reported as structured metadata on the Dagster run

## Important domain context
- Each spreadsheet row represents one laser shock experiment (one "shot")
- Sample_ID / sample_IGSN is the persistent identifier linking to physical samples
- Flyer_X/Y positions are in the HELIX instrument coordinate frame (mm)
- The coordinate transformer converts these to sample-frame coordinates
- PDV files contain raw oscilloscope traces that ALPSS processes for velocity extraction
