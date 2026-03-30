# Issue 4: Remove legacy code, add documentation, finalize

## Context
This is the final PR in the refactor series. It removes the deprecated legacy
job/op code, updates the README with architecture documentation, and adds
any remaining test coverage.

## Depends on
Issues 1, 2, and 3 must be merged first. Issue 3's asset-based pipeline should
be validated in at least one real run against the Girder server before merging
this PR.

## Tasks

### 1. Remove legacy code
- Delete helix_dagster/jobs.py (the old single-op job)
- Remove the process_file() and process_row() functions from processing.py.
  Keep the utility functions that are still used by the new assets:
  list_all_spreadsheet_items(), download_and_read(), _fetch_all_pdv_items(),
  _nan_to_none(). Move these to a helix_dagster/girder_io.py module if it
  improves clarity.
- Remove any imports of the deleted code from __init__.py.
- Verify the old `_legacy_process_helix_file_job` reference is fully removed.

### 2. Update pyproject.toml
- Add pytest to dev dependencies:
  ```toml
  [project.optional-dependencies]
  dev = ["pytest", "dagster[test]"]
  ```
- Verify the [tool.dagster] module_name is still correct.

### 3. Write a proper README.md

Replace the one-line README with documentation covering:

**Overview**: One paragraph explaining what the pipeline does (extract metadata
from HELIX laser shock experiment logs, validate IGSNs, cross-reference PDV data,
transform coordinates, write enriched metadata to Girder).

**Architecture**: The six-asset DAG diagram (can be ASCII art):
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

**Setup**: Environment variables, installation steps, how to run `dagster dev`.

**Configuration**: COORD_TRANSFORMS_YAML, HELIX_FOLDER_ID, PDV_FOLDER_ID, etc.

**Development**: How to run tests, how to add a new validation check, how to
add a new metadata field to the enrichment step.

### 4. Add a Dagster repository screenshot to docs/
- Create a docs/ directory.
- Add a screenshot of the Dagster UI showing the asset DAG (captured during
  Issue 3 verification).
- Reference it from the README.

### 5. Final test pass

Run the full test suite and verify:
- `pytest tests/ -v` — all unit and integration tests pass
- `dagster dev` — webserver starts, DAG renders correctly
- `python -c "from helix_dagster import defs; print(len(list(defs.get_all_job_defs())))"` —
  prints the expected number of jobs

### 6. Verify no dangling imports
Run:
```bash
python -c "
from helix_dagster import defs
from helix_dagster.validation import validate_igsn
from helix_dagster.matching import match_pdv_file
from helix_dagster.coordinates import transform_station_to_sample
from helix_dagster.assets import (
    raw_experiment_log, pdv_inventory, validated_rows,
    pdv_cross_references, enriched_pdv_metadata, quality_report
)
print('All imports OK')
"
```

Commit with message: "chore: remove legacy job/op code, add README and documentation"
