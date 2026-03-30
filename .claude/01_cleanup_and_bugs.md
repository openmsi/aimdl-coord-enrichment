# Issue 1: Clean up dead code, fix bugs, externalize configuration

## Context
This is the first of four PRs to refactor the helix_metadata_extraction_dagster
pipeline from a monolithic single-op job to a Dagster asset-based DAG. This PR
makes no structural changes — it only fixes bugs and removes dead code so that
subsequent PRs start from a clean baseline.

## Tasks

### 1. Remove dead code from processing.py
- Delete the `_build_entry()` function — it is defined but never called anywhere.
- In constants.py, remove `FORM_ID` and `FORM_SCHEMA_FIELDS` — they are defined
  but never imported or used by any module. (If these are planned for future use,
  that code should be added in the PR that actually uses them.)

### 2. Fix the hardcoded CoordinateTransformer path in processing.py
The current code has:
```python
_COORD_TRANSFORMER = CoordinateTransformer.from_yaml(
    "/Users/alirachidi/dev/work/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml"
)
```
Replace this with:
```python
import os
_COORD_YAML = os.environ.get(
    "COORD_TRANSFORMS_YAML",
    "instrument_coordinate_transforms.yaml",
)
_COORD_TRANSFORMER = CoordinateTransformer.from_yaml(_COORD_YAML)
```
This allows configuration via environment variable with a sensible default.

### 3. Fix the COLUMN_MAP / Sample_IGSN case inconsistency
In constants.py, `COLUMN_MAP` maps `"Sample_ID"` → `"sample_IGSN"` (lowercase s).
But in processing.py, `process_row()` reads `row.get("Sample_IGSN")` (uppercase S)
directly from the raw DataFrame — never applying the COLUMN_MAP rename.

This currently works by accident because the rename is never applied. But it will
break when we apply COLUMN_MAP as a DataFrame rename in Issue 3.

Fix: Change `COLUMN_MAP` to map `"Sample_ID"` → `"Sample_IGSN"` (uppercase S) to
match the actual usage in process_row(). Also audit all other `row.get()` calls in
process_row() to confirm they use raw spreadsheet column names (since the rename
isn't applied yet). Add a comment noting the convention.

### 4. Add a minimal test infrastructure
- Create `tests/` directory with `conftest.py` and `test_processing.py`.
- In conftest.py, create a pytest fixture that builds a small sample DataFrame
  matching the expected spreadsheet schema (3–5 rows with valid/invalid IGSNs,
  present/missing PDV filenames, valid coordinates).
- In test_processing.py, write one test that imports processing.py successfully
  (verifying the CoordinateTransformer path fix doesn't crash at import time when
  the env var is set to a nonexistent path — it should fail gracefully or be
  wrapped in a try/except at module level).

### 5. Verify
- Run `pytest tests/` and confirm all tests pass.
- Run `python -c "from helix_dagster import defs; print(defs)"` to confirm the
  Dagster definitions still load.
- Commit with message: "chore: remove dead code, fix hardcoded path and column map inconsistency"

## What NOT to change
- Do not change the Dagster job/op/sensor structure in this PR.
- Do not restructure processing.py into separate functions yet.
- Do not add new features.
