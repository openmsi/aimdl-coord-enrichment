# Issue 2: Extract pure processing functions with unit tests

## Context
This is the second of four PRs refactoring the pipeline to a Dagster asset-based
DAG. This PR extracts the three core processing concerns — IGSN validation, PDV
file matching, and coordinate transformation — into standalone pure functions that
can be independently tested and will become the building blocks of individual
Dagster assets in Issue 3.

## Depends on
Issue 1 (cleanup) must be merged first.

## Tasks

### 1. Create helix_dagster/validation.py — IGSN validation

Extract a pure function from the IGSN validation logic currently inline in
`process_row()`:

```python
from helix_dagster.constants import IGSN_PATTERN

def validate_igsn(sample_id) -> tuple[str | None, dict | None]:
    """Validate a sample identifier against the IGSN pattern.

    Parameters
    ----------
    sample_id : any
        The raw Sample_ID value from the spreadsheet row.

    Returns
    -------
    valid_igsn : str or None
        The matched IGSN string, or None if invalid/missing.
    issue : dict or None
        A structured issue dict if validation failed, or None if valid.
    """
```

The function should handle: None, NaN (float), empty string, non-matching strings,
and valid IGSNs. It should NOT log — logging is the caller's responsibility.

### 2. Create helix_dagster/matching.py — PDV file matching

Extract the PDV cross-reference logic:

```python
def match_pdv_file(pdv_items: list[dict], pdv_filename) -> tuple[dict | None, dict | None]:
    """Match a PDV filename to items in the PDV inventory.

    Parameters
    ----------
    pdv_items : list of dict
        The full PDV inventory (list of Girder item dicts).
    pdv_filename : any
        The PDV_FileName value from the spreadsheet row.

    Returns
    -------
    pdv_item : dict or None
        The matched Girder item, or None if not found/ambiguous.
    issue : dict or None
        A structured issue dict if matching failed, or None if matched.
    """
```

Handle: None, NaN, no matches, exactly one match, multiple matches.

### 3. Create helix_dagster/coordinates.py — Coordinate transformation wrapper

Wrap the coordinate transformation with proper error handling:

```python
def transform_station_to_sample(
    station_x: float | None,
    station_y: float | None,
    instrument: str = "HELIX",
) -> tuple[float | None, float | None]:
    """Transform instrument station coordinates to sample-frame coordinates.

    Returns (None, None) if either input is None or if the transformation fails.
    """
```

This function should catch exceptions from the CoordinateTransformer (e.g.,
missing instrument config) and return (None, None) with a warning rather than
crashing. The module-level `_COORD_TRANSFORMER` initialization should also be
wrapped in a try/except so that the package can be imported even when the YAML
config is missing (for testing, CI, etc.).

### 4. Refactor process_row() to use the new functions

Replace the inline IGSN validation, PDV matching, and coordinate transformation
in `process_row()` with calls to the three new functions. The behavior should be
identical — this is a pure refactor. The function signature and return type of
`process_row()` must not change.

### 5. Write unit tests

Create `tests/test_validation.py`:
- test_valid_igsn: "HTMXYZ00123" → ("HTMXYZ00123", None)
- test_valid_igsn_with_suffix: "HTMXYZ00123-A" → ("HTMXYZ00123-A", None)
- test_invalid_igsn_format: "not-an-igsn" → (None, {"issue": "invalid_format", ...})
- test_missing_igsn_none: None → (None, {"issue": "missing"})
- test_missing_igsn_nan: float("nan") → (None, {"issue": "missing"})
- test_igsn_embedded_in_string: "prefix-HTMXYZ00123-suffix" → ("HTMXYZ00123", None)
  (because IGSN_PATTERN uses .search(), not .fullmatch())

Create `tests/test_matching.py`:
- test_exact_match: one item matches → (item, None)
- test_no_match: zero items match → (None, {"type": "not_found"})
- test_ambiguous_match: two items match → (None, {"type": "ambiguous"})
- test_nan_filename: float("nan") → (None, None) [no issue, just skip]
- test_none_filename: None → (None, None)

Create `tests/test_coordinates.py`:
- test_valid_transform: known station coords → expected sample coords
  (requires COORD_TRANSFORMS_YAML to be set; skip if not available)
- test_none_input: (None, 5.0) → (None, None)
- test_both_none: (None, None) → (None, None)
- test_missing_transformer: when _COORD_TRANSFORMER is None, returns (None, None)

### 6. Verify
- Run `pytest tests/ -v` and confirm all tests pass.
- Run `python -c "from helix_dagster import defs"` to confirm Dagster still loads.
- Commit with message: "refactor: extract IGSN validation, PDV matching, and coordinate
  transformation into standalone functions with unit tests"

## What NOT to change
- Do not change the Dagster job/op/sensor structure yet.
- Do not change process_file() or the sensor.
- The output of process_row() must remain identical.
