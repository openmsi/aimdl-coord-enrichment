# Phase 1, Step 1 — Thread timestamp through `coordinates.py`

## Context

Branch: `refactor/asset-dag`
Repo: `/Users/elbert/Documents/GitHub/openmsi/helix_metadata_extraction_dagster`
Python 3.12, `.venv` in repo root. Activate before running anything.

Read `.claude/CLAUDE.md` first — apply its ground rules (audit before
edits, surgical changes, explicit assumptions, verifiable success
criteria).

## Goal

Update `aimdl_coord_enrichment/coordinates.py` so that `transform_station_to_sample()`:

1. Accepts an optional timezone-aware `datetime` as `timestamp=`.
2. Returns a 3-tuple `(sample_x, sample_y, transform_name)` where
   `transform_name` is the resolved `InstrumentTransform.name` (e.g.
   `"HELIX/v2"`) when the transform succeeds, else `None`.
3. Raises `ValueError` with a clear message if a naive (tz-unaware)
   `datetime` is passed.

Update the one existing caller in `aimdl_coord_enrichment/assets.py`
(`enriched_pdv_metadata`) minimally — just unpack the 3-tuple and
ignore the third element with `_`. Expanding that caller to actually
use the version label and pass a timestamp is the job of Step 3, not
this step.

Update `tests/test_coordinates.py` to cover the new behavior.

Do NOT yet parse timestamps from spreadsheet rows. Do NOT yet build
provenance payloads. Do NOT modify `checks.py`. Those are later steps.

## Audit phase (report findings BEFORE editing)

1. Read `aimdl_coord_enrichment/coordinates.py` in full.
2. Read `tests/test_coordinates.py` in full.
3. Grep for every call site of `transform_station_to_sample` across
   the repository (not only `aimdl_coord_enrichment/`):
   ```bash
   grep -rn transform_station_to_sample . --include='*.py' | grep -v .venv
   ```
4. Read each call site and capture how the return value is currently
   unpacked.
5. Confirm `coordinate-transformer` is installed and has the versioned
   API by running:
   ```bash
   .venv/bin/python -c "import inspect, coordinate_transformer as ct; \
     print(inspect.signature(ct.CoordinateTransformer.transform)); \
     print(inspect.signature(ct.CoordinateTransformer.get_transform))"
   ```
   Both signatures must include `timestamp` as a keyword argument.
6. Read the HELIX entries in
   `../../htmdec/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml`
   (or wherever `COORD_TRANSFORMS_YAML` points in your env). Confirm
   HELIX has both `v1` and `v2` with a `valid_from`/`valid_until`
   boundary — the tests below depend on that.

Report what you found, then wait for confirmation before editing.

## Edits

### `aimdl_coord_enrichment/coordinates.py`

- Add `from datetime import datetime` import.
- Change the signature to:
  ```python
  def transform_station_to_sample(
      station_x,
      station_y,
      instrument: str = "HELIX",
      timestamp: datetime | None = None,
  ) -> tuple[float | None, float | None, str | None]:
  ```
- Add an explicit ValueError if `timestamp is not None and
  timestamp.tzinfo is None`. Wording:
  `"timestamp must be timezone-aware; got naive datetime <...>"`.
- When `station_x` or `station_y` is None, return `(None, None, None)`.
- When `_COORD_TRANSFORMER` is None, return `(None, None, None)`.
- Implementation: get the `InstrumentTransform` via
  `_COORD_TRANSFORMER.get_transform(instrument, timestamp=timestamp)`,
  then call `.transform_point(x, y)` on it. Record `.name` as the
  transform label and return it as the third element.
- On any exception (except ValueError from the naive-timestamp check
  above, which should propagate): log at WARNING with `exc_info=True`
  and return `(None, None, None)`.
- Update the module docstring if there is one (there isn't currently —
  don't add one unless you need to).

### `aimdl_coord_enrichment/assets.py`

Find the one call to `transform_station_to_sample` inside the
`enriched_pdv_metadata` asset. Change:
```python
sample_x, sample_y = transform_station_to_sample(station_x, station_y)
```
to:
```python
sample_x, sample_y, _transform_name = transform_station_to_sample(
    station_x, station_y
)
```
No other changes to `assets.py` in this step. Do NOT pass a timestamp
yet. Do NOT use `_transform_name`. A TODO comment that mentions Step 3
is fine but not required.

### `tests/test_coordinates.py`

Update the existing tests to unpack the 3-tuple (they currently use
2-tuple unpacking). Add four new tests:

1. `test_naive_timestamp_raises` — passes a naive datetime, expects
   `ValueError`.
2. `test_historical_timestamp_selects_v1` — passes a tz-aware datetime
   before HELIX v2's `valid_from` (e.g., `2025-06-01T00:00:00+00:00`)
   and asserts `transform_name` contains `"v1"`.
3. `test_current_timestamp_selects_v2` — passes a tz-aware datetime
   after v2's `valid_from` (e.g., `datetime.now(timezone.utc)`) and
   asserts `transform_name` contains `"v2"`.
4. `test_no_timestamp_returns_current_version` — calls with no
   `timestamp` argument and asserts `transform_name` is non-None and
   contains `"v2"` (the current open-ended HELIX version).

Skip version-selection tests (2, 3, 4) with `pytest.skip` if
`_COORD_TRANSFORMER is None`, following the existing pattern.

## What NOT to modify

- `aimdl_coord_enrichment/checks.py`
- `aimdl_coord_enrichment/assets.py` — except the one 3-tuple unpack line
- `aimdl_coord_enrichment/validation.py`, `matching.py`, `girder_io.py`,
  `resources.py`, `sensors.py`, `constants.py`, `__init__.py`
- The YAML file
- Any file outside `aimdl_coord_enrichment/` or `tests/`

## Success criteria

Run, and confirm each is true:

```bash
source .venv/bin/activate
pytest tests/test_coordinates.py -v
pytest tests/ -v    # whole suite still green
.venv/bin/python -c "from aimdl_coord_enrichment.coordinates import transform_station_to_sample; \
  print(transform_station_to_sample(10.0, 10.0))"
# Expect a 3-tuple like (float, float, 'HELIX/v2')
```

No test in `tests/` should fail. No import warnings beyond what was
already there.

## Commit

One commit on `refactor/asset-dag`:

```
coordinates: accept timestamp, return transform name

Thread an optional timezone-aware timestamp through
transform_station_to_sample so the caller can select the correct
coordinate transform version for a historical shot. Return the
resolved transform name as the third tuple element.

Preparatory for Phase 1 Step 3, which will parse the spreadsheet
Timestamp column and feed it here.
```
