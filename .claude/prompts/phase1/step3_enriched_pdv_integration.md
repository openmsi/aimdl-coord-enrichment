# Phase 1, Step 3 — Wire timestamps and provenance into `enriched_pdv_metadata`

## Context

Branch: `refactor/asset-dag`. Steps 1 and 2 are committed.

This is the biggest of the five steps. Read
`.claude/CLAUDE.md` and the existing
`helix_dagster/assets.py::enriched_pdv_metadata` asset before editing.

## Goal

Make `enriched_pdv_metadata`:

1. Parse the spreadsheet `Timestamp` column into a timezone-aware
   datetime for each row.
2. Pass that timestamp to `transform_station_to_sample`.
3. Build a `coord_provenance` payload via
   `helix_dagster.provenance.build_coord_provenance`.
4. Write the provenance alongside the existing Station_X/Y and
   Sample_X/Y fields.
5. Record per-row coord-version resolution so the asset check in
   Step 4 can verify it.

Explicit timezone policy: treat naive timestamps in the spreadsheet
as UTC. Log this as a warning once per asset run (not per row) with
the count of rows affected. A later step, beyond Phase 1, will add
configurable station-local timezone support.

## Audit phase (report BEFORE editing)

1. Read `helix_dagster/assets.py` — specifically
   `enriched_pdv_metadata` and nearby imports.
2. Read the test file(s) that exercise the existing asset. Grep:
   ```bash
   grep -rn enriched_pdv_metadata tests/ --include='*.py'
   ```
   Report whether any test currently calls the function directly.
3. Read `helix_dagster/coordinates.py` (updated in Step 1) and
   `helix_dagster/provenance.py` (added in Step 2).
4. Read `helix_dagster/__init__.py` for the `__version__` string.
5. Find the YAML-path discovery logic in `coordinates.py` — we need
   the same path for the sha256 call. Report what resolved path you
   would use.
6. Sample three rows from `tests/conftest.py::sample_dataframe` — what
   do their `Timestamp` values look like (timezone present or not?).

Report all findings before editing.

## Edits

### `helix_dagster/assets.py::enriched_pdv_metadata`

Additions (do not reflow unrelated code):

1. **New imports at the top of the file** — `pandas as pd` already
   present; add:
   ```python
   from datetime import datetime, timezone
   from helix_dagster.coordinates import _COORD_YAML
   from helix_dagster.provenance import (
       build_coord_provenance,
       compute_yaml_sha256,
       get_transformer_version,
   )
   ```

2. **At the top of `enriched_pdv_metadata`** (once per asset run, not
   per row), compute:
   ```python
   yaml_sha256 = compute_yaml_sha256(_COORD_YAML)
   transformer_version = get_transformer_version()
   naive_timestamps_count = 0
   version_counter = {}  # transform_name → int
   ```
   Handle `FileNotFoundError` from `compute_yaml_sha256` gracefully:
   log an error and set `yaml_sha256 = None`. In that case provenance
   is still written but the sha256 field will be `None` — downstream
   Step 4 will flag this as an asset check failure.

3. **Add a helper inside the asset body** (not at module scope) to
   parse the timestamp:
   ```python
   def _parse_row_timestamp(raw):
       """Return (tz-aware datetime or None, was_naive: bool, origin: str)."""
       if raw is None:
           return None, False, "missing"
       try:
           ts = pd.to_datetime(raw)
       except (ValueError, TypeError):
           return None, False, "unparseable"
       if pd.isna(ts):
           return None, False, "missing"
       # pandas.Timestamp -> python datetime
       py_ts = ts.to_pydatetime()
       if py_ts.tzinfo is None:
           py_ts = py_ts.replace(tzinfo=timezone.utc)
           return py_ts, True, "spreadsheet_timestamp_col_assumed_utc"
       return py_ts, False, "spreadsheet_timestamp_col"
   ```

4. **Inside the `for row_idx, pdv_item in matches.items():` loop**,
   after computing `station_x`, `station_y`:
   ```python
   shot_ts, was_naive, ts_origin = _parse_row_timestamp(row.get("Timestamp"))
   if was_naive:
       naive_timestamps_count += 1
   sample_x, sample_y, transform_name = transform_station_to_sample(
       station_x, station_y, timestamp=shot_ts
   )
   if transform_name is not None:
       version_counter[transform_name] = version_counter.get(transform_name, 0) + 1
   ```
   Replace the old 2-tuple unpack that Step 1 left as `_transform_name`.

5. **Build coord_provenance** — still inside the row loop, after
   transform:
   ```python
   coord_prov = build_coord_provenance(
       instrument="HELIX",
       transform_version=transform_name,
       transform_yaml_sha256=yaml_sha256 or "",
       transformer_version=transformer_version,
       pipeline_version=PIPELINE_VERSION,
       source_timestamp=shot_ts,
       source_timestamp_origin=ts_origin,
       station_coord_source={
           "kind": "helix_experiment_log",
           "spreadsheet_item_id": None,   # ExperimentLogConfig isn't in scope here;
                                          # leave None and note in return dict
           "spreadsheet_row_index": int(row_idx),
           "spreadsheet_pdv_filename": row.get("PDV_FileName"),
       },
       dagster_run_id=getattr(context.run, "run_id", None),
   )
   ```
   To get `spreadsheet_item_id`, add `config: ExperimentLogConfig`
   as a new argument to the asset (Dagster will route it) — mirror
   how `raw_experiment_log` takes its `config`. Then set
   `spreadsheet_item_id=config.item_id` in the
   `station_coord_source` dict.

6. **Add `coord_provenance` to the metadata dict written per item**:
   ```python
   metadata = {
       "Flyer_Row": nan_to_none(row.get("Flyer_Row")),
       "Flyer_Column": nan_to_none(row.get("Flyer_Column")),
       "Station_X": station_x,
       "Station_Y": station_y,
       "Sample_X": sample_x,
       "Sample_Y": sample_y,
       "coord_provenance": coord_prov,
   }
   ```

7. **After the loop**, log the naive-timestamp count once:
   ```python
   if naive_timestamps_count > 0:
       context.log.warning(
           "Spreadsheet contained %d naive Timestamp values; "
           "interpreted as UTC. Set an explicit timezone in the "
           "station export to remove this ambiguity.",
           naive_timestamps_count,
       )
   ```

8. **Extend the return dict** so Step 4's check can consume the data:
   ```python
   return {
       "written_count": written_count,
       "write_errors": write_errors,
       "coord_failures": coord_failures,
       "version_counter": version_counter,
       "naive_timestamps_count": naive_timestamps_count,
       "yaml_sha256": yaml_sha256,
   }
   ```
   Check that `quality_report` and `processing_manifest` still work
   with the extended dict — they read specific keys, so adding new
   keys is fine, but verify.

9. **Add `context.add_output_metadata` entries** for the new counters:
   ```python
   "naive_timestamps": MetadataValue.int(naive_timestamps_count),
   "transform_versions_used": MetadataValue.text(
       ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
       or "none"
   ),
   ```

### `tests/test_assets.py`

Add one new test function (do not alter existing tests):

`test_enriched_pdv_metadata_writes_provenance`

- Build a single-row DataFrame that has `Timestamp`, `PDV_FileName`,
  `Flyer_Row`, `Flyer_Column`, `Flyer_X_Position_Final_mm`,
  `Flyer_Y_Position_Final_mm`, `Sample_IGSN`, `valid_igsn` fields set
  to plausible values. Use a tz-aware timestamp so no warnings fire.
- Mock a Girder resource (`MagicMock`) that captures
  `addMetadataToItem` calls.
- Call `enriched_pdv_metadata` directly via
  `build_asset_context()` with `config=ExperimentLogConfig(item_id=...,
  filename=...)`.
- Assert `addMetadataToItem` was called once; inspect the payload arg
  and assert:
  - `"Station_X" in payload`
  - `"Sample_X" in payload`
  - `"coord_provenance" in payload`
  - `payload["coord_provenance"]["instrument"] == "HELIX"`
  - `payload["coord_provenance"]["transform_version"]` is not None
  - `payload["coord_provenance"]["station_coord_source"]["kind"] == "helix_experiment_log"`

Skip the whole test with `pytest.skip` if `_COORD_TRANSFORMER is None`.

## What NOT to modify

- `helix_dagster/coordinates.py`, `provenance.py`, `checks.py`
- `helix_dagster/validation.py`, `matching.py`, `girder_io.py`,
  `resources.py`, `sensors.py`, `constants.py`, `__init__.py`
- Other asset functions in `assets.py` except where a signature change
  on `enriched_pdv_metadata` forces a small follow-on (confirm by
  grepping for callers; within this repo there should be none — it's
  called only by Dagster's graph).

## Success criteria

```bash
source .venv/bin/activate
pytest tests/test_assets.py -v
pytest tests/ -v    # whole suite still green
```

Expected behavior changes:
- Previously, `enriched_pdv_metadata` wrote 6 keys per item. Now it
  writes 7: the added `coord_provenance`.
- `processing_manifest` should not crash (it reads specific keys from
  the enriched dict, which are still present).
- With an all-naive-timestamp DataFrame, one WARNING log about naive
  timestamps appears at the end.

## Commit

One commit on `refactor/asset-dag`:

```
enriched_pdv_metadata: pass shot timestamp, write coord_provenance

Parse the spreadsheet Timestamp column per row (assuming UTC when
naive), pass to the now-versioned coordinate transform, and write a
coord_provenance block alongside Station/Sample coordinates on every
matched Girder item.

Extends the asset return dict with version_counter,
naive_timestamps_count, and yaml_sha256 so the coord_transform_check
(Step 4) can verify version resolution.
```
