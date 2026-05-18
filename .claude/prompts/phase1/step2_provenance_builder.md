# Phase 1, Step 2 — Provenance builder module

## Context

Branch: `refactor/asset-dag`. Step 1 is committed.

Read `.claude/CLAUDE.md` and `docs/coordinate_enrichment_dag.md` §6
(the "Metadata schema written to Girder items" section) before
starting.

## Goal

Introduce a small pure-Python module that builds the `coord_provenance`
payload and computes a YAML file's sha256. This module has NO Dagster
dependencies, NO Girder dependencies, and NO network calls. It exists
so Step 3 (and later the new DAG) can produce identical provenance
payloads without duplicating logic.

## Audit phase (report BEFORE editing)

1. Read `aimdl_coord_enrichment/__init__.py`, `assets.py`, `coordinates.py` to
   confirm the surrounding module style (type hints, docstring
   conventions, import ordering).
2. Read `docs/coordinate_enrichment_dag.md` §6.1 and §6.2 for the
   target schema shape.
3. Check whether `coordinate-transformer` exposes a version attribute
   we can read at runtime:
   ```bash
   .venv/bin/python -c "import coordinate_transformer as ct; \
     print(getattr(ct, '__version__', None)); \
     from importlib.metadata import version; \
     print(version('coordinate-transformer'))"
   ```
   Pick whichever method works. If neither does, fall back to the
   string `"unknown"` and log a warning at module import.
4. Report findings.

## Edits

### New file: `aimdl_coord_enrichment/provenance.py`

Add exactly this public surface. Do not add extras.

```python
"""Provenance payload construction for coordinate-enriched Girder items.

Pure functions; no Dagster, Girder, or network dependencies. Called
by enrichment assets to build the coord_provenance dict that goes
alongside Station_X/Y and Sample_X/Y on every enriched item.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def compute_yaml_sha256(yaml_path: str | Path) -> str:
    """Return the hex sha256 digest of the file at yaml_path.

    Raises FileNotFoundError if the file does not exist.
    """
    ...


def get_transformer_version() -> str:
    """Return the installed coordinate-transformer version string.

    Falls back to 'unknown' if the package does not expose a version.
    Does not raise.
    """
    ...


def build_coord_provenance(
    *,
    instrument: str,
    transform_version: str | None,
    transform_yaml_sha256: str,
    transformer_version: str,
    pipeline_version: str,
    source_timestamp: datetime | None,
    source_timestamp_origin: str,
    station_coord_source: dict[str, Any],
    enriched_at: datetime | None = None,
    dagster_run_id: str | None = None,
) -> dict[str, Any]:
    """Build a coord_provenance dict suitable for writing to Girder.

    All parameters are keyword-only. source_timestamp may be None when
    the caller could not resolve one; in that case the output's
    source_timestamp field is null.

    enriched_at defaults to datetime.now(timezone.utc) when None.
    dagster_run_id may be None; when so it is omitted from the dict
    rather than written as null.

    Returns a JSON-serializable dict that matches §6.1 of
    docs/coordinate_enrichment_dag.md.
    """
    ...
```

Implementation notes:

- `compute_yaml_sha256`: read the file in binary, feed into
  `hashlib.sha256()`, return `.hexdigest()`. Do not stream in chunks
  — YAML files are small; simple `.read()` is fine.
- `get_transformer_version`: try `coordinate_transformer.__version__`
  first, then `importlib.metadata.version("coordinate-transformer")`,
  then `"unknown"`. Never raise.
- `build_coord_provenance`:
  - All datetimes go out as ISO-8601 strings (`.isoformat()`).
  - Source timestamps must be tz-aware; raise `ValueError` if a naive
    `source_timestamp` is passed.
  - `station_coord_source` is inserted as-is; callers are responsible
    for its shape. Do NOT validate its contents in this function.
  - Key order in the returned dict does not matter (Python 3.7+
    preserves insertion order; just match the order in §6.1 for
    readability).
  - Omit `dagster_run_id` from the dict if None.

### New file: `tests/test_provenance.py`

Cover:

1. `test_compute_yaml_sha256_stable` — write a fixture YAML to a
   tmp_path, compute sha256, compare to the digest computed via
   `hashlib.sha256(file_bytes).hexdigest()` on the same bytes.
2. `test_compute_yaml_sha256_differs_on_change` — write two slightly
   different YAMLs, expect different digests.
3. `test_compute_yaml_sha256_missing_raises` — point at a path that
   doesn't exist, expect `FileNotFoundError`.
4. `test_get_transformer_version_returns_string` — just asserts the
   return is a string, possibly `"unknown"`. No version-value
   assertion.
5. `test_build_coord_provenance_minimal_shape` — calls the builder
   with plausible values for a HELIX PDV row, asserts the returned
   dict has all §6.1 keys and that nested `station_coord_source` was
   passed through unchanged.
6. `test_build_coord_provenance_omits_run_id_when_none` — run_id=None
   should mean the key is absent from the output dict.
7. `test_build_coord_provenance_naive_timestamp_raises` — passing a
   naive `source_timestamp` raises ValueError.
8. `test_build_coord_provenance_iso_format` — verifies that
   `source_timestamp` and `enriched_at` in the output are valid
   ISO-8601 strings with explicit tz (end with `+00:00` or `Z`).

Use `pytest.fixture` for any shared setup; no mocking needed.

## What NOT to modify

- `aimdl_coord_enrichment/assets.py`, `checks.py`, `coordinates.py`,
  `__init__.py`
- Any existing test file
- `pyproject.toml`, YAMLs

## Success criteria

```bash
source .venv/bin/activate
pytest tests/test_provenance.py -v
pytest tests/ -v    # whole suite still green
```

Run:
```bash
.venv/bin/python -c "
from aimdl_coord_enrichment.provenance import build_coord_provenance
from datetime import datetime, timezone
p = build_coord_provenance(
    instrument='HELIX',
    transform_version='HELIX/v2',
    transform_yaml_sha256='deadbeef'*8,
    transformer_version='0.3.0',
    pipeline_version='0.2.0',
    source_timestamp=datetime(2026, 4, 16, 17, 12, tzinfo=timezone.utc),
    source_timestamp_origin='spreadsheet_timestamp_col',
    station_coord_source={'kind': 'helix_experiment_log',
                          'spreadsheet_item_id': 'abc',
                          'spreadsheet_row_index': 0},
)
import json; print(json.dumps(p, indent=2))
"
```
Output should be a clean JSON dict matching the schema.

## Commit

One commit on `refactor/asset-dag`:

```
provenance: add coord_provenance builder module

Pure-Python helpers for computing the transform YAML sha256, reading
the coordinate-transformer package version, and assembling a
coord_provenance dict matching docs/coordinate_enrichment_dag.md §6.1.

Used by Step 3, which wires this into enriched_pdv_metadata.
```
