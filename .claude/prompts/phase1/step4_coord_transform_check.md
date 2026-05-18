# Phase 1, Step 4 — Extend `coord_transform_check`

## Context

Branch: `refactor/asset-dag`. Steps 1, 2, 3 are committed.

Read `.claude/CLAUDE.md` and `aimdl_coord_enrichment/checks.py` before editing.

## Goal

`coord_transform_check` currently fires WARN only when
`coord_failures > 0`. Extend it so it also fires WARN when:

1. Any row had a resolvable coordinate input (station_x/y both
   non-null) but NO transform version could be resolved — i.e.,
   `sum(version_counter.values()) + coord_failures <
   matched_count_with_coords`.
2. `yaml_sha256` is None (meaning the YAML could not be hashed at
   run time — provenance is incomplete).

Report these conditions in the check's metadata and description so
they are obvious in the Dagster UI.

## Audit phase (report BEFORE editing)

1. Read `aimdl_coord_enrichment/checks.py::coord_transform_check` in full.
2. Read the updated `enriched_pdv_metadata` asset return dict (after
   Step 3): confirm that `version_counter`, `naive_timestamps_count`,
   and `yaml_sha256` keys are present.
3. Read `tests/test_checks.py` to see the current test pattern.
4. Report.

## Edits

### `aimdl_coord_enrichment/checks.py::coord_transform_check`

Replace the check body with logic that consumes the extended return
dict:

```python
@asset_check(asset="enriched_pdv_metadata")
def coord_transform_check(context, enriched_pdv_metadata):
    """WARN on coordinate transform issues.

    Fails (WARN) if any of:
      - coord_failures > 0         (transform raised)
      - version_counter is empty while writes happened
      - yaml_sha256 is None         (YAML could not be hashed)
    """
    failures = enriched_pdv_metadata.get("coord_failures", 0)
    version_counter = enriched_pdv_metadata.get("version_counter", {}) or {}
    yaml_sha256 = enriched_pdv_metadata.get("yaml_sha256")
    written_count = enriched_pdv_metadata.get("written_count", 0)

    unresolved_versions = written_count > 0 and not version_counter
    missing_sha = yaml_sha256 is None

    passed = (failures == 0) and (not unresolved_versions) and (not missing_sha)

    problems = []
    if failures > 0:
        problems.append(f"{failures} transform failures")
    if unresolved_versions:
        problems.append("no transform version resolved for any write")
    if missing_sha:
        problems.append("yaml_sha256 unavailable (provenance incomplete)")

    description = (
        "Transforms OK: " + ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
        if passed else "; ".join(problems)
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "coord_failures": failures,
            "version_counter": str(sorted(version_counter.items())),
            "yaml_sha256_present": not missing_sha,
        },
        description=description,
    )
```

### `tests/test_checks.py`

Add four new tests; do not alter existing tests.

1. `test_coord_transform_check_all_ok`
   - Input: `{"coord_failures": 0, "version_counter": {"HELIX/v2": 3},
     "yaml_sha256": "abc"*21 + "a", "written_count": 3}`
   - Expect `result.passed is True`.

2. `test_coord_transform_check_failures`
   - Input: `coord_failures=2, version_counter={"HELIX/v2": 1},
     yaml_sha256="…", written_count=3`
   - Expect `result.passed is False` and description contains "2".

3. `test_coord_transform_check_no_version_resolved`
   - Input: `coord_failures=0, version_counter={}, yaml_sha256="…",
     written_count=3`
   - Expect `result.passed is False`.

4. `test_coord_transform_check_missing_sha`
   - Input: `coord_failures=0, version_counter={"HELIX/v2": 3},
     yaml_sha256=None, written_count=3`
   - Expect `result.passed is False` and description mentions sha.

Follow the existing mock/context pattern used by sibling tests in
`test_checks.py`.

## What NOT to modify

- Any other check in `checks.py`
- `assets.py`, `coordinates.py`, `provenance.py`
- Other test files

## Success criteria

```bash
source .venv/bin/activate
pytest tests/test_checks.py -v
pytest tests/ -v    # whole suite still green
```

## Commit

One commit on `refactor/asset-dag`:

```
coord_transform_check: verify version resolution and sha256

Extend the check to fire WARN when no transform version could be
resolved for any enriched item (stale YAML boundaries) or when the
YAML sha256 is absent from the provenance block (hashing failed at
run time). Previously the check fired only on raised transform
exceptions.
```
