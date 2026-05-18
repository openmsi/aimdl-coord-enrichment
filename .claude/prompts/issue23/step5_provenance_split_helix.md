# Issue 23, Step 5 — Provenance split part 1: HELIX-scoped tagger

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0–4 complete.
MAXIMA raw is fully detached from the provenance tagger. This step
takes the tangled `provenance_tagged_items` and splits it along
data-flow lines: the mutating HELIX ALPSS piece stays an asset; the
non-mutating MAXIMA verifier moves to an asset check in Step 6.

Before editing, read:

- `.claude/CLAUDE.md`
- `.claude/prompts/issue23/README.md` (invariants)
- `aimdl_coord_enrichment/coord_enrichment/provenance_tagging.py`
- `aimdl_coord_enrichment/coord_enrichment/helix_alpss_leaf.py`
- `aimdl_coord_enrichment/coord_enrichment/maxima_derived_leaf.py`
- `aimdl_coord_enrichment/coord_enrichment/__init__.py`
- `aimdl_coord_enrichment/__init__.py` (Definitions + jobs)
- `aimdl_coord_enrichment/schedules.py`
- `tests/test_coord_enrichment_provenance_tagging.py`
- `tests/test_coord_enrichment_helix_alpss.py`
- `tests/test_coord_enrichment_maxima_derived.py`

## Why this step

`provenance_tagged_items` does two things that belong in different
places:

1. **HELIX ALPSS**: writes `meta.prov.wasDerivedFrom` linking ALPSS
   items to their parent PDV traces. This is a real mutation, and
   `enriched_helix_alpss` reads what it wrote. Asset-level dep is
   correct.
2. **MAXIMA xrd_derived**: verifies that `meta.prov.wasDerivedFrom`
   or `meta.prov.isPartOf` exists (written upstream by the
   `amdee_xrd` Girder plugin). It writes nothing for MAXIMA.

Bundling them forced `enriched_maxima_derived` and (until Step 2)
`enriched_maxima_raw` to wait for a cross-system provenance pass
that didn't affect their inputs. Splitting them lets each path have
the dependencies it actually has.

This step does the HELIX half: rename + scope-down. The MAXIMA half
(new asset check + `enriched_maxima_derived` rewiring) lands in
Step 6.

## Goal

- Rename `provenance_tagged_items` → `helix_alpss_provenance_tagged`.
- Delete the MAXIMA iteration in its body (scope-down to HELIX only).
- Delete the `maxima_prov_targets_resolve` asset check (moves to
  Step 6 as `maxima_xrd_derived_provenance_valid`).
- Update `enriched_helix_alpss` to depend on the renamed asset.
- Drop `deps=["provenance_tagged_items"]` from
  `enriched_maxima_derived`. It stays depless until Step 6 adds the
  correct dep on `enriched_maxima_raw`.
- Update Definitions, jobs, schedules, and tests.

The filename `provenance_tagging.py` is left alone — renaming the
file creates a large diff for no semantic gain.

## Edits

### 1. `aimdl_coord_enrichment/coord_enrichment/provenance_tagging.py`

Rename the function `provenance_tagged_items` →
`helix_alpss_provenance_tagged`. Delete the MAXIMA-facing code:

- Delete the `for dt in sorted(MAXIMA_DERIVED_DATA_TYPES):` loop
  block (the verifier-only part).
- Delete the `maxima_prov_targets_resolve` `@asset_check` function.
- Remove the `MAXIMA_DERIVED_DATA_TYPES` and `INSTRUMENT_MAXIMA`
  imports from `aimdl_coord_enrichment.instruments` if they become unused.

Keep:

- `_decide`, `_merged_prov`, `_apply_decision` helpers (HELIX uses them).
- The `for dt in sorted(HELIX_DERIVED_DATA_TYPES):` HELIX loop.
- The `all_helix_alpss_tagged` asset check — but update its decorator
  to target the renamed asset:

  ```python
  @asset_check(asset="helix_alpss_provenance_tagged")
  def all_helix_alpss_tagged(context, helix_alpss_provenance_tagged):
      ...
  ```

  And rename its function parameter to match
  (`helix_alpss_provenance_tagged` instead of
  `provenance_tagged_items`).

Update the module docstring to say the asset is HELIX-only.

The asset's return dict shape stays the same — it still returns
`{"counters": ..., "unresolved": ..., "write_ops": ..., "dry_run": ...}`
— but `counters` and `unresolved` now only contain HELIX-partition
entries.

### 2. `aimdl_coord_enrichment/coord_enrichment/helix_alpss_leaf.py`

Change the dep on `enriched_helix_alpss`:

```python
@asset(
    partitions_def=HELIX_ALPSS_PARTITIONS,
    deps=["helix_alpss_provenance_tagged"],  # was "provenance_tagged_items"
)
def enriched_helix_alpss(...):
```

No body changes — the asset reads `meta.prov.wasDerivedFrom` from
Girder, not from the tagger's return value. Only the ordering dep
name changes.

### 3. `aimdl_coord_enrichment/coord_enrichment/maxima_derived_leaf.py`

**Drop** `deps=["provenance_tagged_items"]` entirely. Do NOT add a
replacement here — Step 6 adds the correct dep on
`enriched_maxima_raw` with `AllPartitionMapping`. After this step,
`enriched_maxima_derived` has no deps beyond its input parameters
(`config`, `enrichable_items_inventory`, `coord_transform_config_snapshot`,
`girder`), which is fine temporarily.

```python
@asset(
    partitions_def=MAXIMA_DERIVED_PARTITIONS,
    # deps removed; Step 6 will add the correct lineage dep
)
def enriched_maxima_derived(...):
```

### 4. `aimdl_coord_enrichment/coord_enrichment/__init__.py`

Rename the export:

```python
from aimdl_coord_enrichment.coord_enrichment.provenance_tagging import (
    all_helix_alpss_tagged,
    helix_alpss_provenance_tagged,
    # maxima_prov_targets_resolve REMOVED
)
```

Update `__all__` accordingly.

### 5. `aimdl_coord_enrichment/__init__.py`

Imports: rename `provenance_tagged_items` → `helix_alpss_provenance_tagged`
everywhere (import line and the `Definitions(assets=[...])` list).

`Definitions.asset_checks`: remove `maxima_prov_targets_resolve`
(it's deleted in this step; Step 6 adds its replacement under a new
name).

Jobs — update selections:

- `coord_enrichment_job`: replace `provenance_tagged_items` with
  `helix_alpss_provenance_tagged`.
- `coord_enrichment_helix_alpss_job`: same rename.
- `coord_enrichment_maxima_derived_job`: **remove**
  `provenance_tagged_items` from the selection, do not replace it
  here. Becomes:

  ```python
  coord_enrichment_maxima_derived_job = define_asset_job(
      name="coord_enrichment_maxima_derived_job",
      selection=AssetSelection.assets(
          coord_transform_config_snapshot,
          enrichable_items_inventory,
          enriched_maxima_derived,
      ),
  )
  ```

  Step 6 will revisit this selection when it adds the
  `enriched_maxima_raw` dep.

- `coord_enrichment_maxima_raw_job` and
  `coord_enrichment_maxima_raw_partition_job` are unchanged (they
  were slimmed in Step 2 and already have no provenance dep).

### 6. `aimdl_coord_enrichment/schedules.py`

Rename in the ops lists:

```python
_STATE_REPORT_OPS = [
    "helix_alpss_provenance_tagged",  # was "provenance_tagged_items"
    "coord_enrichment_manifest",
]
_HELIX_ALPSS_OPS = [
    "helix_alpss_provenance_tagged",  # was "provenance_tagged_items"
    "enriched_helix_alpss",
]
_MAXIMA_DERIVED_OPS = [
    # "provenance_tagged_items" REMOVED — no replacement in this step
    "enriched_maxima_derived",
]
```

`_MAXIMA_RAW_OPS` is already clean from Step 2.

### 7. `tests/test_coord_enrichment_provenance_tagging.py`

Scope the existing tests to HELIX only. Remove any test that
asserts MAXIMA-derived counting behavior or that uses
`maxima_prov_targets_resolve`. Step 6 will add tests for the new
MAXIMA asset check in its place.

Update all references to `provenance_tagged_items` →
`helix_alpss_provenance_tagged` (function invocation, parameter
names, assertion strings).

### 8. `tests/test_coord_enrichment_helix_alpss.py`

Rename any dep-related references from `provenance_tagged_items` →
`helix_alpss_provenance_tagged`. Body of the asset didn't change,
so behavioral tests should still pass as-is.

### 9. `tests/test_coord_enrichment_maxima_derived.py`

Remove any assertion about a dep on `provenance_tagged_items`.
Do not add assertions about a new dep on `enriched_maxima_raw` —
that's Step 6's scope. Otherwise this test file should remain
behaviorally unchanged, since the asset body doesn't change in this
step.

### 10. Other test files that may reference the old name

Grep the `tests/` directory for `provenance_tagged_items` and
`maxima_prov_targets_resolve` and update any references found.
Likely candidates:

- `tests/test_coord_enrichment_e2e.py`
- `tests/test_coord_enrichment_phase4_e2e.py`
- `tests/test_coord_enrichment_partitioned_jobs.py`
- `tests/test_phase5_artifacts.py`

If an e2e test materializes the DAG end-to-end and fails because
of the topology change, xfail it with
`reason="Step 7: DAG topology refresh"` and remove the xfail in
Step 7. Do not paper over broken tests by silently weakening
assertions.

## Verification

```bash
.venv/bin/pytest
```

Full suite must pass (e2e tests may be xfail-guarded).

Expected test count delta: one test function deleted
(`maxima_prov_targets_resolve` check tests, if any existed as
standalone cases).

## Commit

```
git add aimdl_coord_enrichment/coord_enrichment/provenance_tagging.py \
        aimdl_coord_enrichment/coord_enrichment/helix_alpss_leaf.py \
        aimdl_coord_enrichment/coord_enrichment/maxima_derived_leaf.py \
        aimdl_coord_enrichment/coord_enrichment/__init__.py \
        aimdl_coord_enrichment/__init__.py \
        aimdl_coord_enrichment/schedules.py \
        tests/
git commit -m "Provenance split part 1: HELIX-scoped tagger (#23)

- Rename provenance_tagged_items -> helix_alpss_provenance_tagged.
- Delete the MAXIMA-facing iteration and maxima_prov_targets_resolve
  check; Step 6 will reintroduce the MAXIMA verification as an asset
  check on enriched_maxima_derived.
- enriched_helix_alpss now depends on the renamed asset.
- enriched_maxima_derived drops its stale provenance_tagged_items
  dep; Step 6 adds the semantically correct dep on enriched_maxima_raw.
- Update jobs, schedules, and tests to match."
```

## Success criteria

- `provenance_tagged_items` no longer exists anywhere in the codebase.
- `helix_alpss_provenance_tagged` exists and is scoped to HELIX.
- `maxima_prov_targets_resolve` no longer exists.
- `enriched_helix_alpss` depends on `helix_alpss_provenance_tagged`.
- `enriched_maxima_derived` has no `deps=[...]` line.
- Full `pytest` passes.
- One new commit.

## Out of scope

- Adding `maxima_xrd_derived_provenance_valid` — Step 6.
- Rewiring `enriched_maxima_derived`'s lineage dep — Step 6.
- E2E repair — Step 7.
- Docs — Step 8.
