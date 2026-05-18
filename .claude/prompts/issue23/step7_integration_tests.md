# Issue 23, Step 7 — Integration and E2E test reconciliation

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0–6 complete.
All production code is refactored. This step brings the E2E and
integration tests back into alignment with the new DAG topology,
un-`xfail`-ing anything that was temporarily guarded during earlier
steps.

Before editing, read:

- `.claude/CLAUDE.md`
- Every file in `tests/` that still fails or is marked
  `pytest.mark.xfail(reason="Step 7: ...")`. Likely candidates:
  - `tests/test_coord_enrichment_e2e.py`
  - `tests/test_coord_enrichment_phase4_e2e.py`
  - `tests/test_phase5_artifacts.py`
  - `tests/test_coord_enrichment_partitioned_jobs.py`

Also read, for reference:

- `aimdl_coord_enrichment/__init__.py` (the current Definitions)
- `aimdl_coord_enrichment/coord_enrichment/` (all assets)
- `aimdl_coord_enrichment/sensors.py`, `aimdl_coord_enrichment/schedules.py`

## Why this step

E2E tests exercise whole-DAG materialization and whole-job
selections. Steps 2, 5, and 6 changed:

- `enriched_maxima_raw` partition shape (static 2 → multi-dim
  with dynamic run dim)
- `enriched_maxima_raw` asset signature (dropped inventory +
  provenance inputs)
- `provenance_tagged_items` → `helix_alpss_provenance_tagged`
  (HELIX-only)
- `maxima_prov_targets_resolve` → `maxima_xrd_derived_provenance_valid`
  (now an asset check on derived, not on tagger)
- `enriched_maxima_derived` → new lineage dep on
  `enriched_maxima_raw` (via `AssetDep` + `AllPartitionMapping`;
  declared at the asset level, **not** added to the derived job's
  selection — cross-partition-def co-selection is unsupported and
  semantically wrong here)
- `coord_enrichment_maxima_raw_job` slimmed (inventory + tagger
  removed from selection in Step 2)
- `coord_enrichment_maxima_derived_job` unchanged from pre-refactor
- New job `coord_enrichment_maxima_raw_partition_job`
- New sensor `maxima_raw_discovery_sensor`

Any test that hard-codes the old names, shapes, or topology is now
either xfail-guarded or silently passing by coincidence. Reconcile
all of them.

## Goal

- Remove every `@pytest.mark.xfail(reason="Step 7: ...")` marker
  added during Steps 2, 5, or 6. The underlying test must now pass
  on its own merits.
- Update fixtures, mocks, and assertions to match the new DAG.
- Verify the full suite is green with no skips or xfails introduced
  by this refactor.

## Edits

### 1. Inventory the xfail markers

```bash
grep -rn "Step 7" tests/ --include="*.py"
grep -rn "xfail" tests/ --include="*.py" | grep -i "issue23\|step7\|topology"
```

Every hit is either (a) a test written with the old topology baked
in and temporarily guarded during an earlier step, or (b) a
genuinely new xfail unrelated to this refactor. Inspect each;
handle only (a). Leave (b) alone.

### 2. Reconcile `tests/test_coord_enrichment_e2e.py`

For each broken/xfail'd test, apply these categories of change as
needed:

**Old-name renames:**

- `provenance_tagged_items` → `helix_alpss_provenance_tagged`
- `maxima_prov_targets_resolve` → `maxima_xrd_derived_provenance_valid`
  (note: different asset target now — on `enriched_maxima_derived`,
  not on the tagger)

**Partition-shape changes:**

Any test that materializes `enriched_maxima_raw` for a specific
partition must construct a `MultiPartitionKey`:

```python
from dagster import MultiPartitionKey

partition_key = MultiPartitionKey(
    {"data_type": "xrd_raw", "run": "JHAMAB00001//2026-04-16"}
)
```

And register the run key in the dynamic partitions dim before
materializing:

```python
instance.add_dynamic_partitions("maxima_raw_run", ["JHAMAB00001//2026-04-16"])
```

**Fetch mocks:**

Tests that previously mocked `enrichable_items_inventory` to feed
`enriched_maxima_raw` must now mock `fetch_partition_details`
directly (monkeypatch
`aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.fetch_partition_details`).
Return per-`(data_type, key)` item lists. Include an
`xrd_metadata` key that returns a minimal instructions.txt item.

**DAG-topology assertions:**

If a test asserts "asset A depends on asset B" for some pair,
update the expected edges. The new edges are:

- `enriched_helix_alpss` → `helix_alpss_provenance_tagged` (was `provenance_tagged_items`)
- `enriched_maxima_derived` → `enriched_maxima_raw` (new, via `AssetDep` + `AllPartitionMapping`)
- `enriched_maxima_raw` has no asset deps (removed both inventory and tagger)

### 3. Reconcile `tests/test_coord_enrichment_phase4_e2e.py`

Same categories as Section 2. Phase 4 tests exercise the HELIX ALPSS
and MAXIMA derived paths; they are most affected by the dep
renames, less by the partition-shape change (unless they materialize
the raw asset too).

### 4. Reconcile `tests/test_phase5_artifacts.py`

If this test checks the `Definitions` object's advertised assets,
checks, jobs, and sensors, update the expected lists:

- Add: `helix_alpss_provenance_tagged`, `maxima_xrd_derived_provenance_valid`,
  `maxima_raw_discovery_sensor`, `coord_enrichment_maxima_raw_partition_job`
- Remove: `provenance_tagged_items`, `maxima_prov_targets_resolve`

### 5. Reconcile `tests/test_coord_enrichment_partitioned_jobs.py`

If this file asserts the partitions definitions of each asset,
update:

- `enriched_maxima_raw.partitions_def` is now
  `MultiPartitionsDefinition` with dimensions `{"data_type", "run"}`
  where `run` is dynamic (name `"maxima_raw_run"`).
- `enriched_helix_alpss.partitions_def` unchanged.
- `enriched_maxima_derived.partitions_def` unchanged.

If this file asserts job selections:

- `coord_enrichment_maxima_raw_job`: slimmed to
  `coord_transform_config_snapshot` + `enriched_maxima_raw` (done
  in Step 2; any existing test assertion for this job may already
  have been updated in that step).
- `coord_enrichment_maxima_derived_job`: **unchanged** from its
  pre-refactor shape. No update needed. Do not attempt to add
  `enriched_maxima_raw` to its selection — Dagster rejects
  cross-partition-def co-selection at load time.
- `coord_enrichment_maxima_raw_partition_job`: new in Step 3;
  add an assertion for its selection
  (`coord_transform_config_snapshot` + `enriched_maxima_raw`) if
  the file asserts job selections generally.

### 6. Any other test with Step 7 xfail or orphaned old-name reference

Grep-sweep:

```bash
grep -rn "provenance_tagged_items\|maxima_prov_targets_resolve" tests/ --include="*.py"
# Expect zero results.

grep -rn "Step 7\|xfail" tests/ --include="*.py" | grep -i "issue23\|topology\|dag"
# Expect zero results after fixes.
```

## Verification

```bash
.venv/bin/pytest
```

Every test must pass on its own — no new xfail markers, no new
skips. The total test count should be ≥ the Step 0 baseline count
(we added tests in Steps 1, 3, 4, 6; we deleted one check function
in Step 5, plus maybe 1–2 tests specific to the deleted check).

If a test now looks trivial (e.g., it used to exercise a subtle
interaction between the tagger and raw, and that interaction no
longer exists), deleting it is acceptable — note the deletion in
the commit message.

## Commit

```
git add tests/
git commit -m "Reconcile e2e and integration tests with new DAG topology (#23)

- Remove all Step-7 xfail markers introduced during Steps 2, 5, 6.
- Update fixtures to mock fetch_partition_details and
  xrd_metadata per-key instead of the full flattened inventory.
- Use MultiPartitionKey for enriched_maxima_raw partition refs.
- Rename asset references: provenance_tagged_items ->
  helix_alpss_provenance_tagged; maxima_prov_targets_resolve ->
  maxima_xrd_derived_provenance_valid (now on enriched_maxima_derived).
- Update DAG-topology assertions for new edges."
```

## Success criteria

- Zero `xfail` markers with `reason` mentioning this refactor.
- Zero references to `provenance_tagged_items` or
  `maxima_prov_targets_resolve` anywhere in the repo.
- Full `pytest` suite passes without any skip/xfail caused by
  this refactor.
- One new commit.

## Out of scope

- Architecturally new tests — we're reconciling, not expanding.
- Docs — Step 8.
