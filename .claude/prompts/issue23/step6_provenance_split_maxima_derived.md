# Issue 23, Step 6 — Provenance split part 2: MAXIMA derived rewiring + new check

Tracking: https://github.com/openmsi/helix_metadata_extraction_dagster/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0–5 complete.
HELIX tagging is now isolated. This step adds the MAXIMA side of the
split: a proper lineage dep on `enriched_maxima_raw` and an asset
check that replaces the old verifier.

Before editing, read:

- `.claude/CLAUDE.md`
- `.claude/prompts/issue23/README.md` (invariants — especially the
  α-not-β decision on derived partitioning)
- `helix_dagster/coord_enrichment/maxima_derived_leaf.py`
- `helix_dagster/coord_enrichment/enrichment_leaves.py`
- `helix_dagster/coord_enrichment/__init__.py`
- `helix_dagster/__init__.py`
- `tests/test_coord_enrichment_maxima_derived.py`

## Why this step

`enriched_maxima_derived` reads `parent.meta.Station_X/Y` and
`parent.meta.coord_provenance.*` from Girder. Those fields are
written by `enriched_maxima_raw`. Before this step, the DAG
captures that lineage via the inventory (both assets read the same
inventory) but has **no explicit dep**, so Dagster doesn't know the
ordering — derived can run before raw and fail with "parent not yet
enriched."

Adding `deps=[AssetDep("enriched_maxima_raw", partition_mapping=AllPartitionMapping())]`
makes the lineage explicit. Derived's single partition waits for
all raw partitions (decision α — we don't repartition derived in
this refactor).

The replaced verifier returns as an asset check, not an upstream
asset. The check reads `enriched_maxima_derived`'s own
`resolution_errors` — specifically, items that failed at
`stage="inherit_from_parent"`, which is where missing-prov-link
and parent-not-found errors surface.

## Goal

- Add `deps=[AssetDep("enriched_maxima_raw", partition_mapping=AllPartitionMapping())]`
  to `enriched_maxima_derived`.
- Add a new asset check `maxima_xrd_derived_provenance_valid` on
  `enriched_maxima_derived` that flags items failing at
  `stage="inherit_from_parent"`.
- Register the new check in Definitions.

`coord_enrichment_maxima_derived_job` is **not** modified. See the
note under Section 3 below.

## Edits

### 1. `helix_dagster/coord_enrichment/maxima_derived_leaf.py`

Add imports:

```python
from dagster import (
    AllPartitionMapping,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetDep,
    AssetExecutionContext,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)
```

Update the asset decorator to include the lineage dep:

```python
@asset(
    partitions_def=MAXIMA_DERIVED_PARTITIONS,
    deps=[
        AssetDep(
            "enriched_maxima_raw",
            partition_mapping=AllPartitionMapping(),
        ),
    ],
)
def enriched_maxima_derived(...):
```

The asset body is unchanged — it still reads parent metadata from
Girder via the inventory. The dep affects Dagster's run ordering,
not the runtime data flow.

Add the new asset check at the bottom of the file:

```python
@asset_check(asset="enriched_maxima_derived")
def maxima_xrd_derived_provenance_valid(context, enriched_maxima_derived):
    """ERROR if any xrd_derived item failed parent resolution.

    Parent resolution for xrd_derived reads meta.prov.wasDerivedFrom
    or meta.prov.isPartOf, written upstream by the amdee_xrd Girder
    plugin, and looks the parent up in the inventory. Failures at
    this stage indicate either a missing prov link on the item (a
    data-hygiene problem in Girder) or a parent not present in the
    current inventory slice (an ingest lag).

    Both conditions should be zero in a healthy pipeline.
    """
    resolution_errors = enriched_maxima_derived.get("resolution_errors", [])
    inherit_errors = [
        e for e in resolution_errors
        if e.get("stage") == "inherit_from_parent"
    ]
    passed = len(inherit_errors) == 0
    examples = [
        f"{e.get('item_id', '?')}: {e.get('error', '')}"
        for e in inherit_errors[:3]
    ]
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "unresolved_count": MetadataValue.int(len(inherit_errors)),
            "examples": MetadataValue.text(", ".join(examples) or "none"),
        },
        description=(
            "All xrd_derived items have valid prov links and resolvable parents."
            if passed
            else f"{len(inherit_errors)} xrd_derived item(s) failed parent resolution"
        ),
    )
```

The two existing asset checks on `enriched_maxima_derived`
(`enrichment_success_rate_maxima_derived`,
`no_coord_transform_failures_maxima_derived`) are unchanged.

### 2. `helix_dagster/coord_enrichment/__init__.py`

Export the new check:

```python
from helix_dagster.coord_enrichment.maxima_derived_leaf import (
    MAXIMA_DERIVED_PARTITIONS,
    enriched_maxima_derived,
    enrichment_success_rate_maxima_derived,
    maxima_xrd_derived_provenance_valid,  # new
    no_coord_transform_failures_maxima_derived,
)
```

Update `__all__` to include `maxima_xrd_derived_provenance_valid`.

### 3. `helix_dagster/__init__.py`

Import the new check:

```python
from helix_dagster.coord_enrichment import (
    # ... existing ...
    maxima_xrd_derived_provenance_valid,
    # ... existing ...
)
```

Register it in `Definitions.asset_checks`:

```python
defs = Definitions(
    # ...
    asset_checks=[
        # ... existing checks ...
        maxima_xrd_derived_provenance_valid,  # new
    ],
    # ...
)
```

**`coord_enrichment_maxima_derived_job` stays unchanged.** Do NOT
add `enriched_maxima_raw` to its selection. Rationale:

`AssetSelection.assets(...)` requires uniform partition defs across
the selected assets. After Step 2, `enriched_maxima_raw` is
MultiPartitioned (`data_type × maxima_raw_run`) while
`enriched_maxima_derived` remains `StaticPartitionsDefinition(["MAXIMA/xrd_derived"])`
(decision α). Co-selecting them fails at Definitions load time with
`DagsterInvalidDefinitionError: Selected assets must have the same
partitions definitions`.

This is actually the correct design, not just a constraint. The
new DAG has two distinct materialization responsibilities:

- **`enriched_maxima_raw` is materialized continuously** by
  `maxima_raw_discovery_sensor` as partitions are discovered, and
  by `coord_enrichment_maxima_raw_weekly_schedule` for gap filling.
  Raw is kept current by the sensor/schedule, not by the derived
  job.
- **`coord_enrichment_maxima_derived_job` runs derived only,**
  consuming whatever raw state currently exists. Having it
  re-materialize all ~300 raw partitions on every invocation
  would re-introduce the fan-out Step 2 was designed to eliminate.

The asset-level `AssetDep(enriched_maxima_raw,
partition_mapping=AllPartitionMapping())` is what Dagster consults
for lineage, staleness reasoning, and the asset catalog UI — it
does not require co-selection in a job. Cross-partition-def
lineage expressed via `AssetDep` + materialization by separate
jobs is the standard Dagster pattern for this shape.

### 4. `tests/test_coord_enrichment_maxima_derived.py`

Add two tests.

**Test A: the lineage dep is declared.**

```python
def test_enriched_maxima_derived_depends_on_raw():
    """The Step 6 lineage rewiring: derived depends on raw."""
    from helix_dagster.coord_enrichment import enriched_maxima_derived
    dep_keys = {str(d.asset_key) for d in enriched_maxima_derived.deps}
    assert "enriched_maxima_raw" in " ".join(dep_keys)
```

(Exact API for introspecting deps is Dagster-version-dependent; use
whatever this repo's existing tests use. The intent is: verify the
dep was declared, not that Dagster's internal representation looks
any particular way.)

**Test B: the new asset check flags inherit-stage errors.**

```python
def test_maxima_xrd_derived_provenance_valid_detects_missing_prov():
    """Check fails when resolution_errors include inherit_from_parent entries."""
    from helix_dagster.coord_enrichment.maxima_derived_leaf import (
        maxima_xrd_derived_provenance_valid,
    )
    fake_derived_output = {
        "resolution_errors": [
            {
                "item_id": "X1",
                "name": "foo.tif",
                "stage": "inherit_from_parent",
                "error": "derived item X1 has no prov.wasDerivedFrom or prov.isPartOf",
            },
            {
                "item_id": "X2",
                "name": "bar.tif",
                "stage": "experiment_date",  # different stage, not a prov error
                "error": "item X2 missing meta.experiment_date",
            },
        ],
    }
    result = maxima_xrd_derived_provenance_valid(None, fake_derived_output)
    assert result.passed is False
    assert result.metadata["unresolved_count"].value == 1  # only the inherit one
```

```python
def test_maxima_xrd_derived_provenance_valid_passes_on_clean():
    from helix_dagster.coord_enrichment.maxima_derived_leaf import (
        maxima_xrd_derived_provenance_valid,
    )
    fake_derived_output = {"resolution_errors": []}
    result = maxima_xrd_derived_provenance_valid(None, fake_derived_output)
    assert result.passed is True
```

Existing behavioral tests of `enriched_maxima_derived` should
continue to pass — its body hasn't changed.

### 5. Grep-sweep for remaining references

Grep the whole repo for `provenance_tagged_items` one more time.
There should be zero references left after Steps 5 and 6. If any
remain (e.g., in a comment, docstring, or a forgotten test), clean
them up.

Also grep for `maxima_prov_targets_resolve` — should be zero.

## Verification

```bash
.venv/bin/pytest
```

Full suite must pass (e2e tests may still be xfail-guarded; Step 7
un-guards them).

## Commit

```
git add helix_dagster/coord_enrichment/maxima_derived_leaf.py \
        helix_dagster/coord_enrichment/__init__.py \
        helix_dagster/__init__.py \
        tests/test_coord_enrichment_maxima_derived.py
git commit -m "Provenance split part 2: lineage dep + new check (#23)

- enriched_maxima_derived now depends on enriched_maxima_raw via
  AllPartitionMapping (decision alpha: derived stays single-partition).
- Add maxima_xrd_derived_provenance_valid asset check on
  enriched_maxima_derived, replacing the deleted
  maxima_prov_targets_resolve.
- coord_enrichment_maxima_derived_job is intentionally unchanged;
  cross-partition-def co-selection is unsupported and would re-
  introduce the fan-out Step 2 eliminated."
```

## Success criteria

- `enriched_maxima_derived` has `deps=[AssetDep("enriched_maxima_raw", partition_mapping=AllPartitionMapping())]`.
- `maxima_xrd_derived_provenance_valid` exists as an asset check
  on `enriched_maxima_derived` and is registered in Definitions.
- `coord_enrichment_maxima_derived_job` selection is unchanged
  from its Step-5 state.
- Zero remaining references to `provenance_tagged_items` or
  `maxima_prov_targets_resolve` in the codebase.
- Full `pytest` passes.
- One new commit.

## Out of scope

- Repartitioning `enriched_maxima_derived` to match raw's
  multi-partition shape — deferred to a follow-up (decision β,
  not in this issue).
- E2E test reconciliation — Step 7.
- Docs — Step 8.
