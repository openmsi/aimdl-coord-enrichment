# Issue 23, Step 2 — MAXIMA raw: new partition def + asset rewrite

Tracking: https://github.com/openmsi/helix_metadata_extraction_dagster/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0 and 1 complete.
`aimdl_coord_enrichment/girder_io.py` now exposes `fetch_partition_index` and
`fetch_partition_details`.

Before editing, read:

- `.claude/CLAUDE.md`
- `.claude/prompts/issue23/README.md` (the invariants section)
- `aimdl_coord_enrichment/coord_enrichment/inventory.py`
- `aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py`
- `aimdl_coord_enrichment/coord_enrichment/__init__.py`
- `aimdl_coord_enrichment/__init__.py` (for Definitions and jobs)
- `aimdl_coord_enrichment/schedules.py`
- `aimdl_coord_enrichment/instruments/maxima.py` (for `parse_scan_point_index`, `parse_instructions_json`, `scan_point_coords`)
- `aimdl_coord_enrichment/girder_io.py` (new helpers from Step 1)
- `tests/test_coord_enrichment_maxima_raw.py`
- `tests/test_schedules.py`
- `tests/test_coord_enrichment_partitioned_jobs.py`

## Why this step

Today `MAXIMA_RAW_PARTITIONS = StaticPartitionsDefinition(["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"])`
— two partitions total. Each materialization fans out across every
run for that data_type through a flattened `enrichable_items_inventory`
input. This is the core bug: no per-run lineage, no incremental
work, forced global walks on every invocation.

The AIMD-L API already partition-keys these items by
`"<igsn>//<experiment_date>"` per data_type. We replace the static
two-key partition with a `MultiPartitionsDefinition` whose dynamic
`run` dimension matches the API's native keying, and rewire the
asset to fetch only its own partition's data via the Step 1 scoped
helpers.

The asset also drops two deps it doesn't actually need:

- `enrichable_items_inventory` — replaced by scoped fetches.
- `provenance_tagged_items` — never written anything
  `enriched_maxima_raw` reads; a pure ordering dep that serializes
  the whole fan-out behind one global pass.

## Goal

- Replace `MAXIMA_RAW_PARTITIONS` with
  `MultiPartitionsDefinition({"data_type": Static, "run": Dynamic})`.
- Rewrite `enriched_maxima_raw` to:
  - Partition on the new `MAXIMA_RAW_PARTITIONS`.
  - Drop `enrichable_items_inventory` from its signature.
  - Drop `deps=["provenance_tagged_items"]`.
  - Fetch raw items via `fetch_partition_details(girder, data_type, aimdl_key)`.
  - Fetch `xrd_metadata` (instructions.txt source) via
    `fetch_partition_details(girder, "xrd_metadata", aimdl_key)`.
  - Preserve the per-item processing semantics (scan_point parse,
    station→sample transform, overwrite policy, write).
- Slim the `coord_enrichment_maxima_raw_job` selection.
- Shim the existing weekly schedule so it still loads against the
  new partition def (true gap-filling lands in Step 4).
- Rewrite `tests/test_coord_enrichment_maxima_raw.py` for the new shape.

Do **not** repartition `enriched_maxima_derived` — that stays
single-partition-static in this refactor (decision α). Derived's
rewiring happens in Step 6.

Do **not** add the discovery sensor here — that's Step 3. After
this step, adding partitions is manual (via `dagster asset` CLI or
the UI), which is fine because the schedule is STOPPED by default
and the sensor arrives in the next step.

## Edits

### 1. `aimdl_coord_enrichment/coord_enrichment/inventory.py`

Replace the `MAXIMA_RAW_PARTITIONS` definition. Add the two
component partition defs alongside. Keep imports minimal (don't
import `MultiPartitionKey` — it's not needed here).

```python
from dagster import (
    # ... existing imports ...
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

# ... existing module code ...

MAXIMA_RAW_DATA_TYPE_PARTITIONS = StaticPartitionsDefinition(
    ["xrd_raw", "xrf_raw"]
)

MAXIMA_RUN_PARTITIONS = DynamicPartitionsDefinition(
    name="maxima_raw_run"
)

MAXIMA_RAW_PARTITIONS = MultiPartitionsDefinition(
    {
        "data_type": MAXIMA_RAW_DATA_TYPE_PARTITIONS,
        "run": MAXIMA_RUN_PARTITIONS,
    }
)
```

**The old two-key `StaticPartitionsDefinition` is deleted outright.**
Same name, new shape. No alias, no compatibility shim.

`enrichable_items_inventory` itself is **unchanged**. It continues
to include `xrd_raw`/`xrf_raw` in `PARTITION_AWARE_DATA_TYPES` and
flatten them via `fetch_items_by_partition`, because
`provenance_tagged_items` and downstream reports still consume
those flattened slices. That cleanup, if any, is out of scope here.

### 2. `aimdl_coord_enrichment/coord_enrichment/__init__.py`

Export the two new partition defs alongside the existing
`MAXIMA_RAW_PARTITIONS`:

```python
from aimdl_coord_enrichment.coord_enrichment.inventory import (
    MAXIMA_RAW_DATA_TYPE_PARTITIONS,
    MAXIMA_RAW_PARTITIONS,
    MAXIMA_RUN_PARTITIONS,
    # ... other existing exports ...
)
```

Update `__all__` to include both new names.

### 3. `aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py` — rewrite `enriched_maxima_raw`

The per-item processing code (scan point parse, transform,
provenance build, overwrite, write) stays the same. What changes is
**where items come from** and **how instructions.txt is located**.

Delete the helpers `_fetch_instructions_from_inventory`,
`_find_instructions`, and `_run_key_for` (the (igsn, experiment_date)
lookup machinery is no longer needed — the partition key IS that
pair).

Add a smaller replacement helper:

```python
def _fetch_instructions_for_run(
    girder: GirderConnection,
    aimdl_key: str,
    context: AssetExecutionContext,
) -> tuple[dict | None, dict | None, list[dict]]:
    """Fetch and parse the instructions.txt for a single AIMD-L run.

    Calls the scoped partition-details endpoint for xrd_metadata
    keyed by ``aimdl_key``, filters to instructions.txt items,
    downloads and parses the first one.

    Returns (instr_item, parsed, errors):
      - instr_item: the Girder item dict for the instructions.txt
        used, or None if none was found or parseable.
      - parsed: the parsed JSON dict, or None on failure.
      - errors: list of per-item error dicts (empty on happy path).

    If multiple instructions.txt items are present for the same run,
    the first is used and the rest are recorded as warnings in
    errors.
    """
    # Implementation:
    # 1. metadata_items = fetch_partition_details(girder, "xrd_metadata", aimdl_key)
    # 2. instr_items = [it for it in metadata_items if it.get("name") == "instructions.txt"]
    # 3. If len(instr_items) == 0: return (None, None, [{"stage": "instructions_missing",
    #    "error": f"no instructions.txt in xrd_metadata for {aimdl_key}"}])
    # 4. If len(instr_items) > 1: record warnings for extras, use first
    # 5. Download first via girder.get(f"item/{instr_id}/files") and girder.downloadFile
    # 6. Parse via parse_instructions_json; catch ResolutionError; record error
    # 7. Return (instr_item, parsed, errors)
```

Rewrite the `enriched_maxima_raw` asset:

```python
@asset(
    partitions_def=MAXIMA_RAW_PARTITIONS,
)
def enriched_maxima_raw(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    coord_transform_config_snapshot,
    girder: GirderConnection,
) -> dict[str, Any]:
    """Write Sample_X/Y and coord_provenance to MAXIMA xrd_raw or xrf_raw items.

    Partitioned on MultiPartitionsDefinition({data_type, run}).
    Each partition fetches its own items and the matching
    instructions.txt via the /aimdl/partition/details endpoint,
    keyed on aimdl_key = "<igsn>//<experiment_date>".
    """
    keys = context.partition_key.keys_by_dimension
    data_type = keys["data_type"]
    aimdl_key = keys["run"]
    partition_key_str = str(context.partition_key)

    items = fetch_partition_details(girder, data_type, aimdl_key)
    context.log.info(
        "enriched_maxima_raw (%s, %s): %d items to consider",
        data_type, aimdl_key, len(items),
    )

    instr_item, parsed, instructions_errors = _fetch_instructions_for_run(
        girder, aimdl_key, context,
    )

    # ... per-item loop (scan_point parse, transform, provenance,
    #     overwrite, write) with the same structure as today, but
    #     using (instr_item, parsed) for the run instead of the
    #     per-item _find_instructions(...) lookup.
    #
    # If instr_item is None, every item in this partition gets a
    # resolution_error of stage="instructions" with the error text
    # from instructions_errors[0].

    # Return dict shape preserved for asset checks:
    return {
        "partition_key": partition_key_str,
        "data_type": data_type,
        "aimdl_key": aimdl_key,
        "counts": counts,
        "write_errors": write_errors,
        "resolution_errors": resolution_errors,
        "instructions_errors": instructions_errors,
        "version_counter": version_counter,
        "dry_run": config.dry_run,
    }
```

Key points for the rewrite:

- **No `deps=["provenance_tagged_items"]`** — delete it.
- **No `enrichable_items_inventory` parameter** — delete it.
- The per-item code that does `parse_scan_point_index`,
  `scan_point_coords`, `transform_station_to_sample`,
  `build_coord_provenance`, `should_write`, and
  `girder.addMetadataToItem` is unchanged. Only the source of
  `items` and `(instr_item, parsed)` changes.
- The two asset checks (`enrichment_success_rate_maxima_raw` and
  `no_coord_transform_failures_maxima_raw`) don't need code
  changes — they read from the returned dict, whose shape is
  preserved.
- Add `fetch_partition_details` to the imports from
  `aimdl_coord_enrichment.girder_io`.

Update the module docstring to say the asset is partitioned on the
new MultiPartitionsDefinition.

### 4. `aimdl_coord_enrichment/__init__.py`

Slim `coord_enrichment_maxima_raw_job` — `enriched_maxima_raw` no
longer depends on the inventory or provenance assets, so they
shouldn't be in its selection:

```python
coord_enrichment_maxima_raw_job = define_asset_job(
    name="coord_enrichment_maxima_raw_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enriched_maxima_raw,
    ),
)
```

The other two jobs (`coord_enrichment_helix_alpss_job`,
`coord_enrichment_maxima_derived_job`) are unchanged here. They'll
be revisited in Steps 5–6.

### 5. `aimdl_coord_enrichment/schedules.py`

Two changes:

- `_MAXIMA_RAW_OPS`: drop `"provenance_tagged_items"`. Becomes:

  ```python
  _MAXIMA_RAW_OPS = [
      "enriched_maxima_raw",
  ]
  ```

- `coord_enrichment_maxima_raw_weekly_schedule`: the call to
  `MAXIMA_RAW_PARTITIONS.get_partition_keys()` needs a
  `dynamic_partitions_store` arg now that the partition def has a
  dynamic dim:

  ```python
  for key in MAXIMA_RAW_PARTITIONS.get_partition_keys(
      dynamic_partitions_store=context.instance
  ):
      yield RunRequest(
          run_key=str(key),
          partition_key=str(key),
          run_config=_dry_run_config(_MAXIMA_RAW_OPS),
          tags={"phase5": "sweep", "partition": str(key), "dry_run": "true"},
      )
  ```

This is a shim, not the final form. It still fans out to every
known partition. Step 4 upgrades this to gap-filling semantics.
`partition_key=str(key)` for the RunRequest accepts the string
rendering of a MultiPartitionKey that Dagster understands.

The other two weekly schedules are untouched.

### 6. `tests/test_coord_enrichment_maxima_raw.py` — rewrite

The existing tests assume the static two-key shape and the
`enrichable_items_inventory` input. Rewrite them to match the new
asset shape. Test structure per case:

```python
from dagster import MultiPartitionKey, build_asset_context

def test_enriched_maxima_raw_happy_path(monkeypatch):
    # 1. Mock fetch_partition_details to return:
    #    - for ("xrd_raw", "JHAMAB00001//..."), a list of two scan_point items
    #    - for ("xrd_metadata", same key), a list with one instructions.txt item
    # 2. Mock girder client so item/{id}/files and downloadFile yield a
    #    valid instructions.txt JSON payload with sample.scan_points.
    # 3. Build asset context with partition_key=MultiPartitionKey(
    #        {"data_type": "xrd_raw", "run": "JHAMAB00001//..."}
    #    )
    # 4. Call enriched_maxima_raw(context, config, snapshot, girder) directly.
    # 5. Assert counts.written (or simulated_dry_run) == 2, resolution_errors == 0,
    #    and that addMetadataToItem was called for both items.
```

Cover at minimum these cases:

- Happy path: 2 items in partition, 1 instructions.txt, both items
  yield valid scan points, writes issued.
- Missing instructions.txt: 2 items in partition, 0 in xrd_metadata
  → both items end with `resolution_errors`, instructions_errors
  records the missing-file error.
- Multiple instructions.txt: 1 item in partition, 2 in xrd_metadata
  → first wins, instructions_errors has one warning, per-item
  processing proceeds normally.
- Bad scan_point index: filename doesn't match `scan_point_<i>` →
  resolution_errors recorded, no write.
- Dry run: `config.dry_run=True` → counts.simulated_dry_run
  increments, no `addMetadataToItem` calls.

Use `monkeypatch` on `aimdl_coord_enrichment.coord_enrichment.enrichment_leaves.fetch_partition_details`
to inject per-call return values based on `(data_type, key)` args.

### 7. `tests/test_schedules.py`

If any test invokes
`coord_enrichment_maxima_raw_weekly_schedule(context)` directly, it
will need a `context` with an `instance` attribute that supports
`get_dynamic_partitions`. Use `DagsterInstance.ephemeral()` or
Dagster's `build_schedule_context(instance=...)`.

Tests that asserted exactly 2 RunRequests (one per static key)
should now assert 0 RunRequests on a fresh ephemeral instance
(because no dynamic run keys have been registered yet). If such a
test doesn't exist, no change is needed.

### 8. `tests/test_coord_enrichment_partitioned_jobs.py`

If any test asserts the set of assets in
`coord_enrichment_maxima_raw_job`, update it to match the slimmed
selection (`coord_transform_config_snapshot`, `enriched_maxima_raw`).

## Verification

```bash
.venv/bin/pytest
```

Full suite must pass. Pay special attention to:

- `test_coord_enrichment_maxima_raw.py` — the rewritten tests.
- `test_coord_enrichment_e2e.py` and
  `test_coord_enrichment_phase4_e2e.py` — these may break if they
  materialize `enriched_maxima_raw`. If they do, leave the failure
  for now and repair in Step 7. **Do not try to fix e2e tests in
  this step** — that's Step 7's scope, and conflating the two
  risks an incoherent commit. If e2e tests break here, mark them
  with `@pytest.mark.xfail(reason="Step 7: new DAG topology")` so
  the suite stays green overall, and remove the xfail in Step 7.

## Commit

```
git add aimdl_coord_enrichment/coord_enrichment/inventory.py \
        aimdl_coord_enrichment/coord_enrichment/__init__.py \
        aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py \
        aimdl_coord_enrichment/__init__.py \
        aimdl_coord_enrichment/schedules.py \
        tests/test_coord_enrichment_maxima_raw.py \
        tests/test_schedules.py \
        tests/test_coord_enrichment_partitioned_jobs.py \
        tests/test_coord_enrichment_e2e.py \
        tests/test_coord_enrichment_phase4_e2e.py
git commit -m "MAXIMA raw: dynamic multi-partitioning, drop inventory/prov deps (#23)

- Replace static MAXIMA_RAW_PARTITIONS with MultiPartitionsDefinition
  over static data_type and dynamic run dimensions.
- Rewrite enriched_maxima_raw to fetch its own partition via
  fetch_partition_details, drop enrichable_items_inventory input,
  drop deps=[provenance_tagged_items].
- Slim coord_enrichment_maxima_raw_job selection.
- Shim weekly schedule to iterate dynamic partitions (Step 4 upgrades
  this to gap-filling).
- Rewrite test_coord_enrichment_maxima_raw.py for the new shape."
```

(Only include e2e test files in the `git add` line if you actually
marked them with xfail.)

## Success criteria

- `MAXIMA_RAW_PARTITIONS` is a `MultiPartitionsDefinition` with a
  static `data_type` dim and a dynamic `run` dim named
  `"maxima_raw_run"`.
- `enriched_maxima_raw` has no `enrichable_items_inventory` input
  and no `deps=["provenance_tagged_items"]`.
- `fetch_partition_details` is the only source of items for the asset.
- `coord_enrichment_maxima_raw_job` selects only
  `coord_transform_config_snapshot` and `enriched_maxima_raw`.
- `.venv/bin/pytest` passes (e2e tests may be xfail-guarded;
  Step 7 will un-xfail them).
- One new commit.

## Out of scope

- Discovery sensor — Step 3.
- Gap-filling schedule upgrade — Step 4.
- Anything touching `provenance_tagged_items`, `enriched_helix_alpss`,
  or `enriched_maxima_derived` — Steps 5 and 6.
- E2E test repair — Step 7.
- Docs updates — Step 8.
