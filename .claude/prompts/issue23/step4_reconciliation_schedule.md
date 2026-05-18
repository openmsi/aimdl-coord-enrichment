# Issue 23, Step 4 — Gap-filling reconciliation schedule

Tracking: https://github.com/openmsi/helix_metadata_extraction_dagster/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0–3 complete.
The sensor now discovers and materializes new partitions
automatically. This step upgrades the weekly reconciliation schedule
to fill in the gaps that the sensor might have missed.

Before editing, read:

- `.claude/CLAUDE.md`
- `aimdl_coord_enrichment/schedules.py`
- `aimdl_coord_enrichment/coord_enrichment/inventory.py` (for `MAXIMA_RAW_PARTITIONS`)
- `tests/test_schedules.py`
- Whatever Dagster version is installed (check `pyproject.toml`) —
  the instance API for "which partitions have been materialized"
  varies by version. Likely candidates:
  `DagsterInstance.get_materialized_partitions(asset_key)` or
  `DagsterInstance.get_latest_materialization_event(asset_key, partition=...)`.

## Why this step

Step 2 left the weekly schedule as a shim: iterate all known
partitions, emit a dry-run RunRequest for each. That's
brute-force — every Sunday it fans out over ~300+ partitions
whether or not they need it.

True reconciliation only materializes partitions with no prior
successful materialization. It catches:

- Partitions the sensor registered but whose RunRequests failed.
- Partitions materialized before the sensor existed (manual
  backfills with gaps).
- Runs that crashed mid-materialization.

It leaves alone partitions that are already green.

The schedule stays **STOPPED by default** and **dry-run only**.
Operators enable it manually. Dry-run idempotency means that if
this schedule fires concurrently with the sensor and they both
target the same partition, no damage occurs — dry-run writes
nothing.

## Goal

Upgrade `coord_enrichment_maxima_raw_weekly_schedule` to emit a
RunRequest only for partitions that have no successful
materialization on record.

Behavior invariants:

- If the instance has zero known partitions → 0 RunRequests.
- If all known partitions are materialized → 0 RunRequests.
- If N partitions are known and M have been materialized →
  (N − M) RunRequests.
- Failed/crashed runs do not count as materialized (Dagster
  materialization events fire only on success).

## Edits

### 1. `aimdl_coord_enrichment/schedules.py`

Replace the body of `coord_enrichment_maxima_raw_weekly_schedule`
with gap-filling logic. The fan-out-to-all shim from Step 2 is
removed.

```python
from dagster import AssetKey  # add to imports if not present

# ... existing imports and module setup unchanged ...


@schedule(
    job_name="coord_enrichment_maxima_raw_job",
    cron_schedule="0 4 * * 0",
    execution_timezone=_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)
def coord_enrichment_maxima_raw_weekly_schedule(
    context: ScheduleEvaluationContext,
):
    """Weekly gap-filling reconciliation for MAXIMA raw partitions.

    Enumerates all registered (data_type, run) partitions and emits
    a dry-run RunRequest for each that has no successful
    materialization. Partitions the discovery sensor already
    processed successfully are skipped.

    STOPPED by default; dry-run only. Safe to overlap with the
    sensor — dry-run writes nothing.
    """
    instance = context.instance

    all_keys = MAXIMA_RAW_PARTITIONS.get_partition_keys(
        dynamic_partitions_store=instance
    )

    # Use whichever Dagster API the installed version exposes for
    # "which partitions of this asset have been successfully
    # materialized?" Prefer get_materialized_partitions if available;
    # otherwise fall back to per-key get_latest_materialization_event.
    asset_key = AssetKey("enriched_maxima_raw")
    materialized = _materialized_partitions(instance, asset_key)

    gap_keys = [k for k in all_keys if str(k) not in materialized]

    context.log.info(
        "maxima_raw reconciliation: %d known, %d materialized, %d gaps",
        len(all_keys), len(materialized), len(gap_keys),
    )

    for key in gap_keys:
        yield RunRequest(
            run_key=f"reconciliation|{key}",
            partition_key=str(key),
            run_config=_dry_run_config(_MAXIMA_RAW_OPS),
            tags={
                "phase5": "reconciliation",
                "partition": str(key),
                "dry_run": "true",
            },
        )


def _materialized_partitions(instance, asset_key) -> set[str]:
    """Return the set of partition-key strings with at least one
    successful materialization for the given asset.

    Uses whatever API the installed Dagster version exposes.
    """
    # Prefer the bulk API when available; it's one call.
    if hasattr(instance, "get_materialized_partitions"):
        return set(instance.get_materialized_partitions(asset_key))

    # Fallback: enumerate via event log. This path is slower but
    # correct for older Dagster versions.
    all_keys = MAXIMA_RAW_PARTITIONS.get_partition_keys(
        dynamic_partitions_store=instance
    )
    out: set[str] = set()
    for key in all_keys:
        evt = instance.get_latest_materialization_event(
            asset_key, partition=str(key)
        )
        if evt is not None:
            out.add(str(key))
    return out
```

Note: `_materialized_partitions` is deliberately defined inside
`schedules.py` rather than as a general utility — it's only used
by this schedule, and inlining keeps the module self-contained.
Move it to a shared location only if another step needs it.

The `run_key=f"reconciliation|{key}"` prefix is a precaution against
the edge case where the sensor and reconciliation both emit a
RunRequest for the same partition in a narrow window; distinct
`run_key` prefixes ensure both Dagster runs get created (and both
are harmless in dry-run).

### 2. `tests/test_schedules.py`

Add tests. Use `DagsterInstance.ephemeral()` and record
materializations directly via `instance.report_runless_asset_event`
or the appropriate version-matched API to simulate materialized
partitions. If the repo's existing tests for schedules already show
a pattern for this, follow it.

Test cases:

```python
def test_maxima_raw_reconciliation_empty_instance():
    """Zero known partitions → zero RunRequests."""
    instance = DagsterInstance.ephemeral()
    ctx = build_schedule_context(instance=instance)
    result = list(coord_enrichment_maxima_raw_weekly_schedule(ctx))
    assert result == []


def test_maxima_raw_reconciliation_all_gaps():
    """Registered partitions, none materialized → RunRequest per partition."""
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("maxima_raw_run", ["K1//T1", "K2//T2"])
    ctx = build_schedule_context(instance=instance)
    result = list(coord_enrichment_maxima_raw_weekly_schedule(ctx))
    # 2 aimdl keys × 2 data_types = 4 partitions, all gaps
    assert len(result) == 4
    # All should be dry-run
    assert all(rr.tags.get("dry_run") == "true" for rr in result)


def test_maxima_raw_reconciliation_partial_gaps():
    """Some materialized, some not → only the gaps."""
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("maxima_raw_run", ["K1//T1"])
    # Report a successful materialization for one of the two
    # (data_type, K1//T1) pairs using the repo's established pattern.
    _report_materialization(
        instance,
        asset_key=AssetKey("enriched_maxima_raw"),
        partition_key=MultiPartitionKey(
            {"data_type": "xrd_raw", "run": "K1//T1"}
        ),
    )
    ctx = build_schedule_context(instance=instance)
    result = list(coord_enrichment_maxima_raw_weekly_schedule(ctx))
    # Only the xrf_raw × K1//T1 partition remains a gap
    assert len(result) == 1
    assert "xrf_raw" in str(result[0].partition_key)
```

Where `_report_materialization` is either
`instance.report_runless_asset_event(AssetMaterialization(...))` or
the equivalent in the installed Dagster version. If the right API
is unclear, look at how other tests in this repo record asset
materializations for fixtures.

## Verification

```bash
.venv/bin/pytest
```

Full suite must pass.

## Commit

```
git add aimdl_coord_enrichment/schedules.py tests/test_schedules.py
git commit -m "schedules: gap-filling reconciliation for MAXIMA raw (#23)

- Upgrade coord_enrichment_maxima_raw_weekly_schedule from fan-out
  to gap-filling: emit RunRequests only for registered partitions
  with no successful materialization.
- STOPPED by default; dry-run only; safe to overlap with the
  discovery sensor.
- Add _materialized_partitions helper with fallback to per-key
  event-log queries for older Dagster versions."
```

## Success criteria

- `coord_enrichment_maxima_raw_weekly_schedule` emits RunRequests
  only for partitions without a successful materialization.
- Three new tests cover empty-instance, all-gaps, and partial-gaps.
- Full pytest suite passes.
- Schedule still defaults to STOPPED and writes only in dry-run.
- One new commit.

## Out of scope

- Applying the same upgrade to `helix_alpss` or `maxima_derived`
  weekly schedules. Their partition shapes haven't changed; their
  simple fan-out still works.
- Any concurrency tagging / run queue tuning.
