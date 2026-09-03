# Runbook — Coord Enrichment Production Sweep

## Scope

A single live sweep of the coordinate-enrichment DAG against the
AIMD-L Girder collection at data.htmdec.org. Every in-scope
item gets `Station_X/Y`, `Sample_X/Y`, and `coord_provenance`.

**Not in scope:** automating this. The runbook is for deliberate
operator-initiated sweeps. Scheduled sweeps ship `dry_run=True`
by default; a live sweep is a one-shot action by an operator
who has read and understood this document.

**Also not in scope:** the 47 multi-channel HELIX experiment logs.
They carry `PDV_<n>_FileName` columns rather than a bare
`PDV_FileName`, so the `girder-consumers/helix-otherdata` tagger never
gives them `meta.data_type` and they never appear in
`/aimdl/partition`. They are invisible to this sweep and need an
upstream consumer fix first. This sweep covers the 258 tagged
single-channel logs plus all of MAXIMA.

## The three flows

| Flow | Job | Partitioning | Writes to |
|---|---|---|---|
| HELIX logs | `process_helix_assets_job` | dynamic `helix_experiment_log`, one key per `<igsn>//<experiment_date>` | `pdv_trace` items |
| HELIX ALPSS | `coord_enrichment_helix_alpss_job` | 3 static `HELIX/pdv_alpss_*` partitions | ALPSS items, by inheritance from the parent trace |
| MAXIMA | `coord_enrichment_maxima_job` | dynamic `maxima_run`, one key per AIMD-L run | `xrd_raw`, `xrf_raw`, `xrd_derived`, `xrd_visualization` |

## Reading results under the exclusion policy

Items the pipeline cannot enrich for a structural reason — no
`instructions.txt` in the run, an unparseable filename, a scan point out
of range, a parent that is not yet enriched — are **classified,
counted, and removed from the success-rate denominator**. They are not
errors. See `aimdl_coord_enrichment/coord_enrichment/exclusions.py` for
the reason vocabulary.

The consequence for an operator: **a green check does not by itself mean
work happened.** A partition that excluded everything it saw still
passes `enrichment_success_rate_*`. Read the `in_scope` and
`excluded_by_reason` output metadata on every leaf, not the check
colours alone. `operations/dry_run_readiness.py` blocks GO on a leaf
that saw items but has `in_scope == 0`, for exactly this reason.

Similarly, HELIX logs row every *candidate* shot; the station decides at
fire time. Shots that never fired are reported under
`shots_not_fired` / `not_fired_by_reason` and are **not** counted as
coverage gaps.

## Pre-flight

1. **Env vars set in the Dagster deployment's environment:**
   - `GIRDER_API_URL` — e.g. `https://data.htmdec.org/api/v1`
   - `GIRDER_API_KEY` — must have write access to the
     collection
   - `COORD_TRANSFORMS_YAML` — absolute path to the transform
     YAML the deployment should use
   - `COORD_ENRICHMENT_MANIFEST_ITEM` — Girder item id that
     will receive `meta.coord_enrichment_status`; create this
     item first if it doesn't exist. Must be a bare item id, not a
     URL, and must be a real Mongo item — virtual `wtlocal:` items
     under `Home` reject metadata writes.
   - `HELIX_FOLDER_ID` — unchanged from the existing DAG
2. **Target Girder item exists** for the manifest. If not:
   ```
   girder-client ... addItem <parent_folder_id> coord_enrichment_status_tracker
   ```
   Record the item id in `COORD_ENRICHMENT_MANIFEST_ITEM`.
3. **Full test suite green:**
   ```
   source .venv/bin/activate && pytest tests/ -v
   ```
4. **Dagster dev instance loads without errors:**
   ```
   dagster dev
   ```
   and visually inspect that all five jobs are listed.
5. **Dynamic partitions are registered.** Both dynamic dims are
   populated by their discovery sensor. Register them without
   submitting runs by using each sensor's **Test / single-evaluation**
   button and **leaving the sensor STOPPED** — a *running* sensor also
   submits its RunRequests, up to 1,664 for MAXIMA.
   - `helix_trace_discovery_sensor` → `helix_pdv_trace`
   - `maxima_run_discovery_sensor` → `maxima_run`

## Dry-run rehearsal

> For a full read-only production-readiness evaluation with a GO/NO-GO
> rubric (scripted via `operations/dry_run_readiness.py` or the UI), see
> [`readiness_dry_run.md`](readiness_dry_run.md). The rehearsal below is
> the lighter-weight single-partition smoke check.

Run each flow once with `dry_run=True` (the default). One partition per
flow is enough; you do not need to sweep every run during dry-run
verification.

- `process_helix_assets_job` — pick one `<igsn>//<experiment_date>` key
- `coord_enrichment_helix_alpss_job` — one of the three ALPSS partitions
- `coord_enrichment_maxima_partition_job` — pick one `maxima_run` key
  (a plain string; the multi-dimensional `(data_type, run)` key of the
  issue-23 era is gone)

Verify every run:

- Ended green in the Dagster UI
- The relevant `enrichment_success_rate_*` check passed
- No `no_coord_transform_failures_*` / `coord_transform_check` fired
- The asset output shows `written == 0` and either
  `simulated_dry_run > 0` (coord_enrichment leaves) or
  `items_simulated > 0` (`pdv_data`)
- `in_scope > 0`, and the `excluded_by_reason` breakdown is one you
  can explain

If any run shows resolution errors or unexplained exclusions, stop.
Investigate, fix, and re-run the dry rehearsal before proceeding.

### Enabling the discovery sensors

For ongoing operation, the preferred pattern is to **start
`maxima_run_discovery_sensor`** and
**`helix_trace_discovery_sensor`** in the Dagster UI. Each
tick (hourly minimum) the MAXIMA sensor will:

1. Fetch the partition index for `xrd_raw`, `xrf_raw`, `xrd_derived`,
   and `xrd_metadata`.
2. Register any new `"<igsn>//<experiment_date>"` keys on the
   `maxima_run` dynamic dim.
3. Emit one `RunRequest` per run key, dedupping on a run_key that
   composes every per-data-type content hash plus the `xrd_metadata`
   hash — so unchanged partitions are silently suppressed on
   subsequent ticks.

On first enablement, expect hundreds of new partition keys in a
single tick (bounded by the current AIMD-L partition count).
Treat the first tick as a one-time catch-up event. Sensor runs
inherit the job's default `dry_run=True`; flip `dry_run=False`
in the sensor-launched runs only after a live rehearsal against a
single partition has passed.

## Live sweep

Once the dry rehearsal is green, perform the live sweep through the
Dagster UI. There is no shell-driven entry point — the previous
`operations/run_live_sweep.sh` had drift defects from issue-23 and
was retired (see `docs/archive/run_live_sweep/`). The schedules in
`aimdl_coord_enrichment/schedules.py` already cover automated sweeps; this
section covers the one-shot interactive sweep an operator drives by
hand.

### Order of operations

Run the flows in this order — HELIX ALPSS inherits coordinates from the
`pdv_trace` parents that the HELIX log flow writes:

1. **HELIX logs first.** `process_helix_assets_job` writes
   `Station_X/Y`, `Sample_X/Y`, and `coord_provenance` onto the matched
   `pdv_trace` items.
2. **HELIX ALPSS second.** Inherits from those now-enriched traces.
3. **MAXIMA at any point.** It is fully independent of HELIX — it
   resolves coordinates from each run's `instructions.txt`, not by
   inheritance from a HELIX parent. Running it first, last, or
   concurrently is equally correct.

Running HELIX ALPSS before the HELIX logs is not an *error* under the
exclusion policy — its items are classified `parent_not_enriched`,
dropped from the denominator, and the checks pass. It simply does no
work. That is why order matters even though nothing turns red.

### Operator confirmation discipline

Before clicking Launch on any job with `dry_run: false`:

1. Re-confirm the deployment's env vars point at the intended Girder
   instance and `COORD_ENRICHMENT_MANIFEST_ITEM`.
2. Confirm verbally with at least one other team member, or pause
   long enough to read back the job name, the partition selection,
   and `dry_run: false` to yourself before clicking.
3. The launchpad shows the full run config preview before submission.
   Read it.

This is the protocol that lived in the retired bash script's "Type
LIVE SWEEP to proceed" gate. Without an automated tool, the discipline
moves to the operator.

### Step 1 — HELIX experiment logs

Open the Dagster UI launchpad for `process_helix_assets_job`. Its
partitions are the dynamic `helix_experiment_log` dim, keyed on
`"<igsn>//<experiment_date>"` and populated by
`helix_trace_discovery_sensor`.

For the first live sweep, pick one or a few partitions in the UI's
partition selector. Run config:

```yaml
ops:
  pdv_data:
    config:
      dry_run: false
  pdv_processing_manifest:
    config:
      dry_run: false
```

`pdv_log` takes no config — do not add it, Dagster will reject the run.

Click Launch. Widen the partition selection once a targeted sweep has
verified end-to-end against production.

Wait for these runs to complete before Step 2. HELIX ALPSS depends on
the coordinates now sitting on the `pdv_trace` items.

### Step 2 — HELIX ALPSS

Open the launchpad for `coord_enrichment_helix_alpss_job`. Three
static partitions:

- `HELIX/pdv_alpss_output`
- `HELIX/pdv_alpss_result`
- `HELIX/pdv_alpss_results`

Either select all three at once or run them one at a time. Run config:

```yaml
ops:
  helix_alpss_provenance_tagged:
    config:
      dry_run: false
  enriched_helix_alpss:
    config:
      dry_run: false
```

Click Launch.

### Step 3 — MAXIMA

Open the launchpad for `coord_enrichment_maxima_job`. Its partitions
are the dynamic `maxima_run` dim, populated by
`maxima_run_discovery_sensor`. Choices:

- **For a small, targeted sweep** (recommended for the first live
  sweep): pick one or a few run partitions in the UI's partition
  selector. Provide run config:

  ```yaml
  ops:
    enriched_maxima_run:
      config:
        dry_run: false
  ```

  Click Launch. Repeat for each desired partition.

- **For a full sweep across every registered partition** (recommended
  only after the targeted sweep above has verified end-to-end against
  production): instead of the launchpad, **set
  `coord_enrichment_maxima_weekly_schedule` to STARTED with
  `dry_run: false` in its run config**. The schedule is gap-filling
  reconciliation — it enumerates registered partitions and fires only
  for partitions without a successful materialization. This is what
  the retired bash script tried to do, but the schedule does it
  correctly.

  After one cycle has run to completion, set the schedule back to
  STOPPED. Live writes are not for ongoing automation; the schedule's
  default `dry_run: true` setting should be the steady state.

### Audit trail

Every run's full result — asset materializations, asset check
outcomes, output metadata, error logs — lives on the run's page in
the Dagster UI under Runs. That is the authoritative record. The
retired script wrote a parallel `operations/log/sweep-*.log`; the
Dagster run history replaces it.

The `coord_enrichment_manifest` asset writes a summary record to
`COORD_ENRICHMENT_MANIFEST_ITEM` in Girder via
`meta.coord_enrichment_status`, recording timestamp, run id, pipeline
version, dry-run state, and the per-leaf state report. Confirm this
write completed by inspecting the manifest item in the Girder UI
after each sweep.

## Monitoring the sweep

Watch the Dagster UI asset check panel:

- `all_helix_alpss_tagged` must be green after
  `helix_alpss_provenance_tagged` materializes.
- `igsn_consistency` and `manifest_written` (both ERROR) must be green
  on each `process_helix_assets_job` partition.
- Per-partition `enrichment_success_rate_*`,
  `no_coord_transform_failures_*`, and `coord_transform_check` should
  be green.
- `pdv_match_rate` and `pdv_coverage_above_threshold` are coverage
  signals, not gates. Record them. Expect ~92% pairing where a log is
  tagged; partitions without one pass with `log_items: 0`.

Then read the per-leaf `in_scope` / `excluded_by_reason` metadata, per
the exclusion-policy section above. Green checks over a collapsed
denominator are the failure mode this sweep is most likely to hit.

Check the manifest Girder item: it should now have a
`meta.coord_enrichment_status` payload with today's timestamp,
`dry_run: false`, and per-leaf counts matching the Dagster
asset outputs.

## Rollback

The DAG writes only metadata. No file content is modified. A
rollback, if needed, is just another sweep with a restored
transform YAML or with explicit payload corrections. The
`coord_provenance` on each item records the exact transform
used, so "what did this write?" is always answerable.

If a specific item needs its provenance manually cleared, that
is a Girder UI action — outside the scope of this runbook.

## After the first live sweep

1. Record the observed values for every asset check in
   `docs/runbooks/first_sweep_expected_values.md`. These become the
   reference for future sweeps.
2. Consider turning on `coord_enrichment_state_report_schedule`
   (nightly) once the manifest item and env vars are confirmed
   stable.
3. Consider starting `maxima_run_discovery_sensor` and
   `helix_trace_discovery_sensor`. On first tick each will
   register every current AIMD-L key and emit a RunRequest per key;
   all subsequent ticks suppress unchanged partitions via the
   content-hash dedup key.
4. The weekly `coord_enrichment_maxima_weekly_schedule` is
   gap-filling reconciliation — it enumerates the registered
   partitions and emits RunRequests only for partitions with no
   successful materialization. Still STOPPED by default, still
   dry-run only. Enable once the sensor has been running long
   enough to trust that gaps are real (not sensor-tick latency).
5. Leave the HELIX ALPSS weekly sweep schedule STOPPED until a team
   decision is made on cadence.
