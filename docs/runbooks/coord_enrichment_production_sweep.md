# Runbook — Coord Enrichment Production Sweep

## Scope

A single live sweep of the coordinate-enrichment DAG against the
AIMD-L Girder collection at data.htmdec.org. Every in-scope
item gets `Station_X/Y`, `Sample_X/Y`, and `coord_provenance`.

**Not in scope:** automating this. The runbook is for deliberate
operator-initiated sweeps. Scheduled sweeps ship `dry_run=True`
by default; a live sweep is a one-shot action by an operator
who has read and understood this document.

## Pre-flight

1. **Env vars set in the Dagster deployment's environment:**
   - `GIRDER_API_URL` — e.g. `https://data.htmdec.org/api/v1`
   - `GIRDER_API_KEY` — must have write access to the
     collection
   - `COORD_TRANSFORMS_YAML` — absolute path to the transform
     YAML the deployment should use
   - `COORD_ENRICHMENT_MANIFEST_ITEM` — Girder item id that
     will receive `meta.coord_enrichment_status`; create this
     item first if it doesn't exist
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

## Dry-run rehearsal

> For a full read-only production-readiness evaluation with a GO/NO-GO
> rubric (scripted via `operations/dry_run_readiness.py` or the UI), see
> [`readiness_dry_run.md`](readiness_dry_run.md). The rehearsal below is
> the lighter-weight single-partition smoke check.

Run once with `dry_run=True` (the default).

**MAXIMA raw** is now partitioned on
`MultiPartitionsDefinition({data_type, run})` where `data_type` ∈
`{xrd_raw, xrf_raw}` and `run` is dynamic, keyed on the AIMD-L
partition string `"<igsn>//<experiment_date>"`. Partition keys are
populated by `maxima_raw_discovery_sensor` (STOPPED by default). To
materialize by hand from the Dagster UI, launch
`coord_enrichment_maxima_raw_partition_job` with a
`MultiPartitionKey({"data_type": "<dt>", "run": "<igsn>//<experiment_date>"})`.
Pick one `(data_type, run)` for rehearsal; you do not need to
sweep every run during dry-run verification.

Then dry-run the HELIX and MAXIMA-derived jobs, which still use
static partitions:

- `coord_enrichment_helix_alpss_job` — each of three ALPSS partitions
  (`HELIX/pdv_alpss_output`, `HELIX/pdv_alpss_result`,
  `HELIX/pdv_alpss_results`)
- `coord_enrichment_maxima_derived_job` / `MAXIMA/xrd_derived`

Verify every run:

- Ended green in the Dagster UI
- The relevant `enrichment_success_rate_*` check passed
- No `_coord_transform_failures_*` fired
- For `enriched_maxima_derived`, `maxima_xrd_derived_provenance_valid`
  (ERROR severity) passed
- The asset output shows `simulated_dry_run` count > 0 and
  `written == 0`

If any run shows resolution errors or unexpected skips, stop.
Investigate, fix, and re-run the dry rehearsal before
proceeding.

### Enabling the discovery sensor

For ongoing operation, the preferred pattern is to **start
`maxima_raw_discovery_sensor`** in the Dagster UI. Each tick
(hourly minimum) the sensor will:

1. Fetch the partition index for `xrd_raw`, `xrf_raw`, and
   `xrd_metadata`.
2. Register any new `"<igsn>//<experiment_date>"` keys on the
   `maxima_raw_run` dynamic dim.
3. Emit one `RunRequest` per `(data_type, aimdl_key)`, dedupping
   on a run_key that composes both the raw and `xrd_metadata`
   content hashes — so unchanged partitions are silently
   suppressed on subsequent ticks.

On first enablement, expect hundreds of new partition keys in a
single tick (bounded by the current AIMD-L partition count).
Treat the first tick as a one-time catch-up event. Sensor runs
inherit the job's default `dry_run=True`; flip `dry_run=False`
in the sensor-launched runs only after a live MAXIMA raw rehearsal
against a single partition has passed.

## Live sweep

Once the dry rehearsal is green, perform the live sweep through the
Dagster UI. There is no shell-driven entry point — the previous
`operations/run_live_sweep.sh` had drift defects from issue-23 and
was retired (see `docs/archive/run_live_sweep/`). The schedules in
`aimdl_coord_enrichment/schedules.py` already cover automated sweeps; this
section covers the one-shot interactive sweep an operator drives by
hand.

### Order of operations

The three sibling jobs must run in this order — derived leaves
inherit from parent items written by raw leaves and by the
spreadsheet DAG:

1. **MAXIMA raw first.** Writes `Station_X/Y`, `Sample_X/Y`, and
   `coord_provenance` on `xrd_raw` and `xrf_raw` items.
2. **HELIX ALPSS and MAXIMA derived second**, in either order
   relative to each other. Both inherit from already-enriched
   parents (PDV traces and `xrd_raw` master.h5 respectively).

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

### Step 1 — MAXIMA raw

Open the Dagster UI launchpad for `coord_enrichment_maxima_raw_job`.

The job's partitions are
`MultiPartitionsDefinition({data_type, run})`, where the `run` dim is
dynamic and populated by `maxima_raw_discovery_sensor`. Choices:

- **For a small, targeted sweep** (recommended for the first live
  sweep): pick one or a few `(data_type, run)` partitions in the UI's
  partition selector. Provide run config:

  ```yaml
  ops:
    enriched_maxima_raw:
      config:
        dry_run: false
  ```

  Click Launch. Repeat for each desired partition.

- **For a full sweep across every registered partition** (recommended
  only after the targeted sweep above has verified end-to-end against
  production): instead of the launchpad, **set
  `coord_enrichment_maxima_raw_weekly_schedule` to STARTED with
  `dry_run: false` in its run config**. The schedule is gap-filling
  reconciliation — it enumerates registered partitions and fires only
  for partitions without a successful materialization. This is what
  the retired bash script tried to do, but the schedule does it
  correctly.

  After one cycle has run to completion, set the schedule back to
  STOPPED. Live writes are not for ongoing automation; the schedule's
  default `dry_run: true` setting should be the steady state.

Wait for all MAXIMA raw runs to complete before moving on. Both
inherited-leaf jobs depend on these results being on the items.

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

### Step 3 — MAXIMA derived

Open the launchpad for `coord_enrichment_maxima_derived_job`. Single
static partition `MAXIMA/xrd_derived`. Run config:

```yaml
ops:
  enriched_maxima_derived:
    config:
      dry_run: false
```

Click Launch.

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
- `maxima_xrd_derived_provenance_valid` must be green after
  `enriched_maxima_derived` materializes. It fails if any
  xrd_derived item has a missing or dangling
  `prov.wasDerivedFrom` — a data-hygiene signal about amdee_xrd
  upstream, not a bug in this pipeline.
- Per-partition enrichment success checks should be green.
- `pdv_coverage_above_threshold` reflects the observer — not
  written to by the live sweep but useful cross-check.

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
   `docs/runbooks/first_sweep_expected_values.md` (created in
   Phase 5 Step 4). These become the reference for future
   sweeps.
2. Consider turning on `coord_enrichment_state_report_schedule`
   (nightly) once the manifest item and env vars are confirmed
   stable.
3. Consider starting `maxima_raw_discovery_sensor`. On first
   tick it will register every current AIMD-L `(data_type, run)`
   pair and emit a RunRequest for each; all subsequent ticks
   suppress unchanged partitions via the composed
   raw+xrd_metadata content-hash dedup key.
4. The weekly `coord_enrichment_maxima_raw_weekly_schedule` is
   now gap-filling reconciliation — it enumerates the registered
   partitions and emits RunRequests only for partitions with no
   successful materialization. Still STOPPED by default, still
   dry-run only. Enable once the sensor has been running long
   enough to trust that gaps are real (not sensor-tick latency).
5. Leave the HELIX ALPSS and MAXIMA derived weekly sweep
   schedules STOPPED until a team decision is made on cadence.
