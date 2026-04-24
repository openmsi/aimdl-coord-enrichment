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

Once the dry rehearsal is green, run the one-shot live sweep
script:

```
bash operations/run_live_sweep.sh
```

The script:

1. Re-runs the pre-flight check
2. Confirms with the operator that env vars point at production
3. Invokes each partitioned job per partition key with
   `dry_run=False`
4. Writes a launch log to `operations/log/sweep-<timestamp>.log`

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
