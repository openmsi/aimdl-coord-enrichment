# Runbook — Production-readiness dry run (coord_enrichment)

## Purpose

Decide whether the enrichment DAG is ready to write coordinate metadata
to the live AIMD-L Girder collection. Every Girder write is gated by a
`dry_run` flag (default `True` — `CoordEnrichmentConfig` for the
coord_enrichment leaves, `HelixSpreadsheetConfig` for the HELIX log
flow), so a dry run reads real production data, computes every would-be
write, runs every asset check, and **writes nothing**. The output is a
single **GO / NO-GO** decision.

The real question a dry run answers is **upstream data hygiene** —
whether production items carry the tags the DAG depends on (`meta.igsn`,
`meta.data_type`, and `prov.wasDerivedFrom`/`isPartOf`), and whether the
population that survives the exclusion policy is large enough to be
worth sweeping.

This is read-only. For defense in depth, run it with a **read-only Girder API
key**: dry-run writes nothing, and a read-only key makes any accidental write
fail loudly.

This runbook does **not** cover the live sweep — see
[`coord_enrichment_production_sweep.md`](coord_enrichment_production_sweep.md).

## What gets swept

Three flows, in the same dependency order as a live sweep, then the
state report:

| Flow name | Job | Partitions | Leaf harvested |
|---|---|---|---|
| `helix_traces` | `process_helix_assets_job` | dynamic `helix_experiment_log` | `pdv_data` |
| `helix_alpss` | `coord_enrichment_helix_alpss_job` | 3 static | `enriched_helix_alpss` |
| `maxima` | `coord_enrichment_maxima_partition_job` | dynamic `maxima_run` | `enriched_maxima_run` |
| `state_report` | `coord_enrichment_job` | none | — (snapshot only) |

Multi-channel HELIX logs are out of scope — they are untagged upstream
and never appear in `/aimdl/partition`. See the sweep runbook's Scope
section.

## Go/No-Go rubric

**GO requires ALL of:**

- Every ERROR-severity check PASSES. Currently:
  - `all_helix_alpss_tagged` — no unresolved HELIX ALPSS parents
  - `igsn_consistency` — spreadsheet IGSN agrees with the Girder item's
  - `zero_traces_in_partition` — the partition resolved at least one trace
  - `manifest_written` — a source log item exists and no real write failed
    (True in dry-run; it means "would have been written")
- Every `enrichment_success_rate_*` check PASSES (≥ 0.90)
- Every `no_coord_transform_failures_*` and `coord_transform_check`
  PASSES (zero coord failures)
- `written == 0` on every leaf (confirms dry-run is in force)
- Some real activity occurred — `simulated_dry_run + skipped_no_change > 0`
  across the leaves (the DAG resolved real items; `simulated > 0` on a first
  run, `skipped_no_change` dominates on re-runs of already-enriched data)
- **No leaf has a collapsed denominator** — no leaf with `seen > 0` and
  `in_scope == 0`. See below.
- `resolution_errors == 0` across all leaves
- `coord_transform_config_snapshot` reports a non-null `yaml_sha256`
- `COORD_ENRICHMENT_MANIFEST_ITEM` is set to a real Girder item (the manifest
  is not written in dry-run, but live operation needs it)

**Record but do NOT block on:**

- `inventory_nonempty_per_instrument` (WARN) — note any empty
  `(instrument, data_type)` partitions
- `pdv_coverage_above_threshold` (WARN; threshold is the placeholder `0.5` in
  `pdv_observer.py`) — record actual coverage; feeds the threshold-tuning
  decision in [`first_sweep_expected_values.md`](first_sweep_expected_values.md)
- `pdv_match_rate` (WARN) and `igsn_validity_rate` (WARN) — coverage
  measurements, not correctness gates. Expect ~92% pairing where a log is
  tagged; partitions without one pass with `log_items: 0`.
- The `unresolved` column in the report — in-scope work the leaf neither
  wrote, simulated, skipped, nor recorded as a coord failure. Should be ~0
  on every leaf.

**NO-GO signals:**

- Any blocking check fails → upstream tagging/prov not ready
- `written > 0` → dry-run breach; stop and investigate
- Non-trivial `resolution_errors` → items can't be matched to Girder
- A leaf saw items but has `in_scope == 0` → collapsed denominator
- `yaml_sha256` null → coordinate config broken in the deployment

## The exclusion policy, and why the rubric checks `in_scope`

Items the pipeline cannot enrich for a structural reason
(`no_instructions`, `unparseable_name`, `scan_point_out_of_range`,
`parent_not_enriched`, …) are classified, counted, and **removed from
the success-rate denominator**. They are not errors — see
`aimdl_coord_enrichment/coord_enrichment/exclusions.py`.

The consequence: a leaf that excluded *everything* it saw still passes
`enrichment_success_rate_*`, because the denominator is zero. Green
checks alone therefore cannot answer "did anything happen?". The rubric
blocks GO on `seen > 0 and in_scope == 0`, and the report prints an
"Exclusions by reason" section so the operator can see what was dropped
and why.

The most likely real instance of this: running `helix_alpss` before
`helix_traces` has enriched the parent PDV traces. Every ALPSS item is
then `parent_not_enriched`, and nothing turns red. The script sweeps
the flows in dependency order to avoid manufacturing this.

The HELIX log flow has its own out-of-scope population: candidate shots
that never fired. A log rows every candidate; the station decides at
fire time. Those are reported per-reason and are **not** coverage gaps.

**Verify the transform-version split (report-only, not auto-blocked):** version
selection is timestamp-driven — `pdv_data` (the HELIX root) passes
each shot's timestamp, and `enriched_helix_alpss` inherits its parent's
recorded version. HELIX `v1` applies to shots before the 2026-04-01 recalibration and
`v2` (identity, Station == Sample, after the instrument frame was realigned to
the sample frame) to shots on/after it. The report's "Transform versions
applied" section and per-leaf `versions` column show the `HELIX/v1` vs
`HELIX/v2` breakdown; confirm it matches the expected historical-vs-current
split before the live sweep. (A missing split on `enriched_helix_alpss`
usually means the parent PDV traces are not yet enriched; under the
exclusion policy those items count as `parent_not_enriched` rather than
as errors.)

## Headless path (scripted, captured)

`operations/dry_run_readiness.py` runs every flow in dry-run
in-process, evaluates the rubric, prints a verdict, and writes a report to
`operations/log/readiness_dry_run_<timestamp>.{md,json}` (gitignored).

1. Set the environment (use a **read-only** key):
   ```
   export GIRDER_API_URL="https://data.htmdec.org/api/v1"
   export GIRDER_API_KEY="<read-only key>"
   export COORD_TRANSFORMS_YAML="/abs/path/to/instrument_coordinate_transforms.yaml"
   export COORD_ENRICHMENT_MANIFEST_ITEM="<girder item id>"
   ```
2. Quick smoke to confirm wiring — one partition per dynamically-partitioned
   flow, and skip `helix_alpss`:
   ```
   .venv/bin/python operations/dry_run_readiness.py --sample 1 \
       --flows helix_traces,maxima,state_report
   ```
   `--sample` bounds only the *dynamic* flows. `helix_alpss` has three
   static partitions covering ~66,848 items and cannot be sampled — it is
   the long pole in any run that includes it, so leave it out of a wiring
   check.
3. Full enumeration — every `helix_experiment_log` partition (~214), all
   three ALPSS partitions, and every `maxima_run` partition (~1,664), each
   an in-process run. This takes hours; plan for it:
   ```
   .venv/bin/python operations/dry_run_readiness.py
   ```
4. Read the printed `VERDICT:` line and the generated `.md` report. Exit code
   is `0` for GO, `1` for NO-GO.

`--flows` selects a subset, e.g. `--flows helix_traces,state_report`.
Choices: `helix_traces`, `helix_alpss`, `maxima`, `state_report`. A partial
selection cannot produce a meaningful GO — criteria whose evidence was never
gathered (the `yaml_sha256` snapshot without `state_report`, the ALPSS checks
without `helix_alpss`) simply go unevaluated. A passing subset run therefore
prints `VERDICT: GO (PARTIAL — did not sweep: …)` rather than a bare GO. Use
`--flows` for wiring checks and triage; run the full set for the actual
decision.

## UI path (full enumeration via the Dagster UI)

Equivalent walkthrough for operators who prefer the UI. It produces the same
asset-check outcomes; read them from the asset-check panel.

1. Complete the pre-flight in
   [`coord_enrichment_production_sweep.md`](coord_enrichment_production_sweep.md)
   (env vars, manifest item, `pytest`, `dagster dev` loads).
2. **Register the dynamic partitions.** Use the Test / single-evaluation
   button on `helix_trace_discovery_sensor` and
   `maxima_run_discovery_sensor`, **leaving both STOPPED**. A *running*
   sensor also submits its RunRequests — up to 1,664 for MAXIMA. If you
   do want the sensors to drive the enumeration, starting them is
   equivalent to the full dry run for their flow (runs inherit
   `dry_run=True`); expect hundreds of runs on the first tick.
3. From the launchpad, run in dry-run (`dry_run: true` in run config), in
   this order:
   - `process_helix_assets_job` — `helix_experiment_log` partitions
   - `coord_enrichment_helix_alpss_job` — all three `HELIX/pdv_alpss_*`
     partitions
   - `coord_enrichment_maxima_job` — `maxima_run` partitions
   - `coord_enrichment_job` — the state report (run last, so the report asset
     sees leaf coverage)
4. Read the asset-check panel and the per-leaf output metadata
   (`in_scope`, `excluded_by_reason`, `written`, `simulated_dry_run`,
   `resolution_errors`, …) against the rubric.

## Results table

Fill this in from the report (or the UI) and attach it to the go/no-go
decision. On GO, transcribe the counts into
[`first_sweep_expected_values.md`](first_sweep_expected_values.md) as the
reference baseline.

| Check | Severity | Blocking | Result (PASS/FAIL) | Notes |
|---|---|:-:|:-:|---|
| `zero_traces_in_partition` | ERROR | yes | | per helix_traces partition |
| `igsn_consistency` | ERROR | yes | | per helix_traces partition |
| `manifest_written` | ERROR | yes | | per helix_traces partition |
| `all_helix_alpss_tagged` | ERROR | yes | | |
| `enrichment_success_rate` | WARN | yes | | helix_traces, per partition |
| `coord_transform_check` | WARN | yes | | helix_traces, per partition |
| `enrichment_success_rate_helix_alpss` | WARN | yes | | per partition |
| `no_coord_transform_failures_helix_alpss` | WARN | yes | | per partition |
| `enrichment_success_rate_maxima` | WARN | yes | | per partition |
| `no_coord_transform_failures_maxima` | WARN | yes | | per partition |
| `igsn_validity_rate` | WARN | no | | record rate |
| `pdv_match_rate` | WARN | no | | record rate (~92% of reachable traces) |
| `inventory_nonempty_per_instrument` | WARN | no | | record empties |
| `pdv_coverage_above_threshold` | WARN | no | | record coverage |

| Leaf | seen | in_scope | excluded | written | simulated | skipped | coord_fail | res_err |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `pdv_data` (traces) | | | | | | | | |
| `enriched_helix_alpss` | | | | | | | | |
| `enriched_maxima_run` | | | | | | | | |

**Exclusions by reason:** _________________________________________

**Decision:** GO / NO-GO — _________   **Date:** _________

If NO-GO, the failing ERROR checks name the upstream tagging/prov work to do
before re-running this dry run.
