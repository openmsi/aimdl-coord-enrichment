# First-sweep expected values

This document records what each asset check should report on the
first live sweep against a well-populated test or production
collection. Deviations on subsequent runs are worth
investigating.

**Principle.** These values are the reference point, not the
contract. They will change as the collection grows. Update this
document after each deliberate sweep so the baseline stays
current.

## Reading the counters

Under the exclusion policy
(`aimdl_coord_enrichment/coord_enrichment/exclusions.py`), items the
pipeline cannot enrich for a structural reason are counted and
**removed from the success-rate denominator**. So for every leaf,
compare against `in_scope`, not `seen`, and read `excluded_by_reason`
alongside the check result. A leaf that excluded everything it saw
still passes its success-rate check.

The HELIX flow expresses the same idea as paired vs. unpaired traces. Its
denominator is the traces in the partition; a trace that finds no row, more
than one, or a row declaring another sample is unpaired and reported by
reason. Unpaired is not necessarily a defect — most of it is sessions whose
experiment log was never tagged upstream.

## Measured production survey — 2026-09-01 (full dry sweep, read-only)

290 HELIX trace partitions, 0 job failures, 0 writes.

### HELIX traces (`pdv_data`)

Of the traces whose session has a tagged experiment log, **3,695 of 4,006
(92.2%)** paired to their row and produced coordinates. Residual: 254 found no
row, 57 were refused as `ambiguous_row`.

Transform versions: `HELIX/v1` 965, `HELIX/v2` 2,730. Coord failures 0.
`igsn_consistency` passes on all 290 partitions.

Partitions whose experiment log is not tagged upstream pair nothing; they
record `log_items: 0` and pass. See SPEC‑HELIX‑04c — the fix is upstream, not
here.

### MAXIMA (`enriched_maxima_run`)

- ~195k in-scope items across 1,664 run partitions.
- 1,603 partitions have `instructions.txt`; **61 do not** and are excluded as
  `no_instructions` — expect that reason in the exclusion breakdown.
- Calibrant runs (43) are out of scope by decision: no `instructions.txt`, and
  no `scan_point_<i>` on any of their 125 files.
- **Not yet dry-run at corpus scale** — `maxima_run` partitions are registered
  by `maxima_run_discovery_sensor`.

### HELIX ALPSS (`enriched_helix_alpss`)

- 66,848 ALPSS items resolve to a unique parent trace (100%):
  `pdv_alpss_output` 59,420, `pdv_alpss_result` 7,428, `pdv_alpss_results` 0 —
  the plural partition is empty and will do nothing.
- Run **after** the HELIX traces. Run before and every item is excluded as
  `parent_not_enriched` — the checks still pass and nothing happens.
  `operations/dry_run_readiness.py` blocks GO on that collapsed denominator.
- Only traces that got coordinates can pass them down, so ALPSS inherits the
  same reachability ceiling.

## Test collection (coordinate_dag_test_data, 450 files)

Known composition:

- HELIX: 228 files (one experiment-log CSV, 225 ALPSS outputs
  across the 9 suffix variants, plus raw PDV traces and the
  log itself)
- MAXIMA: 192 files across 3 run folders; JHAMAL00018-009 has
  25 scan points -> 25 xrd_raw + 25 xrf_raw + ~75 xrd_derived
  scan files

All 450 files carry `meta.igsn` and `meta.data_type`.

### coord_enrichment_job (state-report only)

| Asset check                         | Expected            |
|-------------------------------------|---------------------|
| inventory_nonempty_per_instrument   | PASS                |
| all_helix_alpss_tagged              | PASS (0 unresolved) |
| pdv_coverage_above_threshold        | PASS if the HELIX log flow has enriched the PDV traces; otherwise WARN -- this does not block enrichment |

Asset output counters (from the Dagster UI):

- `enrichable_items_inventory.total_items` -- expect ~> 350 (50+
  MAXIMA raw, ~75 MAXIMA derived, ~225 HELIX ALPSS variants,
  after filtering out non-IGSN items).
  `enriched_maxima_run` does not consume this inventory — it
  fetches per-partition items directly from
  `/aimdl/partition/details`.
- `helix_alpss_provenance_tagged.total_writes` on first dry-run:
  roughly equal to the HELIX ALPSS item count; on subsequent
  dry-runs: 0 (already tagged, already correct). MAXIMA
  xrd_derived items are not part of this counter — their prov is
  owned by amdee_xrd upstream.

### process_helix_assets_job -- one `helix_pdv_trace` partition

Partition keys are the plain AIMD-L string `"<igsn>//<experiment_date>"` of
the PDV traces.

| Asset check              | Severity | Expected |
|--------------------------|----------|----------|
| zero_traces_in_partition | ERROR    | PASS (partition holds traces) |
| igsn_consistency         | ERROR    | PASS (0 refused pairs) |
| manifest_written         | ERROR    | PASS |
| enrichment_success_rate  | WARN     | PASS (rate >= 0.9 of paired) |
| coord_transform_check    | WARN     | PASS (0 failures) |
| igsn_validity_rate       | WARN     | record only |
| pdv_match_rate           | WARN     | PASS (passes with `log_items: 0` when no log is tagged) |

Counts (per partition):

- `traces_in_partition` -- the denominator: every annotated trace for this
  session
- `paired_count` -- traces that found exactly one log row naming them
- `unpaired_by_reason` -- `no_row_in_log` / `ambiguous_row` / `igsn_mismatch`
- `log_items` -- 0 means no experiment log is tagged for this session, so
  nothing can pair
- `items_enriched` -- equal to `paired_count` minus coord failures
- `items_simulated` -- 0 when live, equal to `paired_count` in dry-run

### coord_enrichment_maxima_partition_job -- one `maxima_run` partition

Partition keys are the plain AIMD-L string `"<igsn>//<experiment_date>"`.
(The `MultiPartitionKey({data_type, run})` of the issue-23 era is gone;
one partition now covers every data type in the run.)

| Asset check                                  | Expected |
|---------------------------------------------|----------|
| enrichment_success_rate_maxima              | PASS (rate >= 0.9) |
| no_coord_transform_failures_maxima          | PASS     |

Counts (on first live sweep, per partition):

- `seen` -- number of items for this run, all data types
- `in_scope` -- `seen` minus exclusions; the success-rate denominator
- `excluded` / `excluded_by_reason` -- structurally un-enrichable
  items; `no_instructions` dominates on the 61 partitions lacking
  `instructions.txt`
- `written` -- equal to `in_scope` minus resolution_errors and
  coord_failures
- `simulated_dry_run` -- 0 when live
- `skipped_no_change` -- 0 on first live write; will become the
  bulk of subsequent runs triggered by the discovery sensor on
  content-hash unchanged partitions (those are suppressed by the
  dedup run_key anyway and will not produce a run)

### coord_enrichment_helix_alpss_job -- each ALPSS partition

| Asset check                                  | Expected |
|---------------------------------------------|----------|
| all_helix_alpss_tagged                      | PASS (ERROR severity, 0 unresolved) |
| enrichment_success_rate_helix_alpss         | PASS (rate >= 0.9) |
| no_coord_transform_failures_helix_alpss     | PASS     |

`pdv_alpss_output` typically has the highest count (one per
shot). `pdv_alpss_result` and `pdv_alpss_results` reflect
singular/plural filename conventions and will vary by shot.

If `excluded_by_reason` is dominated by `parent_not_enriched`, the
HELIX log flow has not run (or has not covered these shots) yet.

## Tuning decisions after first sweep

1. **`PDV_COVERAGE_WARN_THRESHOLD`** -- currently 0.5 (arbitrary
   placeholder). If the first sweep shows PDV coverage is
   consistently >= 0.9 once the HELIX log flow has caught up,
   bump the threshold to 0.8 or 0.9 so the check flags real
   drops. If coverage is persistently low because PDV traces are
   being added faster than the log flow processes them,
   either lower the threshold or accept that the check reports
   an operational reality worth knowing about.
2. **Weekly sweep cadence** -- if both weekly sweeps
   complete in under an hour and writes are few (mostly
   `skipped_no_change`), weekly is generous and can likely
   move to bi-weekly. Revisit after 4 weeks of data.
3. **State-report schedule** -- if the nightly run consistently
   shows nothing has changed, drop to every other night. This
   is a cost optimization, not a correctness concern.
4. **The 254 traces with no row in a tagged log** -- the residual after the
   85 no-log partitions are set aside. Worth a look once the upstream log
   tagging lands, since that may absorb some of them.

## How to update this document

After every sweep that changes the expected baseline
(new instrument, YAML recalibration, new data types), edit this
file in the same commit as the change. Note the sweep date and
why the numbers changed.

## History

- **2026-09-01**: rewritten for the trace-driven HELIX flow; numbers are from
  the full 290-partition dry sweep of that day. MAXIMA figures are still from
  read-only probes, not a corpus-scale dry run.
