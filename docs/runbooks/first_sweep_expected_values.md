# First-sweep expected values

This document records what each asset check should report on the
first live sweep against a well-populated test or production
collection. Deviations on subsequent runs are worth
investigating.

**Principle.** These values are the reference point, not the
contract. They will change as the collection grows. Update this
document after each deliberate sweep so the baseline stays
current.

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
| maxima_prov_targets_resolve         | PASS (0 unresolved) |
| pdv_coverage_above_threshold        | PASS if prior HELIX DAG has enriched the PDV traces; otherwise WARN -- this does not block enrichment |

Asset output counters (from the Dagster UI):

- `enrichable_items_inventory.total_items` -- expect ~> 350 (50+
  MAXIMA raw, ~75 MAXIMA derived, ~225 HELIX ALPSS variants,
  after filtering out non-IGSN items and non-raw TIFFs)
- `provenance_tagged_items.total_writes` on first dry-run:
  roughly equal to (HELIX ALPSS item count) + (MAXIMA
  xrd_derived item count needing heal); on subsequent dry-runs:
  0 (already tagged, already correct)

### coord_enrichment_maxima_raw_job -- MAXIMA/xrd_raw partition

| Asset check                                  | Expected |
|---------------------------------------------|----------|
| enrichment_success_rate_maxima_raw          | PASS (rate >= 0.9) |
| no_coord_transform_failures_maxima_raw      | PASS     |

Counts (on first live sweep):

- `seen` -- number of in-scope xrd_raw items
- `written` -- equal to `seen` minus resolution_errors and
  coord_failures
- `simulated_dry_run` -- 0 when live
- `skipped_no_change` -- 0 on first live write; will become the
  bulk of subsequent runs

### coord_enrichment_maxima_raw_job -- MAXIMA/xrf_raw partition

Same shape as xrd_raw. Counts reflect xrf_raw items only.

### coord_enrichment_helix_alpss_job -- each ALPSS partition

| Asset check                                  | Expected |
|---------------------------------------------|----------|
| enrichment_success_rate_helix_alpss         | PASS (rate >= 0.9) |
| no_coord_transform_failures_helix_alpss     | PASS     |

`pdv_alpss_output` typically has the highest count (one per
shot). `pdv_alpss_result` and `pdv_alpss_results` reflect
singular/plural filename conventions and will vary by shot.

### coord_enrichment_maxima_derived_job -- MAXIMA/xrd_derived

| Asset check                                  | Expected |
|---------------------------------------------|----------|
| enrichment_success_rate_maxima_derived      | PASS (rate >= 0.9) |
| no_coord_transform_failures_maxima_derived  | PASS     |

## Tuning decisions after first sweep

1. **`PDV_COVERAGE_WARN_THRESHOLD`** -- currently 0.5 (arbitrary
   placeholder). If the first sweep shows PDV coverage is
   consistently >= 0.9 once the existing HELIX DAG has caught up,
   bump the threshold to 0.8 or 0.9 so the check flags real
   drops. If coverage is persistently low because PDV traces are
   being added faster than the HELIX DAG processes them,
   either lower the threshold or accept that the check reports
   an operational reality worth knowing about.
2. **Weekly sweep cadence** -- if all four weekly sweeps
   complete in under an hour and writes are few (mostly
   `skipped_no_change`), weekly is generous and can likely
   move to bi-weekly. Revisit after 4 weeks of data.
3. **State-report schedule** -- if the nightly run consistently
   shows nothing has changed, drop to every other night. This
   is a cost optimization, not a correctness concern.

## How to update this document

After every sweep that changes the expected baseline
(new instrument, YAML recalibration, new data types), edit this
file in the same commit as the change. Note the sweep date and
why the numbers changed.

## History

- **First draft** (Phase 5 Step 4): baseline numbers are
  placeholders -- actual values land in the commit that records
  the first live sweep.
