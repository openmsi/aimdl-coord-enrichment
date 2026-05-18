# Archived — `run_live_sweep.sh`

Archived: 2026-04-27 (branch `refactor/issue23-dynamic-partitions`)
Defect ref: `.claude/issue23_validation_plan.md` §2.3

## What this was

`run_live_sweep.sh` was a one-shot operator script for live
(`dry_run=False`) sweeps of the coord-enrichment DAG. It performed
pre-flight env-var checks, ran pytest, prompted the operator for
explicit confirmation ("Type LIVE SWEEP to proceed"), then invoked
`dagster job launch` once per partition for each of the three sibling
jobs.

## Why it was retired

Three execution-time defects, any one fatal:

1. **Wrong op name in run_config.** Every case branch set
   `ops: { provenance_tagged_items: { config: { dry_run: false } } }`.
   No op named `provenance_tagged_items` existed in the codebase
   after issue-23 step 5; the asset was renamed to
   `helix_alpss_provenance_tagged`. Run config validation would
   reject this.
2. **Wrong partition shape for MAXIMA raw.** After issue-23 step 2,
   `coord_enrichment_maxima_raw_job` partitions on
   `MultiPartitionsDefinition({data_type, run})` where `run` is a
   dynamic dim keyed on `"<igsn>//<experiment_date>"`. The script
   passed single-string keys (`MAXIMA/xrd_raw`, `MAXIMA/xrf_raw`),
   which do not resolve.
3. **Op selection mismatch.** The MAXIMA-raw and MAXIMA-derived
   case branches included `provenance_tagged_items` in their op
   config, but those jobs do not include the prov tagger in their
   asset selection (see `aimdl_coord_enrichment/__init__.py`).

CI did not catch any of these. The only test that referenced the
script (`tests/test_phase5_artifacts.py::test_script_requires_env_vars`)
greps for env-var names in the file, not for op names or partition
shapes.

Retiring the script was the right move because every capability it
attempted is already provided more correctly elsewhere:

- Weekly reconciliation → `coord_enrichment_maxima_raw_weekly_schedule`
  (in `aimdl_coord_enrichment/schedules.py`), which gap-fills against the
  Dagster materialization log instead of blindly firing every
  partition.
- Continuous discovery → `maxima_raw_discovery_sensor`.
- Nightly state-report → `coord_enrichment_state_report_schedule`.
- One-shot live sweep → operator clicks Materialize on the three
  sibling jobs in the Dagster UI launchpad with `dry_run: false`,
  per `docs/runbooks/coord_enrichment_production_sweep.md` "Live
  sweep" section.

## Reviving this script

If a future deployment context requires single-command sweeps from
outside the Dagster UI (CI pipelines, kubernetes CronJobs, restricted
environments), the work is roughly:

1. **Fix op names.** Replace every `provenance_tagged_items` with
   `helix_alpss_provenance_tagged`. Verify against the current
   `aimdl_coord_enrichment/__init__.py` selections — names may have changed
   again.
2. **Drop op-config blocks for ops not in the job's selection.** The
   MAXIMA-raw and MAXIMA-derived case branches do not need a config
   entry for the prov tagger.
3. **Handle multi-partition keys.** For
   `coord_enrichment_maxima_raw_job`, partition keys take the form
   `'{"data_type": "xrd_raw", "run": "<igsn>//<experiment_date>"}'`.
   Verify the `dagster job launch --partition` flag's accepted
   format against the installed Dagster version.
4. **Decide on partition enumeration.** A bash loop hardcoding two
   partitions does not scale to issue-23's hundreds of dynamic run
   keys. Either query the dynamic dim from a Python helper before
   the bash loop, or accept a partition-list file as input.
5. **Reconsider the right tool.** For sweeps that hit hundreds of
   partitions, a bash subprocess-per-launch loop has no concurrency
   control, no retry, no partial-failure recovery, no cancellation.
   A Python wrapper using Dagster's GraphQL client or
   `defs.get_job_def(...).execute_in_process(...)` is a better
   shape. Consider whether to revive the bash version or write a
   Python replacement.

The pre-flight env-var protocol (lines 30-49 of the archived script)
and the operator-confirmation gate (lines 51-65) are reusable
patterns regardless of which tool replaces the script. They are why
this archive exists rather than a `git rm`.

## See also

- `LIVE_SWEEP_SECTION_2026-04-27.md` in this directory — the runbook
  prose that invoked this script, preserved verbatim.
