# Issue-23 Validation & Rehearsal Plan

> **Prepared:** 2026-04-27 from a direct read of every `.py`, every test,
> `pyproject.toml`, `instrument_coordinate_transforms.yaml`,
> `operations/run_live_sweep.sh`, `tests/fixtures/instructions_example.json`,
> and the egg-info on branch `refactor/issue23-dynamic-partitions`.
> When this file disagrees with `README.md`, `CLAUDE.md`, or
> `.claude/helix_dagster_context.md`, this file wins — those are stale.

## 0. Branch & module identity

- Branch: `refactor/issue23-dynamic-partitions`
- Package version: `0.6.0` (`pyproject.toml`); egg-info on disk is `0.1.0`
  and stale — **must run `pip install -e ".[dev]"` before any test or run**.
- Python: ≥3.12. Module name `aimdl_coord_enrichment` (`tool.dagster.module_name`).

## 1. DAG inventory — what actually loads

### 1a. Spreadsheet DAG — `process_helix_assets_job`

| Asset | I/O | Config? |
|---|---|---|
| `raw_experiment_log` | Girder read | `ExperimentLogConfig(item_id, filename)` |
| `pdv_trace_inventory` | Girder read (`/aimdl/datafiles?dataType=pdv_trace`) | — |
| `validated_rows` | pure | — |
| `pdv_cross_references` | pure | — |
| `enriched_pdv_metadata` | **Girder write** | `ExperimentLogConfig` |
| `alpss_results_inventory` | Girder read | — |
| `quality_report` | pure | — |
| `processing_manifest` | **Girder write** | `ExperimentLogConfig` |

Six checks: `zero_inventory`, `igsn_validity_rate`, `pdv_match_rate`,
`igsn_consistency`, `enrichment_success_rate`, `coord_transform_check`.

`enriched_pdv_metadata` now writes `coord_provenance` with timestamp-based
`transform_version` — verified by
`tests/test_assets.py::test_enriched_pdv_metadata_version_boundary_dispatch`.
The README/CLAUDE notes that say otherwise are stale.

### 1b. Coord-enrichment DAG (post issue #23)

Assets: `coord_transform_config_snapshot`, `enrichable_items_inventory`,
`helix_alpss_provenance_tagged`, `enriched_maxima_raw`,
`enriched_helix_alpss`, `enriched_maxima_derived`,
`helix_pdv_coverage_observer`, `coord_enrichment_report`,
`coord_enrichment_manifest`.

Checks: `inventory_nonempty_per_instrument`, `all_helix_alpss_tagged` (ERR),
`enrichment_success_rate_{maxima_raw|helix_alpss|maxima_derived}` (WARN),
`no_coord_transform_failures_{maxima_raw|helix_alpss|maxima_derived}` (WARN),
`maxima_xrd_derived_provenance_valid` (ERR), `pdv_coverage_above_threshold`
(WARN).

| Job | Selection | Partitioning | Config-taking ops |
|---|---|---|---|
| `coord_enrichment_job` | snapshot, inventory, prov-tag, observer, report, manifest | unpartitioned | `helix_alpss_provenance_tagged`, `coord_enrichment_manifest` |
| `coord_enrichment_maxima_raw_job` | snapshot, raw-leaf | `MultiPartitionsDefinition({data_type, run})` | `enriched_maxima_raw` |
| `coord_enrichment_maxima_raw_partition_job` | same | same | same — sensor target |
| `coord_enrichment_helix_alpss_job` | snapshot, inventory, prov-tag, alpss-leaf | static 3 partitions | `helix_alpss_provenance_tagged`, `enriched_helix_alpss` |
| `coord_enrichment_maxima_derived_job` | snapshot, inventory, derived-leaf | static 1 partition (`MAXIMA/xrd_derived`) | `enriched_maxima_derived` |

Sensors: `helix_folder_sensor` (no explicit `default_status` — see §2),
`maxima_raw_discovery_sensor` (STOPPED by default).

Schedules: all four ship `DefaultScheduleStatus.STOPPED`, all build
`run_config` with `dry_run=True`. The MAXIMA-raw weekly is gap-fill
reconciliation against the dynamic-partition store.

### 1c. Partition keys (canonical names from `coord_enrichment/inventory.py` + leaves)

- `MAXIMA_RAW_PARTITIONS` → multi `{data_type ∈ {xrd_raw, xrf_raw}, run ∈ Dynamic("maxima_raw_run")}`
- `MAXIMA_RAW_DATA_TYPE_PARTITIONS` → static `["xrd_raw", "xrf_raw"]`
- `MAXIMA_RUN_PARTITIONS` → dynamic, name `"maxima_raw_run"`
- `HELIX_ALPSS_PARTITIONS` → static `["HELIX/pdv_alpss_output", "HELIX/pdv_alpss_result", "HELIX/pdv_alpss_results"]`
- `MAXIMA_DERIVED_PARTITIONS` → static `["MAXIMA/xrd_derived"]`

The partition string for raw is the AIMD-L key emitted by Girder:
`"<igsn>//<experiment_date>"`. Multi-partition keys are constructed as
`MultiPartitionKey({"data_type": "<dt>", "run": "<key>"})`.

## 2. Defects to know about before launching anything

Each is a fact from the code; the suite does not catch them because no test
exercises a real Dagster job-launch path.

### 2.1 [BLOCKER for spreadsheet DAG]  `helix_folder_sensor` underconfigures the run

`sensors.py::helix_folder_sensor` builds:
```python
run_config={"ops": {"raw_experiment_log": {"config": {...}}}}
```
But `assets.py` declares `ExperimentLogConfig(item_id: str, filename: str)`
(no defaults) on **three** ops: `raw_experiment_log`,
`enriched_pdv_metadata`, `processing_manifest`. Sensor-launched runs will
fail Dagster config validation on the latter two.

Fix shape:
```python
config = {"item_id": item_id, "filename": item["name"]}
run_config = {"ops": {
    "raw_experiment_log":   {"config": config},
    "enriched_pdv_metadata":{"config": config},
    "processing_manifest":  {"config": config},
}}
```

### 2.2 [BLOCKER for state-report path]  `coord_enrichment_job` selection vs. report deps

`__init__.py` selects six assets but `coord_enrichment_report` requires
the three leaves (`enriched_maxima_raw`, `enriched_helix_alpss`,
`enriched_maxima_derived`) as positional inputs. On a fresh instance with
no prior materializations of those leaves, the IOManager load fails. So
the runbook step "first do a state-report dry run" cannot be the first
move.

Workaround for rehearsal: launch the leaf jobs with `dry_run=True` first
(which records materialization events with empty `counts`), then the
state-report job becomes loadable.

Real fix: rewrite `coord_enrichment_report` to query the materialization
event log instead of taking the leaves as ins, OR add the leaves to the
job selection (with explicit `AllPartitionMapping` if necessary).

### 2.3 [BLOCKER for op-script path]  `operations/run_live_sweep.sh`

Three problems; any one fatal:
1. All three case branches set `ops: { provenance_tagged_items: ... }`.
   No op of that name exists. The asset is `helix_alpss_provenance_tagged`.
2. `coord_enrichment_maxima_raw_job` is multi-partitioned now; the script
   passes single-string `--partition MAXIMA/xrd_raw`.
3. `provenance_tagged_items` is repeated in the helix-alpss and
   maxima-derived branches too.

`tests/test_phase5_artifacts.py::test_script_requires_env_vars` only greps
for env-var names — it doesn't validate op names or partition shape.

**Do not run this script.** Use the Dagster UI launchpad until it's
rewritten.

### 2.4 [Latent risk]  `helix_folder_sensor` has no `default_status`

It defaults to whatever Dagster's current default is. Combined with §2.1,
the safer move is to set `default_status=DefaultSensorStatus.STOPPED`
explicitly OR keep `HELIX_FOLDER_ID` pointed somewhere safe during
rehearsal.

### 2.5 [Latent risk]  Test-isolation in `test_processing.py`

`test_import_coordinates_with_missing_yaml` reloads
`aimdl_coord_enrichment.coordinates` against a bogus YAML and does not restore it.
Currently safe under default alphabetical ordering; breaks under
randomized order. A `monkeypatch.undo()` and re-reload after the test, or
isolating to a subprocess, would harden it.

### 2.6 [Forward-compat risk]  `coordinates.py::transform_with_named_version`

Reaches into `_COORD_TRANSFORMER._transforms` (private). TODO in source
acknowledges. Pin `coordinate-transformer` or upstream a public
`get_transform_by_version(...)`.

### 2.7 [Doc drift, not blocking]

- `CLAUDE.md` and `.claude/helix_dagster_context.md` claim
  `coordinates.py` doesn't pass timestamps. It does.
- `.claude/helix_dagster_context.md` §11 says version `0.2.0`. Actual
  is `0.6.0`.
- `README.md` describes only the spreadsheet DAG, not the
  coord-enrichment DAG.

## 3. Rehearsal procedure on data.htmdec.org

Goal: prove the code runs against real Girder data without writing to
production unless explicitly intended.

### 3.0 One-time setup (3–5 min)

```bash
cd /Users/elbert/Documents/GitHub/openmsi/aimdl-coord-enrichment
git status                              # confirm clean tree, branch
source .venv/bin/activate
pip install -e ".[dev]"                 # refresh stale egg-info
pytest tests/ -v                        # baseline — must be green
```

If pytest is not green, stop here and triage. None of the steps below are
worth running on a red baseline.

### 3.1 Environment

```bash
export GIRDER_API_URL=https://data.htmdec.org/api/v1
export GIRDER_API_KEY=<your_key>           # must have RW on the test folder
export HELIX_FOLDER_ID=69e3815e917593d318ab3b5d   # coordinate_dag_test_data
export COORD_TRANSFORMS_YAML=$PWD/instrument_coordinate_transforms.yaml
# create an empty Girder item somewhere (e.g. inside the test folder)
# specifically for receiving meta.coord_enrichment_status:
export COORD_ENRICHMENT_MANIFEST_ITEM=<that_item_id>
```

`HELIX_FOLDER_ID` only scopes the spreadsheet sensor, not the
`/aimdl/datafiles` queries. Those are collection-wide and will return
items from production. Dry-run mode is the actual safety net.

### 3.2 Boot Dagster

```bash
dagster dev
```

In the UI:
1. Confirm all six jobs are listed:
   - `process_helix_assets_job`
   - `coord_enrichment_job`
   - `coord_enrichment_maxima_raw_job`
   - `coord_enrichment_maxima_raw_partition_job`
   - `coord_enrichment_helix_alpss_job`
   - `coord_enrichment_maxima_derived_job`
2. **Stop `helix_folder_sensor`** if it auto-started (see §2.4 + §2.1).
3. Confirm `maxima_raw_discovery_sensor` is STOPPED.
4. Confirm all four schedules are STOPPED.

### 3.3 Smoke checks (read-only, fast)

Materialize from launchpad — no config needed:

| # | Asset | What it proves |
|---|---|---|
| 1 | `coord_transform_config_snapshot` | YAML loads, sha256 computes, both HELIX versions register |
| 2 | `helix_pdv_coverage_observer` | `/aimdl/datafiles?dataType=pdv_trace` works, returns >0 items, baseline coverage rate is recorded |
| 3 | `enrichable_items_inventory` | All other `/aimdl` calls work; per-data-type counts are surfaced as output metadata; `inventory_nonempty_per_instrument` check fires |

If step 3 takes more than ~60 seconds or returns 0 items for everything,
the API key likely lacks read scope across the AIMD-L collection or the
endpoint is unhealthy — stop and check the Girder side.

### 3.4 MAXIMA raw — single-partition dry run

Pick a known-good run key. From `tests/test_coord_enrichment_e2e.py`
the fixture uses `JHAMAL00018-009`; in production data its AIMD-L key
will be `JHAMAL00018-009//<experiment_date_iso>`. Easiest way to find a
real key: in the launchpad of `coord_enrichment_maxima_raw_partition_job`,
the partition picker lists every key the discovery sensor has registered
— but the sensor is STOPPED, so the dynamic dim is empty.

To populate it without enabling the sensor: turn the sensor on for
**one tick**, then back off:
1. UI → Sensors → `maxima_raw_discovery_sensor` → "Test sensor" / single
   evaluation.
2. The single tick registers every current AIMD-L raw key as a dynamic
   partition. No RunRequests fire (the sensor is still STOPPED) — wait,
   re-check: the sensor returns `SensorResult(run_requests=...)` from a
   single-tick evaluation but the runs only launch if the sensor is
   started. If "Test sensor" returns the run requests for review without
   firing them, that's the safe path.
3. UI → confirm a populated run-key list under `maxima_raw_run`.

Then materialize one partition manually:

- Job: `coord_enrichment_maxima_raw_partition_job`
- Partition: `{data_type: xrd_raw, run: <one real key>}`
- Run config (in the launchpad):
  ```yaml
  ops:
    enriched_maxima_raw:
      config:
        dry_run: true
  ```

Expected output metadata on `enriched_maxima_raw`:
- `seen` > 0
- `simulated_dry_run == seen` (no actual writes)
- `instructions_errors` empty (the matching `xrd_metadata` partition
  resolves)
- `transform_versions_used` shows e.g. `MAXIMA/v1=N`
- The two checks on the asset (success-rate WARN, transform-failures WARN)
  pass.

If `instructions_errors` is non-empty for every key tried, the
`xrd_metadata` index is missing entries for those keys upstream — that's
a Girder/amdee_xrd hygiene issue, not a pipeline bug.

Repeat for `{data_type: xrf_raw, run: <same key>}`.

### 3.5 HELIX ALPSS — partition dry run

- Job: `coord_enrichment_helix_alpss_job`
- Partition: `HELIX/pdv_alpss_output`
- Run config:
  ```yaml
  ops:
    helix_alpss_provenance_tagged:
      config:
        dry_run: true
    enriched_helix_alpss:
      config:
        dry_run: true
  ```

Watch the asset checks:
- `all_helix_alpss_tagged` (ERR) on the prov-tag asset must pass; if it
  fails it lists the unresolved items — they have ALPSS-style names that
  no PDV trace matches by stem.
- `enrichment_success_rate_helix_alpss` and
  `no_coord_transform_failures_helix_alpss` (WARN).

The leaf will only succeed for items whose **parent PDV trace is already
fully enriched** (Station_X/Y plus a `coord_provenance` block written by
`process_helix_assets_job`). Any ALPSS item whose parent isn't enriched
appears in `resolution_errors` with `stage="inherit_from_parent"`.

### 3.6 MAXIMA derived — partition dry run

- Job: `coord_enrichment_maxima_derived_job`
- Partition: `MAXIMA/xrd_derived`
- Run config:
  ```yaml
  ops:
    enriched_maxima_derived:
      config:
        dry_run: true
  ```

Required upstream: at least one materialization of `enriched_maxima_raw`
exists (from §3.4). The derived asset depends on raw via
`AllPartitionMapping`, so it expects the upstream lineage to be
populated.

The new `maxima_xrd_derived_provenance_valid` ERROR check fires here —
any xrd_derived item whose `meta.prov.wasDerivedFrom` link doesn't
resolve to a parent in the inventory shows up. This is a data-hygiene
signal about amdee_xrd, not a pipeline defect.

### 3.7 Spreadsheet DAG — caveat

**Do not start `helix_folder_sensor` until §2.1 is fixed.** To exercise
`process_helix_assets_job` against the test folder, launch from the
launchpad with the same config on all three relevant ops:

```yaml
ops:
  raw_experiment_log:
    config:
      item_id: "<spreadsheet_item_id_in_test_folder>"
      filename: "<that_spreadsheet's_name>.csv"
  enriched_pdv_metadata:
    config:
      item_id: "<same>"
      filename: "<same>"
  processing_manifest:
    config:
      item_id: "<same>"
      filename: "<same>"
```

This run will write coord metadata to whatever PDV traces in the AIMD-L
collection match the spreadsheet's filenames — production items
included if the spreadsheet references shots that have production PDV
traces. There is no dry-run mode on the spreadsheet DAG today; the only
isolation is to point at a test spreadsheet whose `PDV_FileName` rows
match only test-folder PDVs.

### 3.8 State-report job

Skip `coord_enrichment_job` until the leaves have been materialized at
least once (see §2.2). Once §3.4–§3.6 have run with `dry_run=true`, the
state-report should be loadable — its three leaf inputs will resolve
to the dry-run results.

## 4. What the tests already cover (for confidence)

- Pure helpers: `instruments/maxima.py` filename/JSON parsing,
  `provenance.py` payload shape, `overwrite.py` decision table, IGSN
  validation, PDV matching, sha256 stability.
- Asset behavior in isolation: every leaf, the prov-tagger, the observer,
  the manifest (config-vs-env-var fallback), the snapshot.
- Sensor: `maxima_raw_discovery_sensor` dedup-key shape and dynamic-
  partition add-request shape.
- Schedules: all four registered, all STOPPED, weekly reconciliation
  emits gaps only.
- Definitions: `defs.get_repository_def()` succeeds; partitioned-job
  selections are correct.

## 5. What the tests do NOT cover (and what we should add next)

Sorted by how likely they are to bite during rehearsal:

1. **Spreadsheet sensor's full RunRequest validates against Dagster's
   config schema.** A test using `dagster.validate_run_config(job, run_config)`
   would catch §2.1.
2. **`coord_enrichment_job` is loadable on an empty instance.** A test
   that calls `defs.get_job_def("coord_enrichment_job").execute_in_process(
   instance=DagsterInstance.ephemeral())` would catch §2.2.
3. **`run_live_sweep.sh` op names match the actual op set.** A test that
   greps the script's `ops:` block keys against
   `defs.get_repository_def()` op names would catch §2.3.
4. **End-to-end on a real (small) Girder fixture.** `test_coord_enrichment_e2e.py`
   currently mocks `fetch_partition_details` directly; a closer-to-real
   test would stand up a fake Girder using the recorded JSON shapes and
   run the actual `dagster.materialize` API.

## 6. Priority order to "validated and working"

In rough order of cost vs. unblocking value:

1. **Fix `helix_folder_sensor` config propagation** (§2.1). Low effort,
   unblocks the spreadsheet sensor entirely.
2. **Add `default_status=STOPPED` to `helix_folder_sensor`** (§2.4) so
   misconfigured runs can't fire on `dagster dev` startup.
3. **Rewrite `operations/run_live_sweep.sh`** (§2.3) to use the correct
   asset/op names and to launch the partition jobs with explicit
   `MultiPartitionKey` arguments. Or, simpler: delete the script and
   document a UI-driven sweep procedure in the runbook.
4. **Fix `coord_enrichment_job`'s report dependency** (§2.2). Either
   include the three leaves in the job selection (cheapest) or refactor
   the report to read from the materialization log.
5. **Add the three CI tests in §5**: run-config validation, on-empty-
   instance job-load, op-name vs. script-grep.
6. **Pin `coordinate-transformer`** in `pyproject.toml` to a known-good
   version, and either (a) drop `transform_with_named_version`'s reach
   into `_transforms`, or (b) upstream a public accessor to that package.
7. **Doc reconciliation**: README + CLAUDE.md + `.claude/helix_dagster_context.md`
   to match the actual code state (timestamp-passing, version 0.6.0,
   coord-enrichment DAG existence).

## 7. Pointers to the source-of-truth files

When in doubt, read these — they are the contract:

- `aimdl_coord_enrichment/__init__.py` — Definitions registry, job selections.
- `aimdl_coord_enrichment/assets.py` — spreadsheet DAG + `ExperimentLogConfig`.
- `aimdl_coord_enrichment/sensors.py` — both sensors; sensors set `run_config` here.
- `aimdl_coord_enrichment/schedules.py` — all four schedules + the
  `_dry_run_config()` helper that lists Config-taking op names per job.
- `aimdl_coord_enrichment/coord_enrichment/inventory.py` — partition definitions
  (`MAXIMA_RAW_PARTITIONS`, `MAXIMA_RUN_PARTITIONS`, etc.).
- `aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py` — the
  multi-partitioned `enriched_maxima_raw` and its scoped
  `fetch_partition_details` calls.
- `aimdl_coord_enrichment/coord_enrichment/{helix_alpss_leaf,maxima_derived_leaf}.py`
  — the inheritance leaves; they consume
  `enrichable_items_inventory[partition_key]` directly.
- `aimdl_coord_enrichment/coord_enrichment/overwrite.py` — the four-key write
  decision (yaml_sha256, transformer_version, transform_version,
  station_coord_source).
- `instrument_coordinate_transforms.yaml` — local YAML; HELIX has v1
  (until 2026-04-01) and v2 (after, identity transform for testing);
  MAXIMA has only v1; SPHINX is out of scope.
- `tests/fixtures/instructions_example.json` — 25 scan_points,
  scan_points[17] == [11.0, 0.0]; matches the e2e test expectations.

## 8. One-paragraph status

Branch is functionally complete for the issue-23 reshape: multi-partitioned
MAXIMA raw, dynamic-partition discovery sensor, prov-split, gap-filling
weekly schedule, asset checks rebuilt around the new ownership boundaries.
The unit-test surface is broad and green. Three execution-time defects
exist that the test suite does not catch — sensor config, job selection,
and the operations script — and all three are blockers for one of the
three rehearsal paths but not for the other two. Dry-run rehearsals on
data.htmdec.org via the UI launchpad work today, in the order MAXIMA-raw
→ HELIX-ALPSS / MAXIMA-derived → state-report job, after the dynamic
partition dim has been seeded by a single sensor evaluation tick.
