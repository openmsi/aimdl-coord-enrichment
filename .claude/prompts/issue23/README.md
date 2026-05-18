# Issue 23 Runbook — Dynamic Partitions for MAXIMA Raw + Provenance Split

A 9-step refactor executed by pasting each `stepN_*.md` file into a
fresh Claude Code session in order.

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## One-time setup (do before running any step)

Assumes you start on a clean `refactor/asset-dag`.

```bash
cd /path/to/aimdl-coord-enrichment

# 1. Create the GitHub issue from the drafted body
gh issue create \
  --title "Dynamic partitions for MAXIMA raw + provenance architecture split" \
  --body-file .claude/prompts/issue23/ISSUE.md \
  --label refactor
# Capture the issue number. If it is not 23, substitute throughout.

# 2. Cut the working branch from refactor/asset-dag
git checkout refactor/asset-dag
git pull origin refactor/asset-dag
git checkout -b refactor/issue23-dynamic-partitions

# 3. Commit the runbook on the new branch
git add .claude/prompts/issue23
git commit -m "docs: issue #23 runbook and step prompts"

# 4. Baseline the test suite. Must be green before Step 0.
.venv/bin/pytest
```

If the baseline is red, stop and fix before running any step.

## Executing the steps

For each step N from 0 through 8:

1. Open a **fresh Claude Code session** in the repo root.
2. Paste the entire contents of `stepN_*.md` as the first message.
3. Let Claude Code execute.
4. Review its diff:
   ```bash
   git diff HEAD~1
   ```
5. Run the full suite:
   ```bash
   .venv/bin/pytest
   ```
6. If green, move to the next step. If red, Claude Code went
   off-script or the prompt had a bug — repair by hand before
   continuing. Do not let a red suite carry forward.

Each step produces **exactly one commit** (Step 0 may produce zero
if `docs/reference/prov_metadata.md` was already on
`refactor/asset-dag`). The commit message is dictated inside each
step's `Commit` section.

## Step index

| # | File | Purpose |
|---|---|---|
| 0 | `step0_branch_setup.md` | Cherry-pick `docs/reference/prov_metadata.md` from `issue21-step2` if missing; baseline tests |
| 1 | `step1_girder_io_helpers.md` | Rename `fetch_partition_keys` → `fetch_partition_index`; add `fetch_partition_details` |
| 2 | `step2_maxima_raw_partition_rewrite.md` | Replace `MAXIMA_RAW_PARTITIONS` with `MultiPartitionsDefinition`; rewrite `enriched_maxima_raw` to use scoped fetches; drop inventory + provenance deps |
| 3 | `step3_discovery_sensor.md` | Add `maxima_raw_discovery_sensor` + `coord_enrichment_maxima_raw_partition_job` |
| 4 | `step4_reconciliation_schedule.md` | Upgrade weekly schedule to gap-filling (materialize only partitions without successful materialization) |
| 5 | `step5_provenance_split_helix.md` | Rename `provenance_tagged_items` → `helix_alpss_provenance_tagged`; scope to HELIX only; delete `maxima_prov_targets_resolve` |
| 6 | `step6_provenance_split_maxima_derived.md` | Rewire `enriched_maxima_derived` to depend on `enriched_maxima_raw` (AllPartitionMapping); add `maxima_xrd_derived_provenance_valid` asset check |
| 7 | `step7_integration_tests.md` | Update `test_coord_enrichment_e2e.py` and `test_coord_enrichment_phase4_e2e.py` for new DAG topology |
| 8 | `step8_docs_and_ship.md` | Update `prov_metadata.md` and other docs; draft PR description |

## Invariants carried through every step

- **Branch**: `refactor/issue23-dynamic-partitions`
- **Tracking issue**: #23
- **Locked names**:
  - `maxima_raw_discovery_sensor`
  - `coord_enrichment_maxima_raw_partition_job`
  - `helix_alpss_provenance_tagged`
  - `maxima_xrd_derived_provenance_valid`
  - `MAXIMA_RAW_PARTITIONS` (name reused; shape changes from static to MultiPartitions)
  - `MAXIMA_RAW_DATA_TYPE_PARTITIONS`
  - `MAXIMA_RUN_PARTITIONS` (dynamic dim name: `"maxima_raw_run"`)
- **Sensor dedup key formula**:
  ```
  f"coord-enrichment|{data_type}|{aimdl_key}|raw={raw_hash}|xrd_metadata={metadata_hash}"
  ```
  with `"no-xrd-metadata"` as the fallback when a run has no
  `xrd_metadata` index entry.
- **AIMD-L partition key semantics**: `aimdl_key = "<igsn>//<experiment_date>"`
  is the literal string emitted by the Girder plugin. Pass through
  unchanged to `/aimdl/partition/details`.
- **Derived repartitioning choice**: α — keep
  `enriched_maxima_derived` single-partition-static; use
  `AllPartitionMapping` for its new dep on `enriched_maxima_raw`.
  β (repartition derived to match raw) is deferred to a follow-up.
- **One commit per step. Green `pytest` between steps.**

## Rollback

To abandon this branch and restart from scratch:

```bash
git checkout refactor/asset-dag
git branch -D refactor/issue23-dynamic-partitions
# resume from "One-time setup" step 2
```

The runbook files live on disk regardless of branch state, so
rollback and re-cut is cheap.
