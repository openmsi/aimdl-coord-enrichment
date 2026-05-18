# Issue 23, Step 8 — Docs update and PR

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0–7 complete.
All production code and tests are aligned with the new architecture.
This step updates the prose documentation to match, then prepares
the pull request.

Before editing, read:

- `.claude/CLAUDE.md`
- `.claude/prompts/issue23/README.md`
- `.claude/prompts/issue23/ISSUE.md`
- `docs/reference/prov_metadata.md` (brought forward in Step 0)
- `docs/coordinate_enrichment_dag.md`
- `docs/coordinate_enrichment_dag_brief.md`
- `docs/README.md`
- `docs/runbooks/coord_enrichment_production_sweep.md`
- `.claude/helix_dagster_context.md`
- Any file under `docs/developer_notes/` or `docs/refactor_notes/`
  that mentions `provenance_tagged_items`,
  `maxima_prov_targets_resolve`, `MAXIMA_RAW_PARTITIONS`, the static
  two-partition scheme, or the old inventory-driven flow.

## Why this step

Docs drift fast when the code moves and nothing in the test suite
catches stale prose. This step closes that drift: every doc that
describes the DAG, provenance, or partition model is brought into
sync with what the code actually does after Steps 1–7.

## Goal

- Update every doc that references the old architecture so it
  reflects the new topology.
- Draft the PR description.
- Prepare a smoke-test checklist the human will walk through before
  merging.

## Edits

### 1. `docs/reference/prov_metadata.md`

This file was brought forward from `refactor/issue21-step2` in
Step 0 and has not been touched since. Update it to describe:

- HELIX ALPSS tagging lives in the asset `helix_alpss_provenance_tagged`
  (was `provenance_tagged_items`). It writes `meta.prov.wasDerivedFrom`
  on ALPSS result items, linking them to their parent PDV traces.
  This is a real mutation; `enriched_helix_alpss` depends on it.
- MAXIMA `xrd_derived` prov links (`meta.prov.wasDerivedFrom` /
  `meta.prov.isPartOf`) are written **upstream by the Girder plugin**
  (`amdee_xrd` in `xarthisius/girder-jsonforms`, `igsn` branch) —
  not by any Dagster asset. The Dagster pipeline only **verifies**
  their presence.
- Verification is an asset check, `maxima_xrd_derived_provenance_valid`,
  on `enriched_maxima_derived`. It inspects `resolution_errors`
  with `stage="inherit_from_parent"` and fails if any exist.
- MAXIMA raw data types (`xrd_raw`, `xrf_raw`) have no prov links
  added by this pipeline — their provenance is captured instead
  through `meta.coord_provenance.*`, written by `enriched_maxima_raw`
  per partition.

Keep the doc tight — one page, bullet-oriented, link to the asset
definitions rather than duplicating code.

### 2. `docs/coordinate_enrichment_dag.md` and `docs/coordinate_enrichment_dag_brief.md`

Update the DAG description. New edges and shapes:

- `enriched_maxima_raw`: partitioned on `MultiPartitionsDefinition({data_type: Static(["xrd_raw", "xrf_raw"]), run: Dynamic("maxima_raw_run")})`. No asset deps — fetches its own items and the matching `xrd_metadata/instructions.txt` via `fetch_partition_details(data_type, aimdl_key)`. Discovered automatically by `maxima_raw_discovery_sensor`.
- `helix_alpss_provenance_tagged` (was `provenance_tagged_items`): HELIX-only.
- `enriched_helix_alpss` depends on `helix_alpss_provenance_tagged`.
- `enriched_maxima_derived` depends on `enriched_maxima_raw` via `AllPartitionMapping` (decision α: derived remains single-partition-static).
- Asset check `maxima_xrd_derived_provenance_valid` on `enriched_maxima_derived` (was asset `maxima_prov_targets_resolve` on the tagger).

If either file contains an ASCII DAG diagram, redraw it.

The brief is ≤ one page; the full version can be longer but should
not duplicate the brief's overview — it extends it.

### 3. `docs/README.md`

Skim for outdated references. Likely sections to update:

- Any mention of "two-partition MAXIMA raw" or
  `StaticPartitionsDefinition(["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"])`.
- Any mention of `provenance_tagged_items` or
  `maxima_prov_targets_resolve`.
- Any reference to the sensor/schedule landscape that doesn't
  include `maxima_raw_discovery_sensor`.

### 4. `docs/runbooks/coord_enrichment_production_sweep.md`

If this runbook describes a manual or scheduled materialization
sequence for MAXIMA raw, update it:

- Operators now enable `maxima_raw_discovery_sensor` (STOPPED by
  default) to let new partitions materialize automatically.
- The weekly schedule is now gap-filling reconciliation, still
  STOPPED by default, still dry-run only.
- Partition targets for manual backfill are now `MultiPartitionKey({"data_type": ..., "run": "<igsn>//<experiment_date>"})`, not the old two static keys.

### 5. `.claude/helix_dagster_context.md`

This file is context for future Claude sessions. Update any
description of assets, partitions, or the DAG that conflicts with
the post-Step-7 state. Focus on:

- The MAXIMA raw partition section.
- The provenance-tagging section.
- The sensor/schedule roster.

### 6. `docs/developer_notes/` and `docs/refactor_notes/`

Grep for the old names:

```bash
grep -rn "provenance_tagged_items\|maxima_prov_targets_resolve" docs/
```

For each hit: if the doc is a historical record (a refactor note
describing what was done in a previous phase), **leave it alone** —
it accurately describes that moment in time. If the doc is
presented as current-state reference, update it or add a header
note pointing to issue #23 for the new architecture.

`docs/refactor_notes/v1.0_rewrite.md` is historical; skip it.

## Verification

```bash
.venv/bin/pytest
```

Full suite must pass (docs-only changes, so this is just
confirming nothing accidentally broke). Also verify docs are
internally consistent:

```bash
grep -rn "provenance_tagged_items\|maxima_prov_targets_resolve" \
  docs/ .claude/
# Results should only appear in:
#   - docs/refactor_notes/ (historical records — OK to leave)
#   - .claude/prompts/issue23/ (these step files themselves — OK)
# Should NOT appear in docs/reference/, docs/*.md at top level,
# docs/runbooks/, docs/developer_notes/, or .claude/CLAUDE.md /
# helix_dagster_context.md.
```

## Commit

```
git add docs/ .claude/helix_dagster_context.md
git commit -m "docs: update for new DAG topology + provenance split (#23)

- prov_metadata.md: HELIX tagging remains an asset; MAXIMA xrd_derived
  prov verification is now an asset check; MAXIMA raw prov is via
  coord_provenance, not prov.wasDerivedFrom.
- coordinate_enrichment_dag{,_brief}.md: multi-partition MAXIMA raw,
  new sensor, gap-filling reconciliation, new derived->raw lineage.
- Production sweep runbook: sensor-enablement procedure replaces the
  manual two-key materialization.
- helix_dagster_context.md: refresh for future agent sessions."
```

## PR preparation

Once committed, push the branch and open the PR:

```bash
git push -u origin refactor/issue23-dynamic-partitions
gh pr create \
  --base refactor/asset-dag \
  --title "Dynamic partitions for MAXIMA raw + provenance architecture split (#23)" \
  --body-file .claude/prompts/issue23/PR_BODY.md
```

Create `.claude/prompts/issue23/PR_BODY.md` in this step, alongside
the doc updates. Suggested structure:

```
Closes #23.

## What changed

- **MAXIMA raw**: replaced `StaticPartitionsDefinition(["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"])` with a `MultiPartitionsDefinition` over a static `data_type` dim (`xrd_raw`, `xrf_raw`) and a dynamic `run` dim (one key per `"<igsn>//<experiment_date>"`). `enriched_maxima_raw` now fetches its own partition's items via the scoped `/aimdl/partition/details` endpoint, and no longer depends on `enrichable_items_inventory` or the provenance tagger.
- **Discovery**: new `maxima_raw_discovery_sensor` polls the AIMD-L partition index for xrd_raw, xrf_raw, and xrd_metadata, adds run keys to the dynamic dim, and emits `RunRequest`s with a dedup key that composes both the raw and xrd_metadata content hashes. Defaults to STOPPED.
- **Reconciliation**: the weekly schedule is now gap-filling — emits RunRequests only for partitions with no successful materialization. Still STOPPED by default, still dry-run only.
- **Provenance architecture**: `provenance_tagged_items` split along data-flow lines. HELIX ALPSS parent tagging → `helix_alpss_provenance_tagged` (HELIX-only). MAXIMA xrd_derived prov-link verification → `maxima_xrd_derived_provenance_valid` asset check on `enriched_maxima_derived` (non-mutating). Old `maxima_prov_targets_resolve` deleted.
- **Lineage**: `enriched_maxima_derived` now explicitly depends on `enriched_maxima_raw` via `AllPartitionMapping`.

## What's out of scope (on purpose)

- Repartitioning `enriched_maxima_derived` to match raw's multi-partition shape — a follow-up; this refactor chose α (single-partition derived, AllPartitionMapping).
- `enriched_helix_alpss` partitioning.
- HELIX folder sensor.

## Execution record

9-step sequence driven from `.claude/prompts/issue23/README.md`.
Each step was a fresh Claude Code session and produced one commit
(Step 0 produced zero since prov_metadata.md was already present /
absent — note which and delete the wrong side).

See commit history on this branch for the per-step detail.

## Smoke test checklist (pre-merge)

- [ ] `.venv/bin/pytest` green on a fresh checkout of the branch.
- [ ] `dagster asset list` (or equivalent introspection) shows:
  - `helix_alpss_provenance_tagged` present
  - `provenance_tagged_items` absent
  - `maxima_xrd_derived_provenance_valid` present as a check on `enriched_maxima_derived`
  - `maxima_prov_targets_resolve` absent
  - `maxima_raw_discovery_sensor` present, STOPPED
  - `coord_enrichment_maxima_raw_partition_job` present
  - `enriched_maxima_raw` partitioned on MultiPartitionsDefinition
- [ ] Ad-hoc sensor tick against a staging Girder: confirm run keys match the expected `coord-enrichment|<dt>|<aimdl_key>|raw=<h>|xrd_metadata=<h>` shape.
- [ ] Dry-run a single partition materialization manually, confirm the expected metadata writes.

## Risks

- The sensor will register hundreds of dynamic partition keys on
  first tick when enabled. This is bounded by the current AIMD-L
  partition count and is a one-time event.
- Any external dashboard that hard-codes the old partition keys
  (`"MAXIMA/xrd_raw"`, `"MAXIMA/xrf_raw"`) will break. Audit before
  merge.
```

Fill in or trim sections based on what actually happened during
execution. Do not leave placeholder bullets unreviewed.

## Success criteria

- `docs/reference/prov_metadata.md`,
  `docs/coordinate_enrichment_dag.md`,
  `docs/coordinate_enrichment_dag_brief.md`,
  `docs/README.md`,
  `docs/runbooks/coord_enrichment_production_sweep.md`, and
  `.claude/helix_dagster_context.md` all reflect the post-Step-7
  architecture.
- No stale references to old names in current-state docs.
- `PR_BODY.md` exists under `.claude/prompts/issue23/` and is
  substantive.
- One new commit (docs + PR body) on the branch.
- Full `pytest` still green.

## Out of scope

- Opening the PR itself — that's a human action after the commit
  lands. The runbook includes the exact `gh pr create` command.
- Merging the PR.
- Any production enablement of the sensor or schedule — that's an
  operator decision, not a refactor decision.
