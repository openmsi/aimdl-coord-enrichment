Closes #23.

## What changed

- **MAXIMA raw partitioning.** Replaced
  `StaticPartitionsDefinition(["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"])`
  with a `MultiPartitionsDefinition` keyed on a static `data_type`
  dim (`xrd_raw`, `xrf_raw`) and a dynamic `run` dim (one key per
  AIMD-L partition string `"<igsn>//<experiment_date>"`, registered
  by name under the dynamic dim `maxima_raw_run`). `enriched_maxima_raw`
  now fetches its own partition's items and the matching
  `xrd_metadata/instructions.txt` via
  `/aimdl/partition/details?dataType=<dt>&key=<aimdl_key>`, and no
  longer depends on `enrichable_items_inventory` or the provenance
  tagger. Helper `fetch_partition_details` added to
  `helix_dagster/girder_io.py`.
- **Discovery sensor.** New `maxima_raw_discovery_sensor` polls the
  AIMD-L partition index for `xrd_raw`, `xrf_raw`, and
  `xrd_metadata` (≥ 3600 s tick), adds observed run keys to the
  `maxima_raw_run` dynamic dim, and emits one `RunRequest` per
  observed `(data_type, aimdl_key)` against
  `coord_enrichment_maxima_raw_partition_job`. Dedup run_key shape:
  `coord-enrichment|<dt>|<aimdl_key>|raw=<h>|xrd_metadata=<h>`
  (falling back to `"no-xrd-metadata"` for runs without an
  `xrd_metadata` index entry). Defaults to STOPPED.
- **Gap-filling reconciliation.** The weekly
  `coord_enrichment_maxima_raw_weekly_schedule` now enumerates
  registered `(data_type, run)` partitions and emits RunRequests
  only for partitions without a successful prior materialization.
  Still STOPPED by default, still dry-run only.
- **Provenance architecture split.** The former combined tagger
  asset was split along data-flow lines:
  - HELIX ALPSS parent tagging (mutating) →
    `helix_alpss_provenance_tagged` (HELIX-only). `enriched_helix_alpss`
    now depends on this asset directly.
  - MAXIMA `xrd_derived` prov-link verification (non-mutating) →
    `maxima_xrd_derived_provenance_valid` ERROR asset check on
    `enriched_maxima_derived`. amdee_xrd remains the sole writer
    for those links; we read-and-verify only.
- **Derived lineage.** `enriched_maxima_derived` now explicitly
  depends on `enriched_maxima_raw` via `AllPartitionMapping`. The
  lineage now matches the data reality — derived items inherit
  coords from their raw-partition parents.
- **Documentation.** `docs/reference/prov_metadata.md`,
  `docs/coordinate_enrichment_dag.md`,
  `docs/coordinate_enrichment_dag_brief.md`,
  `docs/runbooks/coord_enrichment_production_sweep.md`,
  `docs/runbooks/first_sweep_expected_values.md`, and
  `.claude/helix_dagster_context.md` updated to reflect the new
  topology. Brought `docs/reference/prov_metadata.md` forward from
  `refactor/issue21-step2` in Step 0.

Version bumped to 0.6.0.

## What's out of scope (on purpose)

- Repartitioning `enriched_maxima_derived` to match raw's
  multi-partition shape. This refactor chose α (single-partition
  derived, `AllPartitionMapping`); β is a deferred follow-up.
- `enriched_helix_alpss` repartitioning.
- Rebuilding the HELIX folder sensor.
- Changing the existing `process_helix_assets_job` spreadsheet DAG
  or any of its assets.

## Execution record

9-step sequence driven from `.claude/prompts/issue23/README.md`.
Each step ran in a fresh Claude Code session and produced one
commit. One-to-one commit mapping (from branch log):

| Step | Commit |
|------|--------|
| 0 — branch setup / bring-forward | `16c469a` docs: bring forward prov_metadata.md from issue21-step1 |
| 1 — girder_io helpers | `6202906` girder_io: add scoped partition helpers |
| 2 — MAXIMA raw partition rewrite | `9b3ba0e` MAXIMA raw: dynamic multi-partitioning, drop inventory/prov deps |
| 3 — discovery sensor | `eaf2ad8` Add maxima_raw_discovery_sensor + partition job |
| 4 — reconciliation schedule | `d45bebc` schedules: gap-filling reconciliation for MAXIMA raw |
| 5 — provenance split (HELIX tagger) | `a6d3a46` Provenance split part 1: HELIX-scoped tagger |
| 6 — provenance split (derived dep + check) | `c9ce0e8` Provenance split part 2: lineage dep + new check |
| 7 — integration test reconciliation | `2209eaf` Reconcile e2e and integration tests with new DAG topology |
| 8 — docs + PR body | (this commit) |

Full `pytest` was green between every step.

## Smoke test checklist (pre-merge)

- [ ] `.venv/bin/pytest` green on a fresh checkout of the branch.
- [ ] `dagster definitions` / UI introspection shows:
  - [ ] `helix_alpss_provenance_tagged` present
  - [ ] `maxima_xrd_derived_provenance_valid` present as an asset
    check on `enriched_maxima_derived`
  - [ ] `maxima_raw_discovery_sensor` present, STOPPED
  - [ ] `coord_enrichment_maxima_raw_partition_job` present
  - [ ] `enriched_maxima_raw` partitioned on a
    `MultiPartitionsDefinition` with `data_type` + `run` dims
- [ ] Ad-hoc sensor tick against a staging Girder: confirm run_keys
  match `coord-enrichment|<dt>|<aimdl_key>|raw=<h>|xrd_metadata=<h>`.
- [ ] Dry-run one `MultiPartitionKey({"data_type": ..., "run": ...})`
  materialization manually, confirm expected metadata writes.

## Risks

- **First sensor tick is noisy.** Enabling
  `maxima_raw_discovery_sensor` for the first time will register
  hundreds of dynamic partition keys in one tick (bounded by the
  current AIMD-L partition count) and emit a RunRequest for each.
  This is a one-time catch-up event; subsequent ticks suppress
  unchanged partitions via the content-hash dedup key.
- **External hard-coded partition keys.** Any external dashboard,
  script, or saved job config that hard-codes
  `"MAXIMA/xrd_raw"` / `"MAXIMA/xrf_raw"` as partition keys will
  break. Audit before merge.
- **AllPartitionMapping cost.** `enriched_maxima_derived` now
  depends on every `enriched_maxima_raw` partition. Dagster's
  materialization planner has to consider all of them — fine at
  current scale but worth revisiting if the partition count grows
  by another order of magnitude. This is the trade-off for α
  (single-partition derived).
