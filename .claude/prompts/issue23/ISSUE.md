## Summary

Replace MAXIMA raw's static two-partition scheme with a dynamic-partitioned
design keyed on `(data_type, igsn//experiment_date)` — AIMD-L's native
partition shape — and untangle the provenance assets so scientific
derivation (`meta.prov.*`) and coordinate provenance
(`meta.coord_provenance`) are cleanly separated.

## Problem

`enriched_maxima_raw` is currently partitioned on
`StaticPartitionsDefinition(["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"])` —
two partitions total. Each materialization processes every run of
that data_type, pulling a flattened inventory via
`fetch_items_by_partition`. For a production dataset this is ~300+
runs per modality processed in one shot, which:

- Fetches data for every run on every materialization (no incremental work).
- Cannot express per-run idempotency or audit trail.
- Serializes behind a global `provenance_tagged_items` pass that
  doesn't actually touch `xrd_raw` or `xrf_raw` items.

Meanwhile, the AIMD-L API already exposes these items partition-keyed:

- `GET /aimdl/partition?dataType=<dt>` →
  `{"<igsn>//<experiment_date>": "<content_hash>", ...}`
- `GET /aimdl/partition/details?dataType=<dt>&key=<igsn//experiment_date>`
  → scoped items list with full meta preserved.

The Girder plugin (`xarthisius/girder-jsonforms`, `igsn` branch)
already emits these partition keys. We're not using them.

## Goal

1. Partition `enriched_maxima_raw` on
   `MultiPartitionsDefinition({data_type: Static(["xrd_raw", "xrf_raw"]), run: Dynamic})`.
   One Dagster partition per AIMD-L run.
2. Add a discovery sensor that polls the partition index and adds
   new `run` keys as they appear, emitting `RunRequest`s with
   content-hash-based dedup covering both raw and `xrd_metadata`
   inputs.
3. Upgrade the existing weekly reconciliation schedule to
   gap-filling semantics (materialize only partitions with no
   successful prior materialization).
4. Split `provenance_tagged_items`:
   - HELIX ALPSS parent tagging (mutating) →
     `helix_alpss_provenance_tagged`, remains upstream of
     `enriched_helix_alpss`.
   - MAXIMA `xrd_derived` prov-link presence (non-mutating) →
     `maxima_xrd_derived_provenance_valid`, becomes an asset check,
     **not** an upstream dep.
5. Rewire `enriched_maxima_derived` to depend on
   `enriched_maxima_raw` (the semantically correct lineage) via
   `AllPartitionMapping`.

## Out of scope

- Repartitioning `enriched_maxima_derived` to match the new
  multi-partition shape (separate follow-up; tracked as α-vs-β; α
  chosen here).
- `enriched_helix_alpss` repartitioning.
- HELIX folder sensor or the existing spreadsheet DAG.
- Merging anything else from `refactor/issue21-step2`; only
  `docs/reference/prov_metadata.md` is brought forward.

## Execution

Runbook at `.claude/prompts/issue23/README.md`. Nine-step sequence,
one commit per step, green pytest between every step. Each step is
a self-contained prompt executable in a fresh Claude Code session.
