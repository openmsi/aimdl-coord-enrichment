# Runbook — Production-readiness dry run (coord_enrichment)

## Purpose

Decide whether the `coord_enrichment` DAG is ready to write coordinate
metadata to the live AIMD-L Girder collection. The DAG's `dry_run` flag
(default `True`, `CoordEnrichmentConfig`) gates every Girder write, so a dry
run reads real production data, computes every would-be write, runs every
asset check, and **writes nothing**. The output is a single **GO / NO-GO**
decision.

The real question a dry run answers is **upstream data hygiene** — whether
production items carry the tags the DAG depends on (`meta.igsn`,
`meta.data_type`, and `prov.wasDerivedFrom`/`isPartOf`). Those surface as the
ERROR-severity checks below.

This is read-only. For defense in depth, run it with a **read-only Girder API
key**: dry-run writes nothing, and a read-only key makes any accidental write
fail loudly.

This runbook does **not** cover the live sweep — see
[`coord_enrichment_production_sweep.md`](coord_enrichment_production_sweep.md).

## Go/No-Go rubric

**GO requires ALL of:**

- Both ERROR-severity checks PASS:
  - `all_helix_alpss_tagged` — no unresolved HELIX ALPSS parents
  - `maxima_xrd_derived_provenance_valid` — every `xrd_derived` item has a
    resolvable `prov` link
- Every `enrichment_success_rate_*` check PASSES (≥ 0.90)
- Every `no_coord_transform_failures_*` check PASSES (zero coord failures)
- `written == 0` on every leaf (confirms dry-run is in force)
- Some real activity occurred — `simulated_dry_run + skipped_no_change > 0`
  across the leaves (the DAG resolved real items; `simulated > 0` on a first
  run, `skipped_no_change` dominates on re-runs of already-enriched data)
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

**NO-GO signals:**

- Any blocking check fails → upstream tagging/prov not ready
- `written > 0` → dry-run breach; stop and investigate
- Non-trivial `resolution_errors` → items can't be matched to Girder
- `yaml_sha256` null → coordinate config broken in the deployment

**Verify the transform-version split (report-only, not auto-blocked):** version
selection is timestamp-driven — `enriched_pdv_metadata` (the HELIX root) passes
each shot's timestamp, and the derived leaves inherit their parent's recorded
version. HELIX `v1` applies to shots before the 2026-04-01 recalibration and
`v2` (identity, Station == Sample, after the instrument frame was realigned to
the sample frame) to shots on/after it. The report's "Transform versions
applied" section and per-leaf `versions` column show the `HELIX/v1` vs
`HELIX/v2` breakdown; confirm it matches the expected historical-vs-current
split before the live sweep. (For HELIX this split appears via
`enriched_helix_alpss` inheritance, which requires the parent PDV traces to be
enriched first; if they are not, you will see `resolution_errors` instead.)

## Headless path (scripted, captured)

`operations/dry_run_readiness.py` runs every coord_enrichment job in dry-run
in-process, evaluates the rubric, prints a verdict, and writes a report to
`operations/log/readiness_dry_run_<timestamp>.{md,json}` (gitignored).

1. Set the environment (use a **read-only** key):
   ```
   export GIRDER_API_URL="https://data.htmdec.org/api/v1"
   export GIRDER_API_KEY="<read-only key>"
   export COORD_TRANSFORMS_YAML="/abs/path/to/instrument_coordinate_transforms.yaml"
   export COORD_ENRICHMENT_MANIFEST_ITEM="<girder item id>"
   ```
2. Quick smoke (a couple of MAXIMA raw partitions) to confirm wiring:
   ```
   .venv/bin/python operations/dry_run_readiness.py --sample 2
   ```
3. Full enumeration (every registered MAXIMA raw `(data_type, run)` partition —
   hundreds of in-process runs; expect this to take a while):
   ```
   .venv/bin/python operations/dry_run_readiness.py
   ```
4. Read the printed `VERDICT:` line and the generated `.md` report. Exit code
   is `0` for GO, `1` for NO-GO.

`--skip-maxima-raw` evaluates only the state report and the static leaves.

## UI path (full enumeration via the Dagster UI)

Equivalent walkthrough for operators who prefer the UI. It produces the same
asset-check outcomes; read them from the asset-check panel.

1. Complete the pre-flight in
   [`coord_enrichment_production_sweep.md`](coord_enrichment_production_sweep.md)
   (env vars, manifest item, `pytest`, `dagster dev` loads).
2. **Start `maxima_raw_discovery_sensor`.** Its first tick registers every
   current `(igsn//experiment_date)` key on the `maxima_raw_run` dynamic dim
   and emits one dry-run RunRequest per `(data_type, run)` — this *is* the
   full-enumeration dry run for MAXIMA raw (runs inherit `dry_run=True`).
   Expect hundreds of runs on the first tick.
3. From the launchpad, run in dry-run (`dry_run: true` in run config):
   - `coord_enrichment_helix_alpss_job` — all three `HELIX/pdv_alpss_*` partitions
   - `coord_enrichment_maxima_derived_job` — `MAXIMA/xrd_derived`
   - `coord_enrichment_job` — the state report (run last, so the report asset
     sees leaf coverage)
4. Read the asset-check panel and the per-leaf output metadata
   (`written`, `simulated_dry_run`, `resolution_errors`, …) against the rubric.

## Results table

Fill this in from the report (or the UI) and attach it to the go/no-go
decision. On GO, transcribe the counts into
[`first_sweep_expected_values.md`](first_sweep_expected_values.md) as the
reference baseline.

| Check | Severity | Blocking | Result (PASS/FAIL) | Notes |
|---|---|:-:|:-:|---|
| `all_helix_alpss_tagged` | ERROR | yes | | |
| `maxima_xrd_derived_provenance_valid` | ERROR | yes | | |
| `enrichment_success_rate_maxima_raw` | WARN | yes | | per partition |
| `no_coord_transform_failures_maxima_raw` | WARN | yes | | per partition |
| `enrichment_success_rate_helix_alpss` | WARN | yes | | per partition |
| `no_coord_transform_failures_helix_alpss` | WARN | yes | | per partition |
| `enrichment_success_rate_maxima_derived` | WARN | yes | | |
| `no_coord_transform_failures_maxima_derived` | WARN | yes | | |
| `inventory_nonempty_per_instrument` | WARN | no | | record empties |
| `pdv_coverage_above_threshold` | WARN | no | | record coverage |

| Leaf | seen | written | simulated | skipped | coord_fail | res_err |
|---|--:|--:|--:|--:|--:|--:|
| `enriched_maxima_raw` | | | | | | |
| `enriched_helix_alpss` | | | | | | |
| `enriched_maxima_derived` | | | | | | |

**Decision:** GO / NO-GO — _________   **Date:** _________

If NO-GO, the failing ERROR checks name the upstream tagging/prov work to do
before re-running this dry run.
