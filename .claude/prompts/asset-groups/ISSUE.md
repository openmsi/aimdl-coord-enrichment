Tracking: #28

## Summary

Assign every Dagster asset to a logical `group_name` so the asset graph
separates the two DAGs by instrument and role. Pure metadata change — no
lineage, partition, job, schedule, or sensor behavior changes.

## Problem

All 18 assets sit in Dagster's implicit `default` group. The HELIX
spreadsheet/PDV pipeline and the coordinate-enrichment DAG render as one
undifferentiated blob in the UI, which hurts navigation and post-mortem
reasoning. Group names are the standard Dagster mechanism for this and are
orthogonal to execution: jobs here select assets by explicit reference
(`AssetSelection.assets(...)`), and no `AssetSelection.groups(...)` exists, so
grouping cannot alter what runs.

## Goal

Add `group_name=` to each `@asset` decorator per this mapping (18 assets, 6
groups). Instrument is a consistent prefix; the DAG/operational-unit boundary
is preserved.

| Group | Assets | File |
|---|---|---|
| `helix_spreadsheet` | experiment_log_source, raw_experiment_log, pdv_trace_inventory, validated_rows, pdv_cross_references, enriched_pdv_metadata, alpss_results_inventory, quality_report, processing_manifest | aimdl_coord_enrichment/assets.py |
| `helix_alpss` | helix_alpss_provenance_tagged, enriched_helix_alpss | coord_enrichment/provenance_tagging.py, coord_enrichment/helix_alpss_leaf.py |
| `maxima_raw` | enriched_maxima_raw | coord_enrichment/enrichment_leaves.py |
| `maxima_derived` | enriched_maxima_derived | coord_enrichment/maxima_derived_leaf.py |
| `coord_enrichment_core` | coord_transform_config_snapshot, enrichable_items_inventory | coord_enrichment/config_snapshot.py, coord_enrichment/inventory.py |
| `coord_enrichment_reporting` | coord_enrichment_report, coord_enrichment_manifest, helix_pdv_coverage_observer | coord_enrichment/report.py, coord_enrichment/manifest.py, coord_enrichment/pdv_observer.py |

## Directives / constraints

- **Touch only the `@asset` decorators.** Add the `group_name="..."` kwarg.
  Do not change deps, partitions, configs, jobs, schedules, sensors, or
  docstrings.
- **Do not group asset checks.** `@asset_check` has no `group_name`; checks
  inherit their target asset's group in the UI. Leave `checks.py` and the
  per-leaf checks untouched.
- **Project convention:** do NOT add `from __future__ import annotations` to
  any of these modules — they are Dagster-adjacent and a CI test
  (`tests/test_annotations_rule.py`) forbids it. See
  `docs/developer_notes/annotations.md`.
- `helix_spreadsheet` is ONE group of 9 — do not subdivide into stages; do not
  move the two inventory assets out.
- Lock the exact group strings above (snake_case, instrument-prefixed).

## Out of scope

- Renaming or re-scoping any job, schedule, sensor, or partition def.
- Subdividing `helix_spreadsheet` into pipeline stages.
- Moving `helix_pdv_coverage_observer` (stays in `coord_enrichment_reporting`).
- Any change to coordinate logic, Girder I/O, or the `instruments/` registry.

## Test

Add `tests/test_asset_groups.py` asserting each asset's `group_name` matches
the table above (mirrors the convention-enforcement style of
`tests/test_annotations_rule.py`). This locks the assignment so future edits
can't silently drop assets back into `default`. Resolve groups from the loaded
`Definitions` (`aimdl_coord_enrichment.defs`).

## Verification

1. `.venv/bin/python -c "import aimdl_coord_enrichment as m; ..."` — print the
   asset→group map and assert the 6 groups have the expected membership counts
   (9/2/1/1/2/3) and that no asset remains in `default`.
2. `.venv/bin/pytest tests/ -v` — green, including the new group test and the
   existing annotations-rule test.
3. (Optional manual) `dagster dev` → Assets graph shows 6 labeled groups.

## Execution

Single commit on a branch cut from `main` (e.g. `refactor/asset-groups`),
then PR. One mechanical pass; no multi-step runbook needed.
