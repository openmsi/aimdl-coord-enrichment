# Python File Inventory

Every Python file in the repo (69 files, 11,039 lines), grouped by flow.

The **Terminal output?** column is **Yes** only when an asset's
materialization is read by humans/the Dagster UI but is *not* consumed —
as a data input or via its Girder writes — by any downstream asset.
`N/A` marks files that produce no materialization (helpers, registry,
orchestration, tests, scripts).

The three source flows sum to exactly **4,630 lines** (the
`aimdl_coord_enrichment` package). Tests (5,647), the ops script (366),
and a scratch probe (396) make up the rest.

--

## Flow 1 — Spreadsheet DAG (`process_helix_assets_job`)

| File | Lines | Terminal output? |
|---|---|---|
| `aimdl_coord_enrichment/assets.py` | 528 | **Mixed** — 7 assets feed downstream; only `processing_manifest` is a sink |
| `aimdl_coord_enrichment/checks.py` | 175 | **Yes** — asset checks, read-only, never feed a materialization |
| **subtotal** | **703** | |

## Flow 2 — Coord-enrichment DAG

| File | Lines | Terminal output? |
|---|---|---|
| `aimdl_coord_enrichment/coord_enrichment/__init__.py` | 101 | N/A — package wiring |
| `aimdl_coord_enrichment/coord_enrichment/config.py` | 24 | N/A — Config schema |
| `aimdl_coord_enrichment/coord_enrichment/config_snapshot.py` | 70 | No — `coord_transform_config_snapshot` feeds every leaf |
| `aimdl_coord_enrichment/coord_enrichment/inventory.py` | 167 | No — `enrichable_items_inventory` feeds the leaves |
| `aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py` | 353 | No — `enriched_maxima_raw` writes are inherited by `enriched_maxima_derived` |
| `aimdl_coord_enrichment/coord_enrichment/provenance_tagging.py` | 209 | No — `prov` tags are read by `enriched_helix_alpss` inheritance |
| `aimdl_coord_enrichment/coord_enrichment/helix_alpss_leaf.py` | 212 | **Yes** — `enriched_helix_alpss` is a write sink; nothing reads it back |
| `aimdl_coord_enrichment/coord_enrichment/maxima_derived_leaf.py` | 285 | **Yes** — `enriched_maxima_derived` is a write sink |
| `aimdl_coord_enrichment/coord_enrichment/pdv_observer.py` | 102 | No — read-only, but output feeds `coord_enrichment_report` |
| `aimdl_coord_enrichment/coord_enrichment/report.py` | 210 | No — read-only, but output feeds `coord_enrichment_manifest` |
| `aimdl_coord_enrichment/coord_enrichment/manifest.py` | 92 | **Yes** — `coord_enrichment_manifest` is the terminal sink |
| `aimdl_coord_enrichment/coord_enrichment/inheritance.py` | 155 | N/A — helper (parent coord inheritance) |
| `aimdl_coord_enrichment/coord_enrichment/check_support.py` | 138 | N/A — helper (check plumbing) |
| `aimdl_coord_enrichment/coord_enrichment/cache.py` | 42 | N/A — helper |
| `aimdl_coord_enrichment/coord_enrichment/overwrite.py` | 38 | N/A — helper (skip/overwrite policy) |
| `aimdl_coord_enrichment/instruments/maxima.py` | 267 | N/A — data-type registry |
| `aimdl_coord_enrichment/instruments/__init__.py` | 180 | N/A — data-type registry |
| `aimdl_coord_enrichment/instruments/helix.py` | 63 | N/A — data-type registry |
| `aimdl_coord_enrichment/instruments/types.py` | 42 | N/A — registry types |
| `aimdl_coord_enrichment/schedules.py` | 176 | N/A — orchestration (all coord_enrichment) |
| **subtotal** | **2,926** | |

## Flow 3 — Shared / cross-cutting (used by both DAGs)

| File | Lines | Terminal output? |
|---|---|---|
| `aimdl_coord_enrichment/girder_io.py` | 231 | N/A — Girder read/write/query helpers |
| `aimdl_coord_enrichment/__init__.py` | 194 | N/A — `Definitions` registry (both flows) |
| `aimdl_coord_enrichment/sensors.py` | 162 | N/A — `helix_folder_sensor` + `maxima_raw_discovery_sensor` |
| `aimdl_coord_enrichment/coordinates.py` | 104 | N/A — `transform_station_to_sample` (both DAGs) |
| `aimdl_coord_enrichment/provenance.py` | 104 | N/A — `coord_provenance` builder (both DAGs) |
| `aimdl_coord_enrichment/resources.py` | 67 | N/A — `GirderConnection` resource |
| `aimdl_coord_enrichment/constants.py` | 56 | N/A — column map, regex, env vars |
| `aimdl_coord_enrichment/validation.py` | 46 | N/A — IGSN validation helper |
| `aimdl_coord_enrichment/matching.py` | 37 | N/A — PDV filename matching helper |
| **subtotal** | **1,001** | |

## Operations & scratch (not part of any DAG)

| File | Lines | Terminal output? |
|---|---|---|
| `operations/dry_run_readiness.py` | 366 | N/A — standalone read-only readiness script |
| `.claude/scratch/probe_maxima_layout.py` | 396 | N/A — throwaway exploration probe |
| **subtotal** | **762** | |

## Tests (36 files, 5,647 lines — all flow = Tests, terminal = N/A)

| File | Lines | File | Lines |
|---|---|---|---|
| `tests/test_assets.py` | 435 | `tests/test_coord_enrichment_inheritance.py` | 168 |
| `tests/test_coord_enrichment_maxima_derived.py` | 397 | `tests/test_checks.py` | 159 |
| `tests/test_coord_enrichment_phase4_e2e.py` | 390 | `tests/test_schedules.py` | 155 |
| `tests/test_coord_enrichment_helix_alpss.py` | 332 | `tests/test_leaf_check_partition_isolation.py` | 140 |
| `tests/test_coord_enrichment_inventory.py` | 321 | `tests/test_provenance.py` | 126 |
| `tests/test_coord_enrichment_e2e.py` | 310 | `tests/test_coord_enrichment_partitioned_jobs.py` | 113 |
| `tests/test_coord_enrichment_maxima_raw.py` | 282 | `tests/test_sensors_maxima_discovery.py` | 108 |
| `tests/test_instruments_maxima_girder.py` | 221 | `tests/test_coord_enrichment_pdv_observer.py` | 106 |
| `tests/test_girder_io.py` | 219 | `tests/test_coord_enrichment_overwrite.py` | 105 |
| `tests/test_coord_enrichment_provenance_tagging.py` | 218 | `tests/conftest.py` | 96 |
| `tests/test_coordinates.py` | 201 | `tests/test_phase5_artifacts.py` | 94 |
| `tests/test_instruments_contract.py` | 162 | `tests/test_instruments_maxima_pure.py` | 93 |
| `tests/test_coord_enrichment_manifest.py` | 85 | `tests/test_coord_enrichment_report_fresh.py` | 92 |
| `tests/test_asset_groups.py` | 75 | `tests/test_instruments_helix.py` | 73 |
| `tests/test_coord_enrichment_config_snapshot.py` | 68 | `tests/test_coord_enrichment_cache.py` | 65 |
| `tests/test_instruments_registry.py` | 65 | `tests/test_annotations_rule.py` | 59 |
| `tests/test_validation.py` | 40 | `tests/test_matching.py` | 39 |
| `tests/test_processing.py` | 35 | `tests/__init__.py` | 0 |

---

## Terminal sinks across the whole repo

Five assets are true terminal sinks (read but never fed back into another
materialization):

- `processing_manifest` (in `assets.py`)
- `enriched_helix_alpss` (`helix_alpss_leaf.py`)
- `enriched_maxima_derived` (`maxima_derived_leaf.py`)
- `coord_enrichment_manifest` (`manifest.py`)

…plus the read-only `checks.py` asset checks.

### Two judgment calls

- **`pdv_observer.py` / `report.py`** are read-only (they never write to
  Girder) but are marked **No**, because their *output* is consumed by a
  downstream asset. If the question is "does it mutate anything," those two
  are safe alongside the checks.
- **`enriched_maxima_raw`** looks like a leaf but is **not** terminal —
  `enriched_maxima_derived` inherits the coordinates it writes to Girder, so
  its results *are* an input to a later materialization. The dependency runs
  through Girder, encoded as an `AssetDep` with `AllPartitionMapping`.
