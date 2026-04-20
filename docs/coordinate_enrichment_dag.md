# Coordinate Enrichment DAG — Design Document

**Status:** draft
**Branch:** `refactor/asset-dag`
**Prepared:** 2026-04-19
**Scope:** new Dagster workflow that enriches AIMD-L Girder items with
sample-frame coordinate metadata using versioned coordinate transforms from
the `coordinate-transformer` package. HELIX and MAXIMA today; SPHINX deferred.

---

## 1. Motivation and relationship to the existing DAG

The existing `process_helix_assets_job` is a **spreadsheet-driven coordinate
producer**. It reacts to a new HELIX experiment-log CSV appearing in Girder,
reads instrument-frame coordinates out of the spreadsheet, transforms them to
sample-frame coordinates using the current HELIX transform, and writes the
result to the matched PDV trace items. It does not touch MAXIMA, it does not
touch ALPSS outputs, and it does not pass a timestamp to the transformer.

The workflow proposed here is a **scheduled coordinate propagator**. Rather
than reacting to one spreadsheet at a time, it sweeps the AIMD-L collection
for every item that (a) carries `meta.igsn`, (b) has a recognized
`meta.data_type`, and (c) either lacks coordinate metadata or carries
coordinates produced by a now-stale transform version. It writes station and
sample coordinates plus full provenance, dispatches per instrument based on
`data_type` prefix, and selects the correct transform version from the
item's timestamp.

The two jobs are complementary. The existing job writes HELIX PDV trace
coordinates from the spreadsheet (the authoritative source for HELIX). The
new job propagates those to HELIX ALPSS outputs/results, produces MAXIMA
coordinates from `instructions.txt`, and propagates them to MAXIMA derived
products. Once the versioned-transform backport (Phase 1 below) is
complete, both jobs write the same `coord_provenance` schema.

---

## 2. Scope

### In scope

- HELIX: `pdv_trace`, `pdv_alpss_output`, `pdv_alpss_result`, `pdv_alpss_results`
- MAXIMA: `xrd_raw`, `xrf_raw`, `xrd_derived` (in-raw subset only)
- Instrument inference from `meta.data_type` prefix: `pdv_*` → HELIX,
  `xrd_*`/`xrf_*` → MAXIMA
- Timestamp-based transform version selection via the
  `coordinate-transformer` `timestamp=` API
- Writing `Station_X`, `Station_Y`, `Sample_X`, `Sample_Y`, and a full
  `coord_provenance` block to each enriched item
- Overwrite-if-transform-differs policy (see §6)
- An upstream provenance-tagging asset that guarantees
  `meta.prov.wasDerivedFrom` is present and intra-collection-resolvable on
  derived items before enrichment runs

### Out of scope (today)

- SPHINX — the station is not yet producing production data
- MAXIMA root-level `scan_point_N.tiff` files — by policy these do not
  receive coordinate metadata (explained in §7)
- `xrd_metadata` (`instructions.txt`), `xrd_calibrant_*`,
  `pdv_experiment_log`, and `unclassified` — not sample data
- Sample-calibration workflows — a separate future DAG
- Bulk re-tagging of `data_type` or `igsn` on existing items — handled
  upstream by other pipelines (`aimdl-apply`, OpenMSIStream producers)

### Explicitly deferred

- Using the Eiger HDF5 header as a primary timestamp source. MAXIMA items
  already carry `meta.experiment_date`; the HDF5 header is retained as a
  future trust-but-verify mechanism.

---

## 3. Data inventory (grounded in test-data snapshot)

The test folder `coordinate_dag_test_data` contains 450 items after HELIX
remediation. Their metadata shape, verified 2026-04-19:

| data_type | count | `igsn` | `experiment_date` | `prov` | Coordinate source |
|---|---|---|---|---|---|
| `pdv_trace` | 32 | ✓ | — | — | HELIX experiment-log CSV (via existing DAG) |
| `pdv_alpss_output` | 200 | ✓ | — | — | inherit from parent PDV trace |
| `pdv_alpss_results` | 25 | ✓ | — | — | inherit from parent PDV trace |
| `pdv_experiment_log` | 1 | ✓ | — | — | out of scope |
| `xrd_raw` | 54 | ✓ | ✓ | ✓ (`isPartOf` / `hasPart` / `hadDerivation`) | `instructions.txt` scan_points |
| `xrf_raw` | 27 | ✓ | ✓ | — | `instructions.txt` scan_points |
| `xrd_derived` (in `raw/`) | 81 | ✓ | — | ✓ (`wasDerivedFrom` → master.h5) | inherit from parent `xrd_raw` |
| `xrd_derived` (root TIFFs) | 27 | ✓ | ✓ | — | not enriched (by policy, §7) |
| `xrd_metadata` | 3 | ✓ | ✓ | — | out of scope |

Key facts that drive the design:

- **HELIX has no provenance fields.** ALPSS items cannot currently
  inherit from PDV traces; the link must be constructed from filename
  conventions.
- **MAXIMA in-raw derived items have `prov.wasDerivedFrom`**, but in the
  test folder the target item IDs are production IDs that were not
  rewritten when the files were copied. The upstream tagging asset heals
  these dangling pointers.
- **MAXIMA root TIFFs have no prov** and are excluded from enrichment by
  policy — no code needed, they simply fall outside the inventory filter.
- **ALPSS result data_type values vary.** The test folder uses
  `pdv_alpss_results` (plural); production shows both singular and plural
  (549 + 1,526 items respectively). The DAG accepts both and records which
  was seen in the quality report.

---

## 4. DAG architecture

```
                    provenance_tagged_items
                    ✓ all_helix_alpss_tagged
                    ✓ maxima_prov_targets_resolve
                             │
                             ▼
                    enrichable_items_inventory
                    ✓ inventory_nonempty_per_instrument
                             │
                             ▼
                    coord_transform_config_snapshot
                             │
                             ▼
             ┌───────────────┼───────────────┐
             ▼               ▼               ▼
   enriched_helix_pdv  enriched_maxima_raw   (future instruments)
             │               │
             ▼               ▼
   enriched_helix_alpss  enriched_maxima_derived
   ✓ enrichment_success_rate (shared check across all four leaves)
   ✓ no_coord_transform_failures
             │               │
             └───────┬───────┘
                     ▼
             coord_enrichment_report
                     │
                     ▼
             coord_enrichment_manifest
             (writes meta.coord_enrichment_status to a job-scope Girder item)
```

### 4.1 Asset responsibilities

| Asset | I/O | Responsibility |
|---|---|---|
| `provenance_tagged_items` | Girder read + **write** | Ensure every in-scope derived item has a resolvable `meta.prov.wasDerivedFrom`. Writes prov only when missing or dangling. |
| `enrichable_items_inventory` | Girder read | Pull all items with `meta.igsn` and an in-scope `meta.data_type`. Partition by `(instrument, data_type)`. |
| `coord_transform_config_snapshot` | none | Capture the YAML contents, its sha256, the `coordinate-transformer` version, and the version list per instrument. Pure transformation; referenced by all downstream writes. |
| `enriched_helix_pdv` | Girder **write** | Already handled by the existing `process_helix_assets_job`. This asset is an **observer** in the new DAG — it lists PDV traces that already carry `Sample_X/Y` and reports completeness. No writes. |
| `enriched_helix_alpss` | Girder **write** | For each ALPSS item, follow `prov.wasDerivedFrom` to the parent PDV trace, copy `Station_X/Y`, recompute `Sample_X/Y` with the ALPSS item's inherited timestamp, write with fresh provenance. |
| `enriched_maxima_raw` | Girder **write** | For each `xrd_raw` or `xrf_raw`, parse scan-point index from filename, read `instructions.txt` once per run folder, transform, write. |
| `enriched_maxima_derived` | Girder **write** | For each in-raw `xrd_derived`, follow `prov.wasDerivedFrom` to the master.h5, copy its coords, write with fresh provenance. |
| `coord_enrichment_report` | none | Aggregate per-leaf stats: items seen, items skipped (policy), items written, coord transform failures, dangling prov, unresolved timestamps. |
| `coord_enrichment_manifest` | Girder **write** | Write a per-run summary to a known "coordinate DAG status" Girder item so other systems can observe progress. |

### 4.2 Partitioning

Each enrichment leaf runs as a **dynamic partition** keyed by
`(instrument, data_type)`. Concretely:
`HELIX/pdv_alpss_output`, `HELIX/pdv_alpss_result`, `HELIX/pdv_alpss_results`,
`MAXIMA/xrd_raw`, `MAXIMA/xrf_raw`, `MAXIMA/xrd_derived`.

Why: a partial failure on (say) MAXIMA xrf_raw timestamps should not block
HELIX ALPSS propagation. Partitioning also lets Dagster report per-class
coverage and surfaces which subsets still have gaps.

### 4.3 Asset checks

| Check | Asset | Severity | Condition |
|---|---|---|---|
| `all_helix_alpss_tagged` | `provenance_tagged_items` | ERROR | any `pdv_alpss_*` item left with unresolvable prov after the tagging pass |
| `maxima_prov_targets_resolve` | `provenance_tagged_items` | ERROR | any in-scope MAXIMA derived item whose `wasDerivedFrom` target is not an item in the collection |
| `inventory_nonempty_per_instrument` | `enrichable_items_inventory` | WARN | any partition has zero candidates |
| `enrichment_success_rate` | each enrichment leaf | WARN | <90% of partition items successfully enriched |
| `no_coord_transform_failures` | each enrichment leaf | WARN | any transform call raised |

---

## 5. Coordinate and provenance sources (the resolution rules)

One table. For each data_type, where Station_X/Y comes from, where the
timestamp comes from, and what gets recorded in `coord_provenance`.

| data_type | Station_X/Y source | Timestamp source | `coord_provenance.station_coord_source.kind` |
|---|---|---|---|
| `pdv_trace` | HELIX experiment-log CSV (written by existing DAG) | filename regex (`…_YYYY-MM-DD_HH-MM-SS_…`) | `helix_experiment_log` |
| `pdv_alpss_output` | inherited from parent PDV trace | inherited from parent PDV trace | `inherited` |
| `pdv_alpss_result` / `pdv_alpss_results` | inherited from parent PDV trace | inherited from parent PDV trace | `inherited` |
| `xrd_raw` / `xrf_raw` | `instructions.txt` `sample.scan_points[i]` | `meta.experiment_date` | `maxima_instructions` |
| `xrd_derived` (in `raw/`) | inherited from parent `xrd_raw` (master.h5) | inherited from parent | `inherited` |

**Parent resolution rule** (used by every `inherited` case): read
`meta.prov.wasDerivedFrom` → fetch that item → read its `Station_X`,
`Station_Y`, `coord_provenance.source_timestamp`. If any of those three
fields are absent, record the failure and skip — the enrichment of derived
items strictly requires the parent to already be enriched.

**MAXIMA `instructions.txt` lookup.** From any in-scope MAXIMA raw item:
1. Parse scan-point index `i` from the filename via
   `scan_point_(\d+)(?:_|\.)`.
2. Fetch the item's `folderId` (the `raw/` folder).
3. Fetch that folder's `parentId` (the run folder).
4. List items in the run folder, find the one named `instructions.txt`
   (case-sensitive, exact).
5. Download its file, parse as JSON, read `sample.scan_points[i]`.

The parsed `instructions.txt` is cached per run folder within a single DAG
run so repeated lookups for the 25+ scan points of one run folder incur
only one download.

---

## 6. Metadata schema written to Girder items

### 6.1 Enriched item payload

```json
{
  "Station_X": 11.0,
  "Station_Y": 5.0,
  "Sample_X": 25.0,
  "Sample_Y": 25.0,
  "coord_provenance": {
    "instrument": "MAXIMA",
    "transform_version": "v1",
    "transform_yaml_sha256": "a3f2…(64 hex chars)",
    "transformer_version": "0.3.0",
    "pipeline_version": "0.3.0",
    "source_timestamp": "2026-04-16T16:56:16+00:00",
    "source_timestamp_origin": "meta.experiment_date",
    "station_coord_source": {
      "kind": "maxima_instructions",
      "instructions_item_id": "69e381cd917593d318ab3b5e",
      "instructions_sha256": "72cbd53f…",
      "scan_point_index": 17
    },
    "enriched_at": "2026-04-19T14:23:10+00:00",
    "dagster_run_id": "abc123…"
  }
}
```

### 6.2 `station_coord_source` variants

Same key, three shapes depending on how station coordinates were obtained:

```json
"station_coord_source": {
  "kind": "helix_experiment_log",
  "spreadsheet_item_id": "…",
  "spreadsheet_row_index": 12,
  "spreadsheet_pdv_filename": "JHAMAC00003-S1R4C3_…_shot01_ch1.csv"
}
```

```json
"station_coord_source": {
  "kind": "maxima_instructions",
  "instructions_item_id": "…",
  "instructions_sha256": "…",
  "scan_point_index": 17
}
```

```json
"station_coord_source": {
  "kind": "inherited",
  "parent_item_id": "…",
  "parent_data_type": "xrd_raw"
}
```

Every enrichment traces, via one or two hops, back to an authoritative
station-side source artifact (spreadsheet row or `instructions.txt` entry).

### 6.3 Provenance tagging payload (case-specific)

For HELIX ALPSS items missing prov:

```json
"prov": {
  "wasDerivedFrom": "<pdv_trace item_id>",
  "wasGeneratedBy": "alpss-v1.6.0"
}
```

ALPSS version is copied from the existing `meta.version` field on the item
when present; `"alpss"` alone is used as a fallback.

For MAXIMA in-raw items with dangling prov, the `wasDerivedFrom` field is
**overwritten** to point at the master.h5 item_id in the item's own run
folder. `wasGeneratedBy` is left untouched.

For MAXIMA TIFF files at the run-folder root — **no prov is written** and
no enrichment occurs. By policy (§7) these do not receive coordinates.

---

## 7. Policy decisions

### 7.1 MAXIMA TIFFs are not enriched

The root-level `scan_point_N.tiff` files at each MAXIMA run folder are
classified as `xrd_derived` but are not scientifically validated imagery —
adding sample coordinates to them would invite interpretations that are
not defensible. By policy, the inventory excludes them.

Implementation: the MAXIMA `xrd_derived` partition includes only items
whose parent folder is named `raw` (i.e. `path …/raw/scan_point_N_*.{png,csv}`).
Root-level TIFFs fall out of scope purely by path filter — no per-item
special-casing.

### 7.2 Overwrite policy

A write occurs if and only if the `coord_provenance` block **would
differ** from the currently stored one. Concretely, the enrichment asset
computes the new payload, then compares to the existing
`meta.coord_provenance` on the item:

- **no existing coord_provenance** → write
- `transform_yaml_sha256` differs → write
- `transformer_version` differs → write
- `station_coord_source` differs → write (catches `instructions.txt` edits
  and spreadsheet re-matches)
- otherwise → skip (idempotent no-op)

The `source_timestamp`, `enriched_at`, and `dagster_run_id` fields are
**not** compared — they are outputs, not inputs to the transform.

This policy means re-running the DAG after a YAML recalibration
automatically re-enriches every affected item and leaves the rest alone.

### 7.3 Ingestion errors are fatal for the partition, not the DAG

If `instructions.txt` is absent from a run folder, or its JSON is
malformed, or `scan_points[i]` is missing for index `i`, the MAXIMA raw
asset logs an error with the run folder id and **the partition fails**
but sibling partitions continue. Dagster surfaces this in the UI; the
`enrichment_success_rate` check converts it to a visible WARN.

If the Eiger HDF5 header is eventually used and parse fails, raise
explicitly (no silent fallback to filename parsing). For today, we do not
read HDF5 headers — `meta.experiment_date` is authoritative.

### 7.4 Data_type variants

Both `pdv_alpss_result` and `pdv_alpss_results` are accepted for ALPSS
result items. The inventory filter uses an `in` test, not equality. A
note is logged if a single run sees both forms.

### 7.5 Scope gate summary

An item is in scope for coordinate enrichment iff **all** of:
1. `meta.igsn` is present and non-empty
2. `meta.data_type` ∈ {`pdv_trace`, `pdv_alpss_output`, `pdv_alpss_result`,
   `pdv_alpss_results`, `xrd_raw`, `xrf_raw`, `xrd_derived`}
3. if `xrd_derived`: the item's parent folder is named `raw`
4. `meta.data_type` prefix resolves to a known instrument (automatic
   given step 2, but checked explicitly)

Items failing any condition are silently dropped from the inventory. They
are not written to, and their absence does not count toward the
enrichment success rate.

---

## 8. Back-port to the existing DAG (Phase 1)

Before the new DAG is wired up, the existing `process_helix_assets_job`
is modified to produce the same `coord_provenance` block. Three edits:

1. `helix_dagster/coordinates.py` — add optional `timestamp` parameter to
   `transform_station_to_sample`, pass through to the transformer,
   compute and surface the transform version from
   `CoordinateTransformer.get_transform(...).name`.
2. `helix_dagster/assets.py::enriched_pdv_metadata` — parse shot timestamp
   from the spreadsheet `Timestamp` column (timezone-aware), pass to the
   transform wrapper, build a `coord_provenance` block with
   `station_coord_source.kind = "helix_experiment_log"`, include it in
   the per-item metadata write.
3. `helix_dagster/checks.py::coord_transform_check` — extend to also
   verify that the resolved transform version exists (would catch a
   timestamp falling into an unconfigured range).

No new Girder endpoints are used; no schema change to any other asset.
This is the safest place to debug the provenance schema before it starts
being written by the new DAG at scale.

---

## 9. New module layout

```
helix_dagster/                                 (existing — renamed candidate: aimdl_dagster)
├── __init__.py                                # add new job + new assets/checks
├── assets.py                                  # existing + Phase 1 edits
├── checks.py                                  # existing + extended coord_transform_check
├── constants.py                               # existing + new data_type constants
├── coordinates.py                             # Phase 1: accept timestamp
├── girder_io.py                               # existing
├── matching.py                                # existing
├── resources.py                               # existing
├── sensors.py                                 # existing
├── validation.py                              # existing
├── instruments/                               # NEW — per-instrument adapters
│   ├── __init__.py                            #   registry: data_type → instrument
│   ├── base.py                                #   Instrument protocol
│   ├── helix.py                               #   filename timestamp, alpss stem
│   └── maxima.py                              #   scan_point parser, instructions.txt walker
├── coord_enrichment/                          # NEW — the coordinate enrichment DAG
│   ├── __init__.py
│   ├── assets.py                              #   provenance_tagged_items, inventory, 4 enrichment leaves, report, manifest
│   ├── checks.py                              #   asset checks for the new DAG
│   ├── provenance.py                          #   tagging logic (HELIX filename-stem matching, MAXIMA heal)
│   ├── resolution.py                          #   coord + timestamp resolvers per station_coord_source.kind
│   ├── config.py                              #   transform YAML snapshotting, sha256
│   └── writes.py                              #   overwrite-policy evaluator, Girder write wrapper
tests/
├── …existing…
├── test_instruments.py                        # per-adapter contract tests
├── test_coord_enrichment_provenance.py        # tagging, including dangling-prov heal
├── test_coord_enrichment_resolution.py        # parent lookup, instructions.txt cache
├── test_coord_enrichment_writes.py            # overwrite policy decision table
├── test_coord_enrichment_assets.py            # integration against fixture Girder
└── fixtures/
    ├── instructions_JHAMAL00018-009.json      # copy of real test-data instructions.txt
    └── coordinate_transforms.yaml             # test-only YAML with a known v2 boundary
```

The rename of the Python module (`helix_dagster` → `aimdl_dagster`) is
noted but **not** part of this work. It's a larger change that would
cascade through imports, entry points, and the Dagster project layout.

---

## 10. Phased roadmap

### Phase 0 — this design doc (today)

Deliverable: this file, merged on `refactor/asset-dag`.

### Phase 1 — versioned-transform back-port to existing DAG

1. Add `timestamp` parameter to `transform_station_to_sample`.
2. Parse the spreadsheet `Timestamp` column in `enriched_pdv_metadata`.
3. Build and write `coord_provenance`.
4. Extend `coord_transform_check`.
5. Unit tests: timestamp parsing, YAML sha256 stability, provenance
   payload shape.
6. Integration test: re-run against a fixture spreadsheet and confirm
   the Sample_X/Y matches v1 (pre-2026-04-01) for a shot dated
   2026-02-18 and v2 for a shot dated today.

### Phase 2 — `instruments/` adapter module ✅ complete

1. Define `Instrument` protocol (`name_in_yaml`, `data_types`,
   `resolve_timestamp`, `resolve_station_coords`).
2. Implement `HelixInstrument` mirroring current behavior (reads from
   enriched-spreadsheet world, not from item metadata).
3. Implement `MaximaInstrument` (scan_point parser,
   `instructions.txt` walker with per-run caching).
4. Registry keyed by `data_type` prefix → instrument instance.
5. Contract tests exercising each adapter against real fixtures copied
   from the test folder.

**Status (landed on `refactor/asset-dag`):** `helix_dagster/instruments/`
ships a data_type registry, two adapters (HELIX for ALPSS→PDV parent
discovery; MAXIMA for leaf coord resolution and derived-item prov
healing), and package-level `resolve_parent_item_id` /
`resolve_leaf` dispatch helpers. Caching of per-run-folder
instructions.txt lookups is deliberately deferred to Phase 3's DAG
asset.

### Phase 3 — `coord_enrichment` DAG, MAXIMA raw first

1. `provenance_tagged_items` asset — HELIX ALPSS tagging and MAXIMA
   prov-heal only. No coordinate writes.
2. `enrichable_items_inventory` and `coord_transform_config_snapshot`.
3. `enriched_maxima_raw` — `xrd_raw` + `xrf_raw` partitions.
4. `coord_enrichment_report` and `coord_enrichment_manifest` (minimal).
5. Dry-run default; `--live` config flag for writes.
6. Integration test against `JHAMAL00018-009_…_16-56-16` (25 scan
   points): expect 50 writes (25 xrd_raw + 25 xrf_raw) and provenance
   round-tripping.

### Phase 4 — derived inheritance leaves

1. `enriched_maxima_derived` — in-raw `xrd_derived` only, parent lookup
   via healed `wasDerivedFrom`.
2. `enriched_helix_alpss` — parent lookup via freshly-tagged
   `wasDerivedFrom`.
3. `enriched_helix_pdv` — read-only observer asset reporting coverage.
4. Parent-missing failure path tested explicitly (delete a PDV trace
   item in a test-only fixture, confirm ALPSS partition reports
   unresolved parents without crashing).

### Phase 5 — overwrite-policy hardening and production roll-out

1. Overwrite decision evaluator under unit test, including the decision
   table in §7.2.
2. Sensor or schedule on data.htmdec.org.
3. First production sweep, monitoring `enrichment_success_rate` per
   partition.
4. Feedback loop: any surprising skips or failures in the first sweep
   get traced and documented before a second run.

---

## 11. Environment variables

Existing variables are unchanged. New:

| Variable | Description | Required |
|---|---|---|
| `AIMDL_COORD_DAG_MANIFEST_ITEM` | Girder item id that receives `coord_enrichment_status` writes from the manifest asset | Yes |
| `COORD_ENRICHMENT_DRY_RUN` | If set truthy, all enrichment leaves log what they would write and skip the actual Girder PUT | No |

---

## 12. Open questions remaining after this draft

None are blockers, but flagging for the next conversation:

1. **Module rename.** `helix_dagster` no longer describes what the
   module does. Proposed: `aimdl_dagster`. Separate PR; not part of
   this work.
2. **Sensor vs schedule for the new DAG.** Sweep-style propagation
   doesn't naturally fit a folder sensor. A daily or weekly schedule is
   the obvious starting point; Kafka-triggered partial sweeps (enrich
   only the items touched in the last hour) are a possible later
   optimization.
3. **Error-aggregation granularity.** How much structured context does
   the manifest need about failures — is the aggregated count enough,
   or do we want the full failing-item id list to land in Girder?
   Leaning toward counts + capped sample of item ids, but a policy
   call.
4. **Provenance tagging as a standalone job.** The tagging logic is
   conceptually independent of coordinate enrichment. Offering it as a
   separable `tag_provenance_job` that the enrichment DAG depends on
   (rather than invokes) is cleaner architecturally. Low priority;
   useful once a second consumer of `prov.wasDerivedFrom` appears.
5. **Annotation rule.** `from __future__ import annotations`
   breaks Dagster's Config schema resolution. Rule and
   enforcement test documented at
   `docs/developer_notes/annotations.md`. No action required
   unless a new Dagster-adjacent module appears — that module's
   path should be added to the forbidden list in
   `tests/test_annotations_rule.py`.
