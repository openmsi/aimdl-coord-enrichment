# Coordinate Enrichment DAG — Design Document

**Status:** living document — post issue #23 snapshot
**Branch:** `refactor/issue23-dynamic-partitions` (merging to `refactor/asset-dag`)
**Prepared:** 2026-04-19
**Updated:** 2026-04-24 for the dynamic-partition + provenance-split refactor
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
- A HELIX-only provenance-tagging asset
  (`helix_alpss_provenance_tagged`) that writes
  `meta.prov.wasDerivedFrom` on ALPSS items before
  `enriched_helix_alpss` runs. MAXIMA `xrd_derived` prov is
  **not** written here — amdee_xrd owns that upstream and we
  verify it via an asset check on `enriched_maxima_derived`.

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
             ┌─────────────────────────┐
             │ maxima_raw_discovery_   │
             │ sensor (STOPPED default)│
             │ polls /aimdl/partition  │
             │ adds run keys, emits    │
             │ RunRequests per         │
             │ (data_type, aimdl_key)  │
             └────────────┬────────────┘
                          │ (dynamic partition registration
                          │  + run-keyed RunRequests)
                          ▼
                    enriched_maxima_raw
                    MultiPartitions(
                      data_type: {xrd_raw, xrf_raw},
                      run: Dynamic "maxima_raw_run"
                    )
                    ✓ enrichment_success_rate_maxima_raw
                    ✓ no_coord_transform_failures_maxima_raw
                          │ AllPartitionMapping
                          ▼
                    enriched_maxima_derived  (single static partition)
                    ✓ enrichment_success_rate_maxima_derived
                    ✓ no_coord_transform_failures_maxima_derived
                    ✓ maxima_xrd_derived_provenance_valid  (ERROR)

   helix_alpss_provenance_tagged
   ✓ all_helix_alpss_tagged
             │
             ▼
     enriched_helix_alpss  (HELIX/pdv_alpss_* static partitions)
     ✓ enrichment_success_rate_helix_alpss
     ✓ no_coord_transform_failures_helix_alpss

              (all leaves also consume
               coord_transform_config_snapshot and
               enrichable_items_inventory — except
               enriched_maxima_raw, which fetches its own
               partition via /aimdl/partition/details)

                    coord_enrichment_report
                             │
                             ▼
                    coord_enrichment_manifest
                    (writes meta.coord_enrichment_status to a job-scope Girder item)
```

### 4.1 Asset responsibilities

| Asset | I/O | Responsibility |
|---|---|---|
| `helix_alpss_provenance_tagged` | Girder read + **write** | Write `meta.prov.wasDerivedFrom` on HELIX ALPSS items (`pdv_alpss_output`, `pdv_alpss_result`, `pdv_alpss_results`). Resolves the parent PDV trace by filename stem. HELIX-only; MAXIMA prov is owned by amdee_xrd upstream. |
| `enrichable_items_inventory` | Girder read | Pull all items with `meta.igsn` and an in-scope `meta.data_type`. Partition by `(instrument, data_type)`. Consumed by the HELIX ALPSS leaf and the MAXIMA derived leaf. `enriched_maxima_raw` does **not** read this inventory. |
| `coord_transform_config_snapshot` | none | Capture the YAML contents, its sha256, the `coordinate-transformer` version, and the version list per instrument. Pure transformation; referenced by all downstream writes. |
| `helix_pdv_coverage_observer` | Girder read | Read-only observer asset that reports the fraction of PDV traces carrying `Sample_X/Y` (produced by the existing spreadsheet DAG). Feeds the `pdv_coverage_above_threshold` WARN check. |
| `enriched_helix_alpss` | Girder **write** | Depends on `helix_alpss_provenance_tagged`. For each ALPSS item, follow `prov.wasDerivedFrom` to the parent PDV trace, copy `Station_X/Y`, re-apply the parent's recorded HELIX transform version, write with fresh `coord_provenance`. |
| `enriched_maxima_raw` | Girder **write** | For each partition `(data_type ∈ {xrd_raw, xrf_raw}, run="<igsn>//<experiment_date>")`, fetch items via `/aimdl/partition/details?dataType=<dt>&key=<aimdl_key>`, fetch the matching `xrd_metadata/instructions.txt` the same way, parse the scan-point index from filenames, transform using `meta.experiment_date`, write. No dep on the inventory or any provenance asset. |
| `enriched_maxima_derived` | Girder **write** | Depends on `enriched_maxima_raw` via `AllPartitionMapping` (single `MAXIMA/xrd_derived` static partition). For each in-raw `xrd_derived`, read the amdee_xrd-written `prov.wasDerivedFrom` or `prov.isPartOf` to find its master.h5, inherit coords. Does not re-write prov. |
| `coord_enrichment_report` | none | Aggregate per-leaf stats: items seen, items skipped (policy), items written, coord transform failures, dangling prov, unresolved timestamps. |
| `coord_enrichment_manifest` | Girder **write** | Write a per-run summary to a known "coordinate DAG status" Girder item so other systems can observe progress. |

### 4.2 Partitioning

Partition shapes are deliberately heterogeneous because the upstream
realities differ:

- **`enriched_maxima_raw`** —
  `MultiPartitionsDefinition({data_type: Static(["xrd_raw", "xrf_raw"]), run: Dynamic("maxima_raw_run")})`.
  Each partition corresponds to one AIMD-L run identified by the
  literal partition string `"<igsn>//<experiment_date>"` emitted by
  the Girder plugin. `maxima_raw_discovery_sensor` registers new
  `run` keys as they appear upstream. Scope: one sample × one
  modality × one experiment date per partition.
- **`enriched_maxima_derived`** — `StaticPartitionsDefinition(["MAXIMA/xrd_derived"])`,
  single partition, depends on raw via `AllPartitionMapping`. The
  derived coverage problem is a single aggregate question — "did
  every in-raw xrd_derived inherit from its parent?" — so
  repartitioning to match raw's `(data_type, run)` shape was
  explicitly deferred (α-vs-β, α chosen in issue #23).
- **`enriched_helix_alpss`** —
  `StaticPartitionsDefinition(["HELIX/pdv_alpss_output", "HELIX/pdv_alpss_result", "HELIX/pdv_alpss_results"])`.
  Three ALPSS filename variants processed independently. Keeping
  HELIX static until the HELIX folder sensor is rebuilt (out of
  scope for issue #23).

A partial failure on one `(data_type, run)` partition should not
block other partitions — dynamic raw partitioning is what gives us
per-run idempotency and per-run audit trails.

### 4.3 Asset checks

| Check | Asset | Severity | Condition |
|---|---|---|---|
| `all_helix_alpss_tagged` | `helix_alpss_provenance_tagged` | ERROR | any `pdv_alpss_*` item left with unresolvable prov after the tagging pass |
| `maxima_xrd_derived_provenance_valid` | `enriched_maxima_derived` | ERROR | any `xrd_derived` item has a `resolution_errors` entry with `stage="inherit_from_parent"` — i.e. missing/dangling `wasDerivedFrom` or parent outside the inventory slice. Non-mutating — amdee_xrd remains the sole writer. |
| `inventory_nonempty_per_instrument` | `enrichable_items_inventory` | WARN | any partition has zero candidates |
| `enrichment_success_rate_<leaf>` | each enrichment leaf | WARN | <90% of partition items successfully enriched |
| `no_coord_transform_failures_<leaf>` | each enrichment leaf | WARN | any transform call raised |
| `pdv_coverage_above_threshold` | `helix_pdv_coverage_observer` | WARN | PDV traces with `Sample_X/Y` fall below threshold (currently 0.5; placeholder) |

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
├── coord_enrichment/                          # the coordinate enrichment DAG
│   ├── __init__.py
│   ├── config.py                              #   CoordEnrichmentConfig
│   ├── config_snapshot.py                     #   coord_transform_config_snapshot
│   ├── inventory.py                           #   enrichable_items_inventory + MultiPartitionsDefinition
│   ├── provenance_tagging.py                  #   helix_alpss_provenance_tagged (HELIX-only)
│   ├── enrichment_leaves.py                   #   enriched_maxima_raw (partition-scoped fetches)
│   ├── helix_alpss_leaf.py                    #   enriched_helix_alpss (inheritance)
│   ├── maxima_derived_leaf.py                 #   enriched_maxima_derived + maxima_xrd_derived_provenance_valid check
│   ├── inheritance.py                         #   parent-lookup helper used by both inheritance leaves
│   ├── pdv_observer.py                        #   helix_pdv_coverage_observer
│   ├── overwrite.py                           #   overwrite-policy evaluator
│   ├── cache.py                               #   per-run-folder cache helpers
│   ├── report.py                              #   coord_enrichment_report
│   └── manifest.py                            #   coord_enrichment_manifest
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

### Phase 3 — `coord_enrichment` DAG, MAXIMA raw first ✅ complete (superseded by Phase 6)

1. A combined provenance-tagging asset (since split along data-flow
   lines in Phase 6; see there for the current shape).
2. `enrichable_items_inventory` and `coord_transform_config_snapshot`.
3. `enriched_maxima_raw` — initially two static partitions by
   `data_type`; reshaped in Phase 6 to a `(data_type, run)`
   `MultiPartitionsDefinition`.
4. `coord_enrichment_report` and `coord_enrichment_manifest` (minimal).
5. Dry-run default; `--live` config flag for writes.
6. Integration test against `JHAMAL00018-009_…_16-56-16` (25 scan
   points): expect 50 writes (25 xrd_raw + 25 xrf_raw) and provenance
   round-tripping.

**Status (originally landed on `refactor/asset-dag`; reshaped in issue #23 — see Phase 6):**
The six originally-shipped assets are still present in name, but
the tagger asset was split into a HELIX-only mutator plus a
MAXIMA-derived-side asset check, and `enriched_maxima_raw` was
repartitioned onto a `MultiPartitionsDefinition({data_type, run})`.
`coord_enrichment_job` is wired into `defs`. End-to-end integration
test materializes writes against the JHAMAL00018-009 fixture. Dry-run
is the default; `--live` runs flip `CoordEnrichmentConfig.dry_run=False`.

### Phase 4 — derived inheritance leaves ✅ complete

1. `enriched_maxima_derived` — in-raw `xrd_derived` only, parent lookup
   via healed `wasDerivedFrom`.
2. `enriched_helix_alpss` — parent lookup via freshly-tagged
   `wasDerivedFrom`.
3. `enriched_helix_pdv` — read-only observer asset reporting coverage.
4. Parent-missing failure path tested explicitly (delete a PDV trace
   item in a test-only fixture, confirm ALPSS partition reports
   unresolved parents without crashing).

**Status (landed on `refactor/asset-dag`):**
`enriched_helix_alpss` and `enriched_maxima_derived` ship as
inheritance-based leaves that fetch the parent item, re-apply
the parent's recorded transform version verbatim (via a new
`coordinates.transform_with_named_version` helper), and write
fresh `coord_provenance` with `station_coord_source.kind ==
"inherited"`. `helix_pdv_coverage_observer` is a read-only
asset reporting pdv_trace coverage. `coord_enrichment_report`
now aggregates all three enrichment leaves plus the observer.
An integration test materializes the full Phase 4 surface
end-to-end; dry-run default is preserved from Phase 3. Version
bumped to 0.4.0.

### Phase 5 — overwrite-policy hardening and production roll-out ✅ complete

1. Overwrite decision evaluator under unit test, including the decision
   table in §7.2.
2. Sensor or schedule on data.htmdec.org.
3. First production sweep, monitoring `enrichment_success_rate` per
   partition.
4. Feedback loop: any surprising skips or failures in the first sweep
   get traced and documented before a second run.

**Status (landed on `refactor/asset-dag`):**

- Three partitioned sibling jobs registered:
  `coord_enrichment_maxima_raw_job`,
  `coord_enrichment_helix_alpss_job`,
  `coord_enrichment_maxima_derived_job`.
- Four schedules in `helix_dagster/schedules.py`, all default
  STOPPED: one nightly state-report at 03:00 local, three
  weekly sweeps Sunday 04:00–04:30. Default RunConfig
  `dry_run=True`.
- `COORD_ENRICHMENT_MANIFEST_ITEM` env var wired into the
  manifest asset as a fallback for
  `CoordEnrichmentConfig.manifest_tracking_item_id`.
- Operator runbook and one-shot live-sweep script in
  `docs/runbooks/` and `operations/`.
- Expected-values document at
  `docs/runbooks/first_sweep_expected_values.md`.

Version bumped to 0.5.0. The existing
spreadsheet-driven `process_helix_assets_job` is unchanged.

### Phase 6 — dynamic partitions for MAXIMA raw + provenance split ✅ complete

Tracked as issue #23. Nine-step refactor on
`refactor/issue23-dynamic-partitions`. Changes:

1. **MAXIMA raw partitioning.** Replaced
   `StaticPartitionsDefinition(["MAXIMA/xrd_raw", "MAXIMA/xrf_raw"])`
   with
   `MultiPartitionsDefinition({data_type: Static(["xrd_raw", "xrf_raw"]), run: DynamicPartitionsDefinition("maxima_raw_run")})`.
   `enriched_maxima_raw` now fetches its own items per partition
   via `/aimdl/partition/details?dataType=<dt>&key=<igsn//experiment_date>`
   and no longer depends on `enrichable_items_inventory` or the
   provenance-tagging asset.
2. **Discovery sensor.** Added `maxima_raw_discovery_sensor` in
   `helix_dagster/sensors.py`. Polls `/aimdl/partition` for
   `xrd_raw`, `xrf_raw`, and `xrd_metadata`, adds each new
   `"<igsn>//<experiment_date>"` to the dynamic `maxima_raw_run`
   dim, and emits one `RunRequest` per observed `(data_type, aimdl_key)`
   with a dedup run_key of shape
   `coord-enrichment|<dt>|<aimdl_key>|raw=<h>|xrd_metadata=<h>`
   (falling back to `"no-xrd-metadata"` when no metadata entry
   exists). STOPPED by default.
3. **Gap-filling reconciliation.** The weekly
   `coord_enrichment_maxima_raw_weekly_schedule` now enumerates all
   registered `(data_type, run)` partitions and emits a RunRequest
   only for partitions without a successful materialization. Still
   STOPPED by default, still dry-run only.
4. **Provenance split.**
   - HELIX ALPSS parent tagging (mutating) lives in its own asset
     `helix_alpss_provenance_tagged`. HELIX-only; MAXIMA items are
     not touched by the pipeline's provenance writers.
   - MAXIMA `xrd_derived` prov verification (non-mutating) lives
     in the asset check `maxima_xrd_derived_provenance_valid` on
     `enriched_maxima_derived`. It inspects `resolution_errors`
     with `stage="inherit_from_parent"` and fails if any exist.
   - The previously-combined MAXIMA-prov-heal check is removed;
     amdee_xrd is now treated as the sole writer for
     `xrd_derived` prov, and the pipeline only reads and verifies.
5. **Derived lineage.** `enriched_maxima_derived` now explicitly
   depends on `enriched_maxima_raw` via `AllPartitionMapping`. The
   derived partition remains single-static (decision α; β —
   repartitioning derived to match raw — is a deferred follow-up).

Version bumped to 0.6.0.

---

## 11. Environment variables

Existing variables are unchanged. New:

| Variable | Description | Required |
|---|---|---|
| `COORD_ENRICHMENT_MANIFEST_ITEM` | Girder item id that receives `meta.coord_enrichment_status` when the manifest asset runs. Read as a fallback when `CoordEnrichmentConfig.manifest_tracking_item_id` is not set. | No (but required for production scheduling) |
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

   > **Resolved in Phase 5**: schedule (dry-run, STOPPED by default).
   > **Extended in Phase 6 (issue #23)**: `maxima_raw_discovery_sensor`
   > now drives the MAXIMA raw job off AIMD-L partition keys; the
   > weekly MAXIMA-raw schedule is gap-filling reconciliation. HELIX
   > and MAXIMA derived remain schedule-only for now.
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

   > **Deferred**: `helix_alpss_provenance_tagged` runs in every
   > HELIX-ALPSS-touching job as an unpartitioned upstream. Extract
   > to its own job only if a second consumer appears. MAXIMA
   > `xrd_derived` prov is owned by amdee_xrd upstream — no
   > pipeline-side tagging asset exists for it as of issue #23.
5. **Annotation rule.** `from __future__ import annotations`
   breaks Dagster's Config schema resolution. Rule and
   enforcement test documented at
   `docs/developer_notes/annotations.md`. No action required
   unless a new Dagster-adjacent module appears — that module's
   path should be added to the forbidden list in
   `tests/test_annotations_rule.py`.
6. **First live sweep date.** No target date yet — operator
   action pending `COORD_ENRICHMENT_MANIFEST_ITEM` creation on
   data.htmdec.org and team signoff. See
   `docs/runbooks/coord_enrichment_production_sweep.md`.
