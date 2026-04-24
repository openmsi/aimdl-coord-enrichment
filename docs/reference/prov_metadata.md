# Provenance metadata in AIMD-L items

Reference for who writes which `meta.prov.*` key on Girder items in the
AIMD-L collection, and what the coord-enrichment pipeline can rely on.

Last verified: 2026-04-23 against `data.htmdec.org`.
Updated 2026-04-24 to reflect the post–issue #23 split.

## TL;DR

The coord-enrichment pipeline **reads** provenance fields written by
upstream actors wherever it can. For MAXIMA xrd items, it does **not**
write `prov.wasDerivedFrom` or `prov.isPartOf` — those are already
present from `amdee_xrd` and are treated as read-only. The pipeline's
own write-only contribution is `meta.coord_provenance` on enriched
items, which no other actor touches.

For HELIX ALPSS items, which fall outside `amdee_xrd`'s scope, the
pipeline **does** write `prov.wasDerivedFrom` linking the ALPSS result
back to its parent `pdv_trace`. This is the only place the pipeline
ever mutates `meta.prov.*`.

After issue #23 the provenance surface is split along the data-flow:

- HELIX ALPSS parent tagging (a real mutation) lives in the asset
  [`helix_alpss_provenance_tagged`][tag-asset]. `enriched_helix_alpss`
  depends on it.
- MAXIMA `xrd_derived` prov links are **verified**, not written, by
  the asset check
  [`maxima_xrd_derived_provenance_valid`][derived-check] on
  `enriched_maxima_derived`. It inspects the leaf's
  `resolution_errors` for any entry with
  `stage == "inherit_from_parent"` and fails the check if any exist.
  No mutation — the amdee_xrd Girder plugin is the sole writer for
  these links.
- MAXIMA raw (`xrd_raw`, `xrf_raw`) items get no `prov.*` from this
  pipeline either. Their provenance is captured entirely through
  `meta.coord_provenance` written by `enriched_maxima_raw` per
  partition.

[tag-asset]: ../../helix_dagster/coord_enrichment/provenance_tagging.py
[derived-check]: ../../helix_dagster/coord_enrichment/maxima_derived_leaf.py

## Cast of actors

Four upstream actors write to `meta` on AIMD-L items. Each has a
specific scope.

### OpenMSIStream ingestion

Writes on every item arriving via the Kafka → Girder pipeline:

- `KafkaTopic` — name of the topic the item was published to
- `OpenMSIStreamVersion` — version of the streamer at upload time
- `checksum.sha256` — content hash

Does **not** write any `prov` keys.

### The originating instrument software

Writes scientific metadata at upload time, through the producer
application that publishes to Kafka. For MAXIMA this includes:

- `data_type` — `xrd_raw`, `xrf_raw`, `xrd_derived`, `xrd_metadata`, etc.
- `igsn` — sample identifier
- `experiment_date` — ISO timestamp with timezone

Does not write `prov` keys.

### `amdee_xrd` (Dagster analysis workflow)

Runs approximately once per minute against AIMD-L. For every
`scan_point_<N>_master.h5` it finds that has not already been analyzed
(checked via `prov.hadDerivation` + `wasGeneratedBy`), it:

1. Downloads the master and its data chunks
2. Runs pyFAI azimuthal integration and produces three derived files:
   `scan_point_<N>_xrd.csv`, `scan_point_<N>_xrd.png`, `scan_point_<N>_scan.png`
3. Uploads the derived files and stamps provenance across all involved
   items

The prov stamping pattern is:

| Item | `prov` keys written |
|---|---|
| Derived outputs (`*_xrd.csv`, `*_xrd.png`, `*_scan.png`) | `wasDerivedFrom` → master `_id`; `wasGeneratedBy` → `"amdee_xrd-<version>"` |
| Master (`scan_point_<N>_master.h5`) | `hadDerivation` → list of derived `_id`s; `hasPart` → list of data-chunk `_id`s |
| Data chunks (`scan_point_<N>_data_<M>.h5`) | `isPartOf` → master `_id` |

Also stamps `dataflowId`, `runId`, and `igsn` at `meta.*` (top level)
on the derived outputs — these are useful amdee_xrd run markers.

Does not touch `xrf_raw` items, `xrd_metadata` (`instructions.txt`)
items, or non-MAXIMA data types. Source:
[`amdee_xrd.XRDAnalysis.analyze`](https://github.com/openmsi/amdee_xrd).

### `girder-jsonforms` plugin

Server-side Girder plugin. Writes `igsn`, `type`, `targetPath`,
`entryId`, `gdriveFileId`, and similar user-facing fields through form
annotations and Google Drive integration. Does **not** write any `prov`
keys. Verified by grep across the `Xarthisius/girder-jsonforms` `igsn`
branch (2026-04-23).

### `helix_metadata_extraction_dagster` (this pipeline)

Writes two kinds of things, only on items it enriches:

- `coord_provenance` (its own field) — the yaml hash, transformer
  version, transform version, source timestamp, station coord source,
  and Dagster run id. Written by the three enrichment leaves.
  **No other actor touches this field.**
- `prov.wasDerivedFrom` — only on HELIX ALPSS items (by
  `helix_alpss_provenance_tagged`), linking an ALPSS result back to
  its parent `pdv_trace`.

Also writes the scientific coordinate fields: `Station_X`, `Station_Y`,
`Sample_X`, `Sample_Y`.

## Observed prov coverage (test folder, 2026-04-23)

Surveyed items under `coordinate_dag_test_data` whose `data_type`
contains `xrd` or `xrf`, excluding `.tiff` files:

| data_type | count | prov coverage | prov shape |
|---|---|---|---|
| `xrd_raw` (master) | 27 | 100% | `{hadDerivation, hasPart}` |
| `xrd_raw` (data) | 27 | 100% | `{isPartOf}` |
| `xrd_derived` (csv/png/scan_png) | 81 | 100% | `{wasDerivedFrom, wasGeneratedBy}` |
| `xrf_raw` | 27 | 0% | none |
| `xrd_metadata` (instructions.txt) | 3 | 0% | none (not expected to have prov) |

`xrf_raw` has no prov because `amdee_xrd` does not process xrf data.
There is no equivalent `amdee_xrf` workflow at time of writing.
`xrd_metadata` `instructions.txt` items have no prov because they are
upstream inputs, not derived outputs — correct and expected.

## What the coord-enrichment pipeline can rely on

**For items touched by amdee_xrd (xrd_raw and xrd_derived):**

- The parent-link field (`isPartOf` or `wasDerivedFrom`) is present and
  correct. The pipeline can read it to find the master.h5 that carries
  the authoritative coord_provenance to inherit from.
- `experiment_date` is present from upstream (instrument → OpenMSIStream).
- `igsn` is present from upstream.
- These items do not need any `prov` healing by the pipeline.

**For xrf_raw items:**

- No prov. No parent link. The pipeline enriches these directly from
  `instructions.txt` using the scan_point index from the filename
  (same strategy as master.h5 items).

**For xrd_metadata (`instructions.txt`) items:**

- Not enriched. Used only as the authoritative station-coord source for
  scan points within a run.

**For HELIX ALPSS items:**

- `amdee_xrd` does not touch these. The pipeline's
  `helix_alpss_provenance_tagged` asset writes `prov.wasDerivedFrom`
  to link ALPSS outputs to their parent `pdv_trace`. This is the one
  place the pipeline mutates `meta.prov.*`.

**For MAXIMA `xrd_derived` items:**

- `amdee_xrd` already wrote `prov.wasDerivedFrom` (to the master.h5)
  and `prov.wasGeneratedBy` (`"amdee_xrd-<version>"`). The pipeline
  does **not** rewrite these. `enriched_maxima_derived` follows the
  link to inherit coordinates; the asset check
  `maxima_xrd_derived_provenance_valid` fails loudly if any item's
  link cannot be resolved (missing prov, dangling target, or parent
  outside the current inventory slice).

**For MAXIMA raw (`xrd_raw`, `xrf_raw`) items:**

- No `meta.prov.*` is added by this pipeline. `coord_provenance`
  captures the full enrichment trace: the `instructions.txt` that
  sourced the station coords, the transform version applied, and
  the shot timestamp used for version selection.

## What the pipeline should NOT do

- Never overwrite `prov.wasDerivedFrom` on xrd_derived items.
  `amdee_xrd` has already written the authoritative value. Double-writes
  would create race-condition risk and break the convention that
  `amdee_xrd` is the source of truth for xrd derivation provenance.
- Never overwrite `prov.isPartOf` on xrd_raw data chunks. Same reason.
- Never write `prov.hadDerivation` or `prov.hasPart`. Those belong to
  `amdee_xrd` on the master.h5 items.

## Semantic vocabulary

All prov keys in use conform to the [W3C PROV](https://www.w3.org/TR/prov-overview/)
vocabulary:

- `prov:wasDerivedFrom` — entity-to-entity derivation
- `prov:wasGeneratedBy` — which activity produced this entity
- `prov:hadDerivation` — reverse of `wasDerivedFrom` (an entity had
  these derivations)
- `prov:hasPart` / `prov:isPartOf` — composition relationship

Keeping this vocabulary consistent across actors — `amdee_xrd`, this
pipeline, and any future analysis workflows — is an explicit design
goal. New provenance writers should use W3C PROV terms, not ad-hoc names.

## Audit queries

The script used to verify this picture lives in the shared diagnostics
collection. It filters for items whose `data_type` contains `xrd` or
`xrf`, excludes `.tiff` files, and reports prov presence and shape per
data_type. Pattern:

```python
# For each data_type, for each item:
meta = item.get('meta') or {}
prov = meta.get('prov') or {}
# Classify by:
#   - filename pattern (master_h5, data_h5, xrf, xrd_csv, ...)
#   - prov key set (isPartOf, wasDerivedFrom+wasGeneratedBy, ...)
#   - presence of amdee_xrd run markers (dataflowId, runId)
```

Rerun periodically to confirm coverage holds as new data types or
analysis workflows are added.
