# Coordinate Enrichment DAG — One-Page Brief

## The problem

AIMD-L instruments record shot positions in instrument-specific coordinate
frames (millimeters from each station's mechanical zero). Downstream
scientific work needs **sample-frame coordinates** — positions relative to
the sample itself — so that measurements from HELIX (laser shock),
MAXIMA (XRD/XRF), and future instruments can be compared at the same
physical location on the same sample. The `coordinate-transformer` PyPI
package performs the instrument→sample affine transform per instrument
and carries **versioned calibrations** so that historical data is
transformed with the calibration in effect on the day of acquisition.

Today, those transforms happen only for HELIX PDV traces via a
spreadsheet-driven Dagster job. The coord_enrichment DAG described here
propagates sample coordinates to every AIMD-L Girder item that (a)
carries an IGSN and (b) has a recognized instrument data type, doing so
with full provenance so any coordinate value can be traced to an
authoritative station-side source artifact.

## The DAG

```mermaid
flowchart TD
    ExtSpread[["(existing DAG)<br/>HELIX experiment-log → PDV trace<br/>writes Station_X/Y, Sample_X/Y"]]:::external

    Sensor[[maxima_raw_discovery_sensor<br/><small>polls /aimdl/partition<br/>adds run keys, emits RunRequests</small>]]:::sensor

    A[helix_alpss_provenance_tagged<br/><small>writes prov.wasDerivedFrom<br/>on HELIX ALPSS items only</small>] --> B2
    B[enrichable_items_inventory<br/><small>HELIX ALPSS + MAXIMA derived</small>] --> B2
    B --> D3
    C[coord_transform_config_snapshot<br/><small>YAML sha256 + version table</small>]

    Sensor --> D1
    C --> D1[enriched_maxima_raw<br/><small>MultiPartitions(data_type, run)<br/>fetches its own xrd_metadata</small>]
    C --> B2[enriched_helix_alpss<br/><small>inherits from parent PDV trace</small>]
    ExtSpread -.->|PDV traces<br/>with coords| B2

    D1 -->|AllPartitionMapping| D3[enriched_maxima_derived<br/><small>in-raw xrd_derived<br/>inherits from xrd_raw]
    D3 -.- Vcheck([maxima_xrd_derived_provenance_valid<br/><small>asset check — verifies amdee_xrd prov</small>]):::check
    C --> D3

    D1 --> R[coord_enrichment_report]
    B2 --> R
    D3 --> R
    R --> M[coord_enrichment_manifest<br/><small>writes status to tracking<br/>Girder item</small>]

    classDef external fill:#eef,stroke:#66a,stroke-dasharray: 4 4
    classDef sensor fill:#efe,stroke:#393
    classDef check fill:#fef,stroke:#939,stroke-dasharray: 2 2
```

## What each part does

| Asset / mechanism | Writes to Girder? | Role |
|---|---|---|
| `maxima_raw_discovery_sensor` | no | Polls `/aimdl/partition` for `xrd_raw`, `xrf_raw`, and `xrd_metadata`. Adds each new `"<igsn>//<experiment_date>"` to the `maxima_raw_run` dynamic partition dim and emits one `RunRequest` per `(data_type, aimdl_key)` with a dedup key composing both the raw and `xrd_metadata` content hashes. STOPPED by default. |
| `helix_alpss_provenance_tagged` | yes (prov only) | Matches HELIX ALPSS items to their parent PDV traces by filename stem and writes `meta.prov.wasDerivedFrom`. HELIX-only; MAXIMA prov is handled by amdee_xrd upstream. |
| `enrichable_items_inventory` | no | Queries `/aimdl/datafiles` or `/aimdl/partition` for each in-scope `data_type`, keeps only items with `meta.igsn`, partitions by `(instrument, data_type)`. Consumed by the HELIX ALPSS and MAXIMA derived leaves; `enriched_maxima_raw` fetches its own items per partition and does not read this. |
| `coord_transform_config_snapshot` | no | Captures the transform YAML's sha256, version list, and the `coordinate-transformer` package version. Every enrichment write embeds this snapshot in its provenance. |
| `enriched_maxima_raw` | yes (coords + prov) | Partitioned on `MultiPartitionsDefinition({data_type: Static(["xrd_raw","xrf_raw"]), run: Dynamic("maxima_raw_run")})`. Each partition fetches its own items and the matching `xrd_metadata/instructions.txt` via `/aimdl/partition/details?dataType=<dt>&key=<igsn//experiment_date>`. Reads `sample.scan_points[i]`, transforms using `meta.experiment_date`, writes `Station_X/Y`, `Sample_X/Y`, and `coord_provenance`. |
| `enriched_helix_alpss` | yes (coords + prov) | Depends on `helix_alpss_provenance_tagged`. For each ALPSS item: follow `prov.wasDerivedFrom` to its PDV trace, copy `Station_X/Y` and the transform timestamp, recompute `Sample_X/Y` with fresh provenance. |
| `enriched_maxima_derived` | yes (coords + prov) | Depends on `enriched_maxima_raw` via `AllPartitionMapping` (single static partition). Reads the amdee_xrd-written `prov.wasDerivedFrom` on each `xrd_derived` item to find its master.h5, inherits coords. |
| `maxima_xrd_derived_provenance_valid` (asset check) | no | ERROR-severity check on `enriched_maxima_derived`. Fails if any item's `resolution_errors` includes `stage="inherit_from_parent"` — i.e. missing prov or dangling target. Non-mutating; amdee_xrd remains the sole writer. |
| `coord_enrichment_report` | no | Aggregates per-partition counts: seen, skipped-by-policy, written, transform failures, unresolved parents. |
| `coord_enrichment_manifest` | yes (status record) | Writes a job-level processing summary to a tracking Girder item. |

## Coordinate authorities

Every coordinate value traces, in at most two hops, back to one of two
station-side authoritative artifacts:

| Instrument | Authority | How station coords are read |
|---|---|---|
| HELIX | experiment-log CSV | `Flyer_X/Y_Position_Corrected (mm)` column from the row whose `PDV_FileName` matches |
| MAXIMA | `instructions.txt` (JSON) | `sample.scan_points[i]` where `i` is the scan-point index parsed from the filename |

## Key policies

- **Scope gate.** An item is enriched iff `meta.igsn` is set, `meta.data_type` is in the allowed set, and (for `xrd_derived`) the item's parent folder is named `raw`. Everything else silently drops out.
- **MAXIMA root-level TIFFs are explicitly NOT enriched.** They are not scientifically validated imagery and adding coordinates to them would invite unwarranted interpretations. The path-based scope gate excludes them by construction.
- **Overwrite if the transform changed.** A write occurs only when the new `coord_provenance` would differ from what's stored (different YAML sha256, transformer version, or station_coord_source). Re-running after a YAML recalibration re-enriches affected items and leaves the rest alone.
- **Parent missing is a skip, not a crash.** If a derived item's `prov.wasDerivedFrom` target doesn't resolve, the item is reported as unresolved and the DAG moves on. Other partitions continue. For `xrd_derived` this also surfaces as a failed `maxima_xrd_derived_provenance_valid` check.
- **No silent fallbacks on timestamps.** If a MAXIMA item lacks `meta.experiment_date` (future Eiger-HDF5 fallback), the failure is explicit — we do not guess.
- **Provenance split by writer.** The pipeline writes `prov.wasDerivedFrom` only on HELIX ALPSS items. MAXIMA `xrd_derived` prov is owned by `amdee_xrd` upstream and is read, verified, but never overwritten.
