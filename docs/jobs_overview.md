# Dagster Jobs Overview

This repo runs **two DAGs** in one `Definitions` registry, spread across
**five jobs**. One DAG is spreadsheet-driven (HELIX PDV traces); the other is
the partitioned `coord_enrichment` DAG that fans out across HELIX ALPSS and
MAXIMA data types. Every coordinate-writing asset writes the same core fields
to Girder items: `Station_X`, `Station_Y`, `Sample_X`, `Sample_Y`, and a
`coord_provenance` dict.

The transform itself is `Station → Sample` via the `coordinate-transformer`
package, version-selected by shot timestamp. Leaf assets compute `Station_X/Y`
from a primary source (spreadsheet row or instructions file); derived assets
**inherit** `Station_X/Y` from a parent item and re-apply the parent's recorded
transform version.

---

## Job-by-job

### 1. `process_helix_assets_job` — HELIX spreadsheet flow
- **Trigger:** `helix_experiment_log_discovery_sensor` (polls `/aimdl/partition` for `pdv_experiment_log`, registers partitions, emits one partitioned RunRequest per changed log).
- **Partitioning:** `HELIX_EXPERIMENT_LOG_PARTITIONS` = `DynamicPartitionsDefinition("helix_experiment_log")`, keyed on `<igsn>//<experiment_date>`.
- **What it does:** Three durable partitioned assets — `pdv_log` (read + normalize + validate the experiment log), `pdv_data` (fetch the PDV trace inventory, match rows by filename, **write coordinate metadata to `pdv_trace` items**), `pdv_processing_manifest` (stamp `meta.processing_status` onto each source log item).
- **Coordinate source:** each spreadsheet row's `Flyer_X/Y_Position_Corrected (mm)` becomes `Station_X/Y`; the row `Timestamp` selects the HELIX transform version.
- **Writing asset:** `pdv_data` → `pdv_trace`.

### 2. `coord_enrichment_job` — state report (read-only)
- **Trigger:** `coord_enrichment_state_report_schedule` (nightly 03:00 ET, ships STOPPED).
- **Partitioning:** none.
- **What it does:** Inventories all in-scope items, **tags HELIX ALPSS provenance** (`helix_alpss_provenance_tagged` writes `meta.prov.wasDerivedFrom` so ALPSS items point at their parent PDV trace), observes PDV coverage, and emits a status report. Writes provenance links only — **no coordinate writes**.

### 3. `coord_enrichment_maxima_job` — MAXIMA, weekly reconciliation
- **Trigger:** `coord_enrichment_maxima_weekly_schedule` (Sunday 04:00 ET, dry-run, STOPPED).
- **Partitioning:** `MAXIMA_RUN_PARTITIONS` = `DynamicPartitionsDefinition("maxima_run")`, one partition per AIMD-L run (`<igsn>//<experiment_date>`).
- **What it does:** Gap-filling sweep — enumerates all registered run partitions and materializes only those lacking a successful run. **Writes coordinates to every in-scope MAXIMA item the run produced: `xrd_raw`, `xrf_raw`, `xrd_derived`, `xrd_visualization`.**
- **Coordinate source:** `Station_X/Y` parsed from the run's `instructions.txt` scan-point table, selected by the `scan_point_<i>` prefix in each filename; timestamp from `meta.experiment_date`.
- **Writing asset:** `enriched_maxima_run`.

### 4. `coord_enrichment_maxima_partition_job` — MAXIMA, event-driven
- **Trigger:** `maxima_run_discovery_sensor` (polls `/aimdl/partition`, registers new run keys, emits one deduped RunRequest per run; STOPPED).
- **Partitioning / writing asset:** identical selection to job 3. Kept as a separate job purely so sensor-driven runs are filterable from the weekly reconciliation runs in the UI; the partition key — not the job — decides what materializes.

### 5. `coord_enrichment_helix_alpss_job` — HELIX ALPSS derived
- **Trigger:** `coord_enrichment_helix_alpss_weekly_schedule` (Sunday 04:30 ET, STOPPED).
- **Partitioning:** `HELIX_ALPSS_PARTITIONS` = static `["HELIX/pdv_alpss_output", "HELIX/pdv_alpss_result", "HELIX/pdv_alpss_results"]`.
- **What it does:** Ensures ALPSS items are provenance-tagged, then **enriches `pdv_alpss_output`, `pdv_alpss_result`, and `pdv_alpss_results` items** by inheriting `Station_X/Y` from their parent `pdv_trace` (via `prov.wasDerivedFrom`) and re-applying the parent's recorded transform version.
- **Writing asset:** `enriched_helix_alpss`.

> **The run is the unit of work.** One `instructions.txt` per run supplies the
> coordinates for everything the run produced. Storage nests `raw/` inside the
> run folder while lineage runs the other way — the derived products are made
> *from* the raw measurements — but neither shape is a partition boundary, so
> they enrich together. `xrd_derived` no longer inherits from its parent
> `master.h5`: the parent read the same `instructions.txt`, so inheritance
> recorded a path rather than an origin. Any `prov.wasDerivedFrom` link is still
> recorded in `coord_provenance` as a cross-reference.

> All `coord_enrichment` schedules and both `coord_enrichment` sensors ship
> **STOPPED** and dry-run by default; an operator opts in via the Dagster UI.

---

## Coordinate-supply summary

Which job/asset supplies `Station_X/Y` + `Sample_X/Y` + `coord_provenance` to
each HELIX and MAXIMA file type, and where those coordinates come from:

| Instrument | `data_type` (Girder) | Role | Writing asset | Job | Coordinate source |
|---|---|---|---|---|---|
| HELIX | `pdv_trace` | leaf | `pdv_data` | `process_helix_assets_job` | Spreadsheet row `Flyer_X/Y_Position_Corrected` |
| HELIX | `pdv_alpss_output` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| HELIX | `pdv_alpss_result` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| HELIX | `pdv_alpss_results` | derived | `enriched_helix_alpss` | `coord_enrichment_helix_alpss_job` | Inherited from parent `pdv_trace` |
| MAXIMA | `xrd_raw` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrf_raw` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrd_derived` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |
| MAXIMA | `xrd_visualization` | leaf | `enriched_maxima_run` | `coord_enrichment_maxima_job` / `…_partition_job` | MAXIMA `instructions.txt` scan-point |

**Provenance-only (no coordinates):** `helix_alpss_provenance_tagged` writes
`meta.prov.wasDerivedFrom` on the three HELIX ALPSS types so
`enriched_helix_alpss` can find each item's parent `pdv_trace`. It runs in both
`coord_enrichment_job` and `coord_enrichment_helix_alpss_job`.

**Notes**
- `Sample_X/Y` is always `transform_station_to_sample(Station_X, Station_Y)`. Leaf assets pick the transform version by timestamp; derived assets re-apply the parent's recorded version, so a shot and all its derived files share one coordinate frame.
- `coord_provenance` records `transform_version`, the transform-YAML sha256, transformer/pipeline versions, the source timestamp + its origin, the station-coordinate source, and the Dagster run id.
- Out of scope (no coordinate enrichment): `xrd_metadata`, `pdv_experiment_log`, `xrd_calibrant_raw`, `xrd_calibrant_derived`, `pdv_alpss_output` variants beyond the three listed, and `unclassified`.
