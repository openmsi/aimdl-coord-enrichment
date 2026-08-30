# Specification — Coordinate Enrichment for AIMD‑L (`aimdl_coord_enrichment`)

> **Status:** Reverse-engineered from source + test suite at version `0.6.0`.
> **Last refreshed:** 2026-08-30 — §3.2 counts re-measured against live prod;
> §C1/§C9/§C10 updated to the 3-asset partitioned HELIX flow (issue #31); §C4
> updated for the two production filename conventions.
> **Method:** This is a *descriptive* spec — every requirement is extracted from
> code behavior that the existing tests pin. It is written so it can also serve
> as a *prescriptive* spec going forward: each requirement has a stable ID, and
> the test that currently guarantees it is cited so the spec ↔ test ↔ code
> traceability is explicit.
>
> **Why it exists:** to separate the *problem* (what coordinates must end up on
> which Girder items, and why) from the *current solution* (8 jobs, dynamic
> partitions, two DAGs). Use §9 (Scope Tiers) to reason about how much of the
> current machinery the problem actually requires.

--

## 1. Problem statement

Scientific data files for two instruments (HELIX, MAXIMA) live as items in a
Girder repository (AIMD‑L collection). Downstream analysis needs every relevant
item to carry **sample-frame coordinates** (`Sample_X`, `Sample_Y`) plus the
**station-frame coordinates** they were derived from (`Station_X`, `Station_Y`)
and a **provenance record** (`coord_provenance`) describing exactly how those
numbers were produced.

The coordinates are not present on the items at creation time. They must be
**computed and written back** by reading from one of three sources, depending on
the item's role, and applying a versioned `Station → Sample` transform.

The system shall write the same four coordinate fields + provenance to every
in-scope item, regardless of which source the coordinates came from.

--

## 2. Ubiquitous language (glossary)

| Term | Meaning |
|---|---|
| **Item** | A Girder item (a file + `meta` dict). Identified by `_id`. |
| **`data_type`** | `meta.data_type` on an item. The primary routing key (`pdv_trace`, `xrd_raw`, …). |
| **IGSN** | Sample identifier. Pattern `[A-Za-z]{6}\d{5}(?:-[A-Za-z0-9]+)?`. |
| **Station coordinates** | Instrument-frame position in mm (`Station_X/Y`). The raw input. |
| **Sample coordinates** | Sample-frame position in mm (`Sample_X/Y`). The deliverable. |
| **Station → Sample transform** | A versioned coordinate transform, selected by shot timestamp. Provided by the external `coordinate-transformer` package. |
| **Leaf item** | An item whose station coordinates come from a *primary source* (spreadsheet row or `instructions.txt`). |
| **Derived item** | An item that has no primary source; it **inherits** station coordinates from a parent item via `meta.prov.wasDerivedFrom`. |
| **`coord_provenance`** | A dict written next to the coordinates recording transform version, config hash, source, timestamp, and run id. |
| **Enrichment** | The act of computing coordinates + provenance and writing them to an item. |
| **Reconciliation** | A periodic sweep that re-enriches only items whose inputs changed. |
| **AIMD‑L run key** | `"<igsn>//<experiment_date>"` — the natural partition key for MAXIMA. |

--

## 3. System context

### 3.1 Actors
- **`coordinate-transformer` package** — external, versioned `Station→Sample` math, selected by timestamp.
- **Girder API** (`data.htmdec.org/api/v1`) — item store + custom `/aimdl/*` indexed endpoints.
- **Operator** — a human who opts pipelines into running via the Dagster UI (everything ships stopped).
- **Upstream producers** — the HELIX experiment-log spreadsheet author; the `amdee_xrd` Girder plugin that writes `prov.wasDerivedFrom` on MAXIMA derived items.

### 3.2 In-scope item types and the coordinate source for each

Counts from `/aimdl/count`, measured 2026-08-30.

| Instrument | `data_type` | Role | Coordinate source | Count |
|---|---|---|---|---|
| HELIX | `pdv_trace` | leaf* | spreadsheet row (`Flyer_X/Y_Position_Corrected`) | 8,539 |
| HELIX | `pdv_alpss_output` | derived | inherit from parent `pdv_trace` | 59,420 |
| HELIX | `pdv_alpss_result` | derived | inherit from parent `pdv_trace` | 7,428 |
| MAXIMA | `xrd_raw` | leaf | `instructions.txt` scan-point | 42,960 |
| MAXIMA | `xrf_raw` | leaf | `instructions.txt` scan-point | 19,861 |
| MAXIMA | `xrd_derived` | derived | inherit from parent `xrd_raw` master.h5 | 32,886 |

**~171,000 in-scope items** — roughly 2.3× the figure in the original snapshot.

`pdv_alpss_results` (plural) was in scope at 1,526 items in the 2026-05-17
snapshot and **no longer exists** as a `data_type`.

Two filename conventions are in concurrent production use for the HELIX PDV
family — see SPEC‑ALPSS‑01. Both are first-class; neither is legacy.

> **Scope caveat.** `/aimdl/datafiles` returns only items carrying `meta.igsn`,
> so the pipeline sees 7,708 of the 8,539 `pdv_trace` items. The ~831 without an
> IGSN are invisible to discovery and silently outside every coverage metric.

\* `pdv_trace` is enriched by the **spreadsheet DAG**, and is treated as an
*external / pre-enriched* parent by the coord_enrichment DAG's instrument
registry (HELIX leaf set is intentionally empty there).

**Explicitly out of scope:** `xrd_metadata`, `pdv_experiment_log`,
`xrd_calibrant_raw`, `xrd_calibrant_derived`, `unclassified`, and the data types
that appeared after the original snapshot and have never been assessed:
`xrd_visualization` (23,905), `nmd_project` (487), `nmd_raw` (365).

--

## 4. Domain invariants (apply everywhere)

These are the cross-cutting rules. Every capability in §5 inherits them.

- **INV‑1 — Uniform output.** Every successful enrichment writes exactly
  `Station_X`, `Station_Y`, `Sample_X`, `Sample_Y`, `coord_provenance` to the
  item. Derived items write the *parent's* station coordinates verbatim.
- **INV‑2 — Timezone-aware time, always.** Any datetime that selects a transform
  version or is recorded in provenance MUST be tz-aware. Naive datetimes raise
  (`ValueError` in the transform/provenance layer; `ResolutionError` in the
  MAXIMA layer). *Tests: `test_naive_timestamp_raises`,
  `test_build_coord_provenance_naive_timestamp_raises`,
  `_experiment_date` naive branch.*
- **INV‑3 — Version selection is timestamp-driven.** `Sample = transform(Station)`
  where the transform version is the one valid at the shot timestamp.
  `valid_from` is **inclusive**, `valid_until` is **exclusive**.
  *Tests: `test_helix_boundary_just_before_is_v1`, `test_helix_boundary_at_cutover_is_v2`.*
- **INV‑4 — Derived items reuse the parent's exact version.** A derived item does
  not re-resolve the version by its own timestamp; it re-applies the parent's
  recorded `transform_version` label. *Tests: `enriched_helix_alpss` "inherits v2
  when parent is v2".*
- **INV‑5 — Ambiguity is never silently resolved.** Multiple PDV-trace filename
  matches, multiple `instructions.txt`, or a non-unique `raw/` subfolder yield an
  error/`None`, never an arbitrary pick. *Tests:
  `test_find_parent_pdv_item_id_none_on_ambiguous`,
  `test_fetch_instructions_multiple_raises`,
  `test_find_master_h5_item_id_missing_raw_subfolder`.*
- **INV‑6 — Idempotent writes.** A re-run writes an item only if a *meaningful*
  provenance field changed (see SPEC‑PROV‑02). Identity/audit fields never
  trigger a rewrite.
- **INV‑7 — Failure is per-item, not per-run.** A resolution or write failure on
  one item is recorded (counted + listed) and the run continues. *Tests: the
  `resolution_errors` / `write_errors` branches in every leaf test file.*
- **INV‑8 — Safe by default.** Every mutating pipeline supports `dry_run`
  (default `True`), and every sensor/schedule ships **STOPPED**. Nothing writes
  to Girder on deploy without an operator opting in. *Tests:
  `test_all_schedules_default_stopped`, dry-run config tests.*

--

## 5. Capability specifications

Each capability lists requirements (`SPEC‑*`) in EARS / Given‑When‑Then form.

### C1 — HELIX spreadsheet → `pdv_trace` enrichment
*(the "helix_spreadsheet" DAG; **3 partitioned assets** since issue #31)*

Partitioned by the AIMD‑L key `<igsn>//<experiment_date>` on
`HELIX_EXPERIMENT_LOG_PARTITIONS` (`DynamicPartitionsDefinition("helix_experiment_log")`).
Three durable assets — `pdv_log → pdv_data → pdv_processing_manifest` — model
external-state transitions; pure computation lives in `spreadsheet.py`. Both
writing assets take `HelixSpreadsheetConfig` with `dry_run: bool = True`.
Job name `process_helix_assets_job` is preserved from the 9-asset design.

- **SPEC‑HELIX‑01 — Ingest.** When given a spreadsheet item id + filename, the
  system shall download the first file of that item and load it (`.csv` →
  `read_csv`, else `read_excel`), then rename columns via `COLUMN_MAP`. The
  partition may resolve to more than one log item; all are concatenated.
  *(`pdv_log`; `download_and_read` raises if the item has no files. Tests:
  `test_pdv_log_reads_partition`, `test_normalize_experiment_log_renames_columns`.)*
- **SPEC‑HELIX‑02 — IGSN validation.** For each row, the system shall validate
  `Sample_IGSN` against the IGSN pattern, classifying each as `valid`,
  `missing` (None/NaN/empty), or `invalid_format`. *(`pdv_log`; tests:
  `test_validate_log_rows`.)*
- **SPEC‑HELIX‑03 — PDV inventory.** The system shall fetch all `pdv_trace`
  items via the indexed `/aimdl/datafiles` endpoint (paginated 100/page), not by
  crawling folders. *Tests: `test_fetch_all_paginates`,
  `test_fetch_datafiles_respects_limit_cap`.*
- **SPEC‑HELIX‑04 — Matching.** For each row with a non-empty `PDV_FileName`, the
  system shall match it to inventory items by **filename prefix**
  (`item.name.startswith(filename)`). Given 0 matches → `not_found`; given >1 →
  `ambiguous` (no item chosen); given exactly 1 → matched. Blank/NaN filenames
  are skipped silently with no issue. *(`pdv_data`; tests: `test_exact_match`,
  `test_match_pdv_rows_match_and_not_found_and_nan`.)*
- **SPEC‑HELIX‑05 — IGSN consistency.** When a row matches an item and both carry
  a truthy IGSN that differ, the system shall record an `igsn_mismatch` issue
  (but still keep the match). *Tests: `test_match_pdv_rows_flags_igsn_mismatch`.*
- **SPEC‑HELIX‑06 — Enrich.** For each matched item, the system shall compute
  `Station_X/Y` from `Flyer_X/Y_Position_Corrected`, derive `Sample_X/Y` via the
  timestamp-selected transform (rounded to 4 dp), build `coord_provenance`
  (`station_coord_source.kind = "helix_experiment_log"`), and write the
  coordinate payload to the matched item. *(`pdv_data`; tests:
  `test_write_pdv_metadata_writes_coords_and_provenance`,
  `test_pdv_data_version_boundary_dispatch`.)*
- **SPEC‑HELIX‑07 — Timestamp normalization.** The shot timestamp comes from the
  row `Timestamp` column; a naive value is assumed UTC (origin recorded as
  `..._assumed_utc`) and counted; unparseable/missing → no version selection.
- **SPEC‑HELIX‑08 — Audit manifest + idempotent skip.** After processing, the
  system shall write a `processing_status` manifest to the *source spreadsheet*
  item (counts + `status ∈ {completed_clean, completed_with_warnings}`), and the
  sensor shall skip spreadsheets already marked `completed_clean`.
  *(`pdv_processing_manifest`; tests: `test_summarize_pdv_processing_clean`,
  `test_summarize_pdv_processing_with_warnings`,
  `test_write_processing_manifest_success`.)*
- **SPEC‑HELIX‑09 — Completeness reporting.** *(**Withdrawn** in issue #31.)*
  The 9-asset design reported per-IGSN ALPSS coverage via `quality_report` +
  `alpss_results_inventory`; both assets were removed. Issue #31 assigned the
  concern to the observer flow, but `helix_pdv_coverage_observer` measures a
  different thing (coord-provenance coverage on `pdv_trace`, not missing ALPSS
  results). **This capability currently has no owner** — see Q9.
- **SPEC‑HELIX‑10 — Dry run.** `pdv_data` and `pdv_processing_manifest` shall
  perform no Girder write when `config.dry_run` is true, and shall count what
  they *would* have written. Sensor-emitted RunRequests carry no run_config, so
  sensor-launched runs inherit the safe default; a live write requires a manual
  launch with `dry_run: false`. *Tests: `test_pdv_data_dry_run_skips_writes`,
  `test_pdv_processing_manifest_dry_run_skips_write`.*

### C2 — Station → Sample transform (shared)

- **SPEC‑XFORM‑01.** Given `(station_x, station_y, timestamp)`, the system shall
  return `(sample_x, sample_y, transform_name)` using the transform version valid
  at `timestamp`; given `timestamp=None`, the currently-valid version.
- **SPEC‑XFORM‑02 — Graceful degradation.** Given missing inputs, no configured
  transformer, or any transform exception, the system shall return
  `(None, None, None)` (logged), never crash. *Tests: `test_none_input`,
  `test_missing_transformer`.*
- **SPEC‑XFORM‑03 — HELIX calibration boundary.** HELIX v1 (before
  2026‑04‑01T00:00‑04:00) is a flip about x=20 (`x' = 40 − x`); v2 (on/after) is
  the **identity** transform — the station frame was physically realigned to the
  sample frame. *Tests: `test_helix_v1_flip_before_cutover`,
  `test_helix_v2_identity_after_cutover`.*
- **SPEC‑XFORM‑04 — Named-version reuse.** The system shall support transforming
  with an explicitly named version label (bypassing date resolution) for derived
  inheritance. *Tests: `test_transform_with_named_version_happy_path_v1/v2`.*
  *(Implementation note: currently reaches into the transformer's private
  `_transforms`; an upstream public API is a known TODO.)*

### C3 — Provenance & idempotency (shared)

- **SPEC‑PROV‑01 — Provenance shape.** Every enriched item's `coord_provenance`
  shall contain: `instrument`, `transform_version`, `transform_yaml_sha256`,
  `transformer_version`, `pipeline_version`, `source_timestamp` (ISO or null),
  `source_timestamp_origin`, `station_coord_source`, `enriched_at`; plus
  `dagster_run_id` **only when known**. *Tests: `test_build_coord_provenance_*`.*
- **SPEC‑PROV‑02 — Write decision.** The system shall (re)write an item iff there
  is no stored provenance (`first_write`) OR one of these changed:
  `transform_yaml_sha256`, `transformer_version`, `transform_version`,
  `station_coord_source`. It shall NOT rewrite for changes to `enriched_at`,
  `source_timestamp`, `dagster_run_id`, or `pipeline_version`. *Tests:
  `test_coord_enrichment_overwrite.py` (all branches).*
- **SPEC‑PROV‑03 — Config snapshot.** The system shall capture a frozen snapshot
  of the transform config (YAML sha256, transformer version, versions per
  instrument) so provenance references a reproducible config. *Tests:
  `test_coord_enrichment_config_snapshot.py`.*
- **SPEC‑PROV‑04 — `station_coord_source` discriminator.** Its `kind` shall be
  one of: `helix_experiment_log` (C1), `maxima_instructions` (C5),
  `inherited` (C4/C6).

### C4 — HELIX ALPSS derived enrichment (inheritance)

- **SPEC‑ALPSS‑01 — Provenance tagging.** Before enrichment, the system shall set
  `meta.prov.wasDerivedFrom` on each ALPSS item to its parent `pdv_trace`,
  resolved by filename stem (`<stem>.csv`). Unresolvable items are recorded; an
  ERROR check fires if any HELIX item is left unresolved. *Tests:
  `test_coord_enrichment_provenance_tagging.py`, `all_helix_alpss_tagged`.*

  **Two filename conventions are in concurrent production use**, and both must
  resolve. An ALPSS filename is `<stem><sep><output>.<ext>` where `sep` is `-`
  or `_`; the parent is the unique `<stem>.csv` in the `pdv_trace` pool.

  | convention | trace | ALPSS child |
  |---|---|---|
  | IGSN-named | `JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1.csv` | `…_ch1-iq.png` |
  | C-named | `C1--20250807--00001.csv` | `C1--20250807--00001-results.csv` |

  Neither is legacy. Measured 2026‑08‑30, C-named files are ~70% of `pdv_trace`
  and ~71% of both ALPSS types, and the convention is currently in use: it ran
  Aug–Oct 2025, was replaced by IGSN naming Nov 2025 – May 2026, and **reverted**
  in Jun/Jul 2026. 2026‑08 is the largest month on record.

  The stem shape shall **not** be constrained beyond the `<sep><output>.<ext>`
  suffix. Whether a stem names a real shot is settled authoritatively by the
  unique-`<stem>.csv` requirement, which also preserves INV‑5. A prior `_ch<N>`
  stem-tail rule encoded the IGSN convention only and silently failed to resolve
  47,408 C-named items, turning `all_helix_alpss_tagged` red on every sweep; it
  was misdiagnosed for three months as an upstream Girder mis-tagging defect.
  Under the corrected rule all 66,848 ALPSS items resolve to a unique parent
  (0 no-parent, 0 ambiguous), and `pdv_trace` files still yield no stem because
  their stems end in digits. *Tests: `test_alpss_shot_stem_c_named`,
  `test_alpss_shot_stem_returns_none_on_c_named_pdv_trace`,
  `test_find_parent_pdv_item_id_c_named_ambiguity_still_blocked`,
  `test_both_conventions_resolve_against_one_mixed_inventory`.
  Evidence: `.claude/scratch/probe_c1_naming.md`.*
- **SPEC‑ALPSS‑02 — Inherit.** For each ALPSS item with a resolvable parent, the
  system shall read the parent's `Station_X/Y` + recorded `transform_version`,
  re-apply that version to produce `Sample_X/Y`, and write the payload with
  `station_coord_source.kind = "inherited"`. *Tests:
  `test_coord_enrichment_helix_alpss.py`.*
- **SPEC‑ALPSS‑03 — Parent readiness.** Given a parent not yet enriched (missing
  `Station_X/Y` or `coord_provenance`), the item shall be recorded as a
  resolution error, not written.

### C5 — MAXIMA raw enrichment (`xrd_raw`, `xrf_raw`)

- **SPEC‑MAXR‑01 — Self-fetching partition.** Each partition `(data_type, run)`
  shall fetch its own items and its own `instructions.txt` via
  `/aimdl/partition/details` keyed on the AIMD‑L run key. It depends on no
  inventory or provenance asset. *Tests: `test_coord_enrichment_maxima_raw.py`.*
- **SPEC‑MAXR‑02 — Scan-point lookup.** Station coordinates come from
  `instructions.txt` (`sample.scan_points[i]` = `[x, y]`), where `i` is parsed
  from the filename (`scan_point_<i>...`). Timestamp comes from
  `meta.experiment_date`. *Tests: `test_parse_scan_point_index_*`,
  `test_scan_point_coords_*`.*
- **SPEC‑MAXR‑03 — Missing/duplicate instructions.** Given no `instructions.txt`,
  every item in the partition is recorded as a resolution error
  (stage `instructions`). Given multiple, the first is used and the rest are
  recorded as duplicate warnings. *Tests: missing/duplicate instructions tests.*
- **SPEC‑MAXR‑04 — `instructions.txt` validation.** The payload must be JSON with
  `sample.scan_points` a non-empty list of numeric `[x, y]` pairs; any violation
  is a `ResolutionError`. *Tests: `test_parse_instructions_json_*`.*

### C6 — MAXIMA derived enrichment (`xrd_derived`)

- **SPEC‑MAXD‑01 — Inherit from master.h5.** Each `xrd_derived` item shall
  inherit `Station_X/Y` + version from its parent (`scan_point_<i>_master.h5`),
  linked via `prov.wasDerivedFrom` written upstream by `amdee_xrd`. *Tests:
  `test_coord_enrichment_maxima_derived.py`.*
- **SPEC‑MAXD‑02 — Parent-readiness gate.** Given the partition has items but
  **zero** parent `xrd_raw` items are enriched, the asset shall fail fast with a
  message naming `enriched_maxima_raw` and the `0/<total>` ratio; given partial
  enrichment, it shall warn and continue. *Tests:
  `test_fast_fails_when_no_parents_enriched`, partial-warn test.*
- **SPEC‑MAXD‑03 — Provenance validity check.** An ERROR-severity check shall
  verify the `amdee_xrd` prov links are present and resolvable (counts
  `inherit_from_parent`-stage errors). *Tests: `maxima_xrd_derived_provenance_valid`.*
- **SPEC‑MAXD‑04 — Raw scope filter.** Only `xrd_derived` items physically under a
  `raw/` subfolder are in scope. *Tests: `filter_to_raw_subfolder` tests.*

### C7 — Inventory & discovery

- **SPEC‑INV‑01 — In-scope inventory.** The system shall produce an inventory of
  all in-scope items keyed `"<INSTRUMENT>/<data_type>"`, keeping only items with
  a truthy `meta.igsn` and an in-scope `data_type`. Every in-scope key appears
  even when empty. *Tests: `test_inventory_returns_all_in_scope_data_type_keys`.*
- **SPEC‑INV‑02 — Meta-preserving fetch.** MAXIMA partition-aware types shall be
  fetched via `/aimdl/partition*` (preserves `experiment_date`, `prov`), not
  `/aimdl/datafiles` (which strips meta). *Tests:
  `test_items_carry_full_meta_through_inventory`.*
- **SPEC‑DISC‑01 — Event-driven discovery.** A sensor shall poll the partition
  index for `xrd_raw`/`xrf_raw`/`xrd_metadata`, register new AIMD‑L run keys on
  the dynamic dimension, and emit one deduped RunRequest per `(data_type, run)`.
  The dedup key composes both the raw and metadata content hashes; either
  changing re-triggers. *Tests: `test_sensors_maxima_discovery.py`.*
- **SPEC‑DISC‑02 — Gap-filling reconciliation.** A weekly schedule shall
  enumerate registered partitions and emit a (dry-run) RunRequest only for those
  lacking a successful materialization. *Tests:
  `test_maxima_raw_reconciliation_*`.*

### C8 — Reporting & manifests

- **SPEC‑RPT‑01 — State report.** The system shall aggregate per-partition leaf
  counts (`seen/written/simulated/skipped/coord_failures/resolution_errors`),
  tagging state, and PDV coverage into one report — reading leaf state from the
  event log so it runs even with zero leaf materializations. *Tests:
  `test_coord_enrichment_report_fresh.py`.*
- **SPEC‑RPT‑02 — Manifest.** The system shall write a status manifest to a
  configurable Girder tracking item (config id → env → unset), skipping the
  write under dry-run or when no item is configured. *Tests:
  `test_coord_enrichment_manifest.py`.*
- **SPEC‑RPT‑03 — PDV coverage.** The system shall report `pdv_trace` items as
  fully-enriched / partial / unenriched / missing-igsn with a coverage rate.
  *Tests: `test_coord_enrichment_pdv_observer.py`.*

### C9 — Data-quality checks (advisory, non-blocking)

All checks are `@asset_check`; none gate execution. Partitioned-leaf checks read
materialization metadata from the event log (see SPEC‑ORCH‑03).

| Check | Asset | Severity | Fires when |
|---|---|---|---|
| `igsn_validity_rate` | `pdv_log` | WARN | < 80% valid |
| `zero_pdv_inventory` | `pdv_data` | ERROR | 0 items |
| `pdv_match_rate` | `pdv_data` | WARN | < 50% matched |
| `igsn_consistency` | `pdv_data` | ERROR | any mismatch |
| `enrichment_success_rate` | `pdv_data` | WARN | < 90% success |
| `coord_transform_check` | `pdv_data` | WARN | any transform failure |
| `manifest_written` | `pdv_processing_manifest` | ERROR | manifest not written |
| `enrichment_success_rate_*` / `no_coord_transform_failures_*` | coord_enrichment leaves | WARN | as above, per leaf |
| `maxima_xrd_derived_provenance_valid` | `enriched_maxima_derived` | ERROR | any prov-link error |
| `pdv_coverage_above_threshold` | `helix_pdv_coverage_observer` | WARN | < 50% coverage |
| `inventory_nonempty_per_instrument` | `enrichable_items_inventory` | WARN | any empty key |
| `all_helix_alpss_tagged` | `helix_alpss_provenance_tagged` | ERROR | any HELIX unresolved |

*Thresholds (0.8, 0.5, 0.9) are inline literals — see Open Question Q5.*

The seven `helix_spreadsheet` checks are partitioned and therefore read the
event log rather than taking their asset as an input (SPEC‑ORCH‑03); their
verdict logic lives in pure `eval_*` helpers in `checks.py`. Two known
false-positive edge cases are open — see Q10.

### C10 — Orchestration & safety

- **SPEC‑ORCH‑01 — HELIX sensor.** `helix_experiment_log_discovery_sensor` shall
  poll `/aimdl/partition?dataType=pdv_experiment_log`, register new
  `<igsn>//<experiment_date>` keys on the `helix_experiment_log` dynamic
  dimension, and emit one partitioned RunRequest per changed log, deduped by
  content hash and skipping logs already marked `completed_clean`. STOPPED by
  default, ≥3600 s interval. *Tests: `test_sensors_helix_discovery.py`.*
  *(Replaced the folder-crawling `helix_folder_sensor` in issue #31.
  `HELIX_FOLDER_ID` is consequently dead — assigned in `constants.py` and read
  by nothing, though `CLAUDE.md` still lists it as required. See Q11.)*
- **SPEC‑ORCH‑02 — Resource.** A `GirderConnection` resource shall manage session
  + auth and expose the upstream `GirderClient` (`get`, `downloadFile`,
  `addMetadataToItem`) to assets/sensors.
- **SPEC‑ORCH‑03 — Partition isolation.** Per-partition asset checks shall NOT
  take the partitioned asset as an input (which forces an IOManager load across
  the partition cross-product and fails on unmaterialized siblings); they read
  the current partition's materialization metadata instead. *Tests:
  `test_leaf_check_partition_isolation.py`.*
- **SPEC‑ORCH‑04 — Module annotation rule.** Dagster-adjacent modules shall NOT
  use `from __future__ import annotations` (breaks `Config` schema resolution).
  *Tests: `test_annotations_rule.py`.*
- **SPEC‑ORCH‑05 — Asset grouping.** Every asset shall belong to one of six named
  groups; none in the implicit `default` group. *Tests: `test_asset_groups.py`.*

--

## 6. The written data contract

Payload written to an enriched item (`addMetadataToItem`):

```json
{
  "Station_X": 19.725,
  "Station_Y": 20.631,
  "Sample_X": 12.3456,
  "Sample_Y": 7.8901,
  "coord_provenance": {
    "instrument": "HELIX",
    "transform_version": "HELIX/v2",
    "transform_yaml_sha256": "<hex|''>",
    "transformer_version": "1.2.3",
    "pipeline_version": "0.6.0",
    "source_timestamp": "2026-04-16T17:12:19+00:00",
    "source_timestamp_origin": "spreadsheet_timestamp_col",
    "station_coord_source": { "kind": "...", "...": "..." },
    "enriched_at": "2026-06-15T03:00:00+00:00",
    "dagster_run_id": "abc123"
  }
}
```

- HELIX leaf additionally writes `Flyer_Row`, `Flyer_Column`.
- `station_coord_source` by kind:
  - `helix_experiment_log`: `spreadsheet_item_id`, `spreadsheet_row_index`, `spreadsheet_pdv_filename`
  - `maxima_instructions`: `instructions_item_id`, `scan_point_index`
  - `inherited`: `parent_item_id`, `parent_data_type`
- **No IGSN is written** by the HELIX leaf. This is now deliberate and the
  docstrings agree (`pdv_data` says "Station/Sample coordinates +
  coord_provenance"); the policy question itself is still open — see Q6.

--

## 7. Non-functional requirements

- **NFR‑1 Idempotency** — re-running any pipeline is a no-op unless a meaningful
  input changed (SPEC‑PROV‑02). Required because sweeps cover ~171k items
  against a rate-limited API.
- **NFR‑2 Safety** — dry-run default + STOPPED default (INV‑8).
- **NFR‑3 Reproducibility** — provenance pins config hash + versions (SPEC‑PROV‑01/03).
- **NFR‑4 Observability** — per-stage counts + advisory checks surfaced in the UI (C9).
- **NFR‑5 Resilience** — per-item failure isolation (INV‑7); checks survive
  unmaterialized partitions (SPEC‑ORCH‑03).
- **NFR‑6 Scale** — indexed `/aimdl` queries instead of folder crawls; pagination
  at 100/page.

--

## 8. Acceptance (end-to-end, from the test suite)

- **AC‑1 (MAXIMA raw):** 25 `xrd_raw` + 25 `xrf_raw` → 50 enrichment writes + 1
  manifest write; report sees 2 materialized raw partitions.
  *(`test_coord_enrichment_e2e.py`.)*
- **AC‑2 (inheritance):** 2 ALPSS + 1 `xrd_derived` → 3 writes + 1 manifest;
  ALPSS Sample = parent Station via inherited version; coverage 1/1.
  *(`test_coord_enrichment_phase4_e2e.py`.)*
- **AC‑3 (HELIX boundary):** two rows, same station, timestamps straddling
  2026‑04‑01 → v1 yields the flip, v2 yields identity.
  *(`test_pdv_data_version_boundary_dispatch`.)*

--

## 9. Scope tiers — the decision the spec exists to support

The capabilities above are all *real*, but they are not all *equally required for
a first production cut*. This tiering separates "what the problem demands" from
"what the current architecture chose." Use it to decide how much to keep.

### Tier 0 — Minimal viable enrichment (HELIX leaf only)
Covers: **C1, C2, C3.** Source: experiment logs → `pdv_trace` items (~5% of
items). This is the slice that can honestly be expressed as *"partition by
experiment-log; read a shot→x,y table; write coords."* Everything in C2/C3 (the
versioned transform, provenance, idempotency) is still required even here — it is
not optional gold-plating, because the deliverable is *Sample* coordinates and a
reproducible record, not the raw spreadsheet number.
*Q1 is now closed: issue #31 collapsed C1's 9 assets to 3 partitioned ones.*

### Tier 1 — HELIX complete (add ALPSS derived)
Adds: **C4.** Brings in ~66.8k derived items via inheritance + provenance
tagging — by item count the single largest tier. Decision point:
inherit-from-parent (current) **vs** match ALPSS files directly against the same
spreadsheet shot table. See Q2 — note the direct-matching alternative is
weakened by the C-named convention, whose filenames carry no IGSN.

### Tier 2 — MAXIMA leaf (second instrument)
Adds: **C5, C7 (discovery).** This is the part the "one spreadsheet table"
mental model cannot express — MAXIMA has *no spreadsheet*; coordinates come from
`instructions.txt`. This is genuinely new capability, not duplication.
Decision point: dynamic partitions + discovery sensor (current) **vs** a periodic
idempotent sweep. See Q3.

### Tier 3 — MAXIMA derived + full reporting
Adds: **C6, C8.** ~32.9k more derived items + state reporting/manifests.

> **Reading the tiers against the simplification debate:** the engineer's 3-step
> proposal is an accurate description of **Tier 0** (and an arguable Tier 1). It
> is silent on Tiers 2–3, which are ~56% of items and have fundamentally
> different sources. The right first question is not "is the code too complex?"
> but "**which tier are we actually committing to ship first?**" — then cut
> everything above that tier and simplify hard within it.

--

## 10. Open design questions

- **Q1 — C1 granularity.** ~~Can the 9-asset HELIX DAG collapse to
  read→match→write (~3 assets)?~~ **CLOSED** by issue #31 (PR #32, 2026‑06‑18):
  `pdv_log → pdv_data → pdv_processing_manifest`, partitioned. The per-stage UI
  quality chips were preserved by attaching all seven checks to the three assets.
- **Q2 — ALPSS coordinate source.** Inherit from parent (current, two-phase: tag
  then inherit) vs match ALPSS filenames directly to the spreadsheet shot table.
  Direct matching could remove the provenance-tagging asset and a whole job for
  HELIX — but it is now the weaker option: C-named filenames carry no IGSN, so
  ~71% of ALPSS items could not be matched to a shot table by filename alone.
  Inheritance works uniformly across both conventions.
- **Q3 — MAXIMA partitioning.** Is `MultiPartitionsDefinition` + dynamic dim +
  discovery sensor + dual content-hash dedup warranted for ~26k items, or would a
  periodic sweep relying on SPEC‑PROV‑02 idempotency be simpler to operate?
  Dynamic partitions must be pre-registered and orphans accumulate.
- **Q4 — Duplicate jobs.** `coord_enrichment_maxima_raw_job` and
  `..._partition_job` select identical assets, split only for UI filterability.
  Could be one job + tags.
- **Q5 — Threshold config.** Rate thresholds (0.8/0.5/0.9) and rounding (4dp/1dp)
  are inline literals. Centralize?
- **Q6 — IGSN write policy.** Should the HELIX leaf write the IGSN it validated?
  (The docstring/code mismatch that framed this is resolved; the policy is not.)
  Note the ~831 `pdv_trace` items with no `meta.igsn` are invisible to
  `/aimdl/datafiles` entirely, so this cannot fix discovery for them.
- **Q7 — Namespacing.** Flat `Station_X/...` keys vs a nested `coordinates` dict.
- **Q8 — Channel files.** Should one spreadsheet row enrich all channel files for
  a shot, or only the unique match? (Today multi-match = ambiguous = skipped.)
- **Q9 — Orphaned ALPSS completeness reporting.** SPEC‑HELIX‑09 was withdrawn
  with the 9-asset design and never re-homed. Should the coverage metric return,
  and to which asset?
- **Q10 — Advisory-check false positives.** Known from the PR #32 review:
  `enrichment_success_rate` and `coord_transform_check` emit a spurious WARN on a
  partition with zero PDV matches (rate 0/0), and `manifest_written` reports a
  false ERROR when a partition transiently resolves zero log items. Both are
  cosmetic but will be noisy during a live sweep.
- **Q11 — Dead `HELIX_FOLDER_ID`.** Assigned in `constants.py`, read by nothing
  since issue #31, still documented as required in `CLAUDE.md`. Remove both, or
  keep as a documented no-op?
- **Q12 — Items without `meta.igsn`.** ~831 `pdv_trace` items (and an unknown
  number of others) lack an IGSN and are therefore invisible to
  `/aimdl/datafiles`. They are excluded from every coverage denominator, so
  metrics read as complete while these items are silently unenriched. Tag
  upstream, or report them as a first-class gap?

--

## 11. Known spec ↔ code drift (surfaced during extraction)

- ~~**D1** — `enriched_pdv_metadata` docstring says it writes IGSN; it does
  not.~~ **RESOLVED.** The asset no longer exists; its successor `pdv_data`
  documents exactly what it writes. The underlying policy question survives as Q6.
- **D2** — `overwrite.should_write` docstring lists reason order differently from
  the code's check order (`transform_version` is checked before
  `station_coord_source`). No behavioral impact under current tests, which vary
  one field at a time.
- **D3** — `InstructionsCache` (`coord_enrichment/cache.py`) and the
  `maxima.find_run_folder_id` / `fetch_instructions_for_run` folder-walking path
  are **not wired into the leaf** (which self-fetches per partition). They are
  exercised only by their own unit tests — leftovers from the pre‑issue‑#23
  folder-walk design. Dead-relative-to-the-DAG; flag, don't delete blindly.
- **D4** — `.claude/helix_dagster_context.md` describes an old `helix_dagster/`
  module at version 0.2.0/0.4.0; the live package is `aimdl_coord_enrichment` at
  0.6.0. It also documents the pre-issue-#31 9-asset HELIX DAG throughout.
- **D5** — `docs/runbooks/readiness_dry_run.md` still names `enriched_pdv_metadata`,
  and `operations/dry_run_readiness.py` exercises only the four
  `coord_enrichment` jobs — `process_helix_assets_job` is not covered by the
  GO/NO-GO rubric at all.
- **D6** — `.claude/scratch/probe_helix_alpss.md` (2026‑05‑17) diagnoses the
  C-named family as an upstream Girder mis-tagging defect and recommends
  excluding those items. That conclusion is **wrong** and was disproven
  2026‑08‑30; the file carries a superseded banner. See
  `.claude/scratch/probe_c1_naming.md` and SPEC‑ALPSS‑01.

--

## 12. Traceability

Every `SPEC‑*` requirement above cites the test(s) that currently pin it. The
inverse map (test → requirement) can be generated from those citations. New
behavior should add a `SPEC‑*` ID here first, then a test that references it.
