# Specification — Coordinate Enrichment for AIMD‑L (`aimdl_coord_enrichment`)

> **Status:** Reverse-engineered from source + test suite at version `0.6.0`.
> **Last refreshed:** 2026-08-30 — §3.2 counts re-measured against live prod;
> §C1/§C9/§C10 updated to the 3-asset partitioned HELIX flow (issue #31); §C4
> updated for the production filename conventions; IGSN pattern corrected.
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
| **IGSN** | Sample identifier. Pattern `[A-Za-z]{6}\d{5}(?:-[A-Za-z0-9]+)*` — a 6-letter prefix, 5 digits, then **zero or more** hyphen-delimited segments (`JHAMAL00018`, `JHAMAL00018-005`, `NWXMAB00010-002-001`). Matched unanchored, so it also extracts an IGSN embedded in a filename. |
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
| MAXIMA | `xrd_derived` | leaf | `instructions.txt` scan-point | 32,886 |
| MAXIMA | `xrd_visualization` | leaf | `instructions.txt` scan-point | 23,905 |

**~195,000 in-scope items.**

`pdv_alpss_results` (plural) was in scope at 1,526 items in the 2026-05-17
snapshot and **no longer exists** as a `data_type`.

Several filename conventions are in concurrent production use for the HELIX
PDV family — see SPEC‑ALPSS‑01. All are first-class; none is legacy.

\* `pdv_trace` is enriched by the **spreadsheet DAG**, and is treated as an
*external / pre-enriched* parent by the coord_enrichment DAG's instrument
registry (HELIX leaf set is intentionally empty there).

**Explicitly out of scope:** `xrd_metadata`, `pdv_experiment_log`,
`unclassified`, `nmd_raw` (365), `nmd_project` (487), and:

- **Calibrant data** (`xrd_calibrant_raw`, `xrd_calibrant_derived`, and the
  untagged `calibrate/` files). Parked pending a data cleanup — decided
  2026-08-31. Calibrants are physically samples and are processed the same way,
  so this is not a statement about their value; the `calibrate/` folders are
  simply inconsistent as stored (no `instructions.txt` in 43/43 runs, no
  `scan_point_<i>` index on any of 125 files), leaving no key to look a
  coordinate up with. Revisit as its own piece of work.
- **Items with no `meta.igsn`.** `/aimdl/datafiles` and `/aimdl/partition`
  filter on `meta.igsn`, so these are invisible to discovery — ~831 `pdv_trace`,
  ~4,700 `xrd_raw`, and ~9,280 `xrf_raw` (47% of that type). Decided
  2026-08-31: an un-annotated file is a test or otherwise unimportant artifact
  for now, so this is a **deliberate scope boundary**, not a gap to close. It
  does mean coverage denominators count only annotated items.

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

### C1 — HELIX `pdv_trace` enrichment

*(the "helix_spreadsheet" DAG; **3 partitioned assets**)*

**The trace is the unit of work.** Every PDV trace registered in Girder carries
`meta.igsn`, and the `/aimdl/partition` endpoint supplies its experiment date;
together those give the AIMD-L key `<igsn>//<experiment_date>`, which resolves
to the experiment log holding the row that records the shot's flyer position.
The flow therefore iterates traces and, for each, finds its row — not the other
way round.

Two consequences are structural rather than incidental:

- A trace can only take coordinates from a row describing **its own sample**,
  because matching is scoped to the trace's own partition. Cross-sample
  contamination is impossible by construction, not prevented by a check.
- A log row naming a file that was never ingested is a **non-event**. Girder is
  the only place the data exists; a row with no trace has nothing to enrich and
  is not a reported gap.

Traces without `meta.igsn` never appear in the partition index, so "skip
unannotated traces" needs no filter.

Partitioned on `HELIX_TRACE_PARTITIONS`
(`DynamicPartitionsDefinition("helix_pdv_trace")`). Three durable assets —
`pdv_log → pdv_data → pdv_processing_manifest` — model external-state
transitions; pure computation lives in `spreadsheet.py`. Both writing assets
take `HelixSpreadsheetConfig` with `dry_run: bool = True`. Job name
`process_helix_assets_job`.

- **SPEC‑HELIX‑01 — Ingest.** Given a partition key, the system shall fetch the
  `pdv_experiment_log` item(s) for that same key, download each (`.csv` →
  `read_csv`, else `read_excel`), rename columns via `COLUMN_MAP`, and
  concatenate. A key may resolve to one log item, several, or **none** — traces
  exist for sessions whose log was never tagged upstream, and those partitions
  read an empty frame rather than failing. *(`pdv_log`; tests:
  `test_pdv_log_reads_partition`, `test_normalize_experiment_log_renames_columns`.)*
- **SPEC‑HELIX‑02 — IGSN validation.** For each row, the system shall validate
  `Sample_IGSN` against the IGSN pattern, classifying each as `valid`,
  `missing` (None/NaN/empty), or `invalid_format`. *(`pdv_log`; tests:
  `test_validate_log_rows`.)*
- **SPEC‑HELIX‑03 — Trace set.** The system shall fetch the partition's PDV
  traces via `/aimdl/partition/details?dataType=pdv_trace&key=<key>` — the
  partition's traces only, never the whole collection. *(`pdv_data`.)*
- **SPEC‑HELIX‑04 — Filename stem.** A log's `PDV_FileName` may record the
  station-local absolute path the file was written to before it was streamed
  into Girder (`C:\Users\Administrator\Desktop\PDV_DATA\<name>`). Girder is
  the only place the file exists, so the directory part names nothing; the
  system shall reduce the cell to its trailing component with
  `ntpath.basename` (`ntpath`, not `os.path` — this runs on POSIX, which does
  not split on backslashes). Blank/NaN cells name no file and are omitted.
  *Measured 2026‑09‑01: 657 log rows whose file had otherwise gone
  unmatched resolve on this normalization alone. Tests: `test_windows_path_reduces_to_its_basename`,
  `test_blank_cells_name_no_file`,
  `test_trace_matches_a_row_recorded_as_a_windows_path`.*
- **SPEC‑HELIX‑04a — Pairing.** For each trace, the system shall find the single
  log row whose stem prefixes the trace name (`trace.name.startswith(stem)`).
  Given exactly 1 → paired; given 0 → `no_row_in_log`; given >1 →
  `ambiguous_row`, and **no row is chosen**. Two rows claiming one trace means
  the partition holds contradictory logs; guessing would apply the wrong shot's
  coordinates. *Tests: `test_matches_the_single_naming_row`,
  `test_trace_with_no_row_in_the_log`,
  `test_two_rows_claiming_one_trace_is_ambiguous`.*
- **SPEC‑HELIX‑04b — Channel-prefix fallback.** A trace is stored under the
  digitizer channel that recorded it, so its Girder name may carry a leading
  `C<n>--` the log omits. When and only when the exact pass finds nothing, the
  system shall retry with that prefix stripped from the trace name. Exact match
  is tried first and wins outright, so the fallback cannot change an outcome
  that already matched. The unique-match requirement holds on both paths
  (INV‑5). Only `C<n>--` is stripped — an arbitrary leading token still yields
  `no_row_in_log`. *Tests: `test_matches_across_channel_prefix`,
  `test_exact_prefix_match_wins_over_the_fallback`,
  `test_only_a_channel_prefix_is_stripped`.*
- **SPEC‑HELIX‑04c — Shot-identity pairing (multipoint).** A multipoint shot
  writes one trace per probe, and the log-writing software does not record all
  of them — measured 2026‑09‑02, every multipoint log names C1 and C3 and never
  C2 though the two exist in equal numbers, and some earlier runs name only one
  channel of three. This is a known upstream bug.

  The coordinate written is the **flyer position**, a property of the shot, not
  of the probe: every probe in one shot sees the same flyer in the same place.
  An unnamed channel's coordinate is therefore not unknown — it is the value the
  row already gives its named siblings, and identical to what the corrected log
  will supply. When and only when both earlier passes find nothing, the system
  shall retry with the channel prefix stripped from **both** the trace name and
  the row stems. The remainder of the name carries IGSN, run id, timestamp and
  shot number, so it is unique to one shot. Unique match → paired with
  ``pairing = "shot_identity"``; more than one → `ambiguous_row` with
  `via: "shot_identity"`.

  Such pairings shall record `station_coord_source.pairing = "shot_identity"`
  (versus `"filename"`) so the affected items stay queryable and can be
  re-verified against the corrected logs. *Tests:
  `test_unnamed_sibling_channel_pairs_by_shot_identity`,
  `test_shot_identity_does_not_reach_a_different_shot`,
  `test_shot_identity_refuses_two_rows_claiming_one_shot`,
  `test_write_pdv_metadata_records_how_the_trace_was_paired`.*
- **SPEC‑HELIX‑04d — Per-probe filename columns.** A multipoint log has no bare
  `PDV_FileName`; it carries one block per probe, `PDV_<n>_FileName`, where
  `<n>` is the probe id (not the digitizer channel). Several probes may be
  recorded onto one channel file, so a row's probe columns collapse to fewer
  distinct filenames than it has populated probes. The system shall read the
  bare column plus every `PDV_<n>_FileName`, yielding a set of stems per row;
  every file a row names takes that row's coordinates. *Measured on
  `LMI_20260818_JHAMAL00021-003.csv`: 7 probe columns, 4 populated per row,
  2 distinct files (probe 10 → C1, probes 6/9/15 → one shared C3). Tests:
  `test_row_filename_stems_reads_per_probe_columns`,
  `test_one_row_naming_several_files_pairs_each_of_them`.*
- **SPEC‑HELIX‑04e — Untagged experiment logs.** A partition's traces can only
  be paired if the session's experiment log carries `meta.data_type`.
  `girder-consumers/helix-otherdata` gates that tagging on a literal
  `PDV_FileName` column, which multi-channel logs (one column per probe,
  `PDV_<n>_FileName`) do not have. Those partitions pair nothing until the
  upstream consumer is fixed. `pdv_match_rate` records `log_items: 0` and
  passes — there is nothing this pipeline can act on. After the upstream fix,
  `COLUMN_MAP` / `row_filename_stems` must read every `PDV_<n>_FileName` and
  dedupe by filename.
- **SPEC‑HELIX‑04f — Scope.** Only items returned by the `/aimdl/*` endpoints
  are in scope. Traces without `meta.igsn` never appear there — attempted shots
  that never triggered, test shots (`TEST00_000…`), and shots on unregistered
  samples. They can never carry meaningful coordinates and are not counted,
  reported, or reconciled by this pipeline. *Measured 2026‑09‑01: multipoint
  traces are tagged correctly; all three channels of a real shot are indexed
  together.*
- **SPEC‑HELIX‑05 — IGSN authority.** The trace's own `meta.igsn` is
  authoritative. When a trace pairs to a row whose validated IGSN differs, the
  system shall record an `igsn_mismatch` issue and **refuse the pair** — the
  trace is left untouched. A partition's log may hold rows from a restarted run
  written under the previous sample's identifier; applying one would put
  another sample's coordinates on this trace. *Tests:
  `test_pair_traces_to_rows_refuses_a_row_declaring_another_sample`.*
- **SPEC‑HELIX‑06 — Enrich.** For each paired trace, the system shall compute
  `Station_X/Y` from `Flyer_X/Y_Position_Corrected`, derive `Sample_X/Y` via the
  timestamp-selected transform (rounded to 4 dp), build `coord_provenance`
  (`station_coord_source.kind = "helix_experiment_log"`), and write the
  coordinate payload to the trace item. *(`pdv_data`; tests:
  `test_write_pdv_metadata_writes_coords_and_provenance`,
  `test_pdv_data_version_boundary_dispatch`.)*
- **SPEC‑HELIX‑06a — No coordinate, no write.** The corrected flyer position
  (`Flyer_X/Y_Position_Corrected`) is blank on some rows. Where either is
  missing the system shall write nothing and count `no_station_coords`:
  recording `Station/Sample = null` with a provenance block naming a transform
  that never ran would put meaningless metadata on the item. The desired
  position is **not** substituted — it is the commanded value, not the measured
  one. *Tests: `test_write_pdv_metadata_skips_rows_with_no_corrected_flyer_position`.*
- **SPEC‑HELIX‑07 — Timestamp normalization.** The shot timestamp comes from the
  row `Timestamp` column; a naive value is assumed UTC (origin recorded as
  `..._assumed_utc`) and counted; unparseable/missing → no version selection.
- **SPEC‑HELIX‑08 — Audit manifest.** After processing, the system shall write a
  `processing_status` manifest to each *source log* item (trace counts +
  `status ∈ {completed_clean, completed_with_warnings}`). A partition with no
  tagged log has no manifest target; that is an upstream gap already reported by
  `pdv_match_rate`, not a write failure. *(`pdv_processing_manifest`; tests:
  `test_summarize_pdv_processing_clean`,
  `test_summarize_pdv_processing_with_warnings`,
  `test_write_processing_manifest_success`.)*
- **SPEC‑HELIX‑09 — Completeness reporting.** *(**Withdrawn**.)* The 9-asset
  design reported per-IGSN ALPSS coverage via `quality_report` +
  `alpss_results_inventory`; both assets were removed.
  `helix_pdv_coverage_observer` measures a different thing (coord-provenance
  coverage on `pdv_trace`, not missing ALPSS results). **This capability
  currently has no owner** — see Q9.
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

  **Multiple filename conventions are in concurrent production use**, and all
  must resolve. An ALPSS filename is `<stem><sep><output>.<ext>` where `sep` is
  `-` or `_`; the parent is the unique `<stem>.csv` in the `pdv_trace` pool.
  At least three stem shapes are observed in prod:

  | shape | trace | ALPSS child |
  |---|---|---|
  | IGSN-named | `JHAMAC00003-S1R4C3_2026-02-18_18-45-56_shot01_ch1.csv` | `…_ch1-iq.png` |
  | C-named | `C1--20250807--00001.csv` | `C1--20250807--00001-results.csv` |
  | C/IGSN hybrid (from 2026‑07) | `C1--NWXMAB00010-002-001_2026-07-06_21-45-25_shot01--00000.csv` | `…--00000-results.csv` |

  The list is **open** — treat it as observed, not exhaustive. This is precisely
  why the rule keys on structure (`<sep><output>.<ext>` + a unique parent)
  rather than on any convention's shape: a fourth shape needs no code change.
  None is legacy. Measured 2026‑08‑30, C-named files are ~70% of `pdv_trace`
  and ~71% of both ALPSS types, and the family is currently in use: it ran
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

### C5 — MAXIMA enrichment (one run at a time)

*Scope: `xrd_raw`, `xrf_raw`, `xrd_derived`, `xrd_visualization`.*

**The run is the unit of work.** One AIMD-L run — key
`"<igsn>//<experiment_date>"` — produces one `instructions.txt`, and that file
supplies the station coordinates for every file the run produced. Storage nests
`raw/` inside the run folder while lineage runs the other way (the derived
products are made *from* the raw measurements), but neither shape is a partition
boundary. They materialize together, from one fetch of one coordinate table.

- **SPEC‑MAXR‑01 — Self-fetching run partition.** Each partition shall fetch its
  own items — once per in-scope data_type — and its own `instructions.txt` via
  `/aimdl/partition/details` keyed on the run key. It depends on no inventory or
  provenance asset. *Tests: `test_coord_enrichment_maxima_run.py`.*
- **SPEC‑MAXR‑02 — Scan-point lookup.** Station coordinates come from
  `instructions.txt` (`sample.scan_points[i]` = `[x, y]`), where `i` is parsed
  from the **`scan_point_<i>` prefix** of the filename. The index is at the head,
  not the tail: `scan_point_0_data_000001.h5` is scan point **0**, and the
  trailing `000001` is a detector frame counter that would mis-map every
  `_data_` file to `scan_points[1]`. Timestamp comes from `meta.experiment_date`.
  *Tests: `test_parse_scan_point_index_*`, `test_scan_point_coords_*`.*
- **SPEC‑MAXR‑03 — Missing/duplicate instructions.** Given no `instructions.txt`,
  every item in the run is recorded as a resolution error (stage
  `instructions`). Given multiple, the first is used and the rest are recorded as
  duplicate warnings.
- **SPEC‑MAXR‑04 — `instructions.txt` validation.** The payload must be JSON with
  `sample.scan_points` a non-empty list of numeric `[x, y]` pairs; any violation
  is a `ResolutionError`. *Tests: `test_parse_instructions_json_*`.*
- **SPEC‑MAXR‑05 — Uniform treatment, recorded lineage.** Derived products take
  their coordinates from the same `instructions.txt` as the raw measurements, not
  by inheriting from a parent. Where `meta.prov.wasDerivedFrom` exists it is
  copied into `station_coord_source.parent_item_id` as a **cross-reference**, but
  it is not load-bearing: an item with no prov link enriches identically.
  *Tests: `test_one_run_partition_covers_every_maxima_data_type`,
  `test_derived_item_records_parent_lineage_without_depending_on_it`.*

> **Why not inheritance** (the former C6). `xrd_derived` used to inherit
> `Station_X/Y` from its parent `scan_point_<i>_master.h5` via
> `prov.wasDerivedFrom`. Both routes yield identical numbers — the parent reads
> the same `instructions.txt` — so inheritance recorded a *path* rather than an
> *origin*, and made the coordinate depend on links written by an external
> plugin. Measured 2026-08-31, **0 of 528 sampled `xrd_derived` items carry
> `prov.wasDerivedFrom` at all**, so that route could not run. Retiring it also
> removed `AllPartitionMapping` (any raw change re-materialized every derived
> partition), the parent-readiness fast-fail, the prov-validity check, and the
> deferred "decision beta" repartitioning.
>
> Contrast HELIX ALPSS (C4), where inheritance is *necessary*: an ALPSS output
> has no independent coordinate source. Every MAXIMA file carries its own index
> into the run's table.

> **Former SPEC-MAXD-04 (raw/ scope filter) was a bug, now removed.** It kept
> only `xrd_derived` items whose immediate folder was named `raw`. No derived
> item is ever stored there — `raw/` holds the raw measurements — so the
> predicate matched **0 of 30,215** items and the whole derived tier operated on
> an empty set. It described where the parents live and was applied to the
> children.

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
  index for `xrd_raw`/`xrf_raw`/`xrd_derived` plus `xrd_metadata`, register new
  AIMD‑L run keys on the dynamic dimension, and emit **one deduped RunRequest
  per run** — not per `(data_type, run)`. The dedup key composes a content hash
  per data type plus the `xrd_metadata` hash, so any of them changing
  re-triggers; a changed `instructions.txt` moves every coordinate in the run,
  which is why it is in the key. *Tests: `test_sensors_maxima_discovery.py`.*
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
| `zero_traces_in_partition` | `pdv_data` | ERROR | partition holds 0 traces |
| `pdv_match_rate` | `pdv_data` | WARN | < 50% of traces paired, or no tagged log |
| `igsn_consistency` | `pdv_data` | ERROR | any trace paired a row declaring another sample |
| `enrichment_success_rate` | `pdv_data` | WARN | < 90% of paired traces enriched |
| `coord_transform_check` | `pdv_data` | WARN | any transform failure |
| `manifest_written` | `pdv_processing_manifest` | ERROR | manifest not written |
| `enrichment_success_rate_*` / `no_coord_transform_failures_*` | coord_enrichment leaves | WARN | as above, per leaf |
| `pdv_coverage_above_threshold` | `helix_pdv_coverage_observer` | WARN | < 50% coverage |
| `inventory_nonempty_per_instrument` | `enrichable_items_inventory` | WARN | any empty key |
| `all_helix_alpss_tagged` | `helix_alpss_provenance_tagged` | ERROR | any HELIX unresolved |

*Thresholds (0.8, 0.5, 0.9) are inline literals — see Open Question Q5.*

The seven `helix_spreadsheet` checks are partitioned and therefore read the
event log rather than taking their asset as an input (SPEC‑ORCH‑03); their
verdict logic lives in pure `eval_*` helpers in `checks.py`. Two known
false-positive edge cases are open — see Q10.

### C10 — Orchestration & safety

- **SPEC‑ORCH‑01 — HELIX sensor.** `helix_trace_discovery_sensor` shall
  poll `/aimdl/partition?dataType=pdv_trace`, register new
  `<igsn>//<experiment_date>` keys on the `helix_pdv_trace` dynamic
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
- **SPEC‑ORCH‑05 — Asset grouping.** Every asset shall belong to a named group; none in the implicit `default` group. *Tests: `test_asset_groups.py`.*

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

- **AC‑1 (MAXIMA run):** 25 `xrd_raw` + 25 `xrf_raw` in **one** run partition →
  50 enrichment writes + 1 manifest write; report sees 1 materialized partition.
  *(`test_coord_enrichment_e2e.py`.)*
- **AC‑2 (HELIX inheritance):** 2 ALPSS items → 2 writes + 1 manifest; ALPSS
  Sample = parent Station via inherited version; coverage 1/1.
  *(`test_coord_enrichment_phase4_e2e.py`.)*
- **AC‑4 (MAXIMA live dry run, 2026‑08‑31):** three real runs, 510 items each
  (`xrd_raw` 170, `xrd_derived` 170, `xrf_raw` 85, `xrd_visualization` 85) →
  100% success, 0 resolution errors, 0 coordinate failures, 0 writes under
  `dry_run`. Before the refactor `xrd_derived` was 0-in-scope.
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

### Tier 2 — MAXIMA (second instrument)
Adds: **C5, C7 (discovery).** Now one tier, not two: since every MAXIMA file
reads the same `instructions.txt`, raw and derived enrich in a single run-scoped
leaf. The former Tier 3 "MAXIMA derived" collapsed into this one. This is the part the "one spreadsheet table"
mental model cannot express — MAXIMA has *no spreadsheet*; coordinates come from
`instructions.txt`. This is genuinely new capability, not duplication.
Decision point: dynamic partitions + discovery sensor (current) **vs** a periodic
idempotent sweep. See Q3.

### Tier 3 — Full reporting
Adds: **C8.** State reporting + manifests. (The former "MAXIMA derived" content
of this tier merged into Tier 2.)

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
- **Q3 — MAXIMA partitioning.** ~~Is `MultiPartitionsDefinition` warranted?~~
  **Partly closed** 2026-08-31: the `data_type` dimension is gone; partitions are
  now one-per-run on a single dynamic dim. Still open: whether dynamic partitions
  plus a discovery sensor beat a periodic sweep relying on SPEC‑PROV‑02
  idempotency. Dynamic partitions must be pre-registered and orphans accumulate.
- **Q4 — Duplicate jobs.** `coord_enrichment_maxima_job` and
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
- ~~**D7** — `IGSN_PATTERN` allowed at most one hyphen-delimited suffix segment,
  silently truncating two-segment IGSNs (`NWXMAB00010-002-001` →
  `NWXMAB00010-002`) and manufacturing a false `igsn_consistency` ERROR against
  the item's full `meta.igsn`.~~ **RESOLVED** 2026‑08‑30 — found by the first
  dry run of `process_helix_assets_job`; affected 2 of 214 HELIX partitions.
  *Tests: `test_validate_igsn_roundtrips_production_shapes`.*
- **D2** — `overwrite.should_write` docstring lists reason order differently from
  the code's check order (`transform_version` is checked before
  `station_coord_source`). No behavioral impact under current tests, which vary
  one field at a time.
- **D3** — `InstructionsCache` (`coord_enrichment/cache.py`) and the
  `maxima.find_run_folder_id` / `fetch_instructions_for_run` folder-walking path
  are **not wired into the leaf** (which self-fetches per partition). They are
  exercised only by their own unit tests — leftovers from the pre‑issue‑#23
  folder-walk design. Dead-relative-to-the-DAG; flag, don't delete blindly.
- **D5** — `instruments/maxima.heal_maxima_derived_parent` and
  `find_master_h5_item_id` are now **unreachable from the DAG**: with
  `MAXIMA_DERIVED_DATA_TYPES` empty, `resolve_parent_item_id`'s MAXIMA branch can
  never fire. They remain exercised by their own unit tests. Same status as D3 —
  flag, don't delete blindly.
- **D6** — `docs/runbooks/` (`readiness_dry_run.md`,
  `coord_enrichment_production_sweep.md`, `first_sweep_expected_values.md`) and
  `operations/dry_run_readiness.py` still reference `enriched_maxima_raw`,
  `enriched_maxima_derived`, `coord_enrichment_maxima_derived_job` and the
  `(data_type, run)` partition shape. The GO/NO-GO rubric will not run against
  the current DAG until they are updated.
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
