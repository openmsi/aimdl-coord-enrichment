# Live coordinate enrichment — results, 2026-09-03/04

The first live sweep of the coordinate-enrichment pipeline against the
production AIMD-L collection at `data.htmdec.org`. All three flows were run.
**156,117 items were enriched with zero write errors.**

Superseded values live in `docs/runbooks/first_sweep_expected_values.md`; this
file is the record of what actually happened.

## Headline

| flow | enriched | of | coverage |
|---|--:|--:|--:|
| `pdv_trace` (HELIX experiment logs) | 5,867 | 7,764 | 75.6% |
| HELIX ALPSS (derived) | 51,920 | 67,436 | 77.0% |
| MAXIMA (all data types) | 98,330 | 99,791 | **98.5%** |
| **total** | **156,117** | | |

Zero write errors and zero job failures in every flow. Each enriched item
carries `Station_X/Y`, `Sample_X/Y`, and a full `coord_provenance` block:
instrument, transform version, transform YAML sha256, transformer version,
pipeline version, source timestamp and its origin, the coordinate source, and
the Dagster run id.

## Per-flow detail

### HELIX traces — `process_helix_assets_job`

291 partitions, run in month batches.

| | traces |
|---|--:|
| seen | 7,764 |
| paired to a log row | 6,653 |
| **enriched** | **5,867** |
| paired but no corrected flyer position | 786 |
| paired via shot identity (multipoint siblings) | 831 |
| write errors | 0 |

Transform versions: `HELIX/v1` 965, `HELIX/v2` 4,902. Pairing method:
`filename` 5,036, `shot_identity` 831.

`HELIX/v2` is the identity transform — the station frame was physically
realigned to the sample frame on 2026-04-01 — so Station == Sample for shots on
or after that date. Pre-cutover shots show the transform doing real work, e.g.
Station `43.9125 / 2.8875` → Sample `-3.9125 / 2.8875` on a 2025-11-06 shot.

### HELIX ALPSS — `coord_enrichment_helix_alpss_job`

| partition | seen | in scope | enriched | excluded |
|---|--:|--:|--:|--:|
| `HELIX/pdv_alpss_output` | 59,943 | 46,151 | 46,151 | 13,792 |
| `HELIX/pdv_alpss_result` | 7,493 | 5,769 | 5,769 | 1,724 |
| `HELIX/pdv_alpss_results` | 0 | 0 | 0 | 0 |

All 15,516 exclusions are `parent_not_enriched` — children of traces that have
no coordinates, so they resolve when the parents do. ALPSS **inherits** the
parent's recorded transform version rather than recomputing, so a historical
v1 shot keeps v1 in all its derived files.

`pdv_alpss_results` (plural) is an empty partition and does nothing.

### MAXIMA — `coord_enrichment_maxima_partition_job`

1,677 run partitions, one per AIMD-L run.

| | items |
|---|--:|
| seen | 99,791 |
| **enriched** | **98,330** |
| excluded — `no_instructions` | 1,265 |
| excluded — `malformed_instructions` | 196 |
| write errors | 0 |

The 1,461 exclusions fall in **62 runs** with no usable `instructions.txt`.
That is a property of how those runs were produced, not a pipeline failure.

MAXIMA reaches far higher coverage than HELIX because it resolves coordinates
from each run's own `instructions.txt`, rather than depending on a separately
tagged experiment log.

## What is not enriched, and why

Every category below is an upstream data gap. All flows are idempotent, so
re-running after a fix picks up only what newly resolves.

| | items | cause |
|---|--:|---|
| HELIX traces in partitions with no tagged experiment log | 837 | log never given `meta.data_type` upstream |
| HELIX traces whose log row has no corrected flyer position | 786 | `Flyer_X/Y_Position_Corrected` blank; skipped rather than written as null |
| HELIX traces refused as `ambiguous_row` | 57 | two log rows in one partition claim the same trace |
| HELIX traces finding no row in a tagged log | 254 | undiagnosed; the one remaining item that is ours |
| ALPSS children of the above | 15,516 | `parent_not_enriched` |
| MAXIMA items in runs without usable instructions | 1,461 | `no_instructions` / `malformed_instructions` |

## Measured timings

Writes cost roughly **3× a dry run** of the same work. Estimates taken from
read-only samples were substantially low; these are the observed figures.

| flow | partitions | dry run | live |
|---|--:|--:|--:|
| HELIX traces | 291 | ~10 min | ~15 min |
| HELIX ALPSS | 3 | ~7 min | ~45 min |
| MAXIMA | 1,677 | 31 min | 106 min |

## Verification

Counters were not trusted on their own. Each flow was checked by reading full
item records back from Girder:

- **traces** — a 2025-11-06 item confirmed with correct Station/Sample, complete
  provenance, and `pairing: "filename"`.
- **ALPSS** — 5,769 of 7,493 in-scope `pdv_alpss_result` items carry
  coordinates, all `kind: inherited` with `source_timestamp_origin:
  inherited_from_parent` and a resolvable `parent_item_id`.
- **MAXIMA** — 1,224 of 1,224 sampled `xrd_raw` + `xrd_derived` items across
  three runs, source kind `maxima_instructions`, Station `7.0 / -3.0` → Sample
  `21.0 / 17.0` confirming `MAXIMA/v1` is not identity.

> **A verification caveat worth carrying forward.** `/aimdl/datafiles` returns a
> *reduced projection* of `meta` — only `data_type` and `igsn`. A first check
> using it reported 0 enriched items when 5,867 had in fact been written. Verify
> against full item records (`/aimdl/partition/details`, or a direct item GET),
> never against `/aimdl/datafiles`.

## Follow-ups

1. **The 311 HELIX traces** — 254 with no row in a tagged log, 57 ambiguous.
   The only remaining gap that belongs to this repo.
2. **Draft the upstream issue** for the `PDV_FileName` tagging gate in
   `girder-consumers/helix-otherdata` — the last of four write-ups, and the one
   blocking the 837.
3. **Confirm `skipped_no_change` on the next ALPSS run.** Before this sweep the
   ALPSS assets read items through `/aimdl/datafiles`, so stored provenance was
   invisible and every re-run rewrote all 67,436 items. They now fetch per
   partition with full meta, so unchanged items should be skipped — the next run
   should report a large `skipped_no_change` rather than a large `written`.

## Reproducing

```bash
.venv/bin/python operations/run_live_pass.py --all                       # rehearse
.venv/bin/python operations/run_live_pass.py --all --live                # traces
.venv/bin/python operations/run_live_pass.py --flow alpss --all --live   # after traces
.venv/bin/python operations/run_live_pass.py --flow maxima --all --live  # independent
```

Writes happen only with `--live`. The runner reads `.env` and `DAGSTER_HOME`
itself, executes one partition at a time against the real Dagster instance so
runs appear in the UI, and writes a per-partition report to
`operations/log/live_pass_<timestamp>.json`.

Procedure: `docs/runbooks/live_enrichment_pass.md`.
