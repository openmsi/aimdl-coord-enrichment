# RESUME — read this first (2026-09-04)

Handoff for a fresh session. Everything below is current; nothing here describes
superseded designs.

## State in one line

All three coordinate-enrichment flows have **run live against production**.
156,117 items in Girder carry coordinates, zero write errors. `main` is at
`68795a3`, clean, pushed. Suite **371 passed / 1 skipped**.

Full results: `docs/live_enrichment_report_2026-09-03.md`.

---

## 1. The mental model

**The trace is the unit of work.** Not the experiment-log row.

Every PDV trace in Girder carries `meta.igsn` and `meta.experiment_date`, giving
the AIMD-L key `<igsn>//<experiment_date>`. That same key resolves, in the same
key space, to the experiment log holding the row with that shot's flyer
position.

```
pdv_trace partition ──> same key ──> pdv_experiment_log item(s)
   (the work)                          (the coordinates)
       └─ pair by filename within the partition ──> write Station/Sample X/Y
       └─ ALPSS derived files inherit from the enriched parent trace
```

Two properties are structural, not enforced by a check:

- a trace can only take coordinates from a row describing **its own sample**,
  because matching is scoped to its own partition;
- a log row naming a file that was never ingested is a **non-event**. Girder is
  the only place the data exists.

**Scope rule.** Only what the `/aimdl/*` endpoints return is in scope. Traces
without `meta.igsn` never appear there — shots that never triggered, test shots
(`TEST00_000…`), samples with no registered IGSN. They can never carry
meaningful coordinates. Do **not** count, reconcile, or report them; the
endpoints have already filtered them deliberately.

### The three flows

| flow | job | partitioning | file → coordinate |
|---|---|---|---|
| HELIX traces | `process_helix_assets_job` | dynamic `helix_pdv_trace` | trace → its row in that session's log |
| HELIX ALPSS | `coord_enrichment_helix_alpss_job` | 3 static ALPSS data types | filename stem → parent trace, inherit |
| MAXIMA | `coord_enrichment_maxima_partition_job` | dynamic `maxima_run` | `scan_point_<i>` → `instructions.txt` |

### Pairing (`matching.py`)

Three passes, each running only if the previous found nothing:

1. **filename stem** — normalized with `ntpath.basename`, because some logs
   record the station-local Windows path the file was written to before ingest
   (`C:\Users\Administrator\Desktop\PDV_DATA\…`). Use `ntpath`, not `os.path`:
   we run on POSIX, which does not split on backslashes.
2. **channel prefix** — the same with the item's `C<n>--` digitizer prefix
   stripped.
3. **shot identity** — prefix stripped from *both* sides, for multipoint probes
   the log-writer omits. Tagged `pairing: "shot_identity"` in the provenance so
   those items stay queryable.

Exactly one row → paired. Zero → `no_row_in_log`. More than one →
`ambiguous_row`, and **nothing is chosen**. A row whose IGSN disagrees with the
trace is refused as `igsn_mismatch`.

Multipoint logs have no bare `PDV_FileName`, only `PDV_<n>_FileName` per probe,
several of which can share one channel file; all are read. Where the corrected
flyer position is blank, **nothing is written** (`no_station_coords`) rather
than stamping a null coordinate.

---

## 2. What is enriched

| flow | enriched | of |
|---|--:|--:|
| `pdv_trace` | 5,867 | 7,764 |
| HELIX ALPSS | 51,920 | 67,436 |
| MAXIMA | 98,330 | 99,791 |

Not enriched — every category is an upstream data gap, and all flows are
idempotent so a re-run picks up whatever newly resolves:

| | items | cause |
|---|--:|---|
| traces in partitions with no tagged log | 837 | log never given `meta.data_type` |
| traces whose log row has no corrected flyer position | 786 | `Flyer_X/Y_Position_Corrected` blank |
| traces refused `ambiguous_row` | 57 | two rows claim one trace |
| traces with no row in a tagged log | 254 | **undiagnosed — the one item that is ours** |
| ALPSS children of the above | 15,516 | `parent_not_enriched` |
| MAXIMA in runs without usable instructions | 1,461 | 62 runs, `no_instructions` / `malformed_instructions` |

---

## 3. Next steps

1. **The 254 traces with no row in a tagged log.** The only remaining gap that
   belongs to this repo. Four partitions pair at 0% with naming conventions the
   matcher doesn't resolve — `JHAMAL00016-004//2026-04-23`,
   `JHAMAL00016-005//2026-04-23`, `JHAMAL00016-019//2026-06-29` (uses a
   `Ti_2026-06-29_20-47-59_shot01` shape), `JHAMAC00003-S8R5C3//2026-05-22`.
2. **Draft the upstream issue** for the `PDV_FileName` tagging gate in
   `girder-consumers/helix-otherdata` — the last of four write-ups, and the one
   blocking the 837. Drafts live in `docs/upstream_issues/` (untracked).
3. **Confirm `skipped_no_change` on the next ALPSS run.** Before this sweep the
   ALPSS assets read items via `/aimdl/datafiles`, so stored provenance was
   invisible and every re-run rewrote all 67,436 items. They now fetch per
   partition with full meta, so unchanged items should be skipped — expect a
   large `skipped_no_change`, not a large `written`. If it is still 0, the fix
   regressed.

---

## 4. Running things

```bash
# read-only readiness sweep with a GO/NO-GO verdict
set -a; . ./.env; set +a
.venv/bin/python operations/dry_run_readiness.py --flows helix_traces

# the live pass — reads .env and DAGSTER_HOME itself, no shell preamble.
# Writes ONLY with --live; without it everything is a dry run.
.venv/bin/python operations/run_live_pass.py --all                       # rehearse
.venv/bin/python operations/run_live_pass.py --all --live                # traces
.venv/bin/python operations/run_live_pass.py --month 2026-08 --live      # one month
.venv/bin/python operations/run_live_pass.py --flow alpss --all --live   # after traces
.venv/bin/python operations/run_live_pass.py --flow maxima --all --live  # independent
```

Runs execute against the real Dagster instance, so they appear in the UI, and
each invocation writes `operations/log/live_pass_<timestamp>.json` with
per-partition counts.

**Measured timings** — writes cost roughly **3x** a dry run:

| flow | partitions | dry | live |
|---|--:|--:|--:|
| HELIX traces | 291 | ~10 min | ~15 min |
| HELIX ALPSS | 3 | ~7 min | ~45 min |
| MAXIMA | 1,677 | 31 min | 106 min |

Dagster UI:
```bash
set -a; . ./.env; set +a
export DAGSTER_HOME="$PWD/.dagster_home"
dagster dev          # localhost:3000
```
Procedure for the UI path: `docs/runbooks/live_enrichment_pass.md`.

---

## 5. Gotchas that cost real time

- **`/aimdl/datafiles` returns a REDUCED PROJECTION of `meta`** — only
  `data_type` and `igsn`. Verifying enrichment against it reported **0 enriched
  when 5,867 had been written**. Verify against `/aimdl/partition/details` or a
  direct item GET. Any asset that reads existing metadata to decide whether to
  write must not source items from this endpoint.
- **ALPSS ordering fails silently.** Run it before the traces are enriched and
  every item classifies `parent_not_enriched`, drops out of the denominator, and
  the checks go green having done nothing. Read `in_scope`, not check colours.
- **`.dagster_home` is git-tracked.** `git checkout` / `reset --hard` silently
  rewinds registered partitions and run history.
- **Stage commits with explicit file paths.** Never `git add -A`.
  `Untitled.ipynb`, `docs/Untitled.ipynb`, `docs/upstream_issues/` must stay
  untracked.
- **`dagster dev` does not hot-reload code.** After editing assets, hit Reload
  on the `aimdl_coord_enrichment` code location or the UI serves a stale graph.
- **`JHABOX00000` is a reserved test IGSN** — real IGSN, no sample in the
  machine, nothing fired. Its traces can never pair.
- **`.env`** — the key is `elbert`, `admin: True`, i.e. full write, so a dry run
  has no read-only safety net. `COORD_ENRICHMENT_MANIFEST_ITEM=6a95e8ad5c70f7e46fdcfbcd`.
- **A Bash permission rule** allows `run_live_pass.py` without prompting:
  `Bash(.venv/bin/python operations/run_live_pass.py:*)` in
  `.claude/settings.local.json`. Keep the invocation a single command — a
  compound shell line won't match it and gets routed to the classifier.

---

## 6. Where things live

| | |
|---|---|
| results of the live sweep | `docs/live_enrichment_report_2026-09-03.md` |
| requirements, per capability | `docs/SPEC.md` (C1 = the HELIX flow) |
| live pass procedure | `docs/runbooks/live_enrichment_pass.md` |
| readiness rubric | `docs/runbooks/readiness_dry_run.md` |
| pairing logic | `aimdl_coord_enrichment/matching.py` |
| pure helpers | `aimdl_coord_enrichment/spreadsheet.py` |
| the three HELIX assets | `aimdl_coord_enrichment/assets.py` |
| ALPSS leaf / tagger | `aimdl_coord_enrichment/coord_enrichment/` |
| exclusion vocabulary | `aimdl_coord_enrichment/coord_enrichment/exclusions.py` |
| upstream issue drafts | `docs/upstream_issues/` (untracked) |

Recent history: `git log --oneline -8`.
