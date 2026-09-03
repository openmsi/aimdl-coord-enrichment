# RESUME — current state (2026-09-03)

## In one line

**The first live sweep is done.** 57,787 items in production Girder now carry
coordinates — 5,867 `pdv_trace` and 51,920 ALPSS derived files — with zero write
errors. Suite 371 passed / 1 skipped.

## 1. How the HELIX flow works

The **trace is the unit of work**. Every PDV trace in Girder carries
`meta.igsn` and `experiment_date`, giving the AIMD-L key
`<igsn>//<experiment_date>`, which resolves in the *same key space* to the
experiment log holding the row with that shot's flyer position.

```
pdv_trace partition  ──>  same key  ──>  pdv_experiment_log item(s)
   (the work)                              (the coordinates)
        └── pair by filename within the partition ──> write Station/Sample X/Y
        └── ALPSS derived files inherit from the enriched parent trace
```

Two properties are structural, not enforced by a check: a trace can only take
coordinates from a row describing its own sample, because matching is scoped to
its own partition; and a log row naming a file that was never ingested is a
non-event, since Girder is the only place the data exists.

**Pairing** (`matching.py`) runs three passes, each only if the previous found
nothing. (1) filename stem, normalized with `ntpath.basename` because some logs
record the station-local Windows path the file was written to before ingest.
(2) the same with the item's `C<n>--` digitizer prefix stripped. (3) shot
identity — prefix stripped from *both* sides — for multipoint probes the
log-writer omits. Exactly one row → paired; zero → `no_row_in_log`; more than
one → `ambiguous_row`, and nothing is chosen. A row whose IGSN disagrees with
the trace is refused as `igsn_mismatch`.

Multipoint logs carry no bare `PDV_FileName`, only `PDV_<n>_FileName` per probe,
several of which can share one channel file; all are read. Where the corrected
flyer position is blank, nothing is written — `no_station_coords` — rather than
stamping a null coordinate.

| flow | partitioning | file → coordinate |
|---|---|---|
| `process_helix_assets_job` | dynamic `helix_pdv_trace` | trace → its row in that session's log |
| `coord_enrichment_helix_alpss_job` | 3 static ALPSS data types | filename stem → parent trace, inherit |
| `coord_enrichment_maxima_job` | dynamic `maxima_run` | `scan_point_<i>` → `instructions.txt` |

## 2. What is enriched, as of 2026-09-03

| | enriched |
|---|--:|
| `pdv_trace` | 5,867 of 7,764 |
| `pdv_alpss_output` | 46,151 of 59,943 |
| `pdv_alpss_result` | 5,769 of 7,493 |
| `pdv_alpss_results` | 0 (partition is empty) |

Zero write errors. Traces: 5,036 paired by filename, 831 by shot identity;
`HELIX/v1` 965 / `HELIX/v2` 4,902. ALPSS inherits the parent's recorded version
rather than recomputing.

Not enriched, all upstream data gaps, all resolved by a re-run once fixed:

- **837 traces** in partitions with no tagged experiment log
- **786 August traces** whose log row has no corrected flyer position
- **57 traces** refused as `ambiguous_row` (contradictory logs)
- **15,516 ALPSS items** excluded `parent_not_enriched` — children of the above

## 3. Scope — what is deliberately not counted

Only what the `/aimdl/*` endpoints return is in scope. Traces without
`meta.igsn` never appear there: attempted shots that never triggered the laser,
test shots (`TEST00_000…`), and shots on samples with no registered IGSN. They
can never carry meaningful coordinates. **Do not count, reconcile, or report
them** — the endpoints have already filtered them, deliberately.

Verified 2026-09-01: multipoint traces are tagged correctly upstream — all
three channels of a real shot are indexed together. There is no consumer defect
on the trace side.

One upstream dependency does remain, and it is the only one: partitions whose
experiment *log* is untagged have no rows to pair against, because
`helix-otherdata` gates `pdv_experiment_log` tagging on a literal
`PDV_FileName` column that multi-channel logs lack. Those partitions pass with
`log_items: 0`. After the upstream fix, `COLUMN_MAP` / `row_filename_stems`
must read every `PDV_<n>_FileName` and dedupe by filename.

## 4. Next steps

1. **MAXIMA has never been run.** 0 `maxima_run` partitions are registered and
   it has never been dry-run at corpus scale. This is the largest untouched
   piece of work.
2. **Close the residual 311** — 254 traces that find no row in a tagged log,
   57 refused as `ambiguous_row` (contradictory logs; see
   `docs/upstream_issues/helix-03-duplicate-overlapping-experiment-logs.md`).
   This is the remaining gap between 92.2% and complete.
   Register partitions by test-evaluating `maxima_run_discovery_sensor`,
   leaving it **STOPPED** — a running sensor submits up to 1,664 RunRequests.
3. **Draft the upstream issue** for the `PDV_FileName` log-tagging gate.
4. **Re-run HELIX + ALPSS** once the upstream gaps close. Both are idempotent.

## 5. Running it

```bash
# read-only readiness sweep
set -a; . ./.env; set +a
.venv/bin/python operations/dry_run_readiness.py --flows helix_traces   # ~35 min

# the live pass — reads .env and DAGSTER_HOME itself, no preamble.
# Writes only with --live; without it, everything is a dry run.
.venv/bin/python operations/run_live_pass.py --all                      # rehearse
.venv/bin/python operations/run_live_pass.py --month 2026-08 --live
.venv/bin/python operations/run_live_pass.py --all --live
.venv/bin/python operations/run_live_pass.py --flow alpss --all --live  # after traces
```
Runs are recorded in the real Dagster instance and visible in the UI. See
`docs/runbooks/live_enrichment_pass.md`.
Uses an **ephemeral** Dagster instance — does not touch `.dagster_home` or a
running `dagster dev`. Writes `operations/log/readiness_dry_run_<ts>.{md,json}`.
Exit 0 = GO, 1 = NO-GO. `--sample N` bounds only the dynamic flows;
`helix_alpss` is 3 static partitions over 66,848 items and is the long pole.

UI: `docs/runbooks/coord_enrichment_production_sweep.md`.

## 6. Gotchas

- **`.dagster_home` is git-tracked.** `git checkout` / `reset --hard` silently
  rewinds registered partitions and run history. Its stored `pdv_data`
  materializations are from the **old log-driven code** under the retired
  `helix_experiment_log` dim — ignore them; the dim is now `helix_pdv_trace`.
- **Stage commits with explicit file paths.** `docs/Untitled.ipynb`,
  `Untitled.ipynb`, `docs/upstream_issues/*` must stay untracked.
- **`.env` key is `elbert`, `admin: True`** — full write. The readiness runbook
  recommends a read-only key for dry runs; none is wired.
- `COORD_ENRICHMENT_MANIFEST_ITEM=6a95e8ad5c70f7e46fdcfbcd`
  (`coord_enrichment_status.json`, verified reachable, `meta` empty).
- **ALPSS ordering is load-bearing and fails silently.** Run it before the
  traces are enriched and every item classifies `parent_not_enriched`, drops
  from the denominator, and the checks go green having done nothing. Read
  `in_scope`, not colours.

## 7. Upstream issue drafts (untracked, `docs/upstream_issues/`)

- `girder-consumers-01-calibrant-tagging.md`
- `girder-consumers-02-duplicate-suffix-filenames.md`
- `helix-03-duplicate-overlapping-experiment-logs.md`
- **Not yet drafted:** the `PDV_FileName` tagging gate (§3).
