# RESUME — current state (2026-09-01)

## In one line

The HELIX flow is **trace-driven**: it iterates PDV traces and finds each one's
log row. Full 290-partition dry sweep is green apart from the upstream log-tagging
gap. Suite 361 passed / 1 skipped. **No coordinate has ever been written to
Girder** — every run so far has been `dry_run=True`.

Uncommitted; nothing staged or pushed.

## 1. How the HELIX flow works

The **trace is the unit of work**. Every PDV trace in Girder carries
`meta.igsn`; `/aimdl/partition?dataType=pdv_trace` supplies its experiment date.
Together they give the AIMD-L key `<igsn>//<experiment_date>`, which resolves in
the *same key space* to the experiment log holding the row with that shot's
flyer position.

```
pdv_trace partition  ──>  same key  ──>  pdv_experiment_log item(s)
   (the work)                              (the coordinates)
        └── pair by filename within the partition ──> write Station/Sample X/Y
```

Two properties are structural, not enforced by a check:

- A trace can only take coordinates from a row describing **its own sample**,
  because matching is scoped to the trace's own partition.
- A log row naming a file that was never ingested is a **non-event**. Girder is
  the only place the data exists.

Traces without `meta.igsn` never appear in the index, so unannotated files are
out of scope by construction.

**Pairing** (`matching.py`): normalize the row's `PDV_FileName` with
`ntpath.basename` — some logs record the station-local Windows path the file
was written to before ingest, and only the trailing component names anything in
Girder. Then find the single row whose stem prefixes the trace name, retrying
with a leading `C<n>--` digitizer prefix stripped if the exact pass finds
nothing. Exactly one row → paired. Zero → `no_row_in_log`. More than one →
`ambiguous_row`, and **nothing is chosen** — two rows claiming one trace means
the partition holds contradictory logs, and guessing would apply the wrong
shot's coordinates. A row whose IGSN disagrees with the trace is refused as
`igsn_mismatch`.

| flow | partitioning | file → coordinate |
|---|---|---|
| `process_helix_assets_job` | dynamic `helix_pdv_trace`, `<igsn>//<experiment_date>` | trace → its row in that session's log |
| `coord_enrichment_helix_alpss_job` | 3 static ALPSS data types | filename stem → parent trace, inherit |
| `coord_enrichment_maxima_job` | dynamic `maxima_run` | `scan_point_<i>` → `instructions.txt` → `scan_points[i]` |

Each enriched item gets `Station_X/Y`, `Sample_X/Y`, `coord_provenance`.
Re-runs rewrite only when a meaningful provenance field changed.

## 2. Measured — full dry sweep, 2026-09-01

290 partitions, 0 job failures, 0 writes.

**Of the traces whose session has a tagged experiment log: 3,695 of 4,006
(92.2%) paired and produced coordinates.** Residual 254 `no_row_in_log`, 57
`ambiguous_row`. Transform versions `HELIX/v1` 965, `HELIX/v2` 2,730. Coord
failures 0.

Every ERROR check passes on all 290 partitions — `zero_traces_in_partition`,
`igsn_consistency`, `manifest_written` — as do `igsn_validity_rate`,
`coord_transform_check`, and `enrichment_success_rate`.

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

1. **Close the residual 311** — 254 traces that find no row in a tagged log,
   57 refused as `ambiguous_row` (contradictory logs; see
   `docs/upstream_issues/helix-03-duplicate-overlapping-experiment-logs.md`).
   This is the remaining gap between 92.2% and complete.
2. **MAXIMA has 0 `maxima_run` partitions registered.** Test-evaluate
   `maxima_run_discovery_sensor` (leave it **STOPPED** — running it submits up
   to 1,664 RunRequests). MAXIMA has never been dry-run at corpus scale.
3. **Draft the upstream issue** for the `PDV_FileName` log-tagging gate.
4. **Then the live sweep** — order **HELIX traces → HELIX ALPSS → MAXIMA**
   (MAXIMA is independent). Start with 1–2 partitions and verify in Girder
   before widening.

## 5. Running it

```bash
set -a; . ./.env; set +a
.venv/bin/python operations/dry_run_readiness.py --flows helix_traces   # ~35 min, 290 partitions
.venv/bin/python operations/dry_run_readiness.py                        # all flows; hours
```
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
