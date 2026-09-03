# Live enrichment pass — HELIX

Step-by-step instructions for the first live coordinate write. Every command
is given explicitly. Read §1 before starting.

**This writes to production Girder.** Everything before §5 is read-only.

---

## 1. What this pass will and will not do

Measured on the dry sweep of 2026-09-03, 290 partitions:

| | traces |
|---|--:|
| **will be enriched** | **5,813** |
| paired but the log row has no corrected flyer position | 786 |
| in partitions whose experiment log is not tagged upstream | 996 |
| refused — two log rows claim the same trace | 57 |
| no row in a tagged log | 56 |

The 786 and the 996 are upstream data gaps. They are **not** fixed by running
this pass, and running it does not make them harder to fix later — a re-run
picks them up once the logs are corrected.

Writes are idempotent: an item is rewritten only when a meaningful provenance
field changed, so re-running after an upstream fix is safe and cheap.

**MAXIMA is not part of this pass.** It has never been dry-run at corpus
scale. Do it separately, after its own dry run.

---

## 2. Pre-flight

```bash
cd /Users/elbert/Documents/GitHub/openmsi/helix_metadata_extraction_dagster
git status                      # expect a clean tree
source .venv/bin/activate
pytest tests/ -q                # expect 369 passed, 1 skipped
```

Confirm the environment points where you intend:

```bash
set -a; . ./.env; set +a
echo "$GIRDER_API_URL"                    # https://data.htmdec.org/api/v1
echo "$COORD_ENRICHMENT_MANIFEST_ITEM"    # 6a95e8ad5c70f7e46fdcfbcd
```

The key in `.env` is an admin key with write access. That is required for this
pass — but it also means there is no read-only safety net, so re-read the URL
above before continuing.

---

## 3. Start Dagster

```bash
set -a; . ./.env; set +a
export DAGSTER_HOME="$PWD/.dagster_home"
dagster dev
```

Open http://localhost:3000.

> `.dagster_home` is git-tracked. Do **not** run `git checkout` or
> `git reset --hard` while partitions are registered — it silently rewinds the
> partition registry and run history.

---

## 4. Register the trace partitions

The `helix_pdv_trace` dimension is populated by a sensor. Register the keys
**without** letting the sensor submit runs:

1. Dagster UI → **Automation** → `helix_trace_discovery_sensor`
2. Click **Test Sensor** (single evaluation). **Leave the sensor STOPPED.**
3. Confirm it reports ~290 partitions.

A *running* sensor also submits one RunRequest per partition. Those would
inherit `dry_run: true` and so write nothing, but they would flood the run
list. Keep it stopped.

Verify:

```bash
dagster asset list --select pdv_data
```

or in the UI: **Assets → pdv_data → Partitions**, expect ~290 keys.

---

## 5. Dry-run two partitions first

Still `dry_run: true` — this writes nothing. It confirms the wiring end to end.

UI → **Jobs** → `process_helix_assets_job` → **Launchpad**.

Partition: `JHAMAL00016-048//2026-08-05` (single-channel, expect 25/25).
Run config:

```yaml
ops:
  pdv_data:
    config:
      dry_run: true
  pdv_processing_manifest:
    config:
      dry_run: true
```

Launch. Then repeat for `JHAMAL00021-003//2026-08-18` (multipoint, expect
75 traces / 75 paired / 24 enriched — the rest have no flyer position).

On each run check the `pdv_data` output metadata:

- `items_enriched` = **0**, `items_simulated` > 0 — confirms dry-run held
- `paired_count`, `traces_in_partition`
- `unpaired_by_reason` — should be `none` for these two
- `no_station_coords` — 0 for the first, 51 for the second

**If `items_enriched` is not 0, stop.** Dry-run is not in force; do not proceed.

---

## 6. Live — one partition

Same launchpad, same partition `JHAMAL00016-048//2026-08-05`, but:

```yaml
ops:
  pdv_data:
    config:
      dry_run: false
  pdv_processing_manifest:
    config:
      dry_run: false
```

Before clicking Launch, read the config preview back to yourself: the job
name, the partition, and `dry_run: false` on both ops.

`pdv_log` takes no config — do not add it, Dagster will reject the run.

### Verify in Girder before going further

Expect `items_enriched: 25`, `items_simulated: 0`.

Open any trace from that partition in the Girder UI and confirm its metadata
now carries:

- `Station_X`, `Station_Y` — the corrected flyer position from the log
- `Sample_X`, `Sample_Y` — rounded to 4 dp
- `Flyer_Row`, `Flyer_Column`
- `coord_provenance` with `instrument: HELIX`, a `transform_version`
  (`HELIX/v1` before 2026-04-01, `HELIX/v2` after), a `transform_yaml_sha256`,
  the `dagster_run_id` of this run, and
  `station_coord_source.pairing: "filename"`

**Do not continue until you have looked at a real item and are satisfied.**

---

## 7. Live — the rest of HELIX

Same job and config as §6, selecting all partitions in the launchpad's
partition selector, or in batches if you prefer to watch it.

Expect roughly **5,813 items enriched** across 290 partitions.

### While it runs

Watch the asset-check panel. These must stay green:

| check | severity |
|---|---|
| `zero_traces_in_partition` | ERROR |
| `igsn_consistency` | ERROR |
| `manifest_written` | ERROR |
| `coord_transform_check` | WARN |

These will show expected failures — they are reporting upstream data gaps,
not faults in this run:

- `enrichment_success_rate` — fails on **26 partitions, all 2026-08**, where
  the log rows have no corrected flyer position
- `pdv_match_rate` — fails on **5 partitions** using naming conventions the
  matcher does not resolve (`JHAMAB00019-01//2025-10-23`,
  `JHAMAC00003-S8R5C3//2026-05-22`, `JHAMAL00016-004//2026-04-23`,
  `JHAMAL00016-005//2026-04-23`, `JHAMAL00016-019//2026-06-29`)

Anything failing **outside** those two lists is unexpected — stop and
investigate.

---

## 8. HELIX ALPSS — inherit to the derived files

Only after §7 has completed. ALPSS items take their coordinates from the
parent trace, so the traces must be enriched first.

> Run this too early and it does **not** error: every item is classified
> `parent_not_enriched`, dropped from the denominator, and the checks pass
> while nothing happens. Read `in_scope`, not the check colour.

UI → **Jobs** → `coord_enrichment_helix_alpss_job` → **Launchpad**.

Partitions — run all three (`pdv_alpss_results` is empty and will do nothing):

- `HELIX/pdv_alpss_output`
- `HELIX/pdv_alpss_result`
- `HELIX/pdv_alpss_results`

```yaml
ops:
  helix_alpss_provenance_tagged:
    config:
      dry_run: false
  enriched_helix_alpss:
    config:
      dry_run: false
```

`all_helix_alpss_tagged` (ERROR) must be green. Items whose parent trace got
no coordinate will be excluded as `parent_not_enriched` — expected, and they
resolve when the upstream gaps are fixed and the pass is re-run.

---

## 9. Afterwards

1. Check the manifest item `6a95e8ad5c70f7e46fdcfbcd` in Girder: it should
   carry `meta.coord_enrichment_status` with today's timestamp and
   `dry_run: false`.
2. Record the observed counts in
   `docs/runbooks/first_sweep_expected_values.md` as the new baseline.
3. Leave all sensors and schedules **STOPPED**.

### Re-running after the upstream fixes land

When the log-writer records the corrected flyer positions, and when the
untagged experiment logs are tagged, simply repeat §7 and §8. Items already
correct are skipped; only the newly resolvable ones are written.

To find the items enriched from a sibling channel rather than a row that named
them directly — the ones worth re-verifying against corrected logs — query
Girder for:

```
meta.coord_provenance.station_coord_source.pairing == "shot_identity"
```

## 10. Rollback

The pass writes only metadata; no file content is touched. `coord_provenance`
on each item records the exact transform, source row, and run id, so "what did
this write?" is always answerable. A correction is another pass, not a
restore.
