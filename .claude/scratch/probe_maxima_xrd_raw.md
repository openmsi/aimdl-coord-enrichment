# MAXIMA xrd_raw live verification — results

- **Date:** 2026-05-17
- **Branch:** `refactor/issue23-dynamic-partitions`
- **Probe:** `.claude/scratch/verify_xrd_raw_live.py` (read-only, GET only, 0 writes)
- **Target:** live prod (`data.htmdec.org`)
- **Reproduce:**

  ```bash
  set -a; . ./.env.local; set +a
  .venv/bin/python .claude/scratch/verify_xrd_raw_live.py
  ```

Mirrors the exact production path of `enriched_maxima_raw`:
`fetch_partition_index(dt)` (true collection-wide run-key count) →
`fetch_partition_details(dt, key)` (raw items for a run) →
`fetch_partition_details(xrd_metadata, key)` + `instructions.txt`
filter (exactly what `_fetch_instructions_for_run` does). Target run
key: `JHBMAI00003-S1R6C0//2026-05-08T17:28:17+00:00` (the Girder
screenshot item `69fe249ef35ea45f3d90577d`).

## 1. Collection-wide `meta.data_type` values (`/aimdl/datatype`)

`None`, `pdv_alpss_output`, `pdv_alpss_result`, `pdv_alpss_results`,
`pdv_experiment_log`, `pdv_trace`, `xrd_calibrant_derived`,
`xrd_calibrant_raw`, `xrd_derived`, `xrd_metadata`, `xrd_raw`,
`xrf_raw`.

## 2. Partition index sizes (true # of run keys per data_type)

| data_type | run keys | target key present |
|---|--:|:--:|
| xrd_raw | 655 | ✅ |
| xrf_raw | 206 | ✅ |
| xrd_metadata | 658 | ✅ |
| xrd_derived | 207 | ✅ |

## 3. `partition/details` for the target run key

`key = JHBMAI00003-S1R6C0//2026-05-08T17:28:17+00:00`

| data_type | item count | notes |
|---|--:|---|
| xrd_raw | 24 | screenshot item `69fe249ef35ea45f3d90577d` returned ✅; names `scan_point_<i>_{master,data_*}.h5` |
| xrf_raw | 12 | names `scan_point_<i>.xrf` |
| xrd_metadata | 1 | `instructions.txt` present → `_fetch_instructions_for_run` RESOLVES |

## 4. Interpretation

The issue23 `/aimdl/partition` design works end-to-end on real
production data: the partition index returns a finite, sensible set
of run keys per data_type, the target run key resolves in every
relevant data_type, and the raw items + `instructions.txt` for that
run are all retrievable along the exact path `enriched_maxima_raw`
uses. This is the live confirmation of the Defect-4 verdict
(`refactor/issue21-step2` is superseded — do **not** merge it).

## 5. Verbatim console output (for audit)

```
== available meta.data_type values (collection-wide) ==
  [None, 'pdv_alpss_output', 'pdv_alpss_result', 'pdv_alpss_results', 'pdv_experiment_log', 'pdv_trace', 'xrd_calibrant_derived', 'xrd_calibrant_raw', 'xrd_derived', 'xrd_metadata', 'xrd_raw', 'xrf_raw']

== partition index sizes (true # of run keys per data_type) ==
  xrd_raw        keys=  655  target_key_present=True  sample=['APLMAD00001//2026-04-27T19:33:29+00:00', 'APLMAJ00005-01//2025-10-23T15:47:38+00:00', 'APLMAJ00005-01//2025-10-23T16:45:46+00:00']
  xrf_raw        keys=  206  target_key_present=True  sample=['APLMAD00001//2026-04-27T19:33:29+00:00', 'JHACRD00011//2026-04-17T14:13:45+00:00', 'JHACRD00011//2026-04-17T14:19:01+00:00']
  xrd_metadata   keys=  658  target_key_present=True  sample=['APLMAD00001//2026-04-27T19:33:29+00:00', 'APLMAJ00005-01//2025-10-23T15:47:38+00:00', 'APLMAJ00005-01//2025-10-23T16:45:46+00:00']
  xrd_derived    keys=  207  target_key_present=True  sample=['APLMAD00001//2026-04-27T19:33:29+00:00', 'JHACRD00011//2026-04-17T14:13:45+00:00', 'JHACRD00011//2026-04-17T14:19:01+00:00']

== partition/details for the screenshot run key ==
  key = JHBMAI00003-S1R6C0//2026-05-08T17:28:17+00:00
  xrd_raw        count=24
    screenshot item 69fe249ef35ea45f3d90577d returned? True
    sample names: ['scan_point_0_data_000001.h5', 'scan_point_0_master.h5', 'scan_point_10_data_000001.h5', 'scan_point_10_master.h5', 'scan_point_11_data_000001.h5', 'scan_point_11_master.h5', 'scan_point_1_data_000001.h5', 'scan_point_1_master.h5']
  xrf_raw        count=12
    sample names: ['scan_point_0.xrf', 'scan_point_1.xrf', 'scan_point_10.xrf', 'scan_point_11.xrf', 'scan_point_2.xrf', 'scan_point_3.xrf', 'scan_point_4.xrf', 'scan_point_5.xrf']
  xrd_metadata   count=1
    instructions.txt present? True (_fetch_instructions_for_run would RESOLVE)
    sample names: ['instructions.txt']
```
