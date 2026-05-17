# HELIX/ALPSS coverage probe — results

- **Date:** 2026-05-17
- **Branch:** `refactor/issue23-dynamic-partitions`
- **Probe:** `.claude/scratch/probe_helix_alpss_coverage.py` (read-only, GET only, 0 writes)
- **Target:** live prod (`data.htmdec.org`)
- **Reproduce:**
  
  ```bash
  set -a; . ./.env.local; set +a
  .venv/bin/python .claude/scratch/probe_helix_alpss_coverage.py
  ```

The probe mirrors the exact production discovery path of the HELIX
coord-enrichment leaf: `fetch_all_aimdl_datafiles(dt)` →
`_is_in_scope` (`meta.igsn`) gate → `alpss_shot_stem()` +
`"<stem>.csv"` parent lookup against the `pdv_trace` pool (i.e.
`find_parent_pdv_item_id`).

## Gap A (igsn / endpoint discovery) — healthy, NOT the problem

| data_type         | endpoint returned | has igsn | no igsn |
| ----------------- | -----------------:| --------:| -------:|
| pdv_trace         | 2,706             | 2,706    | 0       |
| pdv_alpss_output  | 19,423            | 19,423   | 0       |
| pdv_alpss_result  | 885               | 885      | 0       |
| pdv_alpss_results | 1,526             | 1,526    | 0       |

Every item the endpoint returns is `meta.igsn` + `meta.data_type`
tagged. All four types are present in `/aimdl/datatype`. Discovery is
**not** gated by a tagging gap on the HELIX/ALPSS side.

> Caveat: the `/aimdl/datafiles` endpoint filters `meta.igsn`
> server-side, so a population with *no* igsn would be invisible
> here. But the counts are not suspiciously low, so there is no
> evidence of such a population.

## Gap B (filename-convention parent-link) — raw numbers

| data_type         | in-scope | parent_resolvable | stem_fail | orphan |
| ----------------- | --------:| -----------------:| ---------:| ------:|
| pdv_alpss_output  | 19,423   | 10,005 (52%)      | 8,211     | 1,207  |
| pdv_alpss_result  | 885      | 775 (88%)         | 1         | 109    |
| pdv_alpss_results | 1,526    | 473 (31%)         | 1,017     | 36     |

> **CORRECTION (maintainer, 2026-05-17):** files named `C1--…`
> (e.g. `C1--20251022--00001-results.csv`) are **PDV data files**,
> **not** a source of coordinates or other inputs for this DAG.
> They dominate the `stem_fail` column (~9,200 items: 8,211 of
> `pdv_alpss_output`, 1,017 of `pdv_alpss_results`).

This **reverses** the earlier "extend the regex" reading:

1. **`stem_fail` is mostly CORRECT behavior.** `alpss_shot_stem`
   (`helix_dagster/instruments/helix.py:38`) returning `None` for
   the `C1--…` family is *right* — those items are not coordinate-
   bearing and must not be enriched. The regex does **not** need
   extending.

2. **`orphan`** — stem parses as a real `…_ch<N>` shot but no
   `<stem>.csv` in the `pdv_trace` pool (1,207 / 109 / 36). Several
   have a leading underscore (missing IGSN prefix), so the
   synthesized `<stem>.csv` cannot match. Smaller, mixed with
   test/junk (`test--JHAMAC…`); the genuine residual still needs
   isolating.

## The real issue: a scope/tagging defect that ERRORs every sweep

The `C1--…` PDV files are still tagged
`meta.data_type = pdv_alpss_output` / `pdv_alpss_results` in Girder
(that is the query they were returned from). Consequence, verified
in code:

1. `_is_in_scope` (`coord_enrichment/inventory.py:92`) admits any
   item with `meta.igsn` + an in-scope `data_type`. `C1--…` have
   both → **admitted**.
2. `helix_alpss_provenance_tagged`
   (`coord_enrichment/provenance_tagging.py:141-165`) iterates every
   in-scope item; `resolve_parent_item_id` → `None` for `C1--…` →
   `_decide` marks them **unresolved**.
3. `all_helix_alpss_tagged`
   (`coord_enrichment/provenance_tagging.py:188-209`) is
   **`AssetCheckSeverity.ERROR`** and fails if *any* `HELIX/` item
   is unresolved. ~9,000+ `C1--…` → this check **ERRORs on every
   HELIX/ALPSS sweep**.

So mis-scoped, correctly-non-coordinate PDV files deterministically
turn the HELIX/ALPSS sweep **red** — not an enrichment failure, a
scoping failure. The memory note (`pull-push-resume.md`) attributing
§3.5/§3.6 to "PDV not enriched yet (bootstrap step 1)" is **wrong
for this portion**: the PDV-enrichment bootstrap will not fix it.

## Fix options (maintainer decision — not implemented)

- **(A) Upstream/Girder:** `C1--…` PDV files should not carry
  `data_type=pdv_alpss_*`; retag at the source.
- **(B) This repo:** add a name-shape exclusion so PDV-named files
  are dropped from HELIX/ALPSS scope even if mis-tagged (scope by
  `data_type` *and* name shape).
- **(C) This repo:** make `all_helix_alpss_tagged` distinguish
  "genuine ALPSS item, parent missing" (ERROR) from "not an
  ALPSS-shaped name at all" (ignore).

## What was / was not done

- Wrote and ran the read-only probe; mirrors prod exactly by reusing
  the real production helpers (`fetch_all_aimdl_datafiles`,
  `alpss_shot_stem`).
- **No** pipeline code touched. Fix direction (A/B/C) is a
  maintainer decision pending domain/data-hygiene context.

## Open follow-ups (not yet done)

- (a) Re-run the probe partitioning out the `C1--…` PDV family and
  test/junk names, to quantify the *genuine* residual: real
  `…_ch<N>` ALPSS items whose parent PDV trace is truly missing.
- (b) Record the scope/tagging-defect finding in memory so the
  bootstrap plan reflects it (it is independent of, and unfixed by,
  PDV-enrichment).
