# RESUME — current frontier (2026-05-17, post-merge)

**Read this first.** Auto-memory (`MEMORY.md`) routes here. This is
the chained index; per-step detail is in the linked runbooks.

## State in one line
issue23 work is **MERGED to `main`** (merge commit `c1400c4`).
Active step is now the **package rename**. Suite **hermetic
(311/1/0, env-independent)**. Girder (`data.htmdec.org`) **UP**.

## The chain

### 1. Merge issue23 → main  ✅ DONE 2026-05-17
- Runbook `.claude/scratch/merge_issue23.md` — COMPLETE.
- **Incident (recovered):** the original PR **#24** had base
  `refactor/asset-dag` (WRONG) and was merged there → commit
  `d1a505d` on `refactor/asset-dag`. `main` was untouched; nothing
  lost. Recovered via a NEW PR **#26** (base **asserted == main**)
  → real 2-parent merge commit **`c1400c4`** on `origin/main`,
  curated basis-of-acceptance message in the commit body.
- Post-merge gate 311/1/0; schedules STOPPED. Rollback if ever
  needed: `git reset --hard pre-issue23-merge` (tag @ `04075df`).
- Do NOT be confused by PR #24 / `refactor/asset-dag` — it is the
  superseded wrong-base merge. The canonical merge is `c1400c4`.

### 2. Rename pkg `helix_dagster` → `aimdl_coord_enrichment`  ▶ ACTIVE
- Runbook: **`.claude/scratch/rename_plan.md`** (6 phases, each a
  git checkpoint + safe-to-/clear). Map:
  **`.claude/scratch/rename_blast_radius.md`**.
- **Phase 1: branch off FRESH main** (not the issue23 branch):
  `git fetch origin && git checkout -b refactor/rename-aimdl-coord-enrichment origin/main`.
- Rename-IN-PLACE (GitHub repo rename later; NOT a new repo).
- Guardrail: exact-token `helix_dagster` only — never touch
  Tier-0 instrument-domain `HELIX`/`helix`.

### 3. Production bootstrap (the real scientific goal)
- Detail: memory **`bootstrap-step1-blocked.md`** (its "Girder
  down" status is STALE — server is up).
- Remaining blocker: prod HELIX spreadsheet folder.
  `HELIX_FOLDER_ID=69e3815e…` resolves to a TEST fixture
  (`coordinate_dag_test_data`, 0 spreadsheets). Need the real prod
  folder from the user, or confirm scope.
- `process_helix_assets_job` has **NO dry-run** — safe preview =
  partial-materialize the read-only chain, stop before
  `enriched_pdv_metadata`. Probe: `probe_step1_blast_radius.py`.

## Known downstream caveat (NOT a blocker for step 2)
HELIX/ALPSS coord sweep will **ERROR every run**: ~9,000 `C1--…`
PDV files mis-tagged `data_type=pdv_alpss_*` trip the
ERROR-severity `all_helix_alpss_tagged` check. Independent of the
bootstrap. Detail + fix options (A/B/C): committed
**`.claude/scratch/probe_helix_alpss.md`**.

## Git / env
- `main` = `c1400c4` (issue23 work merged). Branch
  `refactor/issue23-dynamic-partitions` == origin @ `31be701`
  (its content is now also on main via the merge).
- For step 2: branch the rename off **fresh `origin/main`**.
- `.env.local` untracked + gitignored, rotated key. Suite is
  hermetic (no env needed) via `tests/fixtures/coord_transforms_fixture.yaml`.
- Run env: `.venv` (Python 3.12, NOT miniconda),
  `DAGSTER_HOME=/home/elbert/.dagster_home_helix`.
- Ignore junk: `.claude/scratch/out` (5 MB stale).

## First action for the next session
Read this → open `.claude/scratch/rename_plan.md` → Phase 0
(pre-flight) → Phase 1 (branch off fresh `origin/main`).
