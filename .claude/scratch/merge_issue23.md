# Runbook: merge PR #23 (issue23) → main

- **Created:** 2026-05-17
- **Decision:** merge #23 to `main` first, *then* do the package
  rename off fresh main (see `rename_plan.md`).
- **Reality:** solo maintainer (David Elbert, in charge of project),
  agent-assisted. **No CI on this repo** — all controls are local.
- **Why this is defensible:** merging #23 changes *code state only*.
  Schedules ship STOPPED, sensors opt-in, all writes gated by
  `dry_run`. Nothing runs in production on merge. Production
  enablement (the real review gate) stays governed by
  `docs/runbooks/coord_enrichment_production_sweep.md` +
  `first_sweep_expected_values.md`.
- **#23 vs main:** 100 ahead, **0 behind** → conflict-free merge.

---

## ⚠️ Gate precondition (do not skip — it fails silently)

The test gate is **only sound with `COORD_TRANSFORMS_YAML` set**.
Without it, `helix_dagster.coordinates._COORD_TRANSFORMER` is `None`
and ~22 transformer-dependent tests (`*_e2e`, `phase4_e2e`,
`test_leaf_check_partition_isolation`, `test_coordinates`)
**self-skip** — the suite reports `289 passed, 23 skipped` and looks
"green" while the coordinate-transform path (the entire point of
this PR) is **unexercised**. That is NOT a valid merge basis.

- Required env: `COORD_TRANSFORMS_YAML=/home/elbert/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml`
  (external to the repo; the sibling `aimdl_coordinate_systems`
  clone, per README dev layout; convention documented at
  `.claude/issue23_validation_plan.md:194`).
- Sanity check before trusting the gate:
  ```
  .venv/bin/python -c "from helix_dagster.coordinates import _COORD_TRANSFORMER; print(_COORD_TRANSFORMER is None)"
  # MUST print: False
  ```
- **Target result:** `311 passed, 1 skipped, 0 failed`. The single
  skip is expected/permanent: "MAXIMA v2 not registered in YAML;
  test becomes live when a second version is added."

**Last run (2026-05-17, HEAD `333a5d7`, branch
`refactor/issue23-dynamic-partitions`, Python 3.12.8/.venv):**
`_COORD_TRANSFORMER is None? False` → **311 passed, 1 skipped,
0 failed**. ✅ Sound gate established.

---

## Merge path (you execute the merge/push/tag; I can run gates)

### Step 1 — Fresh gate at the merge point ☐
Re-establish, don't trust stale green:
```
export COORD_TRANSFORMS_YAML=/home/elbert/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml
.venv/bin/python -c "from helix_dagster.coordinates import _COORD_TRANSFORMER; print(_COORD_TRANSFORMER is None)"  # False
.venv/bin/python -m pytest tests/ -q          # expect 311 passed, 1 skipped, 0 failed
```

### Step 2 — Confirm no drift ☐
```
git fetch origin -q
git rev-list --count origin/refactor/issue23-dynamic-partitions..origin/main   # expect 0
```

### Step 3 — Tag a one-command rollback ☐
The insurance that makes landing a by-volume-unreviewable PR
responsible:
```
git tag pre-issue23-merge origin/main
git push origin pre-issue23-merge
```

### Step 4 — Merge with a MERGE COMMIT (not squash, not rebase) ☐
```
gh pr merge 23 --merge --subject "Merge PR #23: dynamic partitions for MAXIMA raw + provenance split" --body "<message below>"
```
**Why `--merge` only:** project memory and committed scratch docs
reference specific SHAs (`6ad1aa9`, `651d47a`, `b276835`,
`3915fc8`, `333a5d7`). `--squash`/`--rebase` rewrite every SHA and
invalidate those references; `--merge` keeps SHAs stable and gives
one explicit integration commit to revert to. It also preserves
history for the later rename's git-rename detection.

### Step 5 — Post-merge proof on main ☐
```
git checkout main && git pull
.venv/bin/pip install -e ".[dev]"
export COORD_TRANSFORMS_YAML=/home/elbert/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml
.venv/bin/python -m pytest tests/ -q          # expect 311 passed, 1 skipped, 0 failed
```

### Step 6 — Confirm nothing went operational ☐
- `helix_dagster/schedules.py` schedules still ship STOPPED.
- No sensor auto-enabled. Merge changed code state only.

### Step 7 — Proceed to rename ☐
Branch the rename off fresh `main` per `rename_plan.md` (Phase 1).

---

## Merge commit message (paste into Step 4 `--body` / GitHub UI)

```
Merge PR #23: Dynamic partitions for MAXIMA raw + provenance architecture split

Lands the issue23 refactor: dynamic MAXIMA-raw partitions via the
/aimdl/partition endpoint, the provenance architecture split
(helix_alpss / maxima_derived leaves), the leaf-check IOManager-load
fix, and the coord_enrichment dry-run rehearsal tooling.

Basis of acceptance (no CI on this repo; controls are manual):
- Full test suite: 311 passed, 1 skipped, 0 failed, run with
  COORD_TRANSFORMS_YAML configured so the coordinate-transform path
  is actually exercised (without it the suite silently skips ~22
  transformer tests and under-reports as 289/23 — verified and
  guarded against). The 1 skip is the deliberate forward-compat
  MAXIMA-v2 test.
- Live, READ-ONLY dry-run rehearsals against production (validation
  plan section 3): zero Girder writes performed at any point.
- Leaf-check IOManager-load fix validated live on maxima_raw and
  maxima_derived; Defect-4 verdict verified live.

Explicitly NOT done, by deliberate decision:
- Line-by-line human review: ~100 commits, not feasible or
  meaningful at this volume; the green suite + dry-run rehearsals
  are the substitute control.
- No production WRITE path has been exercised (all dry-run).

Compensating operational controls: schedules ship STOPPED, sensors
opt-in, all writes gated by dry_run. This merge changes code state
only; production enablement remains gated by
docs/runbooks/coord_enrichment_production_sweep.md and
first_sweep_expected_values.md, which is the real review gate.

Rollback: tag pre-issue23-merge marks main immediately prior.
Merged by the project maintainer (solo, agent-assisted) on the
above basis.
```

---

## Resume protocol (fresh session)

1. Read this file + the checkboxes above.
2. `git branch --show-current`, `git log --oneline -5`,
   `gh pr view 23 --json state -q .state` (OPEN vs MERGED).
3. If #23 still OPEN → resume at first unchecked step.
4. If MERGED → go to `rename_plan.md` Phase 1.

## Progress log

- [x] Sound gate established 2026-05-17 (311/1/0, HEAD 333a5d7)
- [ ] Step 1 fresh gate at merge point
- [ ] Step 2 no-drift confirmed
- [ ] Step 3 rollback tag pushed
- [ ] Step 4 merged via --merge — merge commit sha: ____
- [ ] Step 5 post-merge gate on main green
- [ ] Step 6 schedules STOPPED confirmed
- [ ] Step 7 → rename_plan.md
