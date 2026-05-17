# Runbook: merge the issue23 work → main

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
- **issue23 vs main:** 103 ahead, **0 behind** → conflict-free merge.

## ⚠️ INCIDENT 2026-05-17 — wrong-base merge (recovered, nothing lost)

The PR for this work was **#24** (NOT "#23" — that is the
issue/branch name; this runbook originally mislabeled it). PR #24's
**base was `refactor/asset-dag`, not `main`**. It was merged
2026-05-17 23:12Z (davidelbert) → merge commit **`d1a505d`** now on
`refactor/asset-dag`. **`main` is untouched (`04075df`); the
`refactor/issue23-dynamic-partitions` branch is intact (`31be701`);
nothing lost.** asset-dag now == issue23 + that one merge commit
(no foreign commits ride along).

**Corrected path (Option 1):** open a NEW PR, head
`refactor/issue23-dynamic-partitions`, **base `main`**, merge with
a merge commit. Steps 1–3 below still hold for the real main merge
(gate green, 0-behind, rollback tag `pre-issue23-merge` @
`04075df`). Step 4 is rewritten accordingly. Hard lesson:
**verify the PR base is `main` before merging.**

---

## Gate is hermetic (self-contained — no env setup needed)

As of 2026-05-17 the suite is self-contained. `tests/conftest.py`
sets `COORD_TRANSFORMS_YAML` via `os.environ.setdefault` — before any
`helix_dagster` import — to the vendored
`tests/fixtures/coord_transforms_fixture.yaml` (named distinctly
because `.gitignore:214` deliberately ignores the real-config
filename `instrument_coordinate_transforms.yaml` tree-wide). So the gate
yields the same result in ANY environment: fresh clone, CI, a shell
that never sourced `.env.local`. A real `.env.local`
`COORD_TRANSFORMS_YAML` still overrides (your workflow unchanged).

- **Target result:** `311 passed, 1 skipped, 0 failed`. The single
  skip is expected/permanent: "MAXIMA v2 not registered in YAML;
  test becomes live when a second version is added."
- The old footgun (unconfigured env → `289 passed, 23 skipped`
  looking "green") is now structurally impossible.
- Sync caveat: the vendored fixture is a copy of the real
  `aimdl_coordinate_systems/instrument_coordinate_transforms.yaml`;
  if the real transform config changes, re-vendor it.

**Verified 2026-05-17 (HEAD `333a5d7` + conftest fix) with
`COORD_TRANSFORMS_YAML` explicitly UNSET and `.env.local` NOT
sourced:** **311 passed, 1 skipped, 0 failed**. ✅ Hermetic gate.

---

## Merge path (you execute the merge/push/tag; I can run gates)

### Step 1 — Fresh gate at the merge point ☑ (311/1/0 @ 31be701)
Re-establish, don't trust stale green (hermetic — no env setup):
```
.venv/bin/python -m pytest tests/ -q          # expect 311 passed, 1 skipped, 0 failed
```

### Step 2 — Confirm no drift ☑ (0 behind main)
```
git fetch origin -q
git rev-list --count origin/refactor/issue23-dynamic-partitions..origin/main   # expect 0
```

### Step 3 — Tag a one-command rollback ☑ (pre-issue23-merge @ 04075df)
The insurance that makes landing a by-volume-unreviewable PR
responsible:
```
git tag pre-issue23-merge origin/main
git push origin pre-issue23-merge
```

### Step 4 — Create recovery PR (base=main) + merge ☐
```
# 4a. Create the PR with an EXPLICIT base; note the number it returns.
gh pr create --base main --head refactor/issue23-dynamic-partitions \
  --title "Merge issue23 work into main" --body-file <msg-file>
# 4b. ASSERT base is main BEFORE merging (the check skipped last time):
gh pr view <NEW#> --json baseRefName -q .baseRefName    # MUST print: main
# 4c. Merge with a MERGE COMMIT (never squash/rebase):
gh pr merge <NEW#> --merge
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
.venv/bin/python -m pytest tests/ -q          # expect 311 passed, 1 skipped, 0 failed (hermetic)
```

### Step 6 — Confirm nothing went operational ☐
- `helix_dagster/schedules.py` schedules still ship STOPPED.
- No sensor auto-enabled. Merge changed code state only.

### Step 7 — Proceed to rename ☐
Branch the rename off fresh `main` per `rename_plan.md` (Phase 1).

---

## Merge commit message (paste into Step 4 `--body` / GitHub UI)

```
Merge issue23 work into main: dynamic partitions for MAXIMA raw + provenance architecture split

Lands the issue23 refactor: dynamic MAXIMA-raw partitions via the
/aimdl/partition endpoint, the provenance architecture split
(helix_alpss / maxima_derived leaves), the leaf-check IOManager-load
fix, and the coord_enrichment dry-run rehearsal tooling.

Basis of acceptance (no CI on this repo; controls are manual):
- Full test suite: 311 passed, 1 skipped, 0 failed. The suite is
  hermetic (tests/conftest.py points the transformer at a vendored
  fixture YAML), so this result is environment-independent and the
  coordinate-transform path is always exercised. The 1 skip is the
  deliberate forward-compat MAXIMA-v2 test.
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
- [x] Suite made hermetic 2026-05-17 (conftest + vendored fixture;
      311/1/0 with env UNSET) — gate no longer env-dependent
- [x] Step 1 fresh gate 2026-05-17 — 311/1/0 at HEAD 31be701
- [x] Step 2 no-drift — 0 behind main, branch==origin (103 ahead)
- [x] Step 3 rollback tag pushed → pre-issue23-merge @ 04075df (origin/main pre-merge)
- [!] INCIDENT 2026-05-17: PR #24 merged to WRONG base
      (refactor/asset-dag, commit d1a505d). main untouched; nothing
      lost. Recovering via Option 1 (new PR base=main).
- [x] Step 4: recovery PR **#26** (base ASSERTED == main) merged
      2026-05-17 → merge commit **c1400c4** on origin/main, real
      2-parent merge commit, curated basis-of-acceptance message in
      the commit body. main now == 31be701 tree.
- [x] Step 5 post-merge gate: 311/1/0 on merged tree (proven
      byte-identical to origin/main via empty git diff)
- [x] Step 6 schedules STOPPED confirmed (4× STOPPED, 0 RUNNING)
- [ ] Step 7 → rename_plan.md (next phase: rename off fresh main)

**RUNBOOK COMPLETE 2026-05-17.** issue23 work is on `main`
(`c1400c4`). Rollback if ever needed: `pre-issue23-merge` @
`04075df`. Next: `rename_plan.md` Phase 1 (branch off fresh main).
