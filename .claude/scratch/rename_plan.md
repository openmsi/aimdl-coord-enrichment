# Rename plan: `helix_dagster` → `aimdl_coord_enrichment` (rename-in-place)

- **Created:** 2026-05-17
- **Decision:** rename to `aimdl-coord-enrichment` (repo) /
  `aimdl_coord_enrichment` (Python package). **Do NOT move/new-repo.**
  GitHub rename-in-place (lossless, redirected).
- **Working branch base:** `refactor/issue23-dynamic-partitions`
- **Rename branch:** `refactor/rename-aimdl-coord-enrichment`
- **Internal DAG name `coord_enrichment` stays** (already accurate).

## Safety invariants (read every resume)

1. **Tier-0 guardrail.** The literal token `helix_dagster` never
   occurs inside any instrument-domain name (`HELIX_FOLDER_ID`,
   `INSTRUMENT_HELIX`, `"HELIX/"`, `instruments/helix.py`,
   `helix_alpss_leaf.py`). Therefore an **exact-token** replace of
   `helix_dagster` is provably surgical and must NOT be loosened to
   match `helix`/`HELIX`.
2. Two distinct tokens, two separate phases:
   - Phase 2: package token `helix_dagster` → `aimdl_coord_enrichment`
   - Phase 4: repo-name string `helix_metadata_extraction_dagster`
     → `aimdl-coord-enrichment` (prose/docs only)
3. Every phase ends GREEN with a git commit = durable checkpoint.
4. Each phase is bounded (one scripted sweep, not many edits) so a
   session can `/clear` or exit between phases.

## Resume protocol (fresh session)

1. Read this file's checkboxes below.
2. `git branch --show-current` (expect `refactor/rename-aimdl-coord-enrichment`
   once Phase 1 done) and `git log --oneline -8`.
3. `git status --porcelain` (expect clean between phases).
4. Resume at the first unchecked phase.

---

## Phase 0 — Pre-flight (tiny, no mutation) ☐

**Goal:** confirm starting state.
```
git status --porcelain          # expect clean
git branch --show-current       # expect refactor/issue23-dynamic-partitions
git rev-parse --short HEAD      # record baseline in chat
git grep -c "helix_dagster" -- '*.py' | wc -l   # ~ baseline count
```
**Checkpoint:** none (read-only).
**Safe to /clear after this:** yes.

## Phase 1 — Create rename branch ☐

**Goal:** isolate the rename on its own branch.
```
git checkout -b refactor/rename-aimdl-coord-enrichment
```
**Verify:** `git branch --show-current` → the new branch.
**Checkpoint:** branch creation (no commit yet).
**Safe to /clear after this:** yes.
**Caveat to surface to user:** rename PRs conflict-heavily; land it
when the tree is quiet and rebase other in-flight branches onto it.

## Phase 2 — Package rename: dir move + token sweep (the core) ☐

**Goal:** `helix_dagster/` → `aimdl_coord_enrichment/`, all tracked
imports + pyproject + annotations-test paths updated, in ONE commit.

```
# 2a. Move the package directory (history-preserving)
git mv helix_dagster aimdl_coord_enrichment

# 2b. Exact-token sweep across ALL tracked files EXCEPT the scratch
#     dir and binary dagster_home. (-I skips binary; pathspec excludes)
git grep -lI --cached -e 'helix_dagster' \
  -- ':!.claude/scratch/' ':!.dagster_home/' \
  | xargs sed -i 's/helix_dagster/aimdl_coord_enrichment/g'

# 2c. pyproject description prose (not caught by token sweep wording)
#     Manually verify [project].description reads correctly.
```
**Verify (all must pass before commit):**
```
git grep -nI 'helix_dagster' -- ':!.claude/scratch/' ':!.dagster_home/'   # expect EMPTY
grep -n 'aimdl_coord_enrichment' pyproject.toml   # 3 lines: name, packages.find, module_name
git grep -c 'INSTRUMENT_HELIX\|HELIX_FOLDER_ID\|"HELIX/' -- '*.py' | head  # Tier-0 unchanged
ls aimdl_coord_enrichment/__init__.py             # dir moved
.venv/bin/python -m py_compile $(git ls-files 'aimdl_coord_enrichment/*.py')  # syntax OK
```
**Checkpoint:**
```
git add -A
git commit -m "refactor!: rename package helix_dagster -> aimdl_coord_enrichment

Exact-token rename of the Python package only. Tier-0 instrument-
domain HELIX names untouched. pyproject [project].name,
packages.find, and [tool.dagster].module_name updated.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```
**Safe to /clear after this:** yes (committed; Phase 3 re-orients
from git + this file).

## Phase 3 — Reinstall + full verification ☐

**Goal:** prove the renamed package imports, Dagster loads, tests pass.
```
.venv/bin/pip uninstall -y helix_dagster aimdl_coord_enrichment 2>/dev/null
rm -rf *.egg-info aimdl_coord_enrichment.egg-info
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -c "import aimdl_coord_enrichment; from aimdl_coord_enrichment import defs; print('import+defs OK')"
.venv/bin/python -m pytest tests/ -q
```
**Verify:** import line prints OK; pytest green (expect the same
pass/skip counts as pre-rename, ~311 passed / 1 skipped).
**If failures:** fix, then `git commit -m "fix: post-rename fixups"`.
**Checkpoint:** commit only if fixups were needed.
**Safe to /clear after this:** yes.
**Note:** `dagster dev` will show a fresh/empty code-location
history (module_name changed) — expected, not an error.

## Phase 4 — Repo-name string sweep (docs/prose, Tier 1) ☐

**Goal:** update the human-facing repo name; cosmetic, low-risk.
```
git grep -lI -e 'helix_metadata_extraction_dagster' \
  -- ':!.claude/scratch/' \
  | xargs sed -i 's/helix_metadata_extraction_dagster/aimdl-coord-enrichment/g'
# Then eyeball README.md H1 + opening paragraph for natural wording.
```
**Verify:** `git grep -nI 'helix_metadata_extraction_dagster' -- ':!.claude/scratch/'`
→ empty; README reads correctly.
**Checkpoint:**
```
git commit -am "docs: rename repo references to aimdl-coord-enrichment

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```
**Safe to /clear after this:** yes.

## Phase 5 — Push + GitHub rename (user-driven) ☐

**Goal:** publish and rename the remote.
```
git push -u origin refactor/rename-aimdl-coord-enrichment
```
Then **user action** (cannot be automated):
- GitHub → repo Settings → Rename repository →
  `aimdl-coord-enrichment`.
- Optional (old URL keeps working via redirect):
  `git remote set-url origin https://github.com/openmsi/aimdl-coord-enrichment`
- Open PR for `refactor/rename-aimdl-coord-enrichment`; coordinate
  merge order with other `openmsi` contributors.
**Checkpoint:** branch pushed.
**Safe to /clear after this:** yes.

## Phase 6 — Wrap ☐

- Update memory (`pull-push-resume` / a new note): package renamed,
  module entry point now `aimdl_coord_enrichment`, `dagster dev`
  command unchanged but loads new module.
- Optionally update untracked scratch probes' imports
  (`.claude/scratch/*.py` use `from helix_dagster...`) so they keep
  working — opt-in, not committed.
- **Out of scope (flag only, do NOT do here):** the tracked
  `.dagster_home/*.db` SQLite files probably shouldn't be committed
  at all — separate hygiene PR.

---

## Progress log (update after each phase)

- [ ] Phase 0 pre-flight  — baseline HEAD: ____
- [ ] Phase 1 branch created
- [ ] Phase 2 package rename committed — sha: ____
- [ ] Phase 3 reinstall + pytest green (counts: ____)
- [ ] Phase 4 docs string sweep committed — sha: ____
- [ ] Phase 5 pushed + GitHub renamed
- [ ] Phase 6 memory updated
