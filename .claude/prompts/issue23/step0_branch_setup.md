# Issue 23, Step 0 — Branch setup and doc cherry-pick

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions` (cut from
`refactor/asset-dag`). The branch already exists and this runbook
has already been committed on it (see
`.claude/prompts/issue23/README.md` — "One-time setup").

Before editing, read:

- `.claude/CLAUDE.md`
- `.claude/prompts/issue23/README.md`
- `.claude/prompts/issue23/ISSUE.md`

## Why this step

One file lives only on `refactor/issue21-step2` but not on our base
`refactor/asset-dag`: `docs/reference/prov_metadata.md`. That branch
is not being merged — it has a bad `(igsn, experiment_date)`
implementation that this refactor replaces with something cleaner.
But the docs file is useful and should move forward.

This step has **zero code changes**. Its only purpose is to bring
that one file forward if it is not already present, and to confirm
the test suite is green so later steps have a known-good baseline.

## Goal

- Ensure `docs/reference/prov_metadata.md` exists on this branch.
- Confirm a green `pytest` baseline.

## Edits

### 1. Check whether the docs file is already present

```bash
test -f docs/reference/prov_metadata.md && echo "present" || echo "missing"
```

### 2. If missing, bring it forward from `refactor/issue21-step2`

**File-level checkout, not cherry-pick of a commit** — we want the
file content, not whatever commit it rode in on:

```bash
git checkout refactor/issue21-step2 -- docs/reference/prov_metadata.md
git add docs/reference/prov_metadata.md
git commit -m "docs: bring forward prov_metadata.md from issue21-step2 (#23)"
```

**If the file was already present, skip this commit entirely.** Step 0
produces zero commits in that case, and that is fine.

### 3. Baseline the test suite

```bash
.venv/bin/pytest
```

All tests must pass. If any fail, STOP and report. Do not proceed
to Step 1 on a red baseline.

## Commit

At most one commit:

```
docs: bring forward prov_metadata.md from issue21-step2 (#23)
```

(Or zero commits, if the file was already present.)

## Success criteria

- Current branch is `refactor/issue23-dynamic-partitions`
- `docs/reference/prov_metadata.md` exists in the tree
- Full test suite passes
- At most one new commit on this branch beyond the runbook setup commit

## Out of scope

- Any Python changes.
- Any edits to `prov_metadata.md` itself. Step 8 updates the doc
  to reflect the new architecture.
- Any other files from `refactor/issue21-step2`.
