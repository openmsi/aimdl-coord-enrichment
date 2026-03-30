# Refactor Plan: Asset-Based Dagster Pipeline

## Overview

Four sequential commits on a single branch (`refactor/asset-dag`) converting
the monolithic single-op pipeline into a six-asset Dagster DAG. The branch
stays as a **draft PR** until discussed with the team.

## Workflow

```bash
# Create the branch
cd /Users/elbert/Documents/GitHub/openmsi/helix_metadata_extraction_dagster
git checkout -b refactor/asset-dag

# For each issue, run Claude Code with the prompt file:
claude --model claude-opus-4-6
# Then: "Read .claude/01_cleanup_and_bugs.md and execute all tasks."
# Verify: pytest tests/ -v && python -c "from helix_dagster import defs"
# Commit: git add -A && git commit -m "..."

# Repeat for 02, 03, 04

# Push as draft PR
git push -u origin refactor/asset-dag
# Open PR on GitHub as DRAFT — do not merge until team discussion
```

## Commit Sequence

| Commit | Prompt File | Message | What Changes |
|--------|------------|---------|--------------|
| 1 | `.claude/01_cleanup_and_bugs.md` | `chore: remove dead code, fix hardcoded path and column map inconsistency` | Delete `_build_entry()`, `FORM_ID`, `FORM_SCHEMA_FIELDS`; env-var for YAML path; fix IGSN column case; add `tests/` |
| 2 | `.claude/02_extract_pure_functions.md` | `refactor: extract IGSN validation, PDV matching, and coordinate transform into pure functions with tests` | New `validation.py`, `matching.py`, `coordinates.py`; refactor `process_row()`; unit tests |
| 3 | `.claude/03_convert_to_assets.md` | `feat: convert pipeline to six-asset Dagster DAG` | New `assets.py`; asset-based job; updated sensor; integration tests; legacy job kept but deprecated |
| 4 | `.claude/04_cleanup_and_docs.md` | `chore: remove legacy code, add README and documentation` | Delete old `jobs.py`; proper README with DAG diagram; finalize |

## Verification after each commit

After every commit, confirm nothing is broken:
```bash
pytest tests/ -v
python -c "from helix_dagster import defs; print(defs)"
dagster dev  # check the UI renders, then Ctrl-C
```

## Notes for team discussion

When presenting this to the team, the key points are:

1. **Nothing is deleted until commit 4** — the legacy job exists alongside the
   new assets through commits 1–3. If the asset pipeline has problems, the old
   path is still available.

2. **Commits 1 and 2 are pure cleanup** — no Dagster structural changes, no
   behavior changes. These are uncontroversial and could be merged independently
   if the team prefers to take the asset conversion more slowly.

3. **The asset DAG is better for the team, not just for the presentation** —
   the PDV inventory fetch is no longer repeated per spreadsheet, partial
   failures don't lose completed work, each step is independently testable,
   and the Dagster UI shows what's actually happening.

4. **Ali's coordinate integration work is preserved and improved** — the
   transform is now a cleanly isolated function in `coordinates.py` with its
   own error handling, rather than being inline in the monolithic `process_row()`.

If the team wants to split this differently (e.g., merge commits 1–2 immediately
and defer 3–4), the commit boundaries are designed to support that. Each commit
leaves the codebase in a working state.
