# Repo / package rename — blast-radius map

- **Date:** 2026-05-17
- **Branch:** `refactor/issue23-dynamic-partitions`
- **Status:** **MAP ONLY — no action taken, decision deferred.**
  Nothing renamed, no branch created, no files modified. Repo
  state unchanged (`== origin`).
- **Goal of an eventual rename:** the repo/package currently uses
  `helix` as an umbrella, but the workflow enriches **both** HELIX
  (laser-shock / PDV / ALPSS) **and** MAXIMA (XRD/XRF) measurements
  with sample-frame coordinates + coordinate provenance, written
  back to Girder items keyed by IGSN, orchestrated in Dagster.
- **Recommended names:** repo `aimdl-coord-enrichment`; Python
  package `aimdl_coord_enrichment`; leave the internal
  `coord_enrichment` DAG/jobs as-is (already accurate).

---

## Tier 0 — NOT rename targets (domain-correct "HELIX") ⚠️

`HELIX` as an **instrument/station** name is correct and MUST stay.
~38 references in the package. Any rename must surgically touch only
the package token and leave these alone:

- `HELIX_FOLDER_ID` env var (the HELIX station's Girder folder)
- `INSTRUMENT_HELIX`, `"HELIX/…"` partition keys
- `helix_dagster/instruments/helix.py`
- `helix_dagster/coord_enrichment/helix_alpss_leaf.py`
- README / docs prose describing the HELIX station

## Tier 1 — Repo name only (cheap, GitHub auto-redirects)

- GitHub repo `openmsi/helix_metadata_extraction_dagster` → new name
  (GitHub *Settings → Rename repository*: keeps history, issues,
  PRs, tags, branches, stars; auto-redirects old URLs/remotes)
- `git remote set-url` (optional; old URL keeps working via redirect)
- Prose/string references to the repo name in tracked docs:
  `README.md` (H1 + string), `CLAUDE.md`,
  `.claude/prompts/issue23/*` (9 files),
  `.claude/prompts/phase1/*`, `.claude/issue23_validation_plan.md`,
  runbooks. Mostly historical — cosmetic, batchable.

## Tier 2 — Python package `helix_dagster` → `aimdl_coord_enrichment` (the real work)

This is NOT cosmetic. Same amount of work regardless of repo-hosting
choice.

| Surface | Detail |
|---|---|
| Directory | `helix_dagster/` → `aimdl_coord_enrichment/` (`git mv`, preserves history) |
| Imports | **59 `.py` files** reference `helix_dagster` — mechanical sed, must be exact, full test run after |
| `pyproject.toml` | **3 critical lines**: `[project].name`, `[tool.setuptools.packages.find].include = ["helix_dagster*"]`, and `[tool.dagster].module_name` — the last is **THE Dagster entry point**; `dagster dev` / Definitions load breaks if missed. Also update the `[project].description` prose |
| `tests/test_annotations_rule.py` | Hardcodes a **list of ~19 `helix_dagster/…` path strings**; the convention-enforcement test silently stops enforcing (or errors) if these aren't updated |
| Reinstall | `pip install -e ".[dev]"` re-run; stale `helix_dagster` dist-info / egg-link cleaned from `.venv` |
| `.dagster_home/` (tracked) | Committed SQLite DBs (`history/runs.db`, `schedules/schedules.db`, …) are keyed to the old code-location/module; run history won't map after a `module_name` change. Not a blocker. Also a separate "why are these committed at all" hygiene smell — noted, not in scope |
| Docs | `docs/runbooks/coord_enrichment_production_sweep.md`, `docs/archive/*`, `CLAUDE.md`, `.claude/settings.local.json` reference import paths / `dagster dev` module |

## Tier 3 — NOT affected (verified)

- **No `.github/` CI exists** — the annotations rule is a pytest
  test, not a workflow. Nothing to update there.
- No `workspace.yaml` / `dagster_cloud.yaml` / `setup.py` — the
  Dagster entry point is **only** `pyproject.toml [tool.dagster]`.
- Girder server-side, IGSNs, `aimdl_coordinate_systems` /
  `coordinate-transformer`, and all env-var names: unaffected (no
  env var embeds the package name).

---

## Strategy assessment: "new repo + move branch + archive old"

Reasonable in spirit; pushed back on the mechanism:

- **A new repo is not needed to rename.** GitHub rename-in-place is
  lossless and redirected. New-repo + archive discards the linkage —
  including the **issue23 PR/issue thread** that explains the scope
  drift (the very reason given for renaming).
- **"Move just this branch"** to a fresh repo silently drops `main`,
  other branches, tags, history unless `git push --mirror`. A clean
  break should be deliberate, not a side effect.
- **Shared repo:** `openmsi` org; the prior session pulled
  collaborators' commits (e.g. `299bb0a`). Archiving freezes their
  work — a team decision, not unilateral.
- The **Tier-2 package rename is identical work** either way.

**Recommendation:** GitHub rename-in-place + a separate **atomic
Tier-2 package-rename branch** (git mv → sed imports → pyproject 3
lines → annotations-test paths → reinstall → full `pytest` green →
PR). Reserve "new repo + archive" only as a deliberate scope-reset
that abandons old issue/PR history, and coordinate with `openmsi`
contributors first.

## Decision status / next steps (when revisited)

- Decision: **deferred** ("map only — decide later"), pending name
  choice + collaborator discussion.
- Two independent decisions: (1) repo-hosting (rename-in-place vs
  new-repo) — cheap, lossless if rename-in-place; (2) Tier-2 package
  rename — the real work, do as one atomic green branch.
- Hard guardrail for whoever does it: **do not touch Tier 0**
  instrument-domain `HELIX`/`helix`.
