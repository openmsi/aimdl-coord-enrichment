# Phase 1 — Versioned-transform back-port to the existing DAG

## Why this phase

The existing `process_helix_assets_job` writes coordinate metadata to
HELIX PDV traces but uses whichever HELIX transform is currently marked
as `valid_until: null` in the YAML, regardless of when the shot
happened. The `coordinate-transformer` package (v0.3.0+) supports
timestamp-based version selection; we're not using it.

Phase 1 threads the shot timestamp through the existing DAG so the right
transform version is used for every row, and adds a `coord_provenance`
block to every Girder write so that later, in Phase 3+, the new
coordinate-enrichment DAG's writes are schema-identical.

## Deliverables

- `aimdl_coord_enrichment/coordinates.py` returns `(sample_x, sample_y, transform_name)`
  and accepts an optional timezone-aware timestamp.
- `aimdl_coord_enrichment/provenance.py` (new module) builds `coord_provenance`
  payloads and computes the YAML sha256.
- `aimdl_coord_enrichment/assets.py::enriched_pdv_metadata` parses the spreadsheet
  `Timestamp` column and writes `coord_provenance` alongside
  Station_X/Y and Sample_X/Y.
- `aimdl_coord_enrichment/checks.py::coord_transform_check` verifies that the
  transform version resolved for every row.
- New integration test verifying that two rows with different timestamps
  produce different Sample_X/Y values when the transform version changes
  between them.

## Sequence

Run the steps in order. Each step leaves the repo in a passing state
(`pytest tests/ -v` green) before the next step starts.

1. `step1_coordinates_timestamp.md`
2. `step2_provenance_builder.md`
3. `step3_enriched_pdv_integration.md`
4. `step4_coord_transform_check.md`
5. `step5_version_boundary_integration_test.md`

## How to invoke each step

```bash
cd /Users/elbert/Documents/GitHub/openmsi/aimdl-coord-enrichment
claude --model claude-opus-4-6 --dangerously-skip-permissions \
  < .claude/prompts/phase1/stepN_<name>.md
```

Inspect the resulting diff, run tests, commit, then move to the next step.

## Ground rules applied to every prompt

- Always begin with the audit phase. Do not edit files until the audit
  findings have been reported back to the user.
- Every changed line should trace directly to the step's stated goal.
  Don't touch adjacent code, comments, or formatting.
- If assumptions need to be made (e.g., timezone policy on naive
  timestamps), state them out loud and proceed with the safest default
  — do not silently pick.
- Keep each step's commit focused: one step = one commit.
