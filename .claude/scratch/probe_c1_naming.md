# C-named ("C1--…") HELIX family — probe results

- **Date:** 2026-08-30
- **Branch:** `main` @ `c9dc02b`
- **Probe:** `.claude/scratch/probe_c1_naming.py` (+ ad-hoc follow-ups). GET only, 0 writes.
- **Target:** live prod (`data.htmdec.org`)
- **Supersedes:** `.claude/scratch/probe_helix_alpss.md` (2026-05-17) and the
  "mis-tagging" reading in `RESUME.md`. **Both conclusions were wrong.**

## Hypothesis tested

> "All the C1 files are from before we implemented IGSN sample naming, so it
> may be reasonable to set them aside."

**Falsified.** C-naming is not a legacy cohort. It is the *current and dominant*
convention, and it is growing.

## Timeline — `pdv_trace` by embedded filename date

| month | C-named | IGSN-named |
|---|---:|---:|
| 2025-08 | 169 | 0 |
| 2025-10 | 972 | 0 |
| 2025-11 | 0 | 14 |
| 2025-12 | 0 | 356 |
| 2026-01 | 0 | 37 |
| 2026-02 | 0 | 226 |
| 2026-03 | 0 | 49 |
| 2026-04 | 0 | 875 |
| 2026-05 | 0 | 238 |
| 2026-06 | 335 | 540 |
| 2026-07 | 939 | 0 |
| 2026-08 | 2,958 | 0 |

The convention went C → IGSN (Nov 2025) → **back to C (Jun/Jul 2026)**. August
2026 is the largest month on record. C-named files are 70% of `pdv_trace`
(5,373/7,708) and ~71% of both ALPSS types.

161 of 163 distinct C-family IGSNs have **no** IGSN-named `pdv_trace` at all —
these are distinct samples, not duplicate representations of covered ones.

## The tagging is CORRECT — there is no mis-tagging defect

The 2026-05-17 correction conflated two different files that share a stem:

| file | data_type | correct? |
|---|---|---|
| `C1--20250807--00001.csv` | `pdv_trace` | yes — this is the PDV trace |
| `C1--20250807--00001-results.csv` | `pdv_alpss_result` | yes — ALPSS result for it |
| `C1--20250807--00001-inputs.csv`, `-plots.png`, `-velocity--smooth.csv` | `pdv_alpss_output` | yes |
| `C1--20250807--00001_iq.png` | `pdv_alpss_output` | yes |

C-named ALPSS products have C-named parent traces, with exactly the same
`<stem>` + `-<suffix>` structure as the IGSN convention. Nothing is mis-tagged.

## The real defect is in our regex

`aimdl_coord_enrichment/instruments/helix.py`:

- `_SHOT_STEM_TAIL_RE = re.compile(r"_ch\d+$")` (line 39) requires the stem to
  end in `_ch<N>` — an artifact of the IGSN convention only. C-named stems
  (`C1--20250807--00001`) fail it → `alpss_shot_stem` returns `None` → every
  C-named item is reported unresolvable. **Cause of 47,408 of the failures.**
- `_ALPSS_SUFFIX_RE` (line 38) requires `-` before the suffix, so the
  `_iq.png` family (underscore separator) is missed. **7,428 items**, both
  C-named (5,268) and IGSN-named (2,160).

## Verified fix

Allow either separator and drop the `_ch<N>` tail requirement:

```python
_ALPSS_SUFFIX_RE = re.compile(r"[-_][A-Za-z_]+(?:--[A-Za-z_]+)*\.[A-Za-z0-9]+$")
# and remove the _SHOT_STEM_TAIL_RE gate
```

Measured against live prod, resolving `<stem>.csv` in the `pdv_trace` pool:

| data_type | items | resolved to unique parent | no parent | ambiguous |
|---|---:|---:|---:|---:|
| `pdv_alpss_output` | 59,420 | **59,420** | 0 | 0 |
| `pdv_alpss_result` | 7,428 | **7,428** | 0 | 0 |
| **total** | **66,848** | **66,848 (100%)** | **0** | **0** |

INV-5 is preserved: `find_parent_pdv_item_id` still requires exactly one match,
and zero ambiguous cases exist in the current data. The `_ch<N>` tail was a
*proxy* for "is this an ALPSS file"; the authoritative test — a unique
`<stem>.csv` in the trace pool — is already implemented and is strictly better.

## Collateral findings

- `pdv_alpss_results` (plural) no longer exists as a data type (was 1,526 in May).
- The May "orphan" bucket (1,352) is now **0**.
- New data types present: `nmd_project`, `nmd_raw`, `xrd_visualization` (23,905).
- Collection roughly tripled since May (`pdv_alpss_output` 19,423 → 59,420).

## Consequence for the A/B/C decision

Moot, and all three would have been harmful:

- **(A) retag upstream** — would have corrupted correct tagging.
- **(B) exclude by name shape** — would have permanently discarded 47,408 items,
  the majority of HELIX and the part that is actively growing.
- **(C) soften the check** — would have hidden a real bug in our code.

`all_helix_alpss_tagged` (ERROR) was correctly reporting a genuine defect. We
misread it as a data problem for three months.
