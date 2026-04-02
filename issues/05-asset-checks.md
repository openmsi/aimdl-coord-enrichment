# Issue: Add Dagster asset checks for error surfacing

## Problem

The current pipeline treats most bad outcomes as data rather than failures.
Invalid IGSNs become `igsn_issues`, unmatched PDV files become `pdv_issues`,
write exceptions are swallowed into `write_errors`. Dagster always shows green
unless an exception escapes the asset body. An operator looking at the Dagster
UI sees a successful run even when 50% of rows failed validation or matching.

## Proposed Change

Add `@asset_check` functions that produce colored pass/warn/fail indicators
in the Dagster UI, separate from asset materialization status. Six checks:

| Check | Asset | Severity | Trigger |
|-------|-------|----------|---------|
| `zero_inventory` | `pdv_trace_inventory` | ERROR | Inventory returned 0 items |
| `igsn_validity_rate` | `validated_rows` | WARN | <80% valid IGSNs |
| `pdv_match_rate` | `pdv_cross_references` | WARN | <50% rows matched |
| `igsn_consistency` | `pdv_cross_references` | ERROR | Any IGSN mismatch |
| `enrichment_success_rate` | `enriched_pdv_metadata` | WARN | <90% enriched |
| `no_write_errors` | `enriched_pdv_metadata` | ERROR | Any write exception |

Checks are defined in a new `checks.py` module and registered in Definitions.

## Labels

`enhancement`, `backend`, `stage-3`
