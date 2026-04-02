# Issue: Add ALPSS results inventory for quality reporting

## Problem

The pipeline currently has no visibility into whether ALPSS processing has been
completed for matched PDV traces. The quality report only covers IGSN validation,
PDV filename matching, and metadata write errors — it doesn't report on processing
coverage.

## Proposed Change

Add an `alpss_results_inventory` asset that fetches `pdv_alpss_result` items via
the `/aimdl/datafiles` endpoint. Enhance the `quality_report` to include:

- Which matched PDV traces have corresponding ALPSS results?
- Which ALPSS results exist for IGSNs not in the current spreadsheet?
- Overall processing completeness metrics

This gives operators visibility into the ALPSS processing pipeline's health.

## Labels

`enhancement`, `backend`, `stage-3`
