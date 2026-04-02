# Issue: Replace `pdv_inventory` with `/aimdl/datafiles` endpoint

## Problem

The `pdv_inventory` asset fetches up to 100,000 items from the PDV folder via
a single Girder REST call. This is the primary performance bottleneck in the
pipeline. Most items are irrelevant to the spreadsheet being processed.

## Proposed Change

Replace `pdv_inventory` with a new `pdv_trace_inventory` asset that uses the
`/aimdl/datafiles?dataType=pdv_trace` endpoint. This performs an indexed MongoDB
query returning only items with the correct `data_type` and an existing IGSN.

Additionally:
- Update `pdv_cross_references` to depend on `pdv_trace_inventory`
- Add IGSN consistency checking (cross-check spreadsheet IGSN vs item `meta.igsn`)
- Remove `PDV_FOLDER_ID` from constants
- Update all tests and `__init__.py` Definitions

## Migration Note

The `/aimdl/datafiles` endpoint requires `meta.igsn` to exist on items.
Until IGSN tagging is complete on all PDV files, the new inventory may return
fewer items than the old folder-based approach. The implementation should log
a warning if the inventory is suspiciously small.

## Labels

`enhancement`, `backend`, `stage-2`
