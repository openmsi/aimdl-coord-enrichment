# Issue: Add `/aimdl` endpoint helper functions to girder_io.py

## Problem

The pipeline currently discovers PDV data files by fetching ALL items from the PDV
folder via `girder.get("item", parameters={"folderId": PDV_FOLDER_ID, "limit": 100000})`.
This is expensive, slow, and scales poorly as data accumulates.

The Girder instance now has an `/aimdl/datafiles` endpoint that performs an indexed
MongoDB query by `meta.data_type`, returning only relevant items without directory
traversal.

## Proposed Change

Add helper functions to `girder_io.py` for the `/aimdl` endpoint:

- `fetch_aimdl_datafiles(client, data_type, limit, offset)` — single page fetch
- `fetch_all_aimdl_datafiles(client, data_type)` — paginator (endpoint caps at 100/page)
- `fetch_aimdl_datatypes(client)` — list available data types

Add constants to `constants.py`:

- `AIMDL_DATA_TYPES` dict mapping friendly names to `meta.data_type` strings
- `PDV_TRACE_DATA_TYPE` defaulting to `"pdv_trace"`

Add unit tests with mocked Girder responses.

No changes to existing asset behavior in this stage.

## Labels

`enhancement`, `backend`, `stage-1`
