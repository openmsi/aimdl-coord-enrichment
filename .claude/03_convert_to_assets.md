# Issue 3: Convert pipeline to Dagster asset-based DAG

## Context
This is the main structural change. We convert from a single-op job to a
multi-asset DAG where each asset represents a meaningful intermediate result.
The Dagster UI will show a visible DAG with named nodes instead of a single box.

## Depends on
Issues 1 and 2 must be merged first.

## Target DAG

```
raw_experiment_log          pdv_inventory
        │                        │
        ▼                        │
 validated_rows                  │
        │                        │
        ▼                        ▼
      pdv_cross_references ◄─────┘
              │
              ▼
    enriched_pdv_metadata
              │
              ▼
       quality_report
```

## Tasks

### 1. Create helix_dagster/assets.py with six asset definitions

Each asset should use `@asset` decorator, proper type hints, and
`context.add_output_metadata()` for observability in the Dagster UI.

**Asset 1: raw_experiment_log**
- Config: item_id (str), filename (str) — use a Dagster Config object
- Resource: girder (GirderResource)
- Action: Download spreadsheet from Girder, apply COLUMN_MAP rename, return DataFrame
- Metadata: row_count, filename, source_item_id
- This is the only asset that reads from the experiment log folder

**Asset 2: pdv_inventory**
- Resource: girder (GirderResource)
- Action: Fetch all items from PDV_FOLDER_ID via Girder API
- Return: list of Girder item dicts
- Metadata: item_count
- This is independent of raw_experiment_log — it can be materialized separately
- Consider adding a FreshnessPolicy (e.g., maximum_lag_minutes=30) so it
  auto-refreshes but doesn't re-fetch on every spreadsheet run

**Asset 3: validated_rows**
- Input: raw_experiment_log (DataFrame)
- Action: Call validate_igsn() on each row's Sample_IGSN column
- Return: dict with "dataframe" (DataFrame with added "valid_igsn" column)
  and "igsn_issues" (list of issue dicts)
- Metadata: total_rows, valid_igsn_count, invalid_igsn_count, missing_igsn_count
- No network calls — pure transformation

**Asset 4: pdv_cross_references**
- Inputs: validated_rows (dict), pdv_inventory (list)
- Action: Call match_pdv_file() for each row
- Return: dict with "matches" (dict mapping row_index → Girder item) and
  "pdv_issues" (list of issue dicts)
- Metadata: matched_count, not_found_count, ambiguous_count
- No network calls — pure matching

**Asset 5: enriched_pdv_metadata**
- Inputs: pdv_cross_references (dict), validated_rows (dict)
- Resource: girder (GirderResource)
- Action: For each matched row, call transform_station_to_sample() and write
  metadata to the Girder item via client.addMetadataToItem()
- Return: dict with "written_count" and any write errors
- Metadata: items_enriched, coordinate_transform_failures
- This is the ONLY asset that writes to Girder

**Asset 6: quality_report**
- Inputs: validated_rows (dict), pdv_cross_references (dict),
  enriched_pdv_metadata (dict)
- Action: Aggregate all issues from upstream assets into a single structured report
- Return: dict with all issue lists and summary counts
- Metadata: total_igsn_issues, total_pdv_issues, total_write_errors

### 2. Create a Dagster AssetSelection-based job

Replace process_helix_file_job with:
```python
from dagster import define_asset_job, AssetSelection

process_helix_assets_job = define_asset_job(
    name="process_helix_assets_job",
    selection=AssetSelection.all(),
)
```

### 3. Update the sensor to trigger asset materialization

Modify helix_folder_sensor to create RunRequest objects that target the new
asset-based job. The sensor should pass the item_id and filename as run config
for the raw_experiment_log asset.

The sensor's cursor logic (tracking seen item IDs) should remain unchanged.

### 4. Update __init__.py

Replace the job-based Definitions with asset-based Definitions:
```python
from dagster import Definitions, EnvVar
from helix_dagster.assets import (
    raw_experiment_log,
    pdv_inventory,
    validated_rows,
    pdv_cross_references,
    enriched_pdv_metadata,
    quality_report,
)
from helix_dagster.resources import GirderResource
from helix_dagster.sensors import helix_folder_sensor

defs = Definitions(
    assets=[
        raw_experiment_log,
        pdv_inventory,
        validated_rows,
        pdv_cross_references,
        enriched_pdv_metadata,
        quality_report,
    ],
    jobs=[process_helix_assets_job],
    sensors=[helix_folder_sensor],
    resources={
        "girder": GirderResource(
            api_url=EnvVar("GIRDER_API_URL"),
            api_key=EnvVar("GIRDER_TOKEN"),
        ),
    },
)
```

### 5. Keep the old job.py and processing.py temporarily

Don't delete jobs.py or processing.py yet. Rename the old job to
`_legacy_process_helix_file_job` and mark it deprecated in a docstring.
This allows rollback if the asset-based pipeline has issues in production.
Delete in a follow-up PR after the asset pipeline is validated.

### 6. Write integration tests

Create `tests/test_assets.py`:

- test_validated_rows_pure: Create a sample DataFrame, call the validated_rows
  asset function directly (not through Dagster), verify the output has the
  correct valid_igsn column and issue list.

- test_pdv_cross_references_pure: Create a sample validated_rows output and a
  mock PDV inventory list, call pdv_cross_references directly, verify matches
  and issues.

- test_asset_dag_loads: Verify that the Dagster Definitions object loads without
  error and contains all six assets:
  ```python
  from helix_dagster import defs
  asset_keys = {a.key.to_user_string() for a in defs.get_asset_graph().all_asset_keys}
  assert "raw_experiment_log" in asset_keys
  assert "pdv_inventory" in asset_keys
  # ... etc
  ```

### 7. Verify
- Run `pytest tests/ -v` and confirm all tests pass.
- Run `dagster dev` and verify the Dagster UI shows the six-node DAG.
- Take a screenshot of the DAG for documentation.
- Commit with message: "feat: convert pipeline to Dagster asset-based DAG with
  six assets (raw_experiment_log, pdv_inventory, validated_rows,
  pdv_cross_references, enriched_pdv_metadata, quality_report)"

## Important design decisions

### Why assets, not ops?
Assets represent "things that exist" — a DataFrame, a set of enriched Girder items,
a quality report. The Dagster asset catalog becomes a view into the current state
of the metadata extraction. This aligns with the project's data philosophy: the
interesting question is "what is the state of the enriched metadata?" not "did the
job run?"

### Why separate pdv_inventory?
The current code fetches all PDV items (limit=100000 Girder API call) inside every
process_file() invocation. As an independent asset, the inventory is materialized
once and reused across multiple spreadsheet runs. This is both a performance
optimization and a correctness improvement: the cross-reference step now operates
on a consistent snapshot of the PDV inventory.

### Why is enriched_pdv_metadata the only asset that writes to Girder?
All other assets are pure transformations on DataFrames and lists. This makes them
trivially testable without mocking Girder. Only the final "write" step needs
a Girder client, concentrating all side effects in one place.
