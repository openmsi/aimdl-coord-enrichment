# Issue 23, Step 3 — MAXIMA raw discovery sensor + single-asset job

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Steps 0–2 complete.
`enriched_maxima_raw` is now partitioned on the new
`MultiPartitionsDefinition`, but the only way to add partitions is
manually. This step wires up automatic discovery.

Before editing, read:

- `.claude/CLAUDE.md`
- `.claude/prompts/issue23/README.md` (invariants — especially the
  locked dedup-key formula)
- `aimdl_coord_enrichment/sensors.py` (the existing `helix_folder_sensor`
  for style reference)
- `aimdl_coord_enrichment/__init__.py`
- `aimdl_coord_enrichment/coord_enrichment/inventory.py` (for the partition
  defs)
- `aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py`
- `aimdl_coord_enrichment/girder_io.py` (for `fetch_partition_index`)

## Why this step

The multi-partitioned `enriched_maxima_raw` needs its dynamic `run`
dimension populated. Dagster doesn't discover partitions on its
own — something has to call
`MAXIMA_RUN_PARTITIONS.build_add_request([...])` and, ideally, also
issue `RunRequest`s so newly-discovered partitions are materialized
without manual intervention.

A sensor is the right fit (not a schedule) because `RunRequest.run_key`
semantics differ: for sensors, Dagster dedups on run_key **across
all sensor evaluations**, so a content-hash-based run_key gives us
automatic idempotency. A schedule only dedups within a tick.

The dedup key must cover every input to the output. For
`enriched_maxima_raw`, that means both the raw data's content hash
and the `xrd_metadata` (instructions.txt) content hash — because
coordinates can change without the raw data changing if
instructions.txt was edited.

## Goal

- Add `maxima_raw_discovery_sensor` in `aimdl_coord_enrichment/sensors.py`.
- Add `coord_enrichment_maxima_raw_partition_job` in
  `aimdl_coord_enrichment/__init__.py` — the single-asset job the sensor
  targets.
- Wire both into the `Definitions` object.
- Add tests covering dedup-key construction, the union of
  AIMD-L keys for `build_add_request`, and the metadata-hash
  fallback.

## Edits

### 1. `aimdl_coord_enrichment/sensors.py`

Add the sensor. Import the partition defs and helpers:

```python
from dagster import (
    DefaultSensorStatus,
    MultiPartitionKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from aimdl_coord_enrichment.coord_enrichment.inventory import MAXIMA_RUN_PARTITIONS
from aimdl_coord_enrichment.girder_io import fetch_partition_index
from aimdl_coord_enrichment.resources import GirderConnection
```

(Keep the existing imports for `helix_folder_sensor`.)

The sensor definition — at the top of the file, not at the bottom,
keep the `helix_folder_sensor` where it is:

```python
_MAXIMA_RAW_DATA_TYPES = ("xrd_raw", "xrf_raw")


@sensor(
    job_name="coord_enrichment_maxima_raw_partition_job",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
)
def maxima_raw_discovery_sensor(
    context: SensorEvaluationContext,
    girder: GirderConnection,
) -> SensorResult:
    """Discover new AIMD-L partitions for MAXIMA raw and emit materialization requests.

    Per tick:
      1. Fetches the partition index for xrd_raw, xrf_raw, and
         xrd_metadata.
      2. Adds the union of AIMD-L run keys (``igsn//experiment_date``)
         to the dynamic ``maxima_raw_run`` partition dimension.
      3. Emits one RunRequest per observed (data_type, aimdl_key),
         dedupping on a run_key that composes both the raw content
         hash and the xrd_metadata content hash.

    Dedup semantics (sensor-scoped): the same (data_type, aimdl_key,
    raw_hash, metadata_hash) tuple produces the same run_key, so
    unchanged partitions are suppressed across all sensor
    evaluations. Either hash changing triggers a new run.

    The xrd_metadata hash fallback is "no-xrd-metadata" for runs
    that have no xrd_metadata entry — those partitions will still
    materialize once, and the asset will record a resolution error
    for the missing instructions.txt.
    """
    # 1. Fetch indexes
    raw_indexes: dict[str, dict[str, str]] = {
        dt: fetch_partition_index(girder, dt)
        for dt in _MAXIMA_RAW_DATA_TYPES
    }
    metadata_index = fetch_partition_index(girder, "xrd_metadata")

    # 2. Union of AIMD-L keys for the shared dynamic dimension.
    #    The same (igsn, experiment_date) may appear under both
    #    xrd_raw and xrf_raw — it's one dimension value, not two.
    all_aimdl_keys = sorted(
        {
            aimdl_key
            for index in raw_indexes.values()
            for aimdl_key in index.keys()
        }
    )

    # 3. Emit RunRequests for each observed (data_type, aimdl_key) pair.
    run_requests: list[RunRequest] = []
    for data_type, index in raw_indexes.items():
        for aimdl_key, raw_hash in index.items():
            metadata_hash = metadata_index.get(aimdl_key, "no-xrd-metadata")

            dagster_partition_key = MultiPartitionKey(
                {"data_type": data_type, "run": aimdl_key}
            )
            dagster_run_key = (
                f"coord-enrichment"
                f"|{data_type}"
                f"|{aimdl_key}"
                f"|raw={raw_hash}"
                f"|xrd_metadata={metadata_hash}"
            )

            run_requests.append(
                RunRequest(
                    run_key=dagster_run_key,
                    partition_key=dagster_partition_key,
                    tags={
                        "data_type": data_type,
                        "aimdl_partition_key": aimdl_key,
                        "raw_content_hash": raw_hash,
                        "xrd_metadata_content_hash": metadata_hash,
                    },
                )
            )

    context.log.info(
        "maxima_raw_discovery_sensor: %d AIMD-L run keys, %d run requests",
        len(all_aimdl_keys), len(run_requests),
    )

    return SensorResult(
        dynamic_partitions_requests=[
            MAXIMA_RUN_PARTITIONS.build_add_request(all_aimdl_keys)
        ],
        run_requests=run_requests,
    )
```

Variable naming discipline (do not deviate):

- `aimdl_key` — the `"<igsn>//<experiment_date>"` string from the API
- `raw_hash` — content hash for the raw-data partition
- `metadata_hash` — content hash for xrd_metadata (with fallback)
- `dagster_partition_key` — the `MultiPartitionKey` instance
- `dagster_run_key` — the dedup string passed to `RunRequest.run_key`

Do NOT reuse `run_key` for both the AIMD-L partition string and
Dagster's dedup string. This collision bites future maintainers.

### 2. `aimdl_coord_enrichment/__init__.py`

Add the new single-asset job near the other jobs:

```python
coord_enrichment_maxima_raw_partition_job = define_asset_job(
    name="coord_enrichment_maxima_raw_partition_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enriched_maxima_raw,
    ),
)
```

Import the new sensor at the top:

```python
from aimdl_coord_enrichment.sensors import (
    helix_folder_sensor,
    maxima_raw_discovery_sensor,
)
```

Register both in the `Definitions` object:

```python
defs = Definitions(
    # ... assets, checks, schedules unchanged ...
    jobs=[
        process_helix_assets_job,
        coord_enrichment_job,
        coord_enrichment_maxima_raw_job,
        coord_enrichment_maxima_raw_partition_job,  # new
        coord_enrichment_helix_alpss_job,
        coord_enrichment_maxima_derived_job,
    ],
    # ... schedules unchanged ...
    sensors=[
        helix_folder_sensor,
        maxima_raw_discovery_sensor,  # new
    ],
    # ... resources unchanged ...
)
```

### 3. `tests/test_sensors_maxima_discovery.py` (new file)

Create a fresh test file. Keep existing sensor tests (if any) in
their existing files.

```python
"""Tests for maxima_raw_discovery_sensor — dedup key construction,
dynamic partition add requests, metadata-hash fallback."""

from unittest.mock import MagicMock

import pytest
from dagster import (
    DagsterInstance,
    MultiPartitionKey,
    RunRequest,
    SensorResult,
    build_sensor_context,
)

from aimdl_coord_enrichment.sensors import maxima_raw_discovery_sensor


def _make_indexes(xrd_raw=None, xrf_raw=None, xrd_metadata=None):
    return {
        "xrd_raw": xrd_raw or {},
        "xrf_raw": xrf_raw or {},
        "xrd_metadata": xrd_metadata or {},
    }


def _mock_girder_with(indexes: dict[str, dict[str, str]]) -> MagicMock:
    """A MagicMock client whose .get('aimdl/partition', parameters=...) returns
    the matching index, simulating fetch_partition_index's HTTP shape."""
    client = MagicMock()

    def fake_get(path, parameters=None):
        if path == "aimdl/partition":
            return indexes.get(parameters["dataType"], {})
        raise AssertionError(f"unexpected client call: {path} {parameters}")

    client.get.side_effect = fake_get
    return client


def test_sensor_empty_indexes_emits_no_requests():
    girder = _mock_girder_with(_make_indexes())
    ctx = build_sensor_context(instance=DagsterInstance.ephemeral())
    result = maxima_raw_discovery_sensor(ctx, girder)
    assert isinstance(result, SensorResult)
    assert result.run_requests == []
    # The single add_request should be present with an empty list
    assert len(result.dynamic_partitions_requests) == 1
    assert result.dynamic_partitions_requests[0].partition_keys == []


def test_sensor_emits_run_request_per_data_type_per_key():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawhashA", "K2//T2": "rawhashB"},
        xrf_raw={"K1//T1": "rawhashC"},  # same aimdl_key as xrd_raw K1
        xrd_metadata={"K1//T1": "metaA", "K2//T2": "metaB"},
    )
    girder = _mock_girder_with(indexes)
    ctx = build_sensor_context(instance=DagsterInstance.ephemeral())
    result = maxima_raw_discovery_sensor(ctx, girder)

    # 3 RunRequests: xrd_raw×2 + xrf_raw×1
    assert len(result.run_requests) == 3

    # Partition add request has union (2 unique keys) — not 3
    adds = result.dynamic_partitions_requests[0].partition_keys
    assert sorted(adds) == ["K1//T1", "K2//T2"]


def test_sensor_dedup_key_includes_both_hashes():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawA"},
        xrd_metadata={"K1//T1": "metaA"},
    )
    girder = _mock_girder_with(indexes)
    ctx = build_sensor_context(instance=DagsterInstance.ephemeral())
    result = maxima_raw_discovery_sensor(ctx, girder)
    rr = result.run_requests[0]
    assert rr.run_key == (
        "coord-enrichment|xrd_raw|K1//T1|raw=rawA|xrd_metadata=metaA"
    )


def test_sensor_metadata_hash_fallback():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawA"},
        xrd_metadata={},  # no xrd_metadata entries
    )
    girder = _mock_girder_with(indexes)
    ctx = build_sensor_context(instance=DagsterInstance.ephemeral())
    result = maxima_raw_discovery_sensor(ctx, girder)
    rr = result.run_requests[0]
    assert "xrd_metadata=no-xrd-metadata" in rr.run_key


def test_sensor_partition_key_is_multi_dim():
    indexes = _make_indexes(
        xrd_raw={"K1//T1": "rawA"},
        xrd_metadata={"K1//T1": "metaA"},
    )
    girder = _mock_girder_with(indexes)
    ctx = build_sensor_context(instance=DagsterInstance.ephemeral())
    result = maxima_raw_discovery_sensor(ctx, girder)
    rr = result.run_requests[0]
    # Accept either MultiPartitionKey or its string form depending on
    # Dagster version; the important invariant is both dims are encoded.
    pk = rr.partition_key
    if isinstance(pk, MultiPartitionKey):
        assert pk.keys_by_dimension == {"data_type": "xrd_raw", "run": "K1//T1"}
    else:
        assert "xrd_raw" in str(pk) and "K1//T1" in str(pk)
```

If `build_sensor_context` does not accept `instance=` in the version
of Dagster this repo uses, use whatever context-builder the repo
uses elsewhere (check existing sensor tests or the Dagster version
in `pyproject.toml`). The important thing is that the sensor has
access to an ephemeral instance.

## Verification

```bash
.venv/bin/pytest
```

Full suite must pass, including the new sensor tests.

## Commit

```
git add aimdl_coord_enrichment/sensors.py \
        aimdl_coord_enrichment/__init__.py \
        tests/test_sensors_maxima_discovery.py
git commit -m "Add maxima_raw_discovery_sensor + partition job (#23)

- New sensor: per-tick enumerates xrd_raw/xrf_raw/xrd_metadata
  partition indexes, adds run keys to MAXIMA_RUN_PARTITIONS,
  emits RunRequests with content-hash-based dedup covering both
  raw and xrd_metadata inputs.
- New single-asset job coord_enrichment_maxima_raw_partition_job
  targets just enriched_maxima_raw (plus config snapshot).
- Sensor defaults to STOPPED; operator enables when ready."
```

## Success criteria

- `maxima_raw_discovery_sensor` exists in `aimdl_coord_enrichment/sensors.py`
  and is registered in `Definitions`.
- `coord_enrichment_maxima_raw_partition_job` exists and is the
  sensor's target.
- Sensor tests pass: empty-indexes, per-key RunRequest, dedup-key
  format, metadata-hash fallback, multi-dim partition key.
- Full pytest suite passes.
- One new commit.

## Out of scope

- `since` parameter wiring for incremental discovery — future
  optimization.
- Partition deletion when AIMD-L drops a run — intentionally not
  implemented (audit trail preferred).
- The reconciliation schedule upgrade — Step 4.
- Anything provenance-related — Steps 5–6.
