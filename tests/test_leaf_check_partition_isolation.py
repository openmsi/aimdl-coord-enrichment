"""Regression: partitioned-leaf asset checks must not force an IOManager
load across the partition cross-product.

Defect (found 2026-05-15 during the live §3 rehearsal): the leaf
asset checks took their partitioned asset as a positional input, so
running the partitioned job for ONE partition made Dagster's
IOManager load `enriched_maxima_raw` for *other*, unmaterialized
partitions -> FileNotFoundError -> RUN_FAILURE, even though the
asset materialized cleanly. Same defect family as the
coord_enrichment_report fix (it reads the event log via deps=).

This test runs the real partitioned job for one partition while a
second dynamic partition is registered but never materialized. With
the defect present the run fails on the check steps; with the fix
the run succeeds and both checks evaluate.

No test previously executed a real partitioned job *with its checks*
on a multi-partition instance — this closes that gap (validation
plan §5).
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import DagsterInstance, MultiPartitionKey, materialize

FIXTURES = Path(__file__).parent / "fixtures"

from helix_dagster.coord_enrichment.config_snapshot import (
    _COORD_TRANSFORMER,
    coord_transform_config_snapshot,
)
from helix_dagster.coord_enrichment.enrichment_leaves import (
    enriched_maxima_raw,
    enrichment_success_rate_maxima_raw,
    no_coord_transform_failures_maxima_raw,
)

requires_transformer = pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="coordinate-transformer not configured",
)

EXAMPLE_TS = datetime(2026, 5, 8, 17, 28, 17, tzinfo=timezone.utc)
MATERIALIZED_KEY = "JHBMAI00003-S1R6C0//2026-05-08T17:28:17+00:00"
# A second dynamic partition that is registered but never materialized.
# Under the defect, the check's IOManager load reaches for this one.
SIBLING_KEY = "APLMAJ00005-01//2025-10-23T15:47:38+00:00"


def _xrd_items(count=5):
    return [
        {
            "_id": f"xrd-{i:03d}",
            "name": f"scan_point_{i}_master.h5",
            "folderId": "raw-folder-1",
            "meta": {
                "data_type": "xrd_raw",
                "igsn": "JHBMAI00003-S1R6C0",
                "experiment_date": EXAMPLE_TS.isoformat(),
            },
        }
        for i in range(count)
    ]


def _girder_mock():
    mock = MagicMock()
    instr_bytes = (FIXTURES / "instructions_example.json").read_bytes()

    def _get(path, parameters=None):
        if path.startswith("item/") and path.endswith("/files"):
            return [{"_id": f"file-for-{path.split('/')[1]}"}]
        raise AssertionError(f"unexpected girder.get({path!r})")

    mock.get.side_effect = _get
    mock.downloadFile.side_effect = lambda fid, buf: buf.write(instr_bytes)
    return mock


def _fetch_partition_details(girder, data_type, key):
    if data_type == "xrd_raw":
        return _xrd_items()
    if data_type == "xrd_metadata":
        return [{"_id": "instr-1", "name": "instructions.txt"}]
    return []


@requires_transformer
def test_partition_job_succeeds_with_unmaterialized_sibling_partition():
    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions(
            "maxima_raw_run", [MATERIALIZED_KEY, SIBLING_KEY]
        )
        girder = _girder_mock()

        with patch(
            "helix_dagster.coord_enrichment.enrichment_leaves."
            "fetch_partition_details",
            side_effect=_fetch_partition_details,
        ), patch(
            "helix_dagster.coord_enrichment.enrichment_leaves."
            "transform_station_to_sample",
            side_effect=lambda sx, sy, instrument="MAXIMA", timestamp=None: (
                sx + 1.0,
                sy + 1.0,
                "MAXIMA/v1",
            ),
        ):
            result = materialize(
                [
                    coord_transform_config_snapshot,
                    enriched_maxima_raw,
                    enrichment_success_rate_maxima_raw,
                    no_coord_transform_failures_maxima_raw,
                ],
                instance=instance,
                resources={"girder": girder},
                partition_key=MultiPartitionKey(
                    {"data_type": "xrd_raw", "run": MATERIALIZED_KEY}
                ),
                run_config={
                    "ops": {
                        "enriched_maxima_raw": {"config": {"dry_run": True}}
                    }
                },
            )

    assert result.success, "partitioned job run must succeed end-to-end"

    check_evals = {
        e.check_name: e.passed
        for e in result.get_asset_check_evaluations()
    }
    assert check_evals == {
        "enrichment_success_rate_maxima_raw": True,
        "no_coord_transform_failures_maxima_raw": True,
    }, check_evals
