"""Regression: helix_spreadsheet asset checks must not force an IOManager
load across the partition cross-product.

Defect (found 2026-06-15 during the issue-31 dry run): the pdv_log /
pdv_data / pdv_processing_manifest checks took their partitioned asset as
a positional input, so running the partitioned job for ONE partition made
Dagster's IOManager load the asset for *other* partitions ->
FileNotFoundError -> all 7 checks errored, even though the three assets
materialized cleanly. Same defect family as the coord_enrichment leaf
checks (see test_leaf_check_partition_isolation.py and
coord_enrichment/check_support.py).

This runs the real partitioned job for one partition while a second
dynamic partition is registered but never materialized. With the defect
present the run fails on the check steps; with the fix the run succeeds
and all seven checks evaluate.
"""

from unittest.mock import patch

import pandas as pd
import pytest
from dagster import DagsterInstance, materialize

from aimdl_coord_enrichment.assets import (
    pdv_data,
    pdv_log,
    pdv_processing_manifest,
)
from aimdl_coord_enrichment.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    manifest_written,
    pdv_match_rate,
    zero_pdv_inventory,
)
from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER

requires_transformer = pytest.mark.skipif(
    _COORD_TRANSFORMER is None,
    reason="coordinate-transformer not configured",
)

MATERIALIZED_KEY = "JHAMAL00018-005//2026-04-16"
# A second dynamic partition registered but never materialized. Under the
# defect, the checks' IOManager load reaches for this one.
SIBLING_KEY = "JHAMAL00018-007//2026-04-15"


def _log_df():
    """Raw (pre-COLUMN_MAP) experiment-log rows for the materialized key."""
    return pd.DataFrame([
        {
            "Timestamp": "2026-04-16T17:00:00+00:00",
            "Sample_ID": "JHAMAL00018-005",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Corrected (mm)": 8.0,
            "Flyer_Y_Position_Corrected (mm)": 8.0,
        },
    ])


def _pdv_inventory():
    return [
        {"name": "shot001_ch1.tdms", "_id": "pdv1",
         "meta": {"igsn": "JHAMAL00018-005", "data_type": "pdv_trace"}},
    ]


@requires_transformer
def test_partition_job_succeeds_with_unmaterialized_sibling_partition():
    from unittest.mock import MagicMock

    with DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions(
            "helix_experiment_log", [MATERIALIZED_KEY, SIBLING_KEY]
        )
        girder = MagicMock()

        with patch(
            "aimdl_coord_enrichment.assets.fetch_partition_details",
            side_effect=lambda g, dt, key: [{"_id": "log1", "name": "log.csv"}],
        ), patch(
            "aimdl_coord_enrichment.assets.download_and_read",
            side_effect=lambda g, item_id, name: _log_df(),
        ), patch(
            "aimdl_coord_enrichment.assets.fetch_all_aimdl_datafiles",
            side_effect=lambda g, dt: _pdv_inventory(),
        ):
            result = materialize(
                [
                    pdv_log,
                    pdv_data,
                    pdv_processing_manifest,
                    igsn_validity_rate,
                    zero_pdv_inventory,
                    pdv_match_rate,
                    igsn_consistency,
                    enrichment_success_rate,
                    coord_transform_check,
                    manifest_written,
                ],
                instance=instance,
                resources={"girder": girder},
                partition_key=MATERIALIZED_KEY,
                run_config={
                    "ops": {
                        "pdv_data": {"config": {"dry_run": True}},
                        "pdv_processing_manifest": {"config": {"dry_run": True}},
                    }
                },
            )

    assert result.success, "partitioned job run must succeed end-to-end"

    # In dry run nothing is written to Girder.
    girder.addMetadataToItem.assert_not_called()

    check_evals = {
        e.check_name: e.passed for e in result.get_asset_check_evaluations()
    }
    assert check_evals == {
        "igsn_validity_rate": True,
        "zero_pdv_inventory": True,
        "pdv_match_rate": True,
        "igsn_consistency": True,
        "enrichment_success_rate": True,
        "coord_transform_check": True,
        "manifest_written": True,
    }, check_evals
