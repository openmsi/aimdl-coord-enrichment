"""Tests for the three partitioned coord-enrichment sibling jobs."""

import pytest
from dagster import AssetKey

from helix_dagster import defs

REPO = defs.get_repository_def()

PARTITIONED_JOB_NAMES = [
    "coord_enrichment_maxima_raw_job",
    "coord_enrichment_helix_alpss_job",
    "coord_enrichment_maxima_derived_job",
]

UNPARTITIONED_UPSTREAMS = {
    AssetKey("coord_transform_config_snapshot"),
    AssetKey("enrichable_items_inventory"),
    AssetKey("provenance_tagged_items"),
}


def test_all_three_partitioned_jobs_registered():
    for name in PARTITIONED_JOB_NAMES:
        assert name in REPO.job_names, f"{name} not found in defs"


def test_maxima_raw_job_partition_keys():
    job = REPO.get_job("coord_enrichment_maxima_raw_job")
    assert job.partitions_def.get_partition_keys() == [
        "MAXIMA/xrd_raw",
        "MAXIMA/xrf_raw",
    ]


def test_helix_alpss_job_partition_keys():
    job = REPO.get_job("coord_enrichment_helix_alpss_job")
    assert job.partitions_def.get_partition_keys() == [
        "HELIX/pdv_alpss_output",
        "HELIX/pdv_alpss_result",
        "HELIX/pdv_alpss_results",
    ]


def test_maxima_derived_job_partition_keys():
    job = REPO.get_job("coord_enrichment_maxima_derived_job")
    assert job.partitions_def.get_partition_keys() == [
        "MAXIMA/xrd_derived",
    ]


@pytest.mark.parametrize("job_name", PARTITIONED_JOB_NAMES)
def test_each_job_includes_unpartitioned_upstreams(job_name):
    job = REPO.get_job(job_name)
    asset_keys = job.asset_layer.executable_asset_keys
    assert UNPARTITIONED_UPSTREAMS.issubset(asset_keys), (
        f"{job_name} missing upstreams: {UNPARTITIONED_UPSTREAMS - asset_keys}"
    )


def test_original_coord_enrichment_job_unchanged():
    job = REPO.get_job("coord_enrichment_job")
    keys = job.asset_layer.executable_asset_keys
    assert AssetKey("coord_enrichment_manifest") in keys
    assert AssetKey("coord_enrichment_report") in keys
    for leaf in ["enriched_maxima_raw", "enriched_helix_alpss", "enriched_maxima_derived"]:
        assert AssetKey(leaf) not in keys, f"{leaf} should not be in coord_enrichment_job"


def test_defs_loads_without_errors():
    repo = defs.get_repository_def()
    assert repo is not None
    assert len(repo.job_names) >= 5


def test_version_is_0_5_0():
    import helix_dagster

    assert helix_dagster.__version__ == "0.5.0"
