"""Tests for the partitioned coord-enrichment sibling jobs."""

import pytest
from dagster import AssetKey, DagsterInstance, DynamicPartitionsDefinition

from aimdl_coord_enrichment import defs

REPO = defs.get_repository_def()

PARTITIONED_JOB_NAMES = [
    "coord_enrichment_maxima_job",
    "coord_enrichment_helix_alpss_job",
    "coord_enrichment_maxima_partition_job",
]

MAXIMA_RAW_UNPARTITIONED_UPSTREAMS = {
    AssetKey("coord_transform_config_snapshot"),
}

HELIX_ALPSS_UNPARTITIONED_UPSTREAMS = {
    AssetKey("coord_transform_config_snapshot"),
    AssetKey("enrichable_items_inventory"),
    AssetKey("helix_alpss_provenance_tagged"),
}


def test_all_partitioned_jobs_registered():
    for name in PARTITIONED_JOB_NAMES:
        assert name in REPO.job_names, f"{name} not found in defs"


def test_maxima_job_is_partitioned_by_run():
    job = REPO.get_job("coord_enrichment_maxima_job")
    assert isinstance(job.partitions_def, DynamicPartitionsDefinition)
    assert job.partitions_def.name == "maxima_run"
    with DagsterInstance.ephemeral() as instance:
        assert job.partitions_def.get_partition_keys(
            dynamic_partitions_store=instance
        ) == []


def test_helix_alpss_job_partition_keys():
    job = REPO.get_job("coord_enrichment_helix_alpss_job")
    assert job.partitions_def.get_partition_keys() == [
        "HELIX/pdv_alpss_output",
        "HELIX/pdv_alpss_result",
        "HELIX/pdv_alpss_results",
    ]

def test_maxima_raw_job_selection():
    job = REPO.get_job("coord_enrichment_maxima_job")
    asset_keys = job.asset_layer.executable_asset_keys
    assert asset_keys == {
        AssetKey("coord_transform_config_snapshot"),
        AssetKey("enriched_maxima_run"),
    }


def test_helix_alpss_job_includes_unpartitioned_upstreams():
    job = REPO.get_job("coord_enrichment_helix_alpss_job")
    asset_keys = job.asset_layer.executable_asset_keys
    assert HELIX_ALPSS_UNPARTITIONED_UPSTREAMS.issubset(asset_keys), (
        "coord_enrichment_helix_alpss_job missing upstreams: "
        f"{HELIX_ALPSS_UNPARTITIONED_UPSTREAMS - asset_keys}"
    )

def test_original_coord_enrichment_job_unchanged():
    job = REPO.get_job("coord_enrichment_job")
    keys = job.asset_layer.executable_asset_keys
    assert AssetKey("coord_enrichment_manifest") in keys
    assert AssetKey("coord_enrichment_report") in keys
    for leaf in ["enriched_maxima_run", "enriched_helix_alpss"]:
        assert AssetKey(leaf) not in keys, f"{leaf} should not be in coord_enrichment_job"


def test_defs_loads_without_errors():
    repo = defs.get_repository_def()
    assert repo is not None
    assert len(repo.job_names) >= 5


def test_version_is_0_6_0():
    import aimdl_coord_enrichment

    assert aimdl_coord_enrichment.__version__ == "0.6.0"
