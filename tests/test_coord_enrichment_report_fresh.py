"""Regression test: coord_enrichment_report must be loadable on a
fresh Dagster instance with zero leaf materializations.

Defect: previously the report took the three partitioned leaves as
positional asset inputs, which forced the IOManager to load nonexistent
outputs on a fresh instance. The fix moved the leaves to deps= and
queries the event log instead; this test locks that in.
"""

from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    MetadataValue,
    build_asset_context,
)

from aimdl_coord_enrichment.coord_enrichment.report import coord_enrichment_report


def _empty_tagging():
    return {
        "counters": {},
        "unresolved": [],
        "write_ops": [],
        "dry_run": True,
    }


def _empty_observer():
    return {
        "total": 0,
        "fully_enriched": 0,
        "partial": 0,
        "unenriched": 0,
        "missing_igsn": 0,
        "coverage_rate": 0.0,
    }


def test_report_runs_with_zero_leaf_materializations():
    """No leaf has materialized → all three counts are unmaterialized,
    summary shows zero writes, no exception is raised.
    """
    with DagsterInstance.ephemeral() as instance:
        ctx = build_asset_context(instance=instance)
        report = coord_enrichment_report(ctx, _empty_tagging(), _empty_observer())

    assert report["summary"]["total_writes"] == 0
    assert report["summary"]["leaf_partitions_covered"] == 0
    assert report["summary"]["leaf_partitions_unmaterialized"] >= 4
    # HELIX has 3 static partitions, derived has 1, raw dynamic dim
    # is empty on a fresh instance.
    assert report["leaves_unmaterialized"]["enriched_helix_alpss"] == 3
    assert report["leaves_unmaterialized"]["enriched_maxima_derived"] == 1
    assert report["leaves_unmaterialized"]["enriched_maxima_raw"] == 0
    assert report["leaves"] == {}


def test_report_finds_partial_state():
    """One ALPSS partition materialized → it shows up; the others count
    as unmaterialized."""
    with DagsterInstance.ephemeral() as instance:
        instance.report_runless_asset_event(
            AssetMaterialization(
                asset_key=AssetKey("enriched_helix_alpss"),
                partition="HELIX/pdv_alpss_output",
                metadata={
                    "partition": MetadataValue.text("HELIX/pdv_alpss_output"),
                    "seen": MetadataValue.int(10),
                    "written": MetadataValue.int(8),
                    "simulated_dry_run": MetadataValue.int(0),
                    "skipped_no_change": MetadataValue.int(2),
                    "coord_failures": MetadataValue.int(0),
                    "resolution_errors": MetadataValue.int(0),
                    "transform_versions_used": MetadataValue.text("HELIX/v1=8"),
                },
            )
        )

        ctx = build_asset_context(instance=instance)
        report = coord_enrichment_report(ctx, _empty_tagging(), _empty_observer())

    assert report["summary"]["total_writes"] == 8
    assert report["summary"]["leaf_partitions_covered"] == 1
    assert report["leaves_unmaterialized"]["enriched_helix_alpss"] == 2
    assert "HELIX/pdv_alpss_output" in report["leaves"]
    assert report["leaves"]["HELIX/pdv_alpss_output"]["counts"]["written"] == 8
    assert (
        report["leaves"]["HELIX/pdv_alpss_output"]["transform_versions_used"]
        == "HELIX/v1=8"
    )
