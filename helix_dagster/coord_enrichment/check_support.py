"""Shared support for the partitioned-leaf asset checks.

The leaf asset checks must NOT take their partitioned asset as a
positional input: doing so makes Dagster's IOManager load the asset
across the partition cross-product, which fails with FileNotFoundError
on any instance where some partition has no materialized output (the
normal case for a dynamically-partitioned asset). This mirrors the
fix already applied to ``coord_enrichment_report`` (it reads the
event log via ``deps=`` instead of taking leaf outputs as ins).

Each check instead reads its own partition's latest materialization
metadata from the event log via :func:`latest_partition_metadata`,
then applies one of the pure decision helpers below. The helpers are
kept pure so the leaf unit tests can exercise the decision logic
without standing up an instance.
"""

from typing import Any, Optional

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    AssetRecordsFilter,
    MetadataValue,
)


def latest_partition_metadata(
    instance, asset_name: str, partition_key: str
) -> Optional[dict[str, Any]]:
    """Latest materialization metadata for one (asset, partition).

    Returns a flat dict of unwrapped scalar values (the ``.value`` of
    each Dagster ``MetadataValue``), or ``None`` if no materialization
    has been recorded for that partition. Scoped to the single
    partition via ``AssetRecordsFilter``.
    """
    result = instance.fetch_materializations(
        AssetRecordsFilter(
            asset_key=AssetKey(asset_name),
            asset_partitions=[partition_key],
        ),
        limit=1,
    )
    if not result.records:
        return None
    mat = (
        result.records[0]
        .event_log_entry.dagster_event.event_specific_data.materialization
    )
    md = dict(mat.metadata) if mat.metadata else {}
    return {k: getattr(v, "value", v) for k, v in md.items()}


def no_materialization_result(
    severity: AssetCheckSeverity = AssetCheckSeverity.WARN,
) -> AssetCheckResult:
    """Result to return when the partition has no materialization yet.

    A missing materialization is an infrastructure condition, not a
    data condition, so the check passes with an explanatory note
    rather than failing the run.
    """
    return AssetCheckResult(
        passed=True,
        severity=severity,
        metadata={"note": MetadataValue.text("no materialization for partition")},
        description="No materialization recorded for this partition; nothing to check.",
    )


def evaluate_success_rate(
    *,
    seen: int,
    written: int,
    simulated_dry_run: int,
    skipped_no_change: int,
    resolution_errors: int,
    write_errors_count: int,
    partition_label: str,
) -> AssetCheckResult:
    """WARN if <90% of items in the partition ended in a successful decision."""
    if seen == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"note": MetadataValue.text("partition empty")},
            description="Partition empty; no items to check.",
        )
    success = written + simulated_dry_run + skipped_no_change
    rate = success / seen
    return AssetCheckResult(
        passed=rate >= 0.9,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "success_rate": MetadataValue.float(round(rate, 3)),
            "write_errors": MetadataValue.int(write_errors_count),
            "resolution_errors": MetadataValue.int(resolution_errors),
            "partition": MetadataValue.text(partition_label),
        },
        description=f"Success rate: {rate:.1%} ({success}/{seen})",
    )


def evaluate_coord_failures(coord_failures: int) -> AssetCheckResult:
    """WARN if any coordinate transform returned None."""
    passed = coord_failures == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"coord_failures": MetadataValue.int(coord_failures)},
        description=(
            "No coordinate transform failures."
            if passed
            else f"{coord_failures} coordinate transform failure(s)."
        ),
    )


def evaluate_provenance_valid(
    inherit_count: int, examples_text: str
) -> AssetCheckResult:
    """ERROR if any xrd_derived item failed parent (inherit) resolution."""
    passed = inherit_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "unresolved_count": MetadataValue.int(inherit_count),
            "examples": MetadataValue.text(examples_text or "none"),
        },
        description=(
            "All xrd_derived items have valid prov links and resolvable parents."
            if passed
            else f"{inherit_count} xrd_derived item(s) failed parent resolution"
        ),
    )
