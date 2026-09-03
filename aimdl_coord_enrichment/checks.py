"""Dagster asset checks for data quality surfacing.

These checks evaluate the output of each helix_spreadsheet asset and
produce pass/warn/fail indicators visible in the Dagster UI. They do NOT
block pipeline execution — they are advisory.

The assets are partitioned, so the checks must NOT take their asset as a
positional input: that would force the IOManager to load the asset by
partition key, which fails across the dynamic-partition cross-product
(FileNotFoundError). Instead each check reads its partition's latest
materialization metadata from the event log via
``latest_partition_metadata`` and applies a pure decision helper. This
mirrors the coord_enrichment leaf checks (see
``coord_enrichment/check_support.py``).
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from aimdl_coord_enrichment.coord_enrichment.check_support import (
    latest_partition_metadata,
    no_materialization_result,
)


# --- pure decision helpers (unit-tested directly with a flat metadata dict) ---


def eval_igsn_validity(md):
    """WARN if fewer than 80% of rows have valid IGSNs."""
    total = int(md.get("row_count", 0))
    if total == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"total_rows": 0, "validity_rate": 0.0},
            description="No rows to validate.",
        )
    valid_count = int(md.get("valid_igsn_count", 0))
    rate = valid_count / total
    return AssetCheckResult(
        passed=bool(rate >= 0.8),
        severity=AssetCheckSeverity.WARN,
        metadata={
            "total_rows": total,
            "valid_count": valid_count,
            "validity_rate": round(float(rate), 3),
        },
        description=f"IGSN validity rate: {rate:.1%} ({valid_count}/{total})",
    )


def eval_zero_traces(md):
    """ERROR if the partition resolved zero PDV traces.

    Partitions come from the trace index, so every one of them should hold at
    least one trace. Zero means the index and the detail endpoint disagree.
    """
    count = int(md.get("traces_in_partition", 0))
    passed = count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"traces_in_partition": count},
        description=(
            f"Partition holds {count} PDV trace(s)."
            if passed
            else "Partition resolved ZERO traces despite appearing in the "
                 "pdv_trace partition index."
        ),
    )


def eval_pdv_match_rate(md):
    """WARN if fewer than 50% of this partition's traces found their log row.

    The denominator is the traces in the partition — the work that exists.

    A partition whose experiment log is not tagged upstream has no rows to pair
    against, so there is nothing this pipeline can do and nothing for an
    operator to act on. It passes, with ``log_items: 0`` recorded so the
    condition stays queryable without turning the run red.
    """
    traces = int(md.get("traces_in_partition", 0))
    paired = int(md.get("paired_count", 0))
    has_log = int(md.get("log_items", 0)) > 0

    if traces == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"traces_in_partition": 0},
            description="No traces in this partition; nothing to pair.",
        )
    if not has_log:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"traces_in_partition": traces, "paired_count": paired,
                      "log_items": 0},
            description=(
                f"No experiment log tagged for this partition; its {traces} "
                "trace(s) have nothing to pair against."
            ),
        )
    rate = paired / traces
    return AssetCheckResult(
        passed=rate >= 0.5,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "traces_in_partition": traces,
            "paired_count": paired,
            "match_rate": round(rate, 3),
            "unpaired_by_reason": md.get("unpaired_by_reason", ""),
        },
        description=f"Trace pairing rate: {rate:.1%} ({paired}/{traces})",
    )


def eval_igsn_consistency(md):
    """ERROR if any trace paired to a row declaring a different sample.

    Such a row is refused, never applied, so this reports a contradictory log
    in the partition rather than a bad write.
    """
    mismatch_count = int(md.get("igsn_mismatch_count", 0))
    passed = mismatch_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"mismatch_count": mismatch_count},
        description=(
            "No IGSN disagreement between traces and their log rows."
            if passed
            else f"{mismatch_count} trace(s) matched a log row declaring a "
                 "different IGSN; those traces were left untouched."
        ),
    )


def eval_enrichment_success_rate(md):
    """WARN if fewer than 90% of *paired* traces were successfully enriched.

    The denominator is traces that found their row — the work this asset can
    actually do. Whether a trace found a row at all is pdv_match_rate's
    question, so a partition with no log reads as "nothing to enrich" here
    rather than failing twice for one cause.

    Counts dry-run simulated writes as successes so a rehearsal reads as
    representative of a live run.
    """
    paired = int(md.get("paired_count", 0))
    success = int(md.get("items_enriched", 0)) + int(md.get("items_simulated", 0))
    if paired == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"paired_count": 0},
            description="No traces paired to a log row; nothing to enrich.",
        )
    rate = success / paired
    return AssetCheckResult(
        passed=rate >= 0.9,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "paired_count": paired,
            "enriched_or_simulated": success,
            "write_errors": int(md.get("write_errors_count", 0)),
            "success_rate": round(rate, 3),
        },
        description=f"Enrichment success rate: {rate:.1%} ({success}/{paired})",
    )


def eval_coord_transform(md):
    """WARN on coordinate transform issues.

    Fails (WARN) if any of:
      - coordinate_transform_failures > 0   (transform raised)
      - no transform version resolved while writes were attempted
      - the transforms YAML could not be hashed (provenance incomplete)
    """
    failures = int(md.get("coordinate_transform_failures", 0))
    version_count = int(md.get("transform_version_count", 0))
    yaml_present = bool(md.get("yaml_sha256_present", False))
    attempted = int(md.get("items_enriched", 0)) + int(md.get("items_simulated", 0))

    unresolved_versions = attempted > 0 and version_count == 0
    missing_sha = not yaml_present

    passed = (failures == 0) and (not unresolved_versions) and (not missing_sha)

    problems = []
    if failures > 0:
        problems.append(f"{failures} transform failures")
    if unresolved_versions:
        problems.append("no transform version resolved for any write")
    if missing_sha:
        problems.append("yaml_sha256 unavailable (provenance incomplete)")

    description = (
        md.get("transform_versions_used", "transforms OK")
        if passed
        else "; ".join(problems)
    )
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "coord_failures": failures,
            "transform_version_count": version_count,
            "yaml_sha256_present": yaml_present,
        },
        description=description,
    )


def eval_manifest_written(md):
    """ERROR if a real manifest write failed.

    A partition whose log was never tagged upstream has no item to write to.
    That is an upstream gap already reported by pdv_match_rate, not a failure
    of this write, so it passes here with ``has_log`` recording the reason.
    """
    written = bool(md.get("manifest_written", False))
    has_log = bool(md.get("has_log", True))
    return AssetCheckResult(
        passed=written,
        severity=AssetCheckSeverity.ERROR,
        metadata={"status": md.get("status", "unknown"), "has_log": has_log},
        description=(
            "Processing manifest written to source log item(s)."
            if written and has_log
            else "No experiment log tagged for this partition; no manifest target."
            if written
            else "A manifest write to a source log item failed."
        ),
    )


# --- asset checks: read the partition's materialization metadata, then decide ---


@asset_check(asset="pdv_log")
def igsn_validity_rate(context):
    md = latest_partition_metadata(
        context.instance, "pdv_log", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return eval_igsn_validity(md)


@asset_check(asset="pdv_data")
def zero_traces_in_partition(context):
    md = latest_partition_metadata(
        context.instance, "pdv_data", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result(AssetCheckSeverity.ERROR)
    return eval_zero_traces(md)


@asset_check(asset="pdv_data")
def pdv_match_rate(context):
    md = latest_partition_metadata(
        context.instance, "pdv_data", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return eval_pdv_match_rate(md)


@asset_check(asset="pdv_data")
def igsn_consistency(context):
    md = latest_partition_metadata(
        context.instance, "pdv_data", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result(AssetCheckSeverity.ERROR)
    return eval_igsn_consistency(md)


@asset_check(asset="pdv_data")
def enrichment_success_rate(context):
    md = latest_partition_metadata(
        context.instance, "pdv_data", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return eval_enrichment_success_rate(md)


@asset_check(asset="pdv_data")
def coord_transform_check(context):
    md = latest_partition_metadata(
        context.instance, "pdv_data", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result()
    return eval_coord_transform(md)


@asset_check(asset="pdv_processing_manifest")
def manifest_written(context):
    md = latest_partition_metadata(
        context.instance, "pdv_processing_manifest", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result(AssetCheckSeverity.ERROR)
    return eval_manifest_written(md)
