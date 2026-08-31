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


def eval_zero_inventory(md):
    """ERROR if the PDV trace inventory returned zero items."""
    count = int(md.get("inventory_count", 0))
    passed = count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"item_count": count},
        description=(
            f"Inventory contains {count} items."
            if passed
            else "Inventory is EMPTY. Check that meta.igsn is tagged on PDV files."
        ),
    )


def eval_pdv_match_rate(md):
    """WARN if fewer than 50% of *fired* shots were matched to a PDV trace.

    A HELIX log rows every candidate shot; the station decides at fire time
    whether to proceed. Rows for shots that never fired carry no PDV filename
    and have no trace to match, so counting them would make a fully-successful
    partition look half-failed. The denominator is rows with a filename, which
    is the same population as fired shots (see spreadsheet.shot_fired).
    """
    rows_with_pdv = int(md.get("rows_with_pdv", 0))
    matched_count = int(md.get("matched_count", 0))
    not_fired = int(md.get("shots_not_fired", 0))
    rate = matched_count / rows_with_pdv if rows_with_pdv > 0 else 0.0
    if rows_with_pdv == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "shots_not_fired": not_fired,
                "rows_with_pdv_filename": 0,
            },
            description=(
                f"No shots fired in this partition ({not_fired} candidate "
                "shot(s) skipped at the station); nothing to match."
            ),
        )
    suffix = f"; {not_fired} candidate shot(s) not fired" if not_fired else ""
    return AssetCheckResult(
        passed=rate >= 0.5,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "rows_with_pdv_filename": rows_with_pdv,
            "matched_count": matched_count,
            "match_rate": round(rate, 3),
            "shots_not_fired": not_fired,
        },
        description=(
            f"PDV match rate: {rate:.1%} ({matched_count}/{rows_with_pdv})"
            f"{suffix}"
        ),
    )


def eval_igsn_consistency(md):
    """ERROR if any matched PDV item has a different IGSN than the row."""
    mismatch_count = int(md.get("igsn_mismatch_count", 0))
    passed = mismatch_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"mismatch_count": mismatch_count},
        description=(
            "No IGSN mismatches."
            if passed
            else f"{mismatch_count} IGSN mismatch(es) between spreadsheet and Girder items."
        ),
    )


def eval_enrichment_success_rate(md):
    """WARN if fewer than 90% of matched items were successfully enriched.

    Counts dry-run simulated writes as successes so a rehearsal reads as
    representative of a live run.
    """
    matched_count = int(md.get("matched_count", 0))
    success = int(md.get("items_enriched", 0)) + int(md.get("items_simulated", 0))
    rate = success / matched_count if matched_count > 0 else 0.0
    return AssetCheckResult(
        passed=rate >= 0.9,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "matched_count": matched_count,
            "enriched_or_simulated": success,
            "write_errors": int(md.get("write_errors_count", 0)),
            "success_rate": round(rate, 3),
        },
        description=f"Enrichment success rate: {rate:.1%} ({success}/{matched_count})",
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
    """ERROR if the processing manifest was not written to any source item."""
    written = bool(md.get("manifest_written", False))
    return AssetCheckResult(
        passed=written,
        severity=AssetCheckSeverity.ERROR,
        metadata={"status": md.get("status", "unknown")},
        description=(
            "Processing manifest written to source log item(s)."
            if written
            else "No source log item to write to (partition resolved zero "
            "pdv_experiment_log items), or a real write failed."
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
def zero_pdv_inventory(context):
    md = latest_partition_metadata(
        context.instance, "pdv_data", str(context.partition_key)
    )
    if md is None:
        return no_materialization_result(AssetCheckSeverity.ERROR)
    return eval_zero_inventory(md)


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
