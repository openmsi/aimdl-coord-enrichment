"""Dagster asset checks for data quality surfacing.

These checks evaluate the output of each critical asset and produce
pass/warn/fail indicators visible in the Dagster UI. They do NOT block
pipeline execution — they are advisory. Each check reads a single
asset's bundled output dict.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)


@asset_check(asset="pdv_log")
def igsn_validity_rate(context, pdv_log):
    """WARN if fewer than 80% of rows have valid IGSNs."""
    df = pdv_log["dataframe"]
    total = len(df)
    if total == 0:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            metadata={"total_rows": 0, "validity_rate": 0.0},
            description="No rows to validate.",
        )
    valid_count = df["valid_igsn"].notna().sum()
    rate = valid_count / total
    passed = bool(rate >= 0.8)
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "total_rows": total,
            "valid_count": int(valid_count),
            "validity_rate": round(float(rate), 3),
        },
        description=f"IGSN validity rate: {rate:.1%} ({valid_count}/{total})",
    )


@asset_check(asset="pdv_data")
def zero_pdv_inventory(context, pdv_data):
    """ERROR if the PDV trace inventory returned zero items.

    This typically means meta.igsn has not been tagged on PDV files yet,
    so the /aimdl/datafiles endpoint returns nothing.
    """
    count = pdv_data["inventory_count"]
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


@asset_check(asset="pdv_data")
def pdv_match_rate(context, pdv_data):
    """WARN if fewer than 50% of rows with PDV filenames were matched."""
    rows_with_pdv = pdv_data["rows_with_pdv"]
    matched_count = pdv_data["matched_count"]
    rate = matched_count / rows_with_pdv if rows_with_pdv > 0 else 0.0
    passed = rate >= 0.5
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "rows_with_pdv_filename": rows_with_pdv,
            "matched_count": matched_count,
            "match_rate": round(rate, 3),
        },
        description=f"PDV match rate: {rate:.1%} ({matched_count}/{rows_with_pdv})",
    )


@asset_check(asset="pdv_data")
def igsn_consistency(context, pdv_data):
    """ERROR if any matched PDV item has a different IGSN than the spreadsheet row."""
    issues = pdv_data["pdv_issues"]
    mismatches = [i for i in issues if i.get("type") == "igsn_mismatch"]
    passed = len(mismatches) == 0
    metadata = {"mismatch_count": len(mismatches)}
    if mismatches:
        examples = mismatches[:3]
        metadata["examples"] = str(examples)
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata=metadata,
        description=(
            "No IGSN mismatches."
            if passed
            else f"{len(mismatches)} IGSN mismatch(es) between spreadsheet and Girder items."
        ),
    )


@asset_check(asset="pdv_data")
def enrichment_success_rate(context, pdv_data):
    """WARN if fewer than 90% of matched items were successfully enriched.

    Counts dry-run simulated writes as successes so a rehearsal reads as
    representative of a live run.
    """
    matched_count = pdv_data["matched_count"]
    written_count = pdv_data["written_count"]
    simulated_count = pdv_data.get("simulated_count", 0)
    success = written_count + simulated_count
    rate = success / matched_count if matched_count > 0 else 0.0
    error_count = len(pdv_data["write_errors"])
    passed = rate >= 0.9
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "matched_count": matched_count,
            "written_count": written_count,
            "simulated_count": simulated_count,
            "error_count": error_count,
            "success_rate": round(rate, 3),
        },
        description=f"Enrichment success rate: {rate:.1%} ({success}/{matched_count})",
    )


@asset_check(asset="pdv_data")
def coord_transform_check(context, pdv_data):
    """WARN on coordinate transform issues.

    Fails (WARN) if any of:
      - coord_failures > 0         (transform raised)
      - version_counter is empty while writes happened
      - yaml_sha256 is None         (YAML could not be hashed)
    """
    failures = pdv_data.get("coord_failures", 0)
    version_counter = pdv_data.get("version_counter", {}) or {}
    yaml_sha256 = pdv_data.get("yaml_sha256")
    attempted = pdv_data.get("written_count", 0) + pdv_data.get("simulated_count", 0)

    unresolved_versions = attempted > 0 and not version_counter
    missing_sha = yaml_sha256 is None

    passed = (failures == 0) and (not unresolved_versions) and (not missing_sha)

    problems = []
    if failures > 0:
        problems.append(f"{failures} transform failures")
    if unresolved_versions:
        problems.append("no transform version resolved for any write")
    if missing_sha:
        problems.append("yaml_sha256 unavailable (provenance incomplete)")

    description = (
        "Transforms OK: " + ", ".join(f"{k}={v}" for k, v in sorted(version_counter.items()))
        if passed else "; ".join(problems)
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "coord_failures": failures,
            "version_counter": str(sorted(version_counter.items())),
            "yaml_sha256_present": not missing_sha,
        },
        description=description,
    )


@asset_check(asset="pdv_processing_manifest")
def manifest_written(context, pdv_processing_manifest):
    """ERROR if the processing manifest was not written to any source item."""
    written = pdv_processing_manifest.get("manifest_written", False)
    return AssetCheckResult(
        passed=bool(written),
        severity=AssetCheckSeverity.ERROR,
        metadata={"status": pdv_processing_manifest.get("status", "unknown")},
        description=(
            "Processing manifest written to source log item(s)."
            if written
            else "Processing manifest write FAILED for one or more source items."
        ),
    )
