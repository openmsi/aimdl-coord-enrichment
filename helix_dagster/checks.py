"""Dagster asset checks for data quality surfacing.

These checks evaluate the output of each critical asset and produce
pass/warn/fail indicators visible in the Dagster UI. They do NOT block
pipeline execution — they are advisory.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetIn,
    asset_check,
)


@asset_check(asset="pdv_trace_inventory")
def zero_inventory(context, pdv_trace_inventory):
    """ERROR if the PDV trace inventory returned zero items.

    This typically means meta.igsn has not been tagged on PDV files yet,
    so the /aimdl/datafiles endpoint returns nothing.
    """
    count = len(pdv_trace_inventory)
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


@asset_check(asset="validated_rows")
def igsn_validity_rate(context, validated_rows):
    """WARN if fewer than 80% of rows have valid IGSNs."""
    df = validated_rows["dataframe"]
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


@asset_check(asset="pdv_cross_references", additional_ins={"validated_rows": AssetIn("validated_rows")})
def pdv_match_rate(context, pdv_cross_references, validated_rows):
    """WARN if fewer than 50% of rows with PDV filenames were matched."""
    df = validated_rows["dataframe"]
    import math
    rows_with_pdv = sum(
        1
        for _, row in df.iterrows()
        if row.get("PDV_FileName") is not None
        and not (isinstance(row.get("PDV_FileName"), float) and math.isnan(row.get("PDV_FileName")))
        and str(row.get("PDV_FileName")).strip() != ""
    )
    matched_count = len(pdv_cross_references["matches"])
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


@asset_check(asset="pdv_cross_references")
def igsn_consistency(context, pdv_cross_references):
    """ERROR if any matched PDV item has a different IGSN than the spreadsheet row."""
    issues = pdv_cross_references["pdv_issues"]
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


@asset_check(asset="enriched_pdv_metadata", additional_ins={"pdv_cross_references": AssetIn("pdv_cross_references")})
def enrichment_success_rate(context, enriched_pdv_metadata, pdv_cross_references):
    """WARN if fewer than 90% of matched items were successfully enriched."""
    matched_count = len(pdv_cross_references["matches"])
    written_count = enriched_pdv_metadata["written_count"]
    rate = written_count / matched_count if matched_count > 0 else 0.0
    error_count = len(enriched_pdv_metadata["write_errors"])
    passed = rate >= 0.9
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "matched_count": matched_count,
            "written_count": written_count,
            "error_count": error_count,
            "success_rate": round(rate, 3),
        },
        description=f"Enrichment success rate: {rate:.1%} ({written_count}/{matched_count})",
    )


@asset_check(asset="enriched_pdv_metadata")
def coord_transform_check(context, enriched_pdv_metadata):
    """WARN on coordinate transform issues.

    Fails (WARN) if any of:
      - coord_failures > 0         (transform raised)
      - version_counter is empty while writes happened
      - yaml_sha256 is None         (YAML could not be hashed)
    """
    failures = enriched_pdv_metadata.get("coord_failures", 0)
    version_counter = enriched_pdv_metadata.get("version_counter", {}) or {}
    yaml_sha256 = enriched_pdv_metadata.get("yaml_sha256")
    written_count = enriched_pdv_metadata.get("written_count", 0)

    unresolved_versions = written_count > 0 and not version_counter
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
