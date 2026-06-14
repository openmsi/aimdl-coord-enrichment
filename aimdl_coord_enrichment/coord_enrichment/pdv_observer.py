"""helix_pdv_coverage_observer asset.

Read-only: counts pdv_trace items by coord_provenance coverage.
Does not write to Girder.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from aimdl_coord_enrichment.girder_io import fetch_all_aimdl_datafiles
from aimdl_coord_enrichment.resources import GirderConnection

PDV_COVERAGE_WARN_THRESHOLD = 0.5


@asset(group_name="coord_enrichment_reporting")
def helix_pdv_coverage_observer(
    context: AssetExecutionContext,
    girder: GirderConnection,
):
    """Count pdv_trace items by coord_provenance coverage.

    Buckets:
      fully_enriched — Station_X/Y present AND coord_provenance present
      partial        — one but not the other
      unenriched     — neither present
      missing_igsn   — no meta.igsn (tagging backlog)
    """
    items = fetch_all_aimdl_datafiles(girder, "pdv_trace")

    fully_enriched = 0
    partial = 0
    unenriched = 0
    missing_igsn = 0

    for it in items:
        meta = it.get("meta") or {}
        if not meta.get("igsn"):
            missing_igsn += 1
            continue
        has_coords = (
            meta.get("Station_X") is not None
            and meta.get("Station_Y") is not None
        )
        has_prov = isinstance(meta.get("coord_provenance"), dict)
        if has_coords and has_prov:
            fully_enriched += 1
        elif has_coords or has_prov:
            partial += 1
        else:
            unenriched += 1

    total = len(items)
    coverage_rate = fully_enriched / total if total else 0.0

    context.add_output_metadata({
        "total": MetadataValue.int(total),
        "fully_enriched": MetadataValue.int(fully_enriched),
        "partial": MetadataValue.int(partial),
        "unenriched": MetadataValue.int(unenriched),
        "missing_igsn": MetadataValue.int(missing_igsn),
        "coverage_rate": MetadataValue.float(round(coverage_rate, 3)),
    })

    return {
        "total": total,
        "fully_enriched": fully_enriched,
        "partial": partial,
        "unenriched": unenriched,
        "missing_igsn": missing_igsn,
        "coverage_rate": coverage_rate,
    }


@asset_check(asset="helix_pdv_coverage_observer")
def pdv_coverage_above_threshold(context, helix_pdv_coverage_observer):
    """WARN if fully-enriched pdv_trace coverage is below threshold."""
    rate = helix_pdv_coverage_observer["coverage_rate"]
    passed = rate >= PDV_COVERAGE_WARN_THRESHOLD
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "coverage_rate": MetadataValue.float(round(rate, 3)),
            "threshold": MetadataValue.float(PDV_COVERAGE_WARN_THRESHOLD),
            "fully_enriched": MetadataValue.int(
                helix_pdv_coverage_observer["fully_enriched"]
            ),
            "total": MetadataValue.int(helix_pdv_coverage_observer["total"]),
        },
        description=(
            f"PDV coord coverage {rate:.1%} "
            f"{'≥' if passed else '<'} threshold "
            f"{PDV_COVERAGE_WARN_THRESHOLD:.1%}"
        ),
    )
