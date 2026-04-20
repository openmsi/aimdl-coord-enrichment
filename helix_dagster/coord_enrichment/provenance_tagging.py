"""provenance_tagged_items asset.

Tags `meta.prov.wasDerivedFrom` on derived items that either have
no prov at all (HELIX ALPSS) or have a dangling
`wasDerivedFrom` pointer (MAXIMA xrd_derived in test environments).
"""

from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    MetadataValue,
    asset,
    asset_check,
)

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.girder_io import fetch_all_aimdl_datafiles
from helix_dagster.instruments import (
    HELIX_DERIVED_DATA_TYPES,
    INSTRUMENT_HELIX,
    INSTRUMENT_MAXIMA,
    MAXIMA_DERIVED_DATA_TYPES,
    resolve_parent_item_id,
)
from helix_dagster.resources import GirderConnection


def _decide(stored: str | None, resolved: str | None) -> str:
    """Return one of: 'not_resolvable', 'already_correct', 'to_write', 'to_overwrite'."""
    if resolved is None:
        return "not_resolvable"
    if stored is None:
        return "to_write"
    if stored == resolved:
        return "already_correct"
    return "to_overwrite"


def _merged_prov(existing: dict | None, new_parent: str) -> dict:
    """Return a prov dict that preserves existing keys and sets/updates wasDerivedFrom."""
    out: dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}
    out["wasDerivedFrom"] = new_parent
    return out


def _apply_decision(
    *,
    context,
    item,
    stored,
    resolved,
    decision,
    partition_key,
    counters,
    unresolved,
    write_ops,
    girder,
    dry_run,
):
    """Execute the tagging decision: update counters, optionally write."""
    item_id = item.get("_id")
    if decision == "not_resolvable":
        counters["unresolvable"] += 1
        unresolved.append(
            {
                "partition": partition_key,
                "item_id": item_id,
                "name": item.get("name", ""),
            }
        )
        return
    if decision == "already_correct":
        counters["already_correct"] += 1
        return
    merged = _merged_prov((item.get("meta") or {}).get("prov"), resolved)
    op: dict[str, Any] = {
        "partition": partition_key,
        "item_id": item_id,
        "name": item.get("name", ""),
        "decision": decision,
        "stored": stored,
        "new_parent": resolved,
    }
    if dry_run:
        op["simulated"] = True
        counters["skipped_dry_run"] += 1
        write_ops.append(op)
        return
    try:
        girder.addMetadataToItem(item_id, {"prov": merged})
        op["simulated"] = False
        write_ops.append(op)
        if decision == "to_write":
            counters["written"] += 1
        else:
            counters["overwritten"] += 1
    except Exception as exc:
        context.log.error(
            "Failed to write prov for item %s: %s", item_id, exc
        )
        op["simulated"] = False
        op["write_failed"] = True
        op["error"] = str(exc)
        write_ops.append(op)


@asset
def provenance_tagged_items(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    enrichable_items_inventory: dict[str, list[dict[str, Any]]],
    girder: GirderConnection,
) -> dict[str, Any]:
    """Tag prov.wasDerivedFrom on in-scope derived items.

    Returns a structured result with per-partition counts, the set
    of items that remained unresolved, and a list of all write
    operations that were either performed (live) or simulated
    (dry_run).
    """
    pdv_key = f"{INSTRUMENT_HELIX}/pdv_trace"
    pdv_inventory = enrichable_items_inventory.get(pdv_key, [])
    if not pdv_inventory:
        pdv_inventory = [
            it for it in fetch_all_aimdl_datafiles(girder, "pdv_trace")
            if (it.get("meta") or {}).get("igsn")
        ]
        context.log.info(
            "Fetched %d pdv_trace items for HELIX ALPSS parent lookup.",
            len(pdv_inventory),
        )

    counters: dict[str, dict[str, int]] = {}
    unresolved: list[dict[str, str]] = []
    write_ops: list[dict[str, Any]] = []

    for dt in sorted(HELIX_DERIVED_DATA_TYPES):
        partition_key = f"{INSTRUMENT_HELIX}/{dt}"
        items = enrichable_items_inventory.get(partition_key, [])
        c = counters.setdefault(
            partition_key,
            {"already_correct": 0, "written": 0, "overwritten": 0,
             "unresolvable": 0, "skipped_dry_run": 0},
        )
        for item in items:
            stored = ((item.get("meta") or {}).get("prov") or {}).get("wasDerivedFrom")
            try:
                resolved = resolve_parent_item_id(item, pdv_inventory=pdv_inventory)
            except Exception as exc:
                context.log.error(
                    "HELIX parent resolution crashed for item %s: %s",
                    item.get("_id"), exc,
                )
                resolved = None
            decision = _decide(stored, resolved)
            _apply_decision(
                context=context, item=item, stored=stored, resolved=resolved,
                decision=decision, partition_key=partition_key, counters=c,
                unresolved=unresolved, write_ops=write_ops, girder=girder,
                dry_run=config.dry_run,
            )

    for dt in sorted(MAXIMA_DERIVED_DATA_TYPES):
        partition_key = f"{INSTRUMENT_MAXIMA}/{dt}"
        items = enrichable_items_inventory.get(partition_key, [])
        c = counters.setdefault(
            partition_key,
            {"already_correct": 0, "written": 0, "overwritten": 0,
             "unresolvable": 0, "skipped_dry_run": 0},
        )
        for item in items:
            stored = ((item.get("meta") or {}).get("prov") or {}).get("wasDerivedFrom")
            try:
                resolved = resolve_parent_item_id(item, girder=girder)
            except Exception as exc:
                context.log.error(
                    "MAXIMA parent resolution crashed for item %s: %s",
                    item.get("_id"), exc,
                )
                resolved = None
            decision = _decide(stored, resolved)
            _apply_decision(
                context=context, item=item, stored=stored, resolved=resolved,
                decision=decision, partition_key=partition_key, counters=c,
                unresolved=unresolved, write_ops=write_ops, girder=girder,
                dry_run=config.dry_run,
            )

    context.add_output_metadata(
        {
            "total_unresolved": MetadataValue.int(len(unresolved)),
            "total_writes": MetadataValue.int(
                sum(c["written"] + c["overwritten"] for c in counters.values())
            ),
            "dry_run": MetadataValue.bool(config.dry_run),
            "per_partition": MetadataValue.text(
                "\n".join(f"{k}: {v}" for k, v in sorted(counters.items()))
            ),
        }
    )

    return {
        "counters": counters,
        "unresolved": unresolved,
        "write_ops": write_ops,
        "dry_run": config.dry_run,
    }


@asset_check(asset="provenance_tagged_items")
def all_helix_alpss_tagged(context, provenance_tagged_items):
    """ERROR if any HELIX ALPSS item has unresolved prov after the tagging pass."""
    unresolved = provenance_tagged_items.get("unresolved", [])
    helix_unresolved = [
        u for u in unresolved if u["partition"].startswith(f"{INSTRUMENT_HELIX}/")
    ]
    passed = len(helix_unresolved) == 0
    examples = [f"{u['partition']}: {u['name']}" for u in helix_unresolved[:3]]
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "unresolved_count": MetadataValue.int(len(helix_unresolved)),
            "examples": MetadataValue.text(", ".join(examples) or "none"),
        },
        description=(
            "All HELIX ALPSS items have resolvable parent PDV traces."
            if passed
            else f"{len(helix_unresolved)} HELIX ALPSS item(s) unresolved"
        ),
    )


@asset_check(asset="provenance_tagged_items")
def maxima_prov_targets_resolve(context, provenance_tagged_items):
    """ERROR if any MAXIMA derived item's prov target cannot be resolved."""
    unresolved = provenance_tagged_items.get("unresolved", [])
    maxima_unresolved = [
        u for u in unresolved if u["partition"].startswith(f"{INSTRUMENT_MAXIMA}/")
    ]
    passed = len(maxima_unresolved) == 0
    examples = [f"{u['partition']}: {u['name']}" for u in maxima_unresolved[:3]]
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "unresolved_count": MetadataValue.int(len(maxima_unresolved)),
            "examples": MetadataValue.text(", ".join(examples) or "none"),
        },
        description=(
            "All MAXIMA derived items have resolvable master.h5 parents."
            if passed
            else f"{len(maxima_unresolved)} MAXIMA derived item(s) unresolved"
        ),
    )
