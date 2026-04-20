"""coord_enrichment_manifest asset."""

import os
from datetime import datetime, timezone
from typing import Any

from dagster import AssetExecutionContext, MetadataValue, asset

from helix_dagster import __version__ as PIPELINE_VERSION
from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.resources import GirderConnection


@asset
def coord_enrichment_manifest(
    context: AssetExecutionContext,
    config: CoordEnrichmentConfig,
    coord_enrichment_report: dict[str, Any],
    girder: GirderConnection,
) -> dict[str, Any]:
    """Write a status record to a configurable Girder tracking item.

    When config.manifest_tracking_item_id is None the asset logs a
    warning and skips the Girder PUT -- useful during development.

    When config.dry_run is True the asset also skips the PUT but
    still returns the would-be payload.
    """
    try:
        run_id = context.run.run_id
    except Exception:
        run_id = "direct-invocation"

    manifest = {
        "last_processed": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": run_id,
        "pipeline_version": PIPELINE_VERSION,
        "job": "coord_enrichment_job",
        "dry_run": config.dry_run,
        "report": coord_enrichment_report,
    }

    item_id = config.manifest_tracking_item_id
    if item_id is None:
        item_id = os.environ.get("COORD_ENRICHMENT_MANIFEST_ITEM") or None
    tracking_item_source = (
        "config" if config.manifest_tracking_item_id is not None
        else ("env" if item_id is not None else "unset")
    )
    if item_id is None:
        context.log.warning(
            "manifest_tracking_item_id not configured; skipping Girder write. "
            "Set CoordEnrichmentConfig.manifest_tracking_item_id to enable."
        )
        manifest["write_skipped"] = "no_tracking_item_configured"
    elif config.dry_run:
        context.log.info(
            "dry_run=True; would have written manifest to %s", item_id
        )
        manifest["write_skipped"] = "dry_run"
    else:
        try:
            girder.addMetadataToItem(item_id, {"coord_enrichment_status": manifest})
            context.log.info(
                "Wrote coord_enrichment manifest to Girder item %s", item_id
            )
        except Exception as exc:
            context.log.error(
                "Failed to write coord_enrichment manifest to %s: %s", item_id, exc
            )
            manifest["write_failed"] = True
            manifest["error"] = str(exc)

    context.add_output_metadata(
        {
            "tracking_item_id": MetadataValue.text(item_id or "<unset>"),
            "tracking_item_source": MetadataValue.text(tracking_item_source),
            "dry_run": MetadataValue.bool(config.dry_run),
            "written": MetadataValue.bool(
                item_id is not None
                and not config.dry_run
                and not manifest.get("write_failed")
                and not manifest.get("write_skipped")
            ),
        }
    )
    return manifest
