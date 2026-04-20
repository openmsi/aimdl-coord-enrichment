"""Shared Dagster Config class for the coord_enrichment DAG."""

from typing import Optional

from dagster import Config


class CoordEnrichmentConfig(Config):
    """Run-time configuration for the coordinate enrichment DAG.

    dry_run                — if True, assets perform all reads and
                              compute would-be writes but skip the
                              actual Girder PUT. Default True.
    manifest_tracking_item_id
                           — Girder item id where the manifest asset
                             writes `meta.coord_enrichment_status`.
                             May be None during development; assets
                             log and skip the manifest write when
                             not configured. Production runs MUST
                             set this via env var or RunConfig.
    """

    dry_run: bool = True
    manifest_tracking_item_id: Optional[str] = None
