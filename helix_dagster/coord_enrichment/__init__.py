"""Coordinate enrichment DAG for AIMD-L Girder items.

This subpackage contains the new Dagster DAG that propagates
sample-frame coordinates to AIMD-L items carrying `meta.igsn` and
a recognized `meta.data_type`. It is separate from the existing
spreadsheet-driven DAG (`process_helix_assets_job`), which remains
unchanged.

Module layout (populated across Phase 3 steps):

  config.py              — shared Dagster Config class
  config_snapshot.py     — coord_transform_config_snapshot asset
  inventory.py           — enrichable_items_inventory asset + partitions
  provenance_tagging.py  — provenance_tagged_items asset + checks
  enrichment_leaves.py   — enriched_maxima_raw asset + checks
  overwrite.py           — pure overwrite-policy evaluator
  cache.py               — per-run-folder cache helpers
  report.py              — coord_enrichment_report asset
  manifest.py            — coord_enrichment_manifest asset
"""

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import (
    coord_transform_config_snapshot,
)
from helix_dagster.coord_enrichment.inventory import (
    MAXIMA_RAW_PARTITIONS,
    enrichable_items_inventory,
    inventory_nonempty_per_instrument,
)

__all__ = [
    "CoordEnrichmentConfig",
    "coord_transform_config_snapshot",
    "MAXIMA_RAW_PARTITIONS",
    "enrichable_items_inventory",
    "inventory_nonempty_per_instrument",
]
