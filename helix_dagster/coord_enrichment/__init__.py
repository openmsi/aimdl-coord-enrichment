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
from helix_dagster.coord_enrichment.provenance_tagging import (
    all_helix_alpss_tagged,
    maxima_prov_targets_resolve,
    provenance_tagged_items,
)
from helix_dagster.coord_enrichment.enrichment_leaves import (
    enriched_maxima_raw,
    enrichment_success_rate_maxima_raw,
    no_coord_transform_failures_maxima_raw,
)
from helix_dagster.coord_enrichment.inheritance import (
    InheritedCoords,
    inherit_from_parent,
    inherited_station_coord_source,
)
from helix_dagster.coord_enrichment.overwrite import should_write
from helix_dagster.coord_enrichment.report import coord_enrichment_report
from helix_dagster.coord_enrichment.manifest import coord_enrichment_manifest

__all__ = [
    "CoordEnrichmentConfig",
    "coord_transform_config_snapshot",
    "MAXIMA_RAW_PARTITIONS",
    "enrichable_items_inventory",
    "inventory_nonempty_per_instrument",
    "all_helix_alpss_tagged",
    "maxima_prov_targets_resolve",
    "provenance_tagged_items",
    "enriched_maxima_raw",
    "enrichment_success_rate_maxima_raw",
    "no_coord_transform_failures_maxima_raw",
    "InheritedCoords",
    "inherit_from_parent",
    "inherited_station_coord_source",
    "should_write",
    "coord_enrichment_report",
    "coord_enrichment_manifest",
]
