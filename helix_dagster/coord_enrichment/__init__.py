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
  provenance_tagging.py  — helix_alpss_provenance_tagged asset + check
  enrichment_leaves.py   — enriched_maxima_raw asset + checks
  overwrite.py           — pure overwrite-policy evaluator
  cache.py               — per-run-folder cache helpers
  pdv_observer.py        — helix_pdv_coverage_observer asset + check
  report.py              — coord_enrichment_report asset
  manifest.py            — coord_enrichment_manifest asset
"""

from helix_dagster.coord_enrichment.config import CoordEnrichmentConfig
from helix_dagster.coord_enrichment.config_snapshot import (
    coord_transform_config_snapshot,
)
from helix_dagster.coord_enrichment.inventory import (
    MAXIMA_RAW_DATA_TYPE_PARTITIONS,
    MAXIMA_RAW_PARTITIONS,
    MAXIMA_RUN_PARTITIONS,
    enrichable_items_inventory,
    filter_to_raw_subfolder,
    inventory_nonempty_per_instrument,
)
from helix_dagster.coord_enrichment.provenance_tagging import (
    all_helix_alpss_tagged,
    helix_alpss_provenance_tagged,
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
from helix_dagster.coord_enrichment.helix_alpss_leaf import (
    HELIX_ALPSS_PARTITIONS,
    enriched_helix_alpss,
    enrichment_success_rate_helix_alpss,
    no_coord_transform_failures_helix_alpss,
)
from helix_dagster.coord_enrichment.maxima_derived_leaf import (
    MAXIMA_DERIVED_PARTITIONS,
    enriched_maxima_derived,
    enrichment_success_rate_maxima_derived,
    no_coord_transform_failures_maxima_derived,
)
from helix_dagster.coord_enrichment.pdv_observer import (
    helix_pdv_coverage_observer,
    pdv_coverage_above_threshold,
)
from helix_dagster.coord_enrichment.overwrite import should_write
from helix_dagster.coord_enrichment.report import coord_enrichment_report
from helix_dagster.coord_enrichment.manifest import coord_enrichment_manifest

__all__ = [
    "CoordEnrichmentConfig",
    "coord_transform_config_snapshot",
    "MAXIMA_RAW_DATA_TYPE_PARTITIONS",
    "MAXIMA_RAW_PARTITIONS",
    "MAXIMA_RUN_PARTITIONS",
    "enrichable_items_inventory",
    "filter_to_raw_subfolder",
    "inventory_nonempty_per_instrument",
    "all_helix_alpss_tagged",
    "helix_alpss_provenance_tagged",
    "enriched_maxima_raw",
    "enrichment_success_rate_maxima_raw",
    "no_coord_transform_failures_maxima_raw",
    "HELIX_ALPSS_PARTITIONS",
    "enriched_helix_alpss",
    "enrichment_success_rate_helix_alpss",
    "no_coord_transform_failures_helix_alpss",
    "MAXIMA_DERIVED_PARTITIONS",
    "enriched_maxima_derived",
    "enrichment_success_rate_maxima_derived",
    "no_coord_transform_failures_maxima_derived",
    "InheritedCoords",
    "inherit_from_parent",
    "inherited_station_coord_source",
    "helix_pdv_coverage_observer",
    "pdv_coverage_above_threshold",
    "should_write",
    "coord_enrichment_report",
    "coord_enrichment_manifest",
]
