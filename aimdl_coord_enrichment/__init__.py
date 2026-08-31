__version__ = "0.6.0"

from dagster import AssetSelection, Definitions, EnvVar, define_asset_job

from aimdl_coord_enrichment.assets import (
    pdv_data,
    pdv_log,
    pdv_processing_manifest,
    process_helix_assets_job,
)
from aimdl_coord_enrichment.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    manifest_written,
    pdv_match_rate,
    zero_pdv_inventory,
)
from aimdl_coord_enrichment.coord_enrichment import (
    coord_enrichment_manifest,
    coord_enrichment_report,
    coord_transform_config_snapshot,
    enrichable_items_inventory,
    enriched_helix_alpss,
    enriched_maxima_run,
    enrichment_success_rate_helix_alpss,
    enrichment_success_rate_maxima,
    helix_alpss_provenance_tagged,
    helix_pdv_coverage_observer,
    inventory_nonempty_per_instrument,
    no_coord_transform_failures_helix_alpss,
    no_coord_transform_failures_maxima,
    pdv_coverage_above_threshold,
)
from aimdl_coord_enrichment.coord_enrichment.provenance_tagging import (
    all_helix_alpss_tagged,
)
from aimdl_coord_enrichment.resources import GirderConnection, GirderCredentials
from aimdl_coord_enrichment.schedules import (
    coord_enrichment_helix_alpss_weekly_schedule,
    coord_enrichment_maxima_weekly_schedule,
    coord_enrichment_state_report_schedule,
)
from aimdl_coord_enrichment.sensors import (
    helix_experiment_log_discovery_sensor,
    maxima_run_discovery_sensor,
)


coord_enrichment_job = define_asset_job(
    name="coord_enrichment_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        helix_alpss_provenance_tagged,
        helix_pdv_coverage_observer,
        coord_enrichment_report,
        coord_enrichment_manifest,
    ),
)

# NOTE: coord_enrichment_maxima_job and coord_enrichment_maxima_partition_job
# select identical assets on purpose. They are kept as two jobs so the
# sensor-driven and schedule-driven launch paths produce separate, filterable
# run feeds in the UI. The partition_key on each RunRequest — not the job
# identity — decides what materializes, so this split is a convention for
# observability/intent, not a Dagster requirement.

# Schedule target: weekly gap-filling reconciliation sweep. The weekly
# schedule enumerates all registered run partitions and emits a RunRequest
# only for those lacking a successful materialization.
coord_enrichment_maxima_job = define_asset_job(
    name="coord_enrichment_maxima_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enriched_maxima_run,
    ),
)

# Sensor target: single-partition, event-driven discovery. The
# maxima_run_discovery_sensor emits one RunRequest per new/changed AIMD-L
# run, deduped on per-data-type + xrd_metadata content hashes.
coord_enrichment_maxima_partition_job = define_asset_job(
    name="coord_enrichment_maxima_partition_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enriched_maxima_run,
    ),
)

coord_enrichment_helix_alpss_job = define_asset_job(
    name="coord_enrichment_helix_alpss_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        helix_alpss_provenance_tagged,
        enriched_helix_alpss,
    ),
)


defs = Definitions(
    assets=[
        # helix_spreadsheet (3 partitioned assets)
        pdv_log,
        pdv_data,
        pdv_processing_manifest,
        # coord_enrichment (Phase 3)
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        helix_alpss_provenance_tagged,
        enriched_maxima_run,
        coord_enrichment_report,
        coord_enrichment_manifest,
        # coord_enrichment (Phase 4)
        enriched_helix_alpss,
        helix_pdv_coverage_observer,
    ],
    asset_checks=[
        # helix_spreadsheet (retargeted to the 3-asset flow)
        igsn_validity_rate,
        zero_pdv_inventory,
        pdv_match_rate,
        igsn_consistency,
        enrichment_success_rate,
        coord_transform_check,
        manifest_written,
        # coord_enrichment (Phase 3)
        inventory_nonempty_per_instrument,
        all_helix_alpss_tagged,
        enrichment_success_rate_maxima,
        no_coord_transform_failures_maxima,
        # coord_enrichment (Phase 4)
        enrichment_success_rate_helix_alpss,
        no_coord_transform_failures_helix_alpss,
        pdv_coverage_above_threshold,
    ],
    jobs=[
        process_helix_assets_job,
        coord_enrichment_job,
        coord_enrichment_maxima_job,
        coord_enrichment_maxima_partition_job,
        coord_enrichment_helix_alpss_job,
    ],
    schedules=[
        coord_enrichment_state_report_schedule,
        coord_enrichment_maxima_weekly_schedule,
        coord_enrichment_helix_alpss_weekly_schedule,
    ],
    sensors=[
        helix_experiment_log_discovery_sensor,
        maxima_run_discovery_sensor,
    ],
    resources={
        "girder": GirderConnection(
            credentials=GirderCredentials(
                api_url=EnvVar("GIRDER_API_URL"),
                api_key=EnvVar("GIRDER_API_KEY"),
            )
        ),
    },
)
