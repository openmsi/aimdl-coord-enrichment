__version__ = "0.5.0"

from dagster import AssetSelection, Definitions, EnvVar, define_asset_job

from helix_dagster.assets import (
    alpss_results_inventory,
    enriched_pdv_metadata,
    pdv_cross_references,
    pdv_trace_inventory,
    process_helix_assets_job,
    processing_manifest,
    quality_report,
    raw_experiment_log,
    validated_rows,
)
from helix_dagster.checks import (
    coord_transform_check,
    enrichment_success_rate,
    igsn_consistency,
    igsn_validity_rate,
    pdv_match_rate,
    zero_inventory,
)
from helix_dagster.coord_enrichment import (
    coord_enrichment_manifest,
    coord_enrichment_report,
    coord_transform_config_snapshot,
    enrichable_items_inventory,
    enriched_helix_alpss,
    enriched_maxima_derived,
    enriched_maxima_raw,
    enrichment_success_rate_helix_alpss,
    enrichment_success_rate_maxima_derived,
    enrichment_success_rate_maxima_raw,
    helix_pdv_coverage_observer,
    inventory_nonempty_per_instrument,
    no_coord_transform_failures_helix_alpss,
    no_coord_transform_failures_maxima_derived,
    no_coord_transform_failures_maxima_raw,
    pdv_coverage_above_threshold,
    provenance_tagged_items,
)
from helix_dagster.coord_enrichment.provenance_tagging import (
    all_helix_alpss_tagged,
    maxima_prov_targets_resolve,
)
from helix_dagster.resources import GirderConnection, GirderCredentials
from helix_dagster.schedules import (
    coord_enrichment_helix_alpss_weekly_schedule,
    coord_enrichment_maxima_derived_weekly_schedule,
    coord_enrichment_maxima_raw_weekly_schedule,
    coord_enrichment_state_report_schedule,
)
from helix_dagster.sensors import helix_folder_sensor


coord_enrichment_job = define_asset_job(
    name="coord_enrichment_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        provenance_tagged_items,
        helix_pdv_coverage_observer,
        coord_enrichment_report,
        coord_enrichment_manifest,
    ),
)

coord_enrichment_maxima_raw_job = define_asset_job(
    name="coord_enrichment_maxima_raw_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        provenance_tagged_items,
        enriched_maxima_raw,
    ),
)

coord_enrichment_helix_alpss_job = define_asset_job(
    name="coord_enrichment_helix_alpss_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        provenance_tagged_items,
        enriched_helix_alpss,
    ),
)

coord_enrichment_maxima_derived_job = define_asset_job(
    name="coord_enrichment_maxima_derived_job",
    selection=AssetSelection.assets(
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        provenance_tagged_items,
        enriched_maxima_derived,
    ),
)


defs = Definitions(
    assets=[
        # existing
        raw_experiment_log,
        pdv_trace_inventory,
        validated_rows,
        pdv_cross_references,
        enriched_pdv_metadata,
        alpss_results_inventory,
        quality_report,
        processing_manifest,
        # coord_enrichment (Phase 3)
        coord_transform_config_snapshot,
        enrichable_items_inventory,
        provenance_tagged_items,
        enriched_maxima_raw,
        coord_enrichment_report,
        coord_enrichment_manifest,
        # coord_enrichment (Phase 4)
        enriched_helix_alpss,
        enriched_maxima_derived,
        helix_pdv_coverage_observer,
    ],
    asset_checks=[
        # existing
        zero_inventory,
        igsn_validity_rate,
        pdv_match_rate,
        igsn_consistency,
        enrichment_success_rate,
        coord_transform_check,
        # coord_enrichment (Phase 3)
        inventory_nonempty_per_instrument,
        all_helix_alpss_tagged,
        maxima_prov_targets_resolve,
        enrichment_success_rate_maxima_raw,
        no_coord_transform_failures_maxima_raw,
        # coord_enrichment (Phase 4)
        enrichment_success_rate_helix_alpss,
        no_coord_transform_failures_helix_alpss,
        enrichment_success_rate_maxima_derived,
        no_coord_transform_failures_maxima_derived,
        pdv_coverage_above_threshold,
    ],
    jobs=[
        process_helix_assets_job,
        coord_enrichment_job,
        coord_enrichment_maxima_raw_job,
        coord_enrichment_helix_alpss_job,
        coord_enrichment_maxima_derived_job,
    ],
    schedules=[
        coord_enrichment_state_report_schedule,
        coord_enrichment_maxima_raw_weekly_schedule,
        coord_enrichment_helix_alpss_weekly_schedule,
        coord_enrichment_maxima_derived_weekly_schedule,
    ],
    sensors=[helix_folder_sensor],
    resources={
        "girder": GirderConnection(
            credentials=GirderCredentials(
                api_url=EnvVar("GIRDER_API_URL"),
                api_key=EnvVar("GIRDER_API_KEY"),
            )
        ),
    },
)
