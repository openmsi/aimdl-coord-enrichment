from dagster import Definitions, EnvVar

from helix_dagster.assets import (
    enriched_pdv_metadata,
    pdv_cross_references,
    pdv_inventory,
    process_helix_assets_job,
    quality_report,
    raw_experiment_log,
    validated_rows,
)
from helix_dagster.resources import GirderResource
from helix_dagster.sensors import helix_folder_sensor

defs = Definitions(
    assets=[
        raw_experiment_log,
        pdv_inventory,
        validated_rows,
        pdv_cross_references,
        enriched_pdv_metadata,
        quality_report,
    ],
    jobs=[process_helix_assets_job],
    sensors=[helix_folder_sensor],
    resources={
        "girder": GirderResource(
            api_url=EnvVar("GIRDER_API_URL"),
            api_key=EnvVar("GIRDER_TOKEN"),
        ),
    },
)
