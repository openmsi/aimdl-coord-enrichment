from dagster import Definitions, EnvVar

from helix_dagster.jobs import process_helix_file_job
from helix_dagster.resources import GirderResource
from helix_dagster.sensors import helix_folder_sensor

defs = Definitions(
    jobs=[process_helix_file_job],
    sensors=[helix_folder_sensor],
    resources={
        "girder": GirderResource(
            api_url=EnvVar("GIRDER_API_URL"),
            api_key=EnvVar("GIRDER_TOKEN"),
        ),
    },
)
