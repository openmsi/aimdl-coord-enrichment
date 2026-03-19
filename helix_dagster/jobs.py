import json

from dagster import Config, MetadataValue, Output, job, op

from helix_dagster.processing import process_file
from helix_dagster.resources import GirderResource


class FileConfig(Config):
    item_id: str
    filename: str


@op
def process_file_op(context, config: FileConfig):
    client = context.resources.girder.get_client()
    issues = process_file(client, config.item_id, config.filename, context.log)

    yield Output(
        value=None,
        metadata={
            "filename": MetadataValue.text(config.filename),
            "igsn_issues": MetadataValue.json(issues["igsn_issues"]),
            "pdv_issues": MetadataValue.json(issues["pdv_issues"]),
            "form_issues": MetadataValue.json(issues["form_issues"]),
            "igsn_issue_count": MetadataValue.int(len(issues["igsn_issues"])),
            "pdv_issue_count": MetadataValue.int(len(issues["pdv_issues"])),
            "form_issue_count": MetadataValue.int(len(issues["form_issues"])),
        },
    )


@job(resource_defs={"girder": GirderResource})
def process_helix_file_job():
    process_file_op()
