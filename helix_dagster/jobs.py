"""Legacy job module — kept for rollback purposes. Use assets.py instead."""

import json

from dagster import Config, MetadataValue, Output, job, op

from helix_dagster.processing import process_file
from helix_dagster.resources import GirderResource


class FileConfig(Config):
    item_id: str
    filename: str


@op
def process_file_op(context, config: FileConfig, girder: GirderResource):
    """Deprecated: use the asset-based pipeline instead."""
    client = girder.get_client()
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


@job
def _legacy_process_helix_file_job():
    """Deprecated: use process_helix_assets_job instead.

    Kept temporarily for rollback. Will be removed in a follow-up PR
    after the asset-based pipeline is validated in production.
    """
    process_file_op()
