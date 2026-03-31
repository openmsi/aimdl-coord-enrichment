import json

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Config,
    MetadataValue,
    asset,
    define_asset_job,
)

from helix_dagster.constants import COLUMN_MAP, PDV_FOLDER_ID
from helix_dagster.coordinates import transform_station_to_sample
from helix_dagster.girder_io import download_and_read, nan_to_none
from helix_dagster.matching import match_pdv_file
from helix_dagster.resources import GirderConnection
from helix_dagster.validation import NpEncoder, validate_igsn


class ExperimentLogConfig(Config):
    item_id: str
    filename: str


@asset
def raw_experiment_log(
    context: AssetExecutionContext,
    config: ExperimentLogConfig,
    girder: GirderConnection,
) -> pd.DataFrame:
    """Download a spreadsheet from Girder and apply COLUMN_MAP rename."""
    df = download_and_read(girder, config.item_id, config.filename)
    df = df.rename(columns=COLUMN_MAP)
    context.add_output_metadata(
        {
            "row_count": MetadataValue.int(len(df)),
            "filename": MetadataValue.text(config.filename),
            "source_item_id": MetadataValue.text(config.item_id),
        }
    )
    return df


@asset
def pdv_inventory(
    context: AssetExecutionContext,
    girder: GirderConnection,
) -> list:
    """Fetch all items from the PDV folder via Girder API."""
    items = girder.get(
        "item",
        parameters={"folderId": PDV_FOLDER_ID, "limit": 100000},
    )
    context.add_output_metadata(
        {
            "item_count": MetadataValue.int(len(items)),
        }
    )
    return items


@asset
def validated_rows(
    context: AssetExecutionContext,
    raw_experiment_log: pd.DataFrame,
) -> dict:
    """Validate IGSNs for each row. Pure transformation, no network calls."""
    df = raw_experiment_log.copy()
    igsn_issues = []
    valid_igsns = []

    for idx, row in df.iterrows():
        valid_igsn, issue = validate_igsn(row.get("Sample_IGSN"))
        valid_igsns.append(valid_igsn)
        if issue is not None:
            issue["row"] = idx
            igsn_issues.append(issue)

    df["valid_igsn"] = valid_igsns

    valid_count = sum(1 for v in valid_igsns if v is not None)
    missing_count = sum(1 for i in igsn_issues if i["issue"] == "missing")
    invalid_count = sum(1 for i in igsn_issues if i["issue"] == "invalid_format")

    context.add_output_metadata(
        {
            "total_rows": MetadataValue.int(len(df)),
            "valid_igsn_count": MetadataValue.int(valid_count),
            "invalid_igsn_count": MetadataValue.int(invalid_count),
            "missing_igsn_count": MetadataValue.int(missing_count),
        }
    )
    return {"dataframe": df, "igsn_issues": igsn_issues}


@asset
def pdv_cross_references(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_inventory: list,
) -> dict:
    """Match PDV filenames to inventory items. Pure matching, no network calls."""
    df = validated_rows["dataframe"]
    matches = {}
    pdv_issues = []

    for idx, row in df.iterrows():
        pdv_filename = row.get("PDV_FileName")
        pdv_item, issue = match_pdv_file(pdv_inventory, pdv_filename)
        if pdv_item is not None:
            matches[idx] = pdv_item
        if issue is not None:
            issue["row"] = idx
            pdv_issues.append(issue)

    matched_count = len(matches)
    not_found_count = sum(1 for i in pdv_issues if i["type"] == "not_found")
    ambiguous_count = sum(1 for i in pdv_issues if i["type"] == "ambiguous")

    context.add_output_metadata(
        {
            "matched_count": MetadataValue.int(matched_count),
            "not_found_count": MetadataValue.int(not_found_count),
            "ambiguous_count": MetadataValue.int(ambiguous_count),
        }
    )
    return {"matches": matches, "pdv_issues": pdv_issues}


@asset
def enriched_pdv_metadata(
    context: AssetExecutionContext,
    pdv_cross_references: dict,
    validated_rows: dict,
    girder: GirderConnection,
) -> dict:
    """Write coordinate and IGSN metadata to matched Girder PDV items."""
    df = validated_rows["dataframe"]
    matches = pdv_cross_references["matches"]

    written_count = 0
    write_errors = []
    coord_failures = 0

    for row_idx, pdv_item in matches.items():
        row = df.loc[row_idx]

        station_x = nan_to_none(row.get("Flyer_X_Position_Final_mm"))
        station_y = nan_to_none(row.get("Flyer_Y_Position_Final_mm"))
        sample_x, sample_y = transform_station_to_sample(station_x, station_y)
        # ensure that sample_x,y have only 4 meaningful digits to avoid bogus precision
        if sample_x is not None:
            sample_x = round(sample_x, 4)
        if sample_y is not None:
            sample_y = round(sample_y, 4)

        if station_x is not None and station_y is not None and sample_x is None:
            coord_failures += 1

        metadata = {
            "Flyer_Row": nan_to_none(row.get("Flyer_Row")),
            "Flyer_Column": nan_to_none(row.get("Flyer_Column")),
            "Station_X": station_x,
            "Station_Y": station_y,
            "Sample_X": sample_x,
            "Sample_Y": sample_y,
        }
        # Ensure all values are JSON-serializable
        metadata = json.loads(json.dumps(metadata, cls=NpEncoder))

        try:
            girder.addMetadataToItem(pdv_item["_id"], metadata)
            written_count += 1
        except Exception as exc:
            context.log.error(
                f"Failed to write metadata for row {row_idx}, item {pdv_item['_id']}: {exc}"
            )
            write_errors.append({"row": row_idx, "error": str(exc)})

    context.add_output_metadata(
        {
            "items_enriched": MetadataValue.int(written_count),
            "coordinate_transform_failures": MetadataValue.int(coord_failures),
        }
    )
    return {"written_count": written_count, "write_errors": write_errors}


@asset
def quality_report(
    context: AssetExecutionContext,
    validated_rows: dict,
    pdv_cross_references: dict,
    enriched_pdv_metadata: dict,
) -> dict:
    """Aggregate all issues from upstream assets into a single report."""
    igsn_issues = validated_rows["igsn_issues"]
    pdv_issues = pdv_cross_references["pdv_issues"]
    write_errors = enriched_pdv_metadata["write_errors"]

    report = {
        "igsn_issues": igsn_issues,
        "pdv_issues": pdv_issues,
        "write_errors": write_errors,
        "summary": {
            "total_igsn_issues": len(igsn_issues),
            "total_pdv_issues": len(pdv_issues),
            "total_write_errors": len(write_errors),
        },
    }

    context.add_output_metadata(
        {
            "total_igsn_issues": MetadataValue.int(len(igsn_issues)),
            "total_pdv_issues": MetadataValue.int(len(pdv_issues)),
            "total_write_errors": MetadataValue.int(len(write_errors)),
        }
    )
    return report


process_helix_assets_job = define_asset_job(
    name="process_helix_assets_job",
    selection=AssetSelection.all(),
)
