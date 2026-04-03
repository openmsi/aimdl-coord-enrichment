import json

from dagster import RunRequest, SensorEvaluationContext, sensor

from helix_dagster.constants import HELIX_FOLDER_ID
from helix_dagster.assets import process_helix_assets_job
from helix_dagster.girder_io import list_recent_spreadsheets
from helix_dagster.resources import GirderConnection


@sensor(job=process_helix_assets_job, minimum_interval_seconds=3600)
def helix_folder_sensor(context: SensorEvaluationContext, girder: GirderConnection):
    """Poll for new experiment log spreadsheets in the HELIX folder.

    Uses a sorted recent-items query instead of recursive folder crawling.
    Tracks seen item IDs in the cursor to avoid reprocessing.
    """
    cursor_data = json.loads(context.cursor or '{"seen": []}')
    seen = set(cursor_data["seen"])

    items = list_recent_spreadsheets(girder, HELIX_FOLDER_ID, limit=100)

    new_seen = set(seen)
    requests = []
    for item in items:
        item_id = item["_id"]
        if item_id not in seen:
            # Optionally check processing manifest
            existing_meta = item.get("meta", {})
            processing_status = existing_meta.get("processing_status", {})
            if processing_status.get("status") == "completed_clean":
                context.log.info(
                    "Skipping %s — already processed cleanly on %s",
                    item["name"],
                    processing_status.get("last_processed", "unknown"),
                )
                new_seen.add(item_id)
                continue

            requests.append(
                RunRequest(
                    run_key=item_id,
                    run_config={
                        "ops": {
                            "raw_experiment_log": {
                                "config": {
                                    "item_id": item_id,
                                    "filename": item["name"],
                                }
                            }
                        }
                    },
                )
            )
            new_seen.add(item_id)

    context.update_cursor(json.dumps({"seen": list(new_seen)}))
    return requests
