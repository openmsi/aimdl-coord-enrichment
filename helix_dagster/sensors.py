import json

from dagster import RunRequest, SensorEvaluationContext, sensor

from helix_dagster.constants import HELIX_FOLDER_ID
from helix_dagster.assets import process_helix_assets_job
from helix_dagster.processing import list_all_spreadsheet_items
from helix_dagster.resources import GirderResource


@sensor(job=process_helix_assets_job, minimum_interval_seconds=60)
def helix_folder_sensor(context: SensorEvaluationContext, girder: GirderResource):
    cursor_data = json.loads(context.cursor or '{"seen": []}')
    seen = set(cursor_data["seen"])

    client = girder.get_client()
    items = list_all_spreadsheet_items(client, HELIX_FOLDER_ID)

    new_seen = set(seen)
    requests = []
    for item in items:
        item_id = item["_id"]
        if item_id not in seen:
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
