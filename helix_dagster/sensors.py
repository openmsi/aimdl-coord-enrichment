import json

from dagster import (
    DefaultSensorStatus,
    MultiPartitionKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from helix_dagster.assets import process_helix_assets_job
from helix_dagster.constants import HELIX_FOLDER_ID
from helix_dagster.coord_enrichment.inventory import MAXIMA_RUN_PARTITIONS
from helix_dagster.girder_io import (
    fetch_partition_index,
    list_recent_spreadsheets,
)
from helix_dagster.resources import GirderConnection


_MAXIMA_RAW_DATA_TYPES = ("xrd_raw", "xrf_raw")


@sensor(
    job_name="coord_enrichment_maxima_raw_partition_job",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
)
def maxima_raw_discovery_sensor(
    context: SensorEvaluationContext,
    girder: GirderConnection,
) -> SensorResult:
    """Discover new AIMD-L partitions for MAXIMA raw and emit materialization requests.

    Per tick:
      1. Fetches the partition index for xrd_raw, xrf_raw, and
         xrd_metadata.
      2. Adds the union of AIMD-L run keys (``igsn//experiment_date``)
         to the dynamic ``maxima_raw_run`` partition dimension.
      3. Emits one RunRequest per observed (data_type, aimdl_key),
         dedupping on a run_key that composes both the raw content
         hash and the xrd_metadata content hash.

    Dedup semantics (sensor-scoped): the same (data_type, aimdl_key,
    raw_hash, metadata_hash) tuple produces the same run_key, so
    unchanged partitions are suppressed across all sensor
    evaluations. Either hash changing triggers a new run.

    The xrd_metadata hash fallback is "no-xrd-metadata" for runs
    that have no xrd_metadata entry — those partitions will still
    materialize once, and the asset will record a resolution error
    for the missing instructions.txt.
    """
    raw_indexes: dict[str, dict[str, str]] = {
        dt: fetch_partition_index(girder, dt)
        for dt in _MAXIMA_RAW_DATA_TYPES
    }
    metadata_index = fetch_partition_index(girder, "xrd_metadata")

    all_aimdl_keys = sorted(
        {
            aimdl_key
            for index in raw_indexes.values()
            for aimdl_key in index.keys()
        }
    )

    run_requests: list[RunRequest] = []
    for data_type, index in raw_indexes.items():
        for aimdl_key, raw_hash in index.items():
            metadata_hash = metadata_index.get(aimdl_key, "no-xrd-metadata")

            dagster_partition_key = MultiPartitionKey(
                {"data_type": data_type, "run": aimdl_key}
            )
            dagster_run_key = (
                f"coord-enrichment"
                f"|{data_type}"
                f"|{aimdl_key}"
                f"|raw={raw_hash}"
                f"|xrd_metadata={metadata_hash}"
            )

            run_requests.append(
                RunRequest(
                    run_key=dagster_run_key,
                    partition_key=dagster_partition_key,
                    tags={
                        "data_type": data_type,
                        "aimdl_partition_key": aimdl_key,
                        "raw_content_hash": raw_hash,
                        "xrd_metadata_content_hash": metadata_hash,
                    },
                )
            )

    context.log.info(
        "maxima_raw_discovery_sensor: %d AIMD-L run keys, %d run requests",
        len(all_aimdl_keys), len(run_requests),
    )

    return SensorResult(
        dynamic_partitions_requests=[
            MAXIMA_RUN_PARTITIONS.build_add_request(all_aimdl_keys)
        ],
        run_requests=run_requests,
    )


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
