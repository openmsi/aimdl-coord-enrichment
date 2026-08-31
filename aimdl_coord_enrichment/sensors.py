from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from aimdl_coord_enrichment.assets import process_helix_assets_job
from aimdl_coord_enrichment.coord_enrichment.inventory import MAXIMA_RUN_PARTITIONS
from aimdl_coord_enrichment.girder_io import fetch_partition_index
from aimdl_coord_enrichment.partitions import (
    HELIX_EXPERIMENT_LOG_DATA_TYPE,
    HELIX_EXPERIMENT_LOG_PARTITIONS,
)
from aimdl_coord_enrichment.resources import GirderConnection


# Data types whose arrival signals that a run has produced something
# worth enriching. The run — not the data type — is the unit of work.
_MAXIMA_DISCOVERY_DATA_TYPES = ("xrd_raw", "xrf_raw", "xrd_derived")


@sensor(
    job_name="coord_enrichment_maxima_partition_job",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
)
def maxima_run_discovery_sensor(
    context: SensorEvaluationContext,
    girder: GirderConnection,
) -> SensorResult:
    """Discover new AIMD-L runs for MAXIMA and request their materialization.

    Per tick:
      1. Fetches the partition index for each discovery data type and for
         xrd_metadata.
      2. Adds the union of AIMD-L run keys ("igsn//experiment_date") to the
         dynamic ``maxima_run`` partition dimension.
      3. Emits one RunRequest per run key, deduped on a run_key composing the
         content hash of every data type present plus the xrd_metadata hash.

    Dedup semantics (sensor-scoped): the same (aimdl_key, per-type hashes,
    metadata hash) tuple produces the same run_key, so unchanged runs are
    suppressed across evaluations. Any hash changing re-triggers the run —
    including xrd_metadata, since a changed instructions.txt changes the
    coordinates of every file in the run.

    The xrd_metadata fallback is "no-xrd-metadata" for runs with no
    instructions.txt; those still materialize once, and the asset records a
    resolution error per item.
    """
    indexes: dict[str, dict[str, str]] = {
        dt: fetch_partition_index(girder, dt)
        for dt in _MAXIMA_DISCOVERY_DATA_TYPES
    }
    metadata_index = fetch_partition_index(girder, "xrd_metadata")

    all_aimdl_keys = sorted(
        {key for index in indexes.values() for key in index.keys()}
    )

    run_requests: list[RunRequest] = []
    for aimdl_key in all_aimdl_keys:
        metadata_hash = metadata_index.get(aimdl_key, "no-xrd-metadata")
        per_type = {
            dt: indexes[dt].get(aimdl_key, "absent")
            for dt in _MAXIMA_DISCOVERY_DATA_TYPES
        }
        hash_part = "|".join(
            f"{dt}={per_type[dt]}" for dt in _MAXIMA_DISCOVERY_DATA_TYPES
        )
        dagster_run_key = (
            f"coord-enrichment|{aimdl_key}|{hash_part}"
            f"|xrd_metadata={metadata_hash}"
        )

        run_requests.append(
            RunRequest(
                run_key=dagster_run_key,
                partition_key=aimdl_key,
                tags={
                    "aimdl_partition_key": aimdl_key,
                    "xrd_metadata_content_hash": metadata_hash,
                },
            )
        )

    context.log.info(
        "maxima_run_discovery_sensor: %d AIMD-L run keys, %d run requests",
        len(all_aimdl_keys), len(run_requests),
    )

    return SensorResult(
        dynamic_partitions_requests=[
            MAXIMA_RUN_PARTITIONS.build_add_request(all_aimdl_keys)
        ],
        run_requests=run_requests,
    )


@sensor(
    job=process_helix_assets_job,
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
)
def helix_experiment_log_discovery_sensor(
    context: SensorEvaluationContext,
    girder: GirderConnection,
) -> SensorResult:
    """Discover AIMD-L partitions for HELIX experiment logs.

    Per tick, fetches the partition index for ``pdv_experiment_log``
    (keyed ``"<igsn>//<experiment_date>"`` with content-hash values),
    registers every key on the ``helix_experiment_log`` dynamic
    dimension, and emits one partitioned RunRequest per key. The run_key
    embeds the content hash, so unchanged partitions are suppressed and a
    changed log re-triggers.
    """
    index = fetch_partition_index(girder, HELIX_EXPERIMENT_LOG_DATA_TYPE)

    context.log.info(
        "helix_experiment_log_discovery_sensor: %d partitions", len(index)
    )

    return SensorResult(
        dynamic_partitions_requests=[
            HELIX_EXPERIMENT_LOG_PARTITIONS.build_add_request(sorted(index))
        ],
        run_requests=[
            RunRequest(
                run_key=f"helix-pdv-log|{key}|hash={content_hash}",
                partition_key=key,
                tags={
                    "data_type": HELIX_EXPERIMENT_LOG_DATA_TYPE,
                    "content_hash": content_hash,
                },
            )
            for key, content_hash in index.items()
        ],
    )
