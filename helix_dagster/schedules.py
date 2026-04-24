"""Dagster schedules for the coord_enrichment DAG.

Two cadences:
  - coord_enrichment_state_report_schedule: nightly at 03:00 ET,
    runs coord_enrichment_job (no writes).
  - Three weekly sweep schedules: Sunday 04:00/04:30 ET, fan out
    every partition of the three partitioned sibling jobs.
    Default dry_run=True.
"""

from dagster import (
    AssetKey,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    schedule,
)

from helix_dagster.coord_enrichment import (
    HELIX_ALPSS_PARTITIONS,
    MAXIMA_DERIVED_PARTITIONS,
    MAXIMA_RAW_PARTITIONS,
)


_TIMEZONE = "America/New_York"


def _dry_run_config(op_names: list[str]) -> dict:
    """Build a RunConfig that sets dry_run=True on every Config-consuming op."""
    return {
        "ops": {
            name: {"config": {"dry_run": True}} for name in op_names
        }
    }


_STATE_REPORT_OPS = [
    "provenance_tagged_items",
    "coord_enrichment_manifest",
]
_MAXIMA_RAW_OPS = [
    "enriched_maxima_raw",
]
_HELIX_ALPSS_OPS = [
    "provenance_tagged_items",
    "enriched_helix_alpss",
]
_MAXIMA_DERIVED_OPS = [
    "provenance_tagged_items",
    "enriched_maxima_derived",
]


@schedule(
    job_name="coord_enrichment_job",
    cron_schedule="0 3 * * *",
    execution_timezone=_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)
def coord_enrichment_state_report_schedule(
    context: ScheduleEvaluationContext,
):
    """Nightly no-writes state-report run."""
    return RunRequest(
        run_key=None,
        run_config=_dry_run_config(_STATE_REPORT_OPS),
        tags={"phase5": "state_report", "dry_run": "true"},
    )


@schedule(
    job_name="coord_enrichment_maxima_raw_job",
    cron_schedule="0 4 * * 0",
    execution_timezone=_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)
def coord_enrichment_maxima_raw_weekly_schedule(
    context: ScheduleEvaluationContext,
):
    """Weekly gap-filling reconciliation for MAXIMA raw partitions.

    Enumerates all registered (data_type, run) partitions and emits
    a dry-run RunRequest for each that has no successful
    materialization. Partitions the discovery sensor already
    processed successfully are skipped.

    STOPPED by default; dry-run only. Safe to overlap with the
    sensor — dry-run writes nothing.
    """
    instance = context.instance

    all_keys = MAXIMA_RAW_PARTITIONS.get_partition_keys(
        dynamic_partitions_store=instance
    )

    asset_key = AssetKey("enriched_maxima_raw")
    materialized = _materialized_partitions(instance, asset_key)

    gap_keys = [k for k in all_keys if str(k) not in materialized]

    context.log.info(
        "maxima_raw reconciliation: %d known, %d materialized, %d gaps",
        len(all_keys), len(materialized), len(gap_keys),
    )

    for key in gap_keys:
        yield RunRequest(
            run_key=f"reconciliation|{key}",
            partition_key=str(key),
            run_config=_dry_run_config(_MAXIMA_RAW_OPS),
            tags={
                "phase5": "reconciliation",
                "partition": str(key),
                "dry_run": "true",
            },
        )


def _materialized_partitions(instance, asset_key) -> set[str]:
    """Return the set of partition-key strings with at least one
    successful materialization for the given asset.

    Uses whatever API the installed Dagster version exposes.
    """
    if hasattr(instance, "get_materialized_partitions"):
        return set(instance.get_materialized_partitions(asset_key))

    all_keys = MAXIMA_RAW_PARTITIONS.get_partition_keys(
        dynamic_partitions_store=instance
    )
    out: set[str] = set()
    for key in all_keys:
        evt = instance.get_latest_materialization_event(
            asset_key, partition=str(key)
        )
        if evt is not None:
            out.add(str(key))
    return out


@schedule(
    job_name="coord_enrichment_helix_alpss_job",
    cron_schedule="30 4 * * 0",
    execution_timezone=_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)
def coord_enrichment_helix_alpss_weekly_schedule(
    context: ScheduleEvaluationContext,
):
    """Weekly sweep across HELIX ALPSS partitions (30 min after raw)."""
    for key in HELIX_ALPSS_PARTITIONS.get_partition_keys():
        yield RunRequest(
            run_key=key,
            partition_key=key,
            run_config=_dry_run_config(_HELIX_ALPSS_OPS),
            tags={"phase5": "sweep", "partition": key, "dry_run": "true"},
        )


@schedule(
    job_name="coord_enrichment_maxima_derived_job",
    cron_schedule="30 4 * * 0",
    execution_timezone=_TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
)
def coord_enrichment_maxima_derived_weekly_schedule(
    context: ScheduleEvaluationContext,
):
    """Weekly sweep across MAXIMA xrd_derived partitions."""
    for key in MAXIMA_DERIVED_PARTITIONS.get_partition_keys():
        yield RunRequest(
            run_key=key,
            partition_key=key,
            run_config=_dry_run_config(_MAXIMA_DERIVED_OPS),
            tags={"phase5": "sweep", "partition": key, "dry_run": "true"},
        )
