"""Tests for coord_enrichment schedules (Phase 5, Step 2)."""

from dagster import DagsterInstance, DefaultScheduleStatus, build_schedule_context

from helix_dagster import defs
from helix_dagster.schedules import (
    coord_enrichment_helix_alpss_weekly_schedule,
    coord_enrichment_maxima_derived_weekly_schedule,
    coord_enrichment_maxima_raw_weekly_schedule,
    coord_enrichment_state_report_schedule,
)


def _schedule_names() -> set[str]:
    return {s.name for s in defs.get_repository_def().schedule_defs}


def test_all_four_schedules_registered():
    names = _schedule_names()
    assert "coord_enrichment_state_report_schedule" in names
    assert "coord_enrichment_maxima_raw_weekly_schedule" in names
    assert "coord_enrichment_helix_alpss_weekly_schedule" in names
    assert "coord_enrichment_maxima_derived_weekly_schedule" in names


def test_state_report_schedule_cron():
    sched = coord_enrichment_state_report_schedule
    assert sched.cron_schedule == "0 3 * * *"
    assert sched.execution_timezone == "America/New_York"


def test_weekly_sweep_cron_expressions():
    for sched, expected_cron in [
        (coord_enrichment_maxima_raw_weekly_schedule, "0 4 * * 0"),
        (coord_enrichment_helix_alpss_weekly_schedule, "30 4 * * 0"),
        (coord_enrichment_maxima_derived_weekly_schedule, "30 4 * * 0"),
    ]:
        assert sched.cron_schedule == expected_cron
        assert sched.execution_timezone == "America/New_York"


def test_all_schedules_default_stopped():
    for sched in [
        coord_enrichment_state_report_schedule,
        coord_enrichment_maxima_raw_weekly_schedule,
        coord_enrichment_helix_alpss_weekly_schedule,
        coord_enrichment_maxima_derived_weekly_schedule,
    ]:
        assert sched.default_status == DefaultScheduleStatus.STOPPED


def test_state_report_runconfig_is_dry_run():
    context = build_schedule_context()
    result = coord_enrichment_state_report_schedule(context)
    assert result.run_config["ops"]["provenance_tagged_items"]["config"]["dry_run"] is True
    assert result.run_config["ops"]["coord_enrichment_manifest"]["config"]["dry_run"] is True


def test_maxima_raw_emits_no_requests_when_no_dynamic_keys(tmp_path):
    # MAXIMA_RAW_PARTITIONS is now a MultiPartitionsDefinition whose
    # `run` dimension is dynamic. A fresh instance has no registered
    # run keys, so the cartesian product is empty.
    with DagsterInstance.local_temp(tempdir=str(tmp_path)) as instance:
        context = build_schedule_context(instance=instance)
        requests = list(coord_enrichment_maxima_raw_weekly_schedule(context))
    assert len(requests) == 0


def test_helix_alpss_emits_three_run_requests():
    context = build_schedule_context()
    requests = list(coord_enrichment_helix_alpss_weekly_schedule(context))
    assert len(requests) == 3


def test_maxima_derived_emits_one_run_request():
    context = build_schedule_context()
    requests = list(coord_enrichment_maxima_derived_weekly_schedule(context))
    assert len(requests) == 1


def test_sweep_run_requests_include_partition_key():
    context = build_schedule_context()

    for sched, expected_keys in [
        (
            coord_enrichment_helix_alpss_weekly_schedule,
            {"HELIX/pdv_alpss_output", "HELIX/pdv_alpss_result", "HELIX/pdv_alpss_results"},
        ),
        (coord_enrichment_maxima_derived_weekly_schedule, {"MAXIMA/xrd_derived"}),
    ]:
        requests = list(sched(context))
        actual_keys = {r.partition_key for r in requests}
        assert actual_keys == expected_keys, f"{sched.name}: {actual_keys} != {expected_keys}"


def test_sweep_run_requests_tag_dry_run_true(tmp_path):
    with DagsterInstance.local_temp(tempdir=str(tmp_path)) as instance:
        raw_context = build_schedule_context(instance=instance)
        for req in coord_enrichment_maxima_raw_weekly_schedule(raw_context):
            assert req.tags["dry_run"] == "true"

    context = build_schedule_context()
    for sched in [
        coord_enrichment_helix_alpss_weekly_schedule,
        coord_enrichment_maxima_derived_weekly_schedule,
    ]:
        for req in sched(context):
            assert req.tags["dry_run"] == "true", f"{sched.name} partition {req.partition_key}"
