"""Phase 5 artifact-existence and shape checks.

Verifies that the operator-facing deliverables exist and that
the Definitions registry has the expected Phase 5 surface (3
partitioned jobs + 4 schedules). Does not execute any job.
"""

from pathlib import Path
import os
import stat

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_runbook_exists():
    p = REPO_ROOT / "docs" / "runbooks" / "coord_enrichment_production_sweep.md"
    assert p.exists(), f"missing runbook: {p}"
    text = p.read_text()
    for anchor in ("Pre-flight", "Dry-run rehearsal", "Live sweep",
                   "Monitoring", "Rollback"):
        assert anchor in text, f"runbook missing section: {anchor}"


def test_expected_values_doc_exists():
    p = REPO_ROOT / "docs" / "runbooks" / "first_sweep_expected_values.md"
    assert p.exists(), f"missing expected-values doc: {p}"


def test_phase5_jobs_and_schedules_registered():
    """Definitions should expose 5 jobs (2 pre-Phase-5 + 3 new) and 4 schedules."""
    from helix_dagster import defs
    repo = defs.get_repository_def()
    job_names = {j.name for j in repo.get_all_jobs()}
    expected_jobs = {
        "process_helix_assets_job",
        "coord_enrichment_job",
        "coord_enrichment_maxima_raw_job",
        "coord_enrichment_helix_alpss_job",
        "coord_enrichment_maxima_derived_job",
    }
    assert expected_jobs.issubset(job_names), (
        f"missing jobs: {expected_jobs - job_names}"
    )

    schedule_names = {s.name for s in repo.schedule_defs}
    expected_schedules = {
        "coord_enrichment_state_report_schedule",
        "coord_enrichment_maxima_raw_weekly_schedule",
        "coord_enrichment_helix_alpss_weekly_schedule",
        "coord_enrichment_maxima_derived_weekly_schedule",
    }
    assert expected_schedules.issubset(schedule_names), (
        f"missing schedules: {expected_schedules - schedule_names}"
    )


def test_all_schedules_default_stopped():
    """Schedules must ship STOPPED; operators opt in."""
    from dagster import DefaultScheduleStatus
    from helix_dagster import defs
    for s in defs.get_repository_def().schedule_defs:
        if s.name.startswith("coord_enrichment_"):
            assert s.default_status == DefaultScheduleStatus.STOPPED, (
                f"schedule {s.name} ships RUNNING; Phase 5 requires STOPPED"
            )


def test_archived_run_live_sweep_preserved():
    """The retired bash sweep script and its context are preserved.

    Reason: future deployment contexts (CI, kubernetes CronJob,
    environments without UI access) may want to revive a shell-
    driven sweep. The archive captures the env-var protocol and the
    operator-confirmation gate that lived in the original script.

    Intentional: the archived .sh file is NOT executable. It is a
    historical document, not a runnable artifact at this path.
    """
    archive_dir = REPO_ROOT / "docs" / "archive" / "run_live_sweep"
    assert archive_dir.is_dir(), f"missing archive: {archive_dir}"

    script = archive_dir / "run_live_sweep.sh"
    note = archive_dir / "ARCHIVE_NOTE.md"
    section = archive_dir / "LIVE_SWEEP_SECTION_2026-04-27.md"
    for p in (script, note, section):
        assert p.exists(), f"archive missing: {p}"

    # The script is archived, not active. Asserting it is NOT
    # executable prevents accidental "fix it in place" attempts —
    # if someone wants to revive it they should copy it back to
    # operations/ deliberately, not run it from docs/.
    assert not (script.stat().st_mode & stat.S_IXUSR), (
        f"{script} should not be executable in archive"
    )
