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


def test_operator_script_exists_and_executable():
    p = REPO_ROOT / "operations" / "run_live_sweep.sh"
    assert p.exists(), f"missing operator script: {p}"
    mode = p.stat().st_mode
    assert mode & stat.S_IXUSR, (
        f"{p} is not executable; chmod +x operations/run_live_sweep.sh"
    )


def test_script_requires_env_vars():
    p = REPO_ROOT / "operations" / "run_live_sweep.sh"
    text = p.read_text()
    for var in ("GIRDER_API_URL", "GIRDER_API_KEY",
                "COORD_TRANSFORMS_YAML",
                "COORD_ENRICHMENT_MANIFEST_ITEM"):
        assert var in text, f"script does not check for required env var {var}"


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
