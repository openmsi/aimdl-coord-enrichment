cat > docs/runbooks/coord_enrichment_production_sweep.md <<'EOF'
# Coordinate Enrichment Production Sweep Runbook

This runbook describes the operator procedure for running the coordinate-enrichment sweep in production or production-like environments.

The sweep should be executed in stages:

1. inventory and state report,
2. dry-run rehearsal,
3. limited live subset,
4. production live sweep,
5. monitoring and rollback review.

All schedules should remain stopped by default. Operators should opt in to live execution explicitly.

---

## Pre-flight

Before running any coordinate-enrichment job, verify the following.

### Repository and environment

- Confirm the active branch and commit.
- Confirm the Python environment is active.
- Confirm the test suite passes.
- Confirm Dagster loads the repository definitions.
- Confirm all coordinate-enrichment schedules are stopped by default.

Suggested commands:

```bash
git status
pytest -v
dagster dev
