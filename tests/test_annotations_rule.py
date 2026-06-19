"""CI guard for the `from __future__ import annotations` rule.

See docs/developer_notes/annotations.md for the rationale.

This test scans a hand-maintained list of Dagster-adjacent
modules and fails if any of them starts with the forbidden
import. Missing files are skipped (not failed) so this test
can be added before all Phase 3 files exist.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Modules that define Dagster assets, sensors, resources, Config
# subclasses, or register Definitions. These MUST NOT use
# `from __future__ import annotations`.
FORBIDDEN_PATHS = [
    "aimdl_coord_enrichment/__init__.py",
    "aimdl_coord_enrichment/assets.py",
    "aimdl_coord_enrichment/checks.py",
    "aimdl_coord_enrichment/partitions.py",
    "aimdl_coord_enrichment/resources.py",
    "aimdl_coord_enrichment/sensors.py",
    "aimdl_coord_enrichment/instruments/__init__.py",
    "aimdl_coord_enrichment/coord_enrichment/__init__.py",
    "aimdl_coord_enrichment/coord_enrichment/config.py",
    "aimdl_coord_enrichment/coord_enrichment/config_snapshot.py",
    "aimdl_coord_enrichment/coord_enrichment/inventory.py",
    "aimdl_coord_enrichment/coord_enrichment/provenance_tagging.py",
    "aimdl_coord_enrichment/coord_enrichment/enrichment_leaves.py",
    "aimdl_coord_enrichment/coord_enrichment/report.py",
    "aimdl_coord_enrichment/coord_enrichment/manifest.py",
    "aimdl_coord_enrichment/coord_enrichment/inheritance.py",
    "aimdl_coord_enrichment/coord_enrichment/helix_alpss_leaf.py",
    "aimdl_coord_enrichment/coord_enrichment/maxima_derived_leaf.py",
    "aimdl_coord_enrichment/coord_enrichment/pdv_observer.py",
    "aimdl_coord_enrichment/schedules.py",
]

FORBIDDEN_IMPORT = "from __future__ import annotations"


@pytest.mark.parametrize("relative_path", FORBIDDEN_PATHS)
def test_no_future_annotations_import(relative_path: str) -> None:
    """Fail if a Dagster-adjacent module uses PEP 563 annotations.

    See docs/developer_notes/annotations.md for the rule.
    """
    path = REPO_ROOT / relative_path
    if not path.exists():
        pytest.skip(f"{relative_path} not present yet (OK for in-progress phases)")
    text = path.read_text(encoding="utf-8")
    assert FORBIDDEN_IMPORT not in text, (
        f"{relative_path} contains `{FORBIDDEN_IMPORT}`, which is forbidden in "
        "Dagster-adjacent modules because it breaks Dagster's Config schema "
        "resolution at runtime. See docs/developer_notes/annotations.md."
    )
