"""Convention guard for Dagster asset `group_name` assignments.

See .claude/prompts/asset-groups/ISSUE.md (tracking #28) for the
rationale. Group names separate the two DAGs by instrument and role
in the Dagster UI. This test locks the assignment so future edits
can't silently drop assets back into the implicit `default` group.

Groups are resolved from the loaded `Definitions`
(`aimdl_coord_enrichment.defs`), mirroring the convention-enforcement
style of `tests/test_annotations_rule.py`.
"""

from aimdl_coord_enrichment import defs

# Asset key -> expected group_name. 12 assets across 6 groups.
EXPECTED_GROUPS = {
    "pdv_log": "helix_spreadsheet",
    "pdv_data": "helix_spreadsheet",
    "pdv_processing_manifest": "helix_spreadsheet",
    "helix_alpss_provenance_tagged": "helix_alpss",
    "enriched_helix_alpss": "helix_alpss",
    "enriched_maxima_run": "maxima",
    "coord_transform_config_snapshot": "coord_enrichment_core",
    "enrichable_items_inventory": "coord_enrichment_core",
    "coord_enrichment_report": "coord_enrichment_reporting",
    "coord_enrichment_manifest": "coord_enrichment_reporting",
    "helix_pdv_coverage_observer": "coord_enrichment_reporting",
}

EXPECTED_COUNTS = {
    "helix_spreadsheet": 3,
    "helix_alpss": 2,
    "maxima": 1,
    "coord_enrichment_core": 2,
    "coord_enrichment_reporting": 3,
}


def _actual_groups():
    """Map asset key string -> group_name from the loaded Definitions."""
    groups = {}
    for assets_def in defs.assets:
        for key in assets_def.keys:
            groups[key.to_user_string()] = assets_def.group_names_by_key[key]
    return groups


def test_every_asset_has_expected_group():
    actual = _actual_groups()
    for key, expected in EXPECTED_GROUPS.items():
        assert actual.get(key) == expected, (
            f"{key}: expected group {expected!r}, got {actual.get(key)!r}"
        )


def test_no_asset_in_default_group():
    actual = _actual_groups()
    in_default = sorted(k for k, g in actual.items() if g == "default")
    assert not in_default, f"assets still in default group: {in_default}"


def test_group_membership_counts():
    actual = _actual_groups()
    counts = {}
    for group in actual.values():
        counts[group] = counts.get(group, 0) + 1
    assert counts == EXPECTED_COUNTS
