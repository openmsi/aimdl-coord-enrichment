# Phase 1, Step 5 — Version-boundary integration test

## Context

Branch: `refactor/asset-dag`. Steps 1–4 are committed. This is the
final Phase 1 step.

Read `.claude/CLAUDE.md` and
`../../../../htmdec/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml`
before editing. HELIX in that YAML has v1 (valid until
`2026-04-01T00:00:00-04:00`) and v2 (valid from the same instant).

## Goal

Add one integration test that proves the end-to-end timestamp→version
dispatch works. Two DataFrame rows with identical station coordinates
but different timestamps (one pre-boundary, one post-boundary) must
produce different Sample_X/Y values and different
`coord_provenance.transform_version` values.

This is the single most important guarantee of Phase 1: the right
transform version gets applied based on when the shot was taken.

## Audit phase (report BEFORE editing)

1. Read `tests/test_assets.py` to see how
   `test_enriched_pdv_metadata_writes_provenance` (added in Step 3)
   sets up its DataFrame and mocks.
2. Read
   `../../../../htmdec/aimdl_coordinate_systems/instrument_coordinate_transforms.yaml`
   to confirm the HELIX v1 and v2 affine transforms differ (they
   should — v1 maps (8,8)→(32,8), v2 maps (8,8)→(8,8)). Calculate
   by hand what each row in the test below should transform to, so
   the assertions are exact, not approximate.
3. Report.

## Edits

### New test function in `tests/test_assets.py`

Add at the bottom of the file, next to the other asset tests:

```python
def test_enriched_pdv_metadata_version_boundary_dispatch():
    """Two rows with identical station coords and different timestamps
    straddling HELIX v1/v2 must produce different Sample_X/Y values.
    """
    import pytest
    from datetime import datetime, timezone
    from unittest.mock import MagicMock
    from dagster import build_asset_context
    from aimdl_coord_enrichment.assets import (
        enriched_pdv_metadata as enriched_fn,
        ExperimentLogConfig,
    )
    from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER

    if _COORD_TRANSFORMER is None:
        pytest.skip("COORD_TRANSFORMS_YAML not available")

    # Two rows: same station coords, different timestamps.
    # Pick an (x,y) where v1 and v2 predict clearly different outputs.
    # Station (8, 8):  v1 -> (32, 8);  v2 -> (8, 8).
    df = pd.DataFrame([
        {
            "Timestamp": "2025-06-01T12:00:00+00:00",  # pre-2026-04-01 -> v1
            "Sample_IGSN": "ABCDEF00001",
            "valid_igsn": "ABCDEF00001",
            "PDV_FileName": "shot_v1_ch1",
            "Flyer_Row": 1,
            "Flyer_Column": 1,
            "Flyer_X_Position_Final_mm": 8.0,
            "Flyer_Y_Position_Final_mm": 8.0,
        },
        {
            "Timestamp": "2026-05-01T12:00:00+00:00",  # post-boundary -> v2
            "Sample_IGSN": "ABCDEF00002",
            "valid_igsn": "ABCDEF00002",
            "PDV_FileName": "shot_v2_ch1",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 8.0,
            "Flyer_Y_Position_Final_mm": 8.0,
        },
    ])

    validated = {"dataframe": df, "igsn_issues": []}
    matches = {
        0: {"_id": "itemv1", "meta": {"igsn": "ABCDEF00001"}, "name": "shot_v1_ch1.csv"},
        1: {"_id": "itemv2", "meta": {"igsn": "ABCDEF00002"}, "name": "shot_v2_ch1.csv"},
    }
    xrefs = {"matches": matches, "pdv_issues": []}

    captured = []
    mock_girder = MagicMock()
    mock_girder.addMetadataToItem.side_effect = lambda item_id, meta: captured.append(
        (item_id, meta)
    )

    config = ExperimentLogConfig(item_id="src_sheet_item", filename="test.csv")
    ctx = build_asset_context()
    result = enriched_fn(
        context=ctx,
        config=config,
        pdv_cross_references=xrefs,
        validated_rows=validated,
        girder=mock_girder,
    )

    assert result["written_count"] == 2
    assert len(captured) == 2

    by_id = {iid: m for iid, m in captured}
    v1_payload = by_id["itemv1"]
    v2_payload = by_id["itemv2"]

    # Provenance records the resolved version per row
    assert "v1" in v1_payload["coord_provenance"]["transform_version"]
    assert "v2" in v2_payload["coord_provenance"]["transform_version"]

    # Same inputs, different transforms → different Sample_X/Y
    assert (v1_payload["Sample_X"], v1_payload["Sample_Y"]) != (
        v2_payload["Sample_X"],
        v2_payload["Sample_Y"],
    )

    # Exact values per the YAML calibration points.
    # Station (8, 8): v1 -> (32, 8),  v2 -> (8, 8)
    assert v1_payload["Sample_X"] == pytest.approx(32.0, abs=1e-4)
    assert v1_payload["Sample_Y"] == pytest.approx(8.0, abs=1e-4)
    assert v2_payload["Sample_X"] == pytest.approx(8.0, abs=1e-4)
    assert v2_payload["Sample_Y"] == pytest.approx(8.0, abs=1e-4)

    # The asset's version_counter should reflect both versions
    assert result["version_counter"].get("HELIX/v1", 0) == 1
    assert result["version_counter"].get("HELIX/v2", 0) == 1
```

Adjust the expected `Sample_X/Y` values only if the audit step
reveals the YAML's calibration points differ from those listed in
step 2 of the audit. The point of the test is exactness — not
approximate agreement — so recompute by hand if needed.

## What NOT to modify

- Any source file under `aimdl_coord_enrichment/`
- Other tests
- YAML

## Success criteria

```bash
source .venv/bin/activate
pytest tests/test_assets.py::test_enriched_pdv_metadata_version_boundary_dispatch -v
pytest tests/ -v    # whole suite still green
```

If the test fails because the expected (32, 8) / (8, 8) values don't
match the YAML, stop and report the mismatch — don't adjust the
assertions to make the test pass. A surprise in the affine transform
output would indicate either a YAML change or a regression in Step 1.

## Commit

One commit on `refactor/asset-dag`:

```
test: end-to-end timestamp-dispatched transform version

Two-row integration test verifying that enriched_pdv_metadata
dispatches to HELIX/v1 or HELIX/v2 based on the per-row timestamp,
producing the expected exact Sample_X/Y from the YAML calibration
points and recording the resolved version in coord_provenance.

Closes Phase 1.
```

## After Step 5

Phase 1 is done. The existing DAG now:
- Respects the YAML's versioned calibration boundaries
- Writes full coord_provenance on every Girder item
- Surfaces version-resolution failures through coord_transform_check

The next phase (Phase 2) will extract per-instrument adapters into
`aimdl_coord_enrichment/instruments/`, setting up for MAXIMA support in the
new coordinate-enrichment DAG.
