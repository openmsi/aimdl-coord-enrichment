"""Unit tests for the pure helpers in aimdl_coord_enrichment.spreadsheet.

These replace the coverage the old test_validated_rows_pure /
test_pdv_cross_references_pure asset tests gave, now that the in-memory
stages are context-free functions rather than assets.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER
from aimdl_coord_enrichment.spreadsheet import (
    classify_shots,
    shot_fired,
    count_rows_with_pdv,
    pair_traces_to_rows,
    row_filename_stems,
    normalize_experiment_log,
    summarize_pdv_processing,
    validate_log_rows,
    write_pdv_metadata,
    write_processing_manifest,
)


def test_normalize_experiment_log_renames_columns():
    df = pd.DataFrame([
        {"Sample_ID": "ABCDEF12345", "Flyer_X_Position_Corrected (mm)": 10.5},
    ])
    out = normalize_experiment_log(df)
    assert "Sample_IGSN" in out.columns
    assert "Flyer_X_Position_Final_mm" in out.columns
    assert "Sample_ID" not in out.columns


def test_validate_log_rows():
    df = pd.DataFrame([
        {"Sample_IGSN": "ABCDEF12345"},
        {"Sample_IGSN": "INVALID"},
        {"Sample_IGSN": float("nan")},
        {"Sample_IGSN": "XYZABC67890-sub1"},
    ])
    out_df, issues = validate_log_rows(df)

    assert out_df.loc[0, "valid_igsn"] == "ABCDEF12345"
    assert pd.isna(out_df.loc[1, "valid_igsn"])
    assert pd.isna(out_df.loc[2, "valid_igsn"])
    assert out_df.loc[3, "valid_igsn"] == "XYZABC67890-sub1"

    assert len(issues) == 2
    issue_types = {i["issue"] for i in issues}
    assert issue_types == {"invalid_format", "missing"}


def test_row_filename_stems_skips_rows_naming_no_file():
    df = pd.DataFrame([
        {"PDV_FileName": "shot001"},
        {"PDV_FileName": float("nan")},
        {"PDV_FileName": r"C:\\PDV_DATA\\shot003"},
    ])
    assert row_filename_stems(df) == {0: {"shot001"}, 2: {"shot003"}}


def test_row_filename_stems_reads_per_probe_columns():
    """A multipoint log has no bare PDV_FileName, only PDV_<n>_FileName per
    probe. Probes sharing a digitizer channel collapse to one stem."""
    df = pd.DataFrame([
        {"PDV_10_FileName": "C1--shot01", "PDV_6_FileName": "C3--shot01",
         "PDV_9_FileName": "C3--shot01", "PDV_15_FileName": float("nan")},
    ])
    assert row_filename_stems(df) == {0: {"C1--shot01", "C3--shot01"}}


def _trace(name, _id, igsn):
    return {"name": name, "_id": _id,
            "meta": {"igsn": igsn, "data_type": "pdv_trace"}}


def test_pair_traces_to_rows_pairs_and_reports_traces_with_no_row():
    df = pd.DataFrame([
        {"PDV_FileName": "shot001", "valid_igsn": "ABCDEF12345"},
        {"PDV_FileName": float("nan"), "valid_igsn": "ABCDEF12345"},
    ])
    traces = [
        _trace("shot001_ch1.tdms", "a1", "ABCDEF12345"),
        _trace("shot999_ch1.tdms", "z9", "ABCDEF12345"),
    ]
    pairs, issues = pair_traces_to_rows(traces, df)

    assert [(t["_id"], r, p) for t, r, p in pairs] == [("a1", 0, "filename")]
    assert len(issues) == 1
    assert issues[0]["type"] == "no_row_in_log"
    assert issues[0]["trace_id"] == "z9"


def test_pair_traces_to_rows_refuses_a_row_declaring_another_sample():
    """The trace's own IGSN is authoritative. A partition's log may hold rows
    from a restarted run written under the previous sample's identifier;
    applying one would put another sample's coordinates on this trace."""
    df = pd.DataFrame([
        {"PDV_FileName": "shot001", "valid_igsn": "XXXXXX99999"},
    ])
    traces = [_trace("shot001_ch1.tdms", "a1", "ABCDEF12345")]
    pairs, issues = pair_traces_to_rows(traces, df)

    assert pairs == []
    assert len(issues) == 1
    assert issues[0]["type"] == "igsn_mismatch"
    assert issues[0]["trace_igsn"] == "ABCDEF12345"
    assert issues[0]["row_igsn"] == "XXXXXX99999"


def test_pair_traces_to_rows_with_no_log_pairs_nothing():
    """A session whose log was never tagged upstream yields an empty frame;
    its traces are simply unpaired, not errors."""
    traces = [_trace("shot001_ch1.tdms", "a1", "ABCDEF12345")]
    pairs, issues = pair_traces_to_rows(traces, pd.DataFrame())
    assert pairs == []
    assert [i["type"] for i in issues] == ["no_row_in_log"]


def test_count_rows_with_pdv():
    df = pd.DataFrame([
        {"PDV_FileName": "shot001"},
        {"PDV_FileName": ""},
        {"PDV_FileName": float("nan")},
        {"PDV_FileName": "shot004"},
    ])
    assert count_rows_with_pdv(df) == 2


def test_summarize_pdv_processing_clean():
    df = pd.DataFrame([{"valid_igsn": "ABCDEF12345"}])
    pdv_log = {"dataframe": df, "igsn_issues": []}
    pdv_data = {
        "pair_issues": [],
        "write_errors": [],
        "traces_in_partition": 1,
        "paired_count": 1,
        "written_count": 1,
        "coord_failures": 0,
    }
    summary = summarize_pdv_processing(pdv_log, pdv_data)
    assert summary["status"] == "completed_clean"
    assert summary["has_issues"] is False
    assert summary["total_rows"] == 1
    assert summary["rows_valid_igsn"] == 1
    assert summary["traces_enriched"] == 1
    assert all(v == 0 for v in summary["issues_summary"].values())


def test_summarize_pdv_processing_with_warnings():
    df = pd.DataFrame([{"valid_igsn": None}])
    pdv_log = {
        "dataframe": df,
        "igsn_issues": [{"issue": "invalid_format", "row": 0}],
    }
    pdv_data = {
        "pair_issues": [{"type": "no_row_in_log", "trace_id": "z9"}],
        "write_errors": [],
        "traces_in_partition": 1,
        "paired_count": 0,
        "written_count": 0,
        "coord_failures": 0,
    }
    summary = summarize_pdv_processing(pdv_log, pdv_data)
    assert summary["status"] == "completed_with_warnings"
    assert summary["issues_summary"]["igsn_invalid"] == 1
    assert summary["issues_summary"]["trace_no_row"] == 1


def test_write_processing_manifest_success():
    girder = MagicMock()
    summary = {
        "status": "completed_clean",
        "issues_summary": {"igsn_invalid": 0},
        "total_rows": 1,
        "rows_valid_igsn": 1,
        "traces_in_partition": 1,
        "traces_paired": 1,
        "traces_enriched": 1,
    }
    manifest = write_processing_manifest(girder, "item123", summary, run_id="run1")
    assert manifest["status"] == "completed_clean"
    assert manifest["dagster_run_id"] == "run1"
    assert "write_failed" not in manifest
    girder.addMetadataToItem.assert_called_once()
    item_id_arg, payload = girder.addMetadataToItem.call_args[0]
    assert item_id_arg == "item123"
    assert "processing_status" in payload


def test_write_processing_manifest_write_failure():
    girder = MagicMock()
    girder.addMetadataToItem.side_effect = RuntimeError("boom")
    summary = {
        "status": "completed_clean",
        "issues_summary": {},
        "total_rows": 0,
        "rows_valid_igsn": 0,
        "traces_in_partition": 0,
        "traces_paired": 0,
        "traces_enriched": 0,
    }
    manifest = write_processing_manifest(girder, "item123", summary, run_id="run1")
    assert manifest["write_failed"] is True


def test_write_processing_manifest_dry_run_skips_write():
    girder = MagicMock()
    summary = {
        "status": "completed_clean",
        "issues_summary": {},
        "total_rows": 1,
        "rows_valid_igsn": 1,
        "traces_in_partition": 1,
        "traces_paired": 1,
        "traces_enriched": 1,
    }
    manifest = write_processing_manifest(
        girder, "item123", summary, run_id="run1", dry_run=True
    )
    girder.addMetadataToItem.assert_not_called()
    assert "write_failed" not in manifest
    assert manifest["status"] == "completed_clean"


def test_write_pdv_metadata_writes_coords_and_provenance():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1,
            "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    pairs = [
        ({"_id": "pdvitem1", "name": "shot001_ch1.tdms",
          "meta": {"igsn": "ABCDEF12345"}}, 0, "filename"),
    ]
    girder = MagicMock()
    summary = write_pdv_metadata(
        girder, df, pairs,
        run_id="run1",
        source_item_id="src_sheet",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )

    assert summary["written_count"] == 1
    assert summary["write_errors"] == []
    assert summary["version_counter"]
    girder.addMetadataToItem.assert_called_once()
    item_id_arg, payload = girder.addMetadataToItem.call_args[0]
    assert item_id_arg == "pdvitem1"
    assert "Station_X" in payload
    assert "Sample_X" in payload
    prov = payload["coord_provenance"]
    assert prov["instrument"] == "HELIX"
    assert prov["station_coord_source"]["spreadsheet_item_id"] == "src_sheet"
    assert prov["station_coord_source"]["spreadsheet_row_index"] == 0


def test_write_pdv_metadata_per_row_source_item_id():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
            "_source_item_id": "log_item_B",
        },
    ])
    pairs = [({"_id": "pdvitem1", "name": "shot001_ch1.tdms", "meta": {}}, 0, "filename")]
    girder = MagicMock()
    write_pdv_metadata(
        girder, df, pairs,
        run_id="run1",
        source_item_id="log_item_A_fallback",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )
    _, payload = girder.addMetadataToItem.call_args[0]
    assert payload["coord_provenance"]["station_coord_source"]["spreadsheet_item_id"] == "log_item_B"


def test_write_pdv_metadata_records_write_errors():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    pairs = [({"_id": "pdvitem1", "name": "shot001_ch1.tdms", "meta": {}}, 0, "filename")]
    girder = MagicMock()
    girder.addMetadataToItem.side_effect = RuntimeError("girder down")
    summary = write_pdv_metadata(
        girder, df, pairs,
        run_id="run1",
        source_item_id="src",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )
    assert summary["written_count"] == 0
    assert len(summary["write_errors"]) == 1
    assert summary["write_errors"][0]["row"] == 0


def test_write_pdv_metadata_dry_run_skips_writes():
    if _COORD_TRANSFORMER is None:
        pytest.skip("CoordinateTransformer unavailable (YAML missing)")

    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    pairs = [({"_id": "pdvitem1", "name": "shot001_ch1.tdms", "meta": {}}, 0, "filename")]
    girder = MagicMock()
    summary = write_pdv_metadata(
        girder, df, pairs,
        run_id="run1",
        source_item_id="src",
        yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
        dry_run=True,
    )
    girder.addMetadataToItem.assert_not_called()
    assert summary["written_count"] == 0
    assert summary["simulated_count"] == 1
    assert summary["write_errors"] == []
    # Transform still computed in a dry run.
    assert summary["version_counter"]


# --- candidate shots that never fired (station survey 2026-08-31) -----------
# A HELIX log rows every *candidate* shot. The station measures PDV return
# power and decides whether to fire; when it declines, the row keeps a real
# flyer position but produces no trace. Measured across all 214 production
# logs: 3,584/3,584 rows with a PDV_FileName read Notes == "Laser triggered",
# and all 1,506 rows without one carry a skip reason.

def _row(pdv=None, notes="Laser triggered"):
    return {"PDV_FileName": pdv, "Notes": notes}


@pytest.mark.parametrize("notes", [
    "Laser skipped due to invalid signal",
    "Laser skipped due to detection failure",
    "Laser skipped",
    "['Laser skipped', 'Laser skipped']",     # list-valued cell, seen in prod
    "Skipped due to failed flyer detection",
    'Heating error: TypeError("He',
    "",                                        # unrecorded
])
def test_shot_not_fired_for_every_observed_skip_note(notes):
    assert shot_fired(_row(pdv=float("nan"), notes=notes)) is False


def test_shot_fired_only_on_the_positive_note():
    """Tested as a positive match so a new upstream skip string still reads as
    not-fired, rather than silently counting as a fired shot."""
    assert shot_fired(_row(pdv=float("nan"), notes="Laser triggered")) is True
    assert shot_fired(_row(pdv=float("nan"), notes="Laser skipped")) is False


def test_shot_fired_accepts_the_multi_channel_trailing_period():
    """Multi-channel logs write "Laser triggered." with a period. Those logs are
    out of scope today (untagged upstream), but exact equality would mark every
    fired shot as not-fired -- enriching nothing while the checks pass, because
    not-fired rows leave the success-rate denominator. Prefix match avoids that
    trap ahead of time."""
    assert shot_fired(_row(pdv=float("nan"), notes="Laser triggered.")) is True
    assert shot_fired(_row(pdv=float("nan"), notes="Laser triggered")) is True


def test_a_named_row_counts_as_fired_regardless_of_notes():
    assert shot_fired(_row(pdv="C1--20251023--00282", notes="anything")) is True


def test_classify_shots_groups_skip_reasons():
    df = pd.DataFrame([
        _row(pdv="C1--a"),
        _row(pdv="C1--b"),
        _row(pdv=float("nan"), notes="Laser skipped due to invalid signal"),
        _row(pdv=float("nan"), notes="Laser skipped due to invalid signal"),
        _row(pdv=None, notes="Skipped due to failed flyer detection"),
    ])
    out = classify_shots(df)
    assert out["fired"] == 2
    assert out["not_fired"] == 3
    assert out["not_fired_by_reason"] == {
        "Laser skipped due to invalid signal": 2,
        "Skipped due to failed flyer detection": 1,
    }
    assert out["fired_but_unnamed"] == 0


def test_classify_shots_surfaces_fired_but_unnamed():
    """Zero across the current corpus; surfaced so a change upstream is visible
    rather than silently reducing coverage."""
    df = pd.DataFrame([_row(pdv=float("nan"), notes="Laser triggered")])
    out = classify_shots(df)
    assert out["fired"] == 1 and out["fired_but_unnamed"] == 1


def test_laser_energy_is_not_a_valid_discriminator():
    """623 of 1,506 skipped rows carry a non-zero Laser_Target_Energy_mJ (the
    intended energy, recorded before the decision), so energy would
    misclassify 41% of them as fired. Notes is the signal."""
    skipped_with_energy = {
        "PDV_FileName": float("nan"),
        "Notes": "Laser skipped due to invalid signal",
        "Laser_Target_Energy_mJ": 1116.54,
    }
    assert shot_fired(skipped_with_energy) is False


def test_write_pdv_metadata_skips_rows_with_no_corrected_flyer_position():
    """The corrected flyer position is blank on some rows. Writing
    Station/Sample = null would put meaningless metadata on the item and a
    provenance block claiming a transform that never ran."""
    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "shot001",
            "Flyer_Row": 1, "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": float("nan"),
            "Flyer_Y_Position_Final_mm": float("nan"),
        },
    ])
    pairs = [({"_id": "pdvitem1", "name": "shot001_ch1.tdms", "meta": {}}, 0, "filename")]
    girder = MagicMock()
    summary = write_pdv_metadata(
        girder, df, pairs,
        run_id="run1", source_item_id="src", yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )
    girder.addMetadataToItem.assert_not_called()
    assert summary["written_count"] == 0
    assert summary["no_station_coords"] == 1


def test_write_pdv_metadata_records_how_the_trace_was_paired():
    """A trace paired via a sibling channel must say so in its provenance, so
    those items can be re-verified once the log-writer records every probe."""
    df = pd.DataFrame([
        {
            "Timestamp": datetime(2026, 8, 18, 12, 0, 0, tzinfo=timezone.utc),
            "valid_igsn": "ABCDEF12345",
            "PDV_FileName": "C1--shot01",
            "Flyer_Row": 1, "Flyer_Column": 2,
            "Flyer_X_Position_Final_mm": 10.5,
            "Flyer_Y_Position_Final_mm": 20.3,
        },
    ])
    girder = MagicMock()
    pairs = [({"_id": "c2item", "name": "C2--shot01--00000.csv", "meta": {}},
              0, "shot_identity")]
    summary = write_pdv_metadata(
        girder, df, pairs,
        run_id="run1", source_item_id="src", yaml_sha256="deadbeef",
        transformer_version="0.0.0-test",
    )
    assert summary["paired_by_shot_identity"] == 1
    _, payload = girder.addMetadataToItem.call_args[0]
    assert payload["coord_provenance"]["station_coord_source"]["pairing"] == "shot_identity"
    # and it carries the same coordinates the named siblings get
    assert payload["Station_X"] == 10.5
