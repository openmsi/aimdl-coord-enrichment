from aimdl_coord_enrichment.matching import (
    PAIRING_FILENAME,
    PAIRING_SHOT_IDENTITY,
    log_filename_stem,
    match_trace_to_row,
)


# Row stems are {row_index: stem}, as produced by spreadsheet.row_filename_stems.
ROWS = {0: {"shot001"}, 1: {"shot002"}}


def test_matches_the_single_naming_row():
    row, pairing, issue = match_trace_to_row("shot002_ch1.tdms", ROWS)
    assert row == 1
    assert issue is None


def test_trace_with_no_row_in_the_log():
    row, pairing, issue = match_trace_to_row("shot999_ch1.tdms", ROWS)
    assert row is None
    assert issue["type"] == "no_row_in_log"


def test_two_rows_claiming_one_trace_is_ambiguous():
    """Contradictory logs in one partition must never be resolved by guessing:
    picking either row could write a coordinate from the wrong shot."""
    row, pairing, issue = match_trace_to_row("shot001_ch1.tdms", {3: {"shot001"}, 7: {"shot001"}})
    assert row is None
    assert issue["type"] == "ambiguous_row"
    assert issue["rows"] == [3, 7]


# --- filename stem normalization -------------------------------------------
# Some logs record the station-local path the file was written to before it was
# streamed into Girder. Girder is the only place the file exists, so the
# directory part names nothing and only the trailing component identifies it.

WIN = r"C:\Users\Administrator\Desktop\PDV_DATA\JHAMAL00018-005_69d6_1_894_shot01_ch1"
STEM = "JHAMAL00018-005_69d6_1_894_shot01_ch1"


def test_windows_path_reduces_to_its_basename():
    assert log_filename_stem(WIN) == STEM


def test_plain_filename_is_unchanged():
    assert log_filename_stem("C1--20251023--00001") == "C1--20251023--00001"


def test_blank_cells_name_no_file():
    # Distinguishes "this row names no file" from "named a file we can't match".
    assert log_filename_stem(None) is None
    assert log_filename_stem(float("nan")) is None
    assert log_filename_stem("   ") is None


def test_trace_matches_a_row_recorded_as_a_windows_path():
    row, pairing, issue = match_trace_to_row(f"{STEM}.csv", {4: {log_filename_stem(WIN)}})
    assert issue is None
    assert row == 4


# --- channel-prefix fallback -----------------------------------------------
# A trace is stored under the digitizer channel that recorded it, so its Girder
# name may carry a leading "C<n>--" that the log's PDV_FileName omits.

SHEET = "JHAMAC00003-S1R5C3_68efdf9ebe3476695206a18e_0_1503_2026-07-14_13-21-58_shot01"


def test_matches_across_channel_prefix():
    row, pairing, issue = match_trace_to_row(f"C1--{SHEET}--00000.csv", {2: {SHEET}})
    assert issue is None
    assert row == 2


def test_exact_prefix_match_wins_over_the_fallback():
    """The fallback must never change the outcome for a trace that already
    matched, so it only runs when the exact pass finds nothing."""
    row, pairing, issue = match_trace_to_row(f"{SHEET}--00000.csv", {5: {SHEET}, 6: {"C1--" + SHEET}})
    assert issue is None
    assert row == 5


def test_unrelated_names_still_find_no_row():
    row, pairing, issue = match_trace_to_row("C1--SOMETHINGELSE_2026-01-01_shot01--00000.csv",
                                    {1: {SHEET}})
    assert row is None
    assert issue["type"] == "no_row_in_log"


def test_only_a_channel_prefix_is_stripped():
    """Guard the rule stays narrow: an arbitrary leading token is not ignored."""
    row, pairing, issue = match_trace_to_row(f"XX--{SHEET}--00000.csv", {1: {SHEET}})
    assert row is None
    assert issue["type"] == "no_row_in_log"


def test_one_row_naming_several_files_pairs_each_of_them():
    """A multipoint row records one file per probe; probes sharing a digitizer
    channel collapse to one stem. Every distinct file the row names is that
    shot's, so each pairs to the same row and takes the same coordinates."""
    rows = {0: {"C1--shot01", "C3--shot01"}}
    assert match_trace_to_row("C1--shot01--00000.csv", rows) == (0, PAIRING_FILENAME, None)
    assert match_trace_to_row("C3--shot01--00000.csv", rows) == (0, PAIRING_FILENAME, None)


# --- shot-identity pass (multipoint probes the log-writer omits) ------------
# A multipoint shot writes one trace per probe and the log-writer misses some
# (measured: every multipoint log names C1 and C3, never C2). The coordinate is
# the flyer position, a property of the shot, so an unnamed channel's value is
# the same one the row already gives its named siblings.

SHOT = "JHAMAL00021-003_6a83_2_2038_2026-08-18_12-23-49_shot01--00000"
MULTI = {0: {f"C1--{SHOT}", f"C3--{SHOT}"}}


def test_named_channels_pair_by_filename():
    for ch in ("C1", "C3"):
        row, pairing, issue = match_trace_to_row(f"{ch}--{SHOT}.csv", MULTI)
        assert (row, pairing, issue) == (0, PAIRING_FILENAME, None)


def test_unnamed_sibling_channel_pairs_by_shot_identity():
    row, pairing, issue = match_trace_to_row(f"C2--{SHOT}.csv", MULTI)
    assert row == 0
    assert pairing == PAIRING_SHOT_IDENTITY
    assert issue is None


def test_shot_identity_does_not_reach_a_different_shot():
    row, pairing, issue = match_trace_to_row(
        "C2--OTHERSHOT_2026-08-18_12-99-99_shot99--00000.csv", MULTI
    )
    assert row is None
    assert issue["type"] == "no_row_in_log"


def test_shot_identity_refuses_two_rows_claiming_one_shot():
    rows = {3: {f"C1--{SHOT}"}, 7: {f"C3--{SHOT}"}}
    row, pairing, issue = match_trace_to_row(f"C2--{SHOT}.csv", rows)
    assert row is None
    assert issue["type"] == "ambiguous_row"
    assert issue["via"] == "shot_identity"
