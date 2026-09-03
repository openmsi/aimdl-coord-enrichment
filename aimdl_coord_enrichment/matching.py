"""Pair a PDV trace with the experiment-log row that describes its shot.

The trace is the unit of work. Within one ``"<igsn>//<experiment_date>"``
partition we hold the partition's traces and the partition's log row(s), and
the only link between the two is the filename the log recorded for the shot.
Matching is therefore scoped to a single partition — a few dozen rows — not to
the whole collection.

Two normalizations are needed before the two strings can be compared.

**The log's filename may be a station-local absolute path.** Some logs record
where the file was written on the acquisition PC before it was streamed into
Girder:

    log:  C:\\Users\\Administrator\\Desktop\\PDV_DATA\\JHAMAL00018-005_..._shot01_ch1
    item: JHAMAL00018-005_..._shot01_ch1.csv

The directory part is a historical artifact and names nothing in Girder, which
is the only place the file exists. Only the trailing component identifies the
object, so the stem is taken with ``ntpath.basename`` — ``ntpath``, not
``os.path``, because this runs on POSIX and ``os.path`` does not split on
backslashes.

**The item may carry a leading digitizer-channel prefix the log omits.**

    log:  JHAMAC00003-S1R5C3_68efdf9e..._2026-07-14_13-21-58_shot01
    item: C1--JHAMAC00003-S1R5C3_68efdf9e..._2026-07-14_13-21-58_shot01--00000.csv

A prefix match tolerates the trailing ``--00000.csv`` but not the leading
``C1--``, so the prefix is stripped on a second pass.

**A multipoint shot writes one trace per probe, and the log-writing software
does not record all of them.** Measured 2026-09-02: every multipoint log names
C1 and C3 and never C2, though C2 and C3 exist in equal numbers; some earlier
runs name only one channel of three. The omission is a known upstream bug,
weeks from a fix.

The coordinate written is the **flyer position**, which is a property of the
shot, not of the probe — every probe in one shot sees the same flyer in the
same place. So an unnamed channel's coordinate is not unknown: it is exactly
the value the row already gives for its named siblings, and identical to what
the corrected log will supply. A third pass therefore matches on shot identity,
stripping the channel prefix from *both* sides. Pairings made that way are
tagged ``shot_identity`` in the provenance so they can be re-verified against
the corrected logs later.
"""

import math
import ntpath
import re

_CHANNEL_PREFIX_RE = re.compile(r"^C\d+--")


def _strip_channel_prefix(name):
    return _CHANNEL_PREFIX_RE.sub("", name)


def log_filename_stem(value):
    """Normalize a log row's PDV filename to the part that names a Girder item.

    Returns None for blank/NaN cells, so callers can treat "this row names no
    file" and "this row names a file we could not match" as distinct.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    stem = ntpath.basename(str(value).strip())
    return stem or None


PAIRING_FILENAME = "filename"
PAIRING_SHOT_IDENTITY = "shot_identity"


def match_trace_to_row(trace_name, row_stems):
    """Find the single log row whose filename stem names this trace.

    Parameters
    ----------
    trace_name : str
        The Girder item name of the PDV trace.
    row_stems : dict
        ``{row_index: {stem, ...}}`` for the rows of this partition's log(s)
        that name at least one file, as produced by ``row_filename_stems``. A
        multipoint row names one stem per distinct file it recorded.

    Returns
    -------
    (row_index, pairing, issue)
        On success ``issue`` is None and ``pairing`` says how the match was
        made — ``PAIRING_FILENAME`` when the row names this exact file, or
        ``PAIRING_SHOT_IDENTITY`` when it names a sibling channel of the same
        shot. On failure ``row_index`` and ``pairing`` are None.

        The passes run narrowest-first and each runs only if the previous
        found nothing, so a wider rule can never change an outcome an earlier
        one already decided. More than one candidate row on any pass is an
        ambiguity, never an arbitrary pick: two rows claiming one trace means
        the partition holds contradictory logs, and guessing would write a
        coordinate from the wrong shot.
    """
    hits = [i for i, stems in row_stems.items()
            if any(trace_name.startswith(s) for s in stems)]
    if len(hits) == 1:
        return hits[0], PAIRING_FILENAME, None
    if len(hits) > 1:
        return None, None, {"trace_name": trace_name, "type": "ambiguous_row",
                            "rows": sorted(hits)}

    # The item carries a channel prefix the log omitted.
    bare = _strip_channel_prefix(trace_name)
    relaxed = [i for i, stems in row_stems.items()
               if any(bare.startswith(s) for s in stems)]
    if len(relaxed) == 1:
        return relaxed[0], PAIRING_FILENAME, None
    if len(relaxed) > 1:
        return None, None, {"trace_name": trace_name, "type": "ambiguous_row",
                            "rows": sorted(relaxed), "via": "channel_prefix"}

    # Same shot, different probe: the log names a sibling channel but not this
    # one. Compare with the channel stripped from both sides. The rest of the
    # name carries the IGSN, run id, timestamp and shot number, so it is unique
    # to one shot.
    shot = [i for i, stems in row_stems.items()
            if any(bare.startswith(_strip_channel_prefix(s)) for s in stems)]
    if len(shot) == 1:
        return shot[0], PAIRING_SHOT_IDENTITY, None
    if len(shot) > 1:
        return None, None, {"trace_name": trace_name, "type": "ambiguous_row",
                            "rows": sorted(shot), "via": "shot_identity"}

    return None, None, {"trace_name": trace_name, "type": "no_row_in_log"}
