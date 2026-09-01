import math
import re

# A PDV trace is stored under the digitizer channel that recorded it, so its
# Girder name may carry a leading channel prefix that the experiment log's
# PDV_FileName does not:
#
#   log:  JHAMAC00003-S1R5C3_68efdf9e..._2026-07-14_13-21-58_shot01
#   item: C1--JHAMAC00003-S1R5C3_68efdf9e..._2026-07-14_13-21-58_shot01--00000.csv
#
# `startswith` tolerates the trailing "--00000.csv" but not the leading "C1--".
# Measured across all 214 tagged log partitions (2026-08-31), that prefix alone
# accounted for 1,156 fired shots -- 32% of every fired shot on record.
_CHANNEL_PREFIX_RE = re.compile(r"^C\d+--")


def _strip_channel_prefix(name):
    return _CHANNEL_PREFIX_RE.sub("", name)


def match_pdv_file(pdv_items, pdv_filename):
    """Match a PDV filename to items in the PDV inventory.

    Exact prefix match is tried first and wins outright; only when it finds
    nothing is the channel prefix ignored. Ordering it that way means the
    fallback can never change the outcome for a filename that already matched.

    The unique-match requirement is preserved on both paths (INV-5): more than
    one candidate is an ambiguity, never an arbitrary pick. Measured on the
    current corpus the fallback yields exactly one candidate for all 1,156 rows
    it newly resolves -- zero ambiguities -- but the guard stays, so a future
    ingest that does produce two channels for one row fails loudly instead of
    silently enriching the wrong file.

    Parameters
    ----------
    pdv_items : list of dict
        The full PDV inventory (list of Girder item dicts).
    pdv_filename : any
        The PDV_FileName value from the spreadsheet row.

    Returns
    -------
    pdv_item : dict or None
        The matched Girder item, or None if not found/ambiguous.
    issue : dict or None
        A structured issue dict if matching failed, or None if matched.
    """
    if pdv_filename is None or (isinstance(pdv_filename, float) and math.isnan(pdv_filename)):
        return None, None

    fname = str(pdv_filename)
    if fname == "":
        return None, None

    matches = [i for i in pdv_items if i["name"].startswith(fname)]

    if len(matches) == 1:
        return matches[0], None

    if len(matches) == 0:
        relaxed = [
            i for i in pdv_items
            if _strip_channel_prefix(i["name"]).startswith(fname)
        ]
        if len(relaxed) == 1:
            return relaxed[0], None
        if len(relaxed) > 1:
            return None, {
                "pdv_filename": pdv_filename,
                "type": "ambiguous",
                "matches": [m["name"] for m in relaxed],
                "via": "channel_prefix",
            }
        return None, {"pdv_filename": pdv_filename, "type": "not_found"}

    names = [m["name"] for m in matches]
    return None, {"pdv_filename": pdv_filename, "type": "ambiguous", "matches": names}
