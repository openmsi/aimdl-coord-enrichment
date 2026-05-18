import math


def match_pdv_file(pdv_items, pdv_filename):
    """Match a PDV filename to items in the PDV inventory.

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

    if len(matches) == 0:
        return None, {"pdv_filename": pdv_filename, "type": "not_found"}

    if len(matches) > 1:
        names = [m["name"] for m in matches]
        return None, {"pdv_filename": pdv_filename, "type": "ambiguous", "matches": names}

    return matches[0], None
