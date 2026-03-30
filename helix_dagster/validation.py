import math

from helix_dagster.constants import IGSN_PATTERN


def validate_igsn(sample_id):
    """Validate a sample identifier against the IGSN pattern.

    Parameters
    ----------
    sample_id : any
        The raw Sample_ID value from the spreadsheet row.

    Returns
    -------
    valid_igsn : str or None
        The matched IGSN string, or None if invalid/missing.
    issue : dict or None
        A structured issue dict if validation failed, or None if valid.
    """
    if sample_id is None or (isinstance(sample_id, float) and math.isnan(sample_id)):
        return None, {"value": None, "issue": "missing"}

    sample_str = str(sample_id)
    if sample_str == "":
        return None, {"value": None, "issue": "missing"}

    match = IGSN_PATTERN.search(sample_str)
    if not match:
        return None, {"value": sample_str, "issue": "invalid_format"}

    return match.group(0), None
