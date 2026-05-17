"""Overwrite-policy evaluator for coordinate enrichment.

Pure decision function: given a newly-computed coord_provenance
payload and the item's currently-stored coord_provenance, should
this run write?
"""

from __future__ import annotations

from typing import Any


def should_write(
    new_prov: dict[str, Any], stored_prov: dict[str, Any] | None
) -> tuple[bool, str]:
    """Decide whether to write.

    Returns ``(write?, reason)``. Reason strings:
      - "first_write" — no stored provenance exists
      - "yaml_sha256_changed"
      - "transformer_version_changed"
      - "station_coord_source_changed"
      - "transform_version_changed"
      - "no_change" (write=False)
    """
    if not isinstance(stored_prov, dict):
        return True, "first_write"

    if new_prov.get("transform_yaml_sha256") != stored_prov.get("transform_yaml_sha256"):
        return True, "yaml_sha256_changed"
    if new_prov.get("transformer_version") != stored_prov.get("transformer_version"):
        return True, "transformer_version_changed"
    if new_prov.get("transform_version") != stored_prov.get("transform_version"):
        return True, "transform_version_changed"
    if new_prov.get("station_coord_source") != stored_prov.get("station_coord_source"):
        return True, "station_coord_source_changed"

    return False, "no_change"
