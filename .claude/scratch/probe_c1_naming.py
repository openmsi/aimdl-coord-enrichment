"""Read-only probe: is the C1-named ALPSS family a pre-IGSN-naming cohort?

GET only. Writes nothing to Girder.

Reproduce:
    set -a; . ./.env; set +a
    .venv/bin/python .claude/scratch/probe_c1_naming.py
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

import girder_client

from aimdl_coord_enrichment.girder_io import fetch_all_aimdl_datafiles
from aimdl_coord_enrichment.instruments.helix import (
    alpss_shot_stem,
    find_parent_pdv_item_id,
)

DATA_TYPES = ["pdv_trace", "pdv_alpss_output", "pdv_alpss_result", "pdv_alpss_results"]

C_FAMILY_RE = re.compile(r"^C\d+--")
IGSN_HEAD_RE = re.compile(r"^[A-Za-z]{6}\d{5}")
C_DATE_RE = re.compile(r"--(\d{8})--")
ISO_DATE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_")


def family(name):
    if C_FAMILY_RE.match(name):
        return "c_family"
    if IGSN_HEAD_RE.match(name):
        return "igsn_named"
    if name.startswith("_"):
        return "leading_underscore"
    if "test" in name.lower():
        return "test_junk"
    return "other"


def embedded_date(name):
    """YYYY-MM from the filename itself, or None."""
    m = C_DATE_RE.search(name)
    if m:
        d = m.group(1)
        return f"{d[0:4]}-{d[4:6]}"
    m = ISO_DATE_RE.search(name)
    if m:
        return m.group(1)[:7]
    return None


def created_month(item):
    c = item.get("created") or ""
    return c[:7] or None


def main():
    gc = girder_client.GirderClient(apiUrl=os.environ["GIRDER_API_URL"])
    gc.authenticate(apiKey=os.environ["GIRDER_API_KEY"])

    pools = {}
    for dt in DATA_TYPES:
        items = fetch_all_aimdl_datafiles(gc, dt)
        pools[dt] = items
        print(f"fetched {len(items):>6} {dt}", flush=True)

    pdv_pool = pools["pdv_trace"]
    report = {"counts": {dt: len(v) for dt, v in pools.items()}, "by_data_type": {}}

    for dt, items in pools.items():
        fam_counts = Counter()
        fam_dates = defaultdict(Counter)
        fam_created = defaultdict(Counter)
        fam_igsn = defaultdict(set)
        no_igsn = 0
        for it in items:
            name = it.get("name", "")
            meta = it.get("meta") or {}
            f = family(name)
            fam_counts[f] += 1
            if not meta.get("igsn"):
                no_igsn += 1
            else:
                fam_igsn[f].add(meta["igsn"])
            d = embedded_date(name)
            if d:
                fam_dates[f][d] += 1
            cm = created_month(it)
            if cm:
                fam_created[f][cm] += 1
        report["by_data_type"][dt] = {
            "total": len(items),
            "no_igsn": no_igsn,
            "families": dict(fam_counts),
            "distinct_igsn_by_family": {k: len(v) for k, v in fam_igsn.items()},
            "embedded_date_range_by_family": {
                k: [min(v), max(v)] for k, v in fam_dates.items() if v
            },
            "created_range_by_family": {
                k: [min(v), max(v)] for k, v in fam_created.items() if v
            },
            "embedded_month_hist_by_family": {
                k: dict(sorted(v.items())) for k, v in fam_dates.items() if v
            },
        }

    # --- parent resolution, ALPSS types only, mirroring production ---
    resolution = {}
    for dt in ["pdv_alpss_output", "pdv_alpss_result", "pdv_alpss_results"]:
        items = pools[dt]
        buckets = defaultdict(lambda: Counter())
        orphan_examples = defaultdict(list)
        for it in items:
            name = it.get("name", "")
            f = family(name)
            stem = alpss_shot_stem(name)
            if stem is None:
                buckets[f]["stem_fail"] += 1
                continue
            parent = find_parent_pdv_item_id(it, pdv_pool)
            if parent:
                buckets[f]["resolvable"] += 1
            else:
                buckets[f]["orphan"] += 1
                if len(orphan_examples[f]) < 5:
                    orphan_examples[f].append(name)
        resolution[dt] = {
            "by_family": {k: dict(v) for k, v in buckets.items()},
            "orphan_examples": dict(orphan_examples),
        }
    report["resolution"] = resolution

    # --- do C-family IGSNs also have IGSN-named pdv_trace coverage? ---
    pdv_igsn_by_family = defaultdict(set)
    for it in pdv_pool:
        ig = (it.get("meta") or {}).get("igsn")
        if ig:
            pdv_igsn_by_family[family(it.get("name", ""))].add(ig)
    pdv_igsn_named = pdv_igsn_by_family["igsn_named"]

    c_igsns = set()
    for dt in ["pdv_alpss_output", "pdv_alpss_result", "pdv_alpss_results"]:
        for it in pools[dt]:
            if family(it.get("name", "")) == "c_family":
                ig = (it.get("meta") or {}).get("igsn")
                if ig:
                    c_igsns.add(ig)
    report["c_family_igsn_coverage"] = {
        "distinct_c_family_igsns": len(c_igsns),
        "also_have_igsn_named_pdv_trace": len(c_igsns & pdv_igsn_named),
        "no_igsn_named_pdv_trace": len(c_igsns - pdv_igsn_named),
        "examples_uncovered": sorted(c_igsns - pdv_igsn_named)[:10],
    }

    out = os.path.join(os.path.dirname(__file__), "probe_c1_naming_result.json")
    with open(out, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:400])
    print(f"\nfull result -> {out}")


if __name__ == "__main__":
    sys.exit(main())
