#!/usr/bin/env python3
"""Probe MAXIMA run-folder layout on data.htmdec.org for diagnosis.

Diagnostic only. Walks a Girder folder hierarchy and tabulates what
items exist where, with their `meta.data_type` tags. Output is meant
to drive the Defect 4 design discussion, not to be committed as
pipeline code.

What this answers
-----------------
Given a MAXIMA run folder (or the parent `automatic_mode` folder),
for every item it can find:

  * What's at the run-folder root vs in `raw/` vs in any other
    subfolder
  * What `meta.data_type` each item carries
  * What naming pattern each (location, data_type) combination
    follows
  * Which run folders are missing `instructions.txt`

Then it cross-references against `/aimdl/partition/details` to show
whether the items the partition endpoint returns for a given
data_type match what the folder walk found.

Usage
-----
    export GIRDER_API_URL=https://data.htmdec.org/api/v1
    export GIRDER_API_KEY=<your_key>

    # Probe a single run folder you already know:
    python .claude/scratch/probe_maxima_layout.py \\
        --folder-id 69efe29a360b5a8ea59a78fa

    # Probe N sample run folders under automatic_mode:
    python .claude/scratch/probe_maxima_layout.py \\
        --folder-id <automatic_mode_folder_id> --max-runs 8

    # Output:
    #   stderr: human-readable summary
    #   probe_output.json: full structured probe (share back for analysis)

The script auto-detects whether the target folder is itself a run
folder or a container of run folders.
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

try:
    import girder_client
except ImportError:
    sys.exit(
        "ERROR: girder_client not installed. Run from the project venv:\n"
        "  source .venv/bin/activate"
    )


# ─── Girder helpers ─────────────────────────────────────────────────


def make_client():
    url = os.environ.get("GIRDER_API_URL")
    key = os.environ.get("GIRDER_API_KEY")
    if not url or not key:
        sys.exit("ERROR: set GIRDER_API_URL and GIRDER_API_KEY")
    gc = girder_client.GirderClient(apiUrl=url)
    gc.authenticate(apiKey=key)
    return gc


def get_folder(gc, folder_id):
    return gc.get(f"folder/{folder_id}")


def list_items_paginated(gc, folder_id, page=1000):
    """Fetch every item in a folder, paging via offset."""
    out = []
    offset = 0
    while True:
        batch = gc.get(
            "item",
            parameters={"folderId": folder_id, "limit": page, "offset": offset},
        )
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def list_subfolders(gc, parent_id, limit=1000):
    return gc.get(
        "folder",
        parameters={"parentType": "folder", "parentId": parent_id, "limit": limit},
    )


def fetch_partition_details(gc, data_type, key):
    return gc.get(
        "aimdl/partition/details",
        parameters={"dataType": data_type, "key": key},
    )


# ─── Item summarization ─────────────────────────────────────────────


def item_summary(item):
    """Extract diagnostic fields from a Girder item."""
    meta = item.get("meta") or {}
    return {
        "_id": item.get("_id"),
        "name": item.get("name"),
        "folderId": item.get("folderId"),
        "size": item.get("size"),
        "data_type": meta.get("data_type"),
        "igsn": meta.get("igsn"),
        "experiment_date": meta.get("experiment_date"),
        "kafka_topic": meta.get("KafkaTopic"),
    }


def name_suffix(name):
    """Group filenames by their non-index pattern.

    scan_point_0.tiff           → .tiff
    scan_point_24_master.h5     → _master.h5
    scan_point_3_data_000001.h5 → _data_NNNNNN.h5
    scan_point_5_xrd.csv        → _xrd.csv
    scan_point_2.xrf            → .xrf
    instructions.txt            → instructions.txt (full)
    other                       → name as-is
    """
    if not name:
        return "(unnamed)"
    if name == "instructions.txt":
        return name
    m = re.match(r"^scan_point_\d+(.*)$", name)
    if not m:
        return name
    suffix = m.group(1) or "(empty)"
    suffix = re.sub(r"_data_\d+", "_data_NNNNNN", suffix)
    return suffix


# ─── Single-run probe ───────────────────────────────────────────────


def probe_run_folder(gc, run_folder, log):
    rf_id = run_folder["_id"]
    rf_name = run_folder["name"]
    print(f"  probing run: {rf_name}", file=log)

    root_items = list_items_paginated(gc, rf_id)
    root_summary = [item_summary(it) for it in root_items]
    has_instructions = any(it["name"] == "instructions.txt" for it in root_items)

    subs = list_subfolders(gc, rf_id)
    sub_results = {}
    for sub in subs:
        sub_items = list_items_paginated(gc, sub["_id"])
        sub_results[sub["name"]] = {
            "folder_id": sub["_id"],
            "items": [item_summary(it) for it in sub_items],
        }

    # If the items have a partition key, capture it for cross-reference.
    partition_key = None
    for it in root_items + [
        sub_it for sub in sub_results.values() for sub_it in sub["items"]
    ]:
        igsn = it.get("igsn")
        exp_date = it.get("experiment_date")
        if igsn and exp_date:
            partition_key = f"{igsn}//{exp_date}"
            break

    return {
        "folder_id": rf_id,
        "folder_name": rf_name,
        "has_instructions_txt": has_instructions,
        "inferred_partition_key": partition_key,
        "root_items": root_summary,
        "subfolders": sub_results,
    }


# ─── Aggregation ────────────────────────────────────────────────────


def aggregate_counts(probes):
    """Counts of items per (location, data_type)."""
    counts = defaultdict(Counter)
    for p in probes:
        for it in p["root_items"]:
            counts["root"][str(it["data_type"])] += 1
        for sub_name, sub in p["subfolders"].items():
            for it in sub["items"]:
                counts[f"sub:{sub_name}"][str(it["data_type"])] += 1
    return {loc: dict(ctr) for loc, ctr in counts.items()}


def aggregate_name_patterns(probes):
    """Counts of (location, data_type, name_suffix)."""
    out = defaultdict(Counter)
    for p in probes:
        for it in p["root_items"]:
            out[("root", str(it["data_type"]))][name_suffix(it["name"])] += 1
        for sub_name, sub in p["subfolders"].items():
            for it in sub["items"]:
                out[(f"sub:{sub_name}", str(it["data_type"]))][
                    name_suffix(it["name"])
                ] += 1
    return {f"{loc} | {dt}": dict(ctr) for (loc, dt), ctr in out.items()}


def cross_reference_partition_endpoint(gc, probes, log):
    """For each probed run folder with an inferred partition key, ask
    the partition endpoint what items it returns for each data_type
    seen in the folder walk. Compares item-id sets so we can see if
    the endpoint is missing items that exist on disk.
    """
    results = []
    seen_data_types = set()
    for p in probes:
        for it in p["root_items"]:
            if it["data_type"]:
                seen_data_types.add(it["data_type"])
        for sub in p["subfolders"].values():
            for it in sub["items"]:
                if it["data_type"]:
                    seen_data_types.add(it["data_type"])

    if not seen_data_types:
        print("  (no data_type tags found; skipping cross-ref)", file=log)
        return []

    for p in probes:
        key = p["inferred_partition_key"]
        if not key:
            continue
        print(f"  cross-ref: {p['folder_name']} key={key}", file=log)
        for dt in sorted(seen_data_types):
            try:
                returned = fetch_partition_details(gc, dt, key)
            except Exception as exc:  # noqa: BLE001
                results.append({
                    "folder_name": p["folder_name"],
                    "key": key,
                    "data_type": dt,
                    "error": str(exc),
                })
                continue
            returned_ids = {it["_id"] for it in returned}
            walk_ids = {
                it["_id"]
                for it in p["root_items"] + [
                    si for sub in p["subfolders"].values() for si in sub["items"]
                ]
                if it["data_type"] == dt
            }
            results.append({
                "folder_name": p["folder_name"],
                "key": key,
                "data_type": dt,
                "partition_endpoint_count": len(returned_ids),
                "folder_walk_count": len(walk_ids),
                "in_endpoint_not_walk": sorted(returned_ids - walk_ids),
                "in_walk_not_endpoint": sorted(walk_ids - returned_ids),
            })
    return results


# ─── Main ───────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser(description="Probe MAXIMA folder layout")
    ap.add_argument("--folder-id", required=True,
                    help="A run folder id, or a container (e.g. automatic_mode).")
    ap.add_argument("--max-runs", type=int, default=5,
                    help="If folder-id is a container, sample this many runs.")
    ap.add_argument("--out", default="probe_output.json")
    ap.add_argument("--skip-cross-ref", action="store_true",
                    help="Skip the /aimdl/partition/details cross-reference.")
    args = ap.parse_args()

    log = sys.stderr
    gc = make_client()

    target = get_folder(gc, args.folder_id)
    print(f"target: {target['name']} (id={target['_id']})", file=log)

    target_items = list_items_paginated(gc, target["_id"])
    target_subs = list_subfolders(gc, target["_id"])

    is_run_folder = (
        any(it["name"] == "instructions.txt" for it in target_items)
        or any(re.match(r"^scan_point_\d+", it["name"] or "") for it in target_items)
        or (target_items and not target_subs)  # leaf folder with items, no subfolders
    )

    if is_run_folder:
        print("target appears to be a single run folder", file=log)
        probes = [probe_run_folder(gc, target, log)]
    else:
        if not target_subs:
            sys.exit("ERROR: target has neither items nor subfolders")
        sample = target_subs[: args.max_runs]
        print(
            f"target appears to be a container; sampling "
            f"{len(sample)} of {len(target_subs)} subfolders",
            file=log,
        )
        probes = [probe_run_folder(gc, sub, log) for sub in sample]

    counts = aggregate_counts(probes)
    name_patterns = aggregate_name_patterns(probes)

    cross_ref = []
    if not args.skip_cross_ref:
        print("cross-referencing partition endpoint...", file=log)
        cross_ref = cross_reference_partition_endpoint(gc, probes, log)

    output = {
        "target_folder_id": target["_id"],
        "target_folder_name": target["name"],
        "probed_run_count": len(probes),
        "counts_by_location_data_type": counts,
        "name_pattern_breakdown": name_patterns,
        "missing_instructions_txt": [
            p["folder_name"] for p in probes if not p["has_instructions_txt"]
        ],
        "partition_endpoint_cross_ref": cross_ref,
        "probes": probes,
    }

    with open(args.out, "w") as f:
        json.dump(output, f, indent=2, default=str)

    # Human-readable summary on stderr
    print("\n" + "=" * 60, file=log)
    print(f"SUMMARY ({len(probes)} run folder(s) probed)", file=log)
    print("=" * 60, file=log)
    miss = output["missing_instructions_txt"]
    print(f"\nMissing instructions.txt: {len(miss)}/{len(probes)}", file=log)
    if miss:
        for n in miss[:10]:
            print(f"  - {n}", file=log)
        if len(miss) > 10:
            print(f"  ... and {len(miss) - 10} more", file=log)

    print("\nItems per (location, data_type):", file=log)
    for loc in sorted(counts):
        print(f"  {loc}", file=log)
        for dt, n in sorted(counts[loc].items(), key=lambda x: -x[1]):
            print(f"    {dt:30s} {n}", file=log)

    print("\nName-pattern breakdown:", file=log)
    for key in sorted(name_patterns):
        print(f"  {key}", file=log)
        for suf, n in sorted(name_patterns[key].items(), key=lambda x: -x[1]):
            print(f"    {suf:30s} {n}", file=log)

    if cross_ref:
        print("\nPartition-endpoint cross-reference:", file=log)
        # Aggregate across runs by data_type
        by_dt = defaultdict(lambda: {"endpoint": 0, "walk": 0,
                                     "endpoint_only": 0, "walk_only": 0})
        for r in cross_ref:
            if "error" in r:
                continue
            dt = r["data_type"]
            by_dt[dt]["endpoint"] += r["partition_endpoint_count"]
            by_dt[dt]["walk"] += r["folder_walk_count"]
            by_dt[dt]["endpoint_only"] += len(r["in_endpoint_not_walk"])
            by_dt[dt]["walk_only"] += len(r["in_walk_not_endpoint"])
        for dt in sorted(by_dt):
            row = by_dt[dt]
            print(
                f"  {dt:25s} endpoint={row['endpoint']:5d}  walk={row['walk']:5d}"
                f"  endpoint_only={row['endpoint_only']}  walk_only={row['walk_only']}",
                file=log,
            )

    print(f"\nFull JSON: {args.out}", file=log)


if __name__ == "__main__":
    main()
