#!/usr/bin/env python
"""Execute the HELIX enrichment pass, partition by partition.

Runs ``process_helix_assets_job`` against the real Dagster instance (so every
run is recorded and visible in the UI at :3000) and writes coordinate metadata
to Girder.

Writes are gated on ``--live``. Without it every op runs with ``dry_run=True``
and nothing is written, which is the same rehearsal the readiness script does.

See docs/runbooks/live_enrichment_pass.md.

Usage:
    .venv/bin/python operations/run_live_pass.py --partitions KEY [KEY ...]
    .venv/bin/python operations/run_live_pass.py --all --live
    .venv/bin/python operations/run_live_pass.py --month 2026-08 --live
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

OPS = ["pdv_data", "pdv_processing_manifest"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument("--partitions", nargs="+", help="explicit partition keys")
    sel.add_argument("--all", action="store_true", help="every registered partition")
    sel.add_argument("--month", help="partitions whose date starts with this, e.g. 2026-08")
    ap.add_argument("--live", action="store_true",
                    help="perform real Girder writes. Without this, dry run.")
    ap.add_argument("--limit", type=int, default=None, help="cap the number of partitions")
    args = ap.parse_args()

    for v in ("GIRDER_API_URL", "GIRDER_API_KEY", "COORD_TRANSFORMS_YAML"):
        if not os.environ.get(v):
            sys.exit(f"Missing required env var: {v}")
    if not os.environ.get("DAGSTER_HOME"):
        sys.exit("DAGSTER_HOME must be set so runs are recorded in the real instance.")

    import requests
    from dagster import DagsterInstance

    from aimdl_coord_enrichment import defs
    from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER, _COORD_YAML
    from aimdl_coord_enrichment.girder_io import fetch_partition_index
    from aimdl_coord_enrichment.partitions import (
        HELIX_TRACE_DATA_TYPE,
        HELIX_TRACE_PARTITIONS,
    )
    from aimdl_coord_enrichment.resources import GirderClientWithSession

    if _COORD_TRANSFORMER is None:
        sys.exit(f"coordinate-transformer failed to load (COORD_TRANSFORMS_YAML={_COORD_YAML}).")

    client = GirderClientWithSession(
        apiUrl=os.environ["GIRDER_API_URL"], apiKey=os.environ["GIRDER_API_KEY"],
        session=requests.Session(),
    )
    index = fetch_partition_index(client, HELIX_TRACE_DATA_TYPE)

    if args.partitions:
        keys = list(args.partitions)
        unknown = [k for k in keys if k not in index]
        if unknown:
            sys.exit("Not in the pdv_trace partition index: " + ", ".join(unknown))
    elif args.month:
        keys = sorted(k for k in index if k.split("//")[1].startswith(args.month))
    else:
        keys = sorted(index)
    if args.limit:
        keys = keys[:args.limit]
    if not keys:
        sys.exit("No partitions selected.")

    instance = DagsterInstance.get()
    instance.add_dynamic_partitions(HELIX_TRACE_PARTITIONS.name, sorted(index))

    mode = "LIVE — writing to Girder" if args.live else "dry run — no writes"
    print(f"{mode}: {len(keys)} partition(s)", flush=True)
    print(f"  girder   : {os.environ['GIRDER_API_URL']}", flush=True)
    print(f"  instance : {os.environ['DAGSTER_HOME']}", flush=True)

    run_config = {"ops": {op: {"config": {"dry_run": not args.live}} for op in OPS}}
    job = defs.resolve_job_def("process_helix_assets_job")

    totals = {"traces": 0, "paired": 0, "enriched": 0, "simulated": 0,
              "no_station_coords": 0, "shot_identity": 0, "write_errors": 0}
    failures, per_partition = [], []

    for i, pk in enumerate(keys, 1):
        result = job.execute_in_process(
            instance=instance, partition_key=pk,
            run_config=run_config, raise_on_error=False,
        )
        if not result.success:
            failures.append(pk)
            print(f"  [{i}/{len(keys)}] {pk}  FAILED", flush=True)
            continue
        out = result.output_for_node("pdv_data")
        totals["traces"] += out["traces_in_partition"]
        totals["paired"] += out["paired_count"]
        totals["enriched"] += out["written_count"]
        totals["simulated"] += out["simulated_count"]
        totals["no_station_coords"] += out["no_station_coords"]
        totals["shot_identity"] += out["paired_by_shot_identity"]
        totals["write_errors"] += len(out["write_errors"])
        per_partition.append({"partition": pk, **{k: out[k] for k in (
            "traces_in_partition", "paired_count", "written_count",
            "simulated_count", "no_station_coords", "paired_by_shot_identity")}})
        print(f"  [{i}/{len(keys)}] {pk}  traces={out['traces_in_partition']} "
              f"paired={out['paired_count']} enriched={out['written_count']} "
              f"no_coords={out['no_station_coords']}", flush=True)

    print("\n=== totals ===", flush=True)
    for k, v in totals.items():
        print(f"  {k:20s} {v:>7,}")
    if failures:
        print(f"\n  FAILED partitions ({len(failures)}): {', '.join(failures[:10])}")

    log_dir = os.path.join(os.path.dirname(__file__), "log")
    os.makedirs(log_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(log_dir, f"live_pass_{stamp}.json")
    with open(path, "w") as f:
        json.dump({"timestamp_utc": stamp, "live": args.live, "totals": totals,
                   "failures": failures, "partitions": per_partition}, f, indent=2)
    print(f"\nReport: {path}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
