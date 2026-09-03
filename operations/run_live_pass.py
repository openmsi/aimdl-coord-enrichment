#!/usr/bin/env python
"""Execute the HELIX enrichment pass, partition by partition.

Runs ``process_helix_assets_job`` against the real Dagster instance (so every
run is recorded and visible in the UI at :3000) and writes coordinate metadata
to Girder.

Writes are gated on ``--live``. Without it every op runs with ``dry_run=True``
and nothing is written, which is the same rehearsal the readiness script does.

See docs/runbooks/live_enrichment_pass.md.

Reads ``.env`` from the repository root and defaults ``DAGSTER_HOME`` to
``<repo>/.dagster_home``, so it needs no shell preamble. Values already in the
environment win, so an explicit export still overrides the file.

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

# Per-flow wiring: the job, the ops that take a dry_run config, and the leaf
# whose output carries the counters.
FLOWS = {
    "traces": {
        "job": "process_helix_assets_job",
        "ops": ["pdv_data", "pdv_processing_manifest"],
        "leaf": "pdv_data",
    },
    "alpss": {
        "job": "coord_enrichment_helix_alpss_job",
        "ops": ["helix_alpss_provenance_tagged", "enriched_helix_alpss"],
        "leaf": "enriched_helix_alpss",
    },
}
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env():
    """Populate the environment from the repo-root .env, without overriding it.

    Keeps the invocation a single command so it can be matched by one
    permission rule, rather than a compound shell line.
    """
    path = os.path.join(REPO_ROOT, ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
    os.environ.setdefault("DAGSTER_HOME", os.path.join(REPO_ROOT, ".dagster_home"))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flow", choices=sorted(FLOWS), default="traces",
                    help="traces: enrich pdv_trace from the experiment logs. "
                         "alpss: inherit those coordinates to the ALPSS "
                         "derived files. Run alpss only after traces.")
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument("--partitions", nargs="+", help="explicit partition keys")
    sel.add_argument("--all", action="store_true", help="every registered partition")
    sel.add_argument("--month", help="partitions whose date starts with this, e.g. 2026-08")
    ap.add_argument("--live", action="store_true",
                    help="perform real Girder writes. Without this, dry run.")
    ap.add_argument("--limit", type=int, default=None, help="cap the number of partitions")
    args = ap.parse_args()

    load_env()
    for v in ("GIRDER_API_URL", "GIRDER_API_KEY", "COORD_TRANSFORMS_YAML"):
        if not os.environ.get(v):
            sys.exit(f"Missing required env var: {v}")

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
    from aimdl_coord_enrichment.coord_enrichment import HELIX_ALPSS_PARTITIONS

    if args.flow == "alpss":
        index = {k: "" for k in HELIX_ALPSS_PARTITIONS.get_partition_keys()}
        if args.month:
            sys.exit("--month applies to the traces flow; ALPSS partitions are static.")
    else:
        index = fetch_partition_index(client, HELIX_TRACE_DATA_TYPE)

    if args.partitions:
        keys = list(args.partitions)
        unknown = [k for k in keys if k not in index]
        if unknown:
            sys.exit("Not a known partition: " + ", ".join(unknown))
    elif args.month:
        keys = sorted(k for k in index if k.split("//")[1].startswith(args.month))
    else:
        keys = sorted(index)
    if args.limit:
        keys = keys[:args.limit]
    if not keys:
        sys.exit("No partitions selected.")

    instance = DagsterInstance.get()
    if args.flow == "traces":
        instance.add_dynamic_partitions(HELIX_TRACE_PARTITIONS.name, sorted(index))

    flow = FLOWS[args.flow]
    mode = "LIVE — writing to Girder" if args.live else "dry run — no writes"
    print(f"{mode}: flow={args.flow}, {len(keys)} partition(s)", flush=True)
    print(f"  girder   : {os.environ['GIRDER_API_URL']}", flush=True)
    print(f"  instance : {os.environ['DAGSTER_HOME']}", flush=True)

    run_config = {"ops": {op: {"config": {"dry_run": not args.live}}
                          for op in flow["ops"]}}
    job = defs.resolve_job_def(flow["job"])

    totals = {}
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
        out = result.output_for_node(flow["leaf"])
        if args.flow == "traces":
            row = {
                "partition": pk,
                "seen": out["traces_in_partition"],
                "in_scope": out["paired_count"],
                "enriched": out["written_count"],
                "simulated": out["simulated_count"],
                "no_station_coords": out["no_station_coords"],
                "shot_identity": out["paired_by_shot_identity"],
                "write_errors": len(out["write_errors"]),
            }
        else:
            counts = out["counts"]
            excluded = out.get("excluded", {}) or {}
            row = {
                "partition": pk,
                "seen": counts["seen"],
                "in_scope": counts["seen"] - int(excluded.get("total", 0)),
                "enriched": counts["written"],
                "simulated": counts["simulated_dry_run"],
                "skipped_no_change": counts["skipped_no_change"],
                "excluded": int(excluded.get("total", 0)),
                "excluded_by_reason": excluded.get("by_reason", {}),
                "write_errors": len(out.get("write_errors", [])),
            }
        for k in ("seen", "in_scope", "enriched", "simulated",
                  "no_station_coords", "shot_identity", "write_errors",
                  "skipped_no_change", "excluded"):
            if k in row:
                totals[k] = totals.get(k, 0) + row[k]
        per_partition.append(row)
        extra = (f"no_coords={row['no_station_coords']}" if args.flow == "traces"
                 else f"skipped={row['skipped_no_change']} excluded={row['excluded']}")
        print(f"  [{i}/{len(keys)}] {pk}  seen={row['seen']} "
              f"in_scope={row['in_scope']} enriched={row['enriched']} {extra}",
              flush=True)

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
