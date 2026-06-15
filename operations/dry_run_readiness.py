#!/usr/bin/env python
"""Read-only production-readiness dry run for the coord_enrichment DAG.

Runs every coord_enrichment job in dry-run (``dry_run=True`` — no Girder
writes) against the configured Girder instance, collects asset-check
outcomes and per-leaf counters, evaluates them against the go/no-go
rubric, and writes a report to ``operations/log/``.

Dry-run performs no writes. For defense in depth, run this with a
READ-ONLY Girder API key so that any accidental write fails loudly.

See docs/runbooks/readiness_dry_run.md for the rubric and the
UI-driven equivalent.

Usage:
    .venv/bin/python operations/dry_run_readiness.py            # full enumeration
    .venv/bin/python operations/dry_run_readiness.py --sample 2 # quick smoke
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone


REQUIRED_ENV = ["GIRDER_API_URL", "GIRDER_API_KEY", "COORD_TRANSFORMS_YAML"]

HELIX_ALPSS_KEYS = [
    "HELIX/pdv_alpss_output",
    "HELIX/pdv_alpss_result",
    "HELIX/pdv_alpss_results",
]
MAXIMA_DERIVED_KEY = "MAXIMA/xrd_derived"
MAXIMA_RAW_DATA_TYPES = ("xrd_raw", "xrf_raw")

# A check blocks GO if it is ERROR-severity or its name starts with one of
# these prefixes. Everything else (inventory_nonempty_per_instrument,
# pdv_coverage_above_threshold) is recorded but non-blocking.
BLOCKING_WARN_PREFIXES = ("enrichment_success_rate", "no_coord_transform_failures")


def preflight():
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        sys.exit("Missing required env vars: " + ", ".join(missing))
    warnings = []
    if not os.environ.get("COORD_ENRICHMENT_MANIFEST_ITEM"):
        warnings.append(
            "COORD_ENRICHMENT_MANIFEST_ITEM is not set — the manifest write "
            "path is not exercised; live operation requires it."
        )
    return warnings


def _plain(value):
    """Best-effort unwrap of a Dagster MetadataValue for JSON output."""
    return getattr(value, "value", str(value))


def _is_blocking(check_name, severity):
    return severity == "ERROR" or check_name.startswith(BLOCKING_WARN_PREFIXES)


def _version_breakdown(leaves):
    """Aggregate transform_version counts across all leaves.

    Keys look like ``HELIX/v1``, ``HELIX/v2``, ``MAXIMA/v1``. For HELIX the
    count comes from enriched_helix_alpss inheriting its parent PDV trace's
    recorded version, so v1 = historical shots, v2 = post-2026-04-01 shots
    (identity, Station == Sample).
    """
    totals = {}
    for v in leaves.values():
        for ver, n in v.get("versions", {}).items():
            totals[ver] = totals.get(ver, 0) + n
    return totals


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--sample", type=int, default=None,
        help="Limit MAXIMA raw to the first N (data_type, run) partitions "
             "for a quick smoke pass. Default: full enumeration.",
    )
    parser.add_argument(
        "--skip-maxima-raw", action="store_true",
        help="Skip the MAXIMA raw dynamic-partition sweep entirely.",
    )
    args = parser.parse_args()

    pre_warnings = preflight()

    # Imports deferred until after env preflight: aimdl_coord_enrichment.coordinates
    # loads the coordinate-transformer at import time from COORD_TRANSFORMS_YAML.
    import requests
    from dagster import DagsterInstance, MultiPartitionKey

    from aimdl_coord_enrichment import defs
    from aimdl_coord_enrichment.coord_enrichment import MAXIMA_RUN_PARTITIONS
    from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER, _COORD_YAML
    from aimdl_coord_enrichment.girder_io import fetch_partition_index
    from aimdl_coord_enrichment.resources import GirderClientWithSession
    from aimdl_coord_enrichment.schedules import (
        _HELIX_ALPSS_OPS,
        _MAXIMA_DERIVED_OPS,
        _MAXIMA_RAW_OPS,
        _STATE_REPORT_OPS,
        _dry_run_config,
    )

    if _COORD_TRANSFORMER is None:
        sys.exit(
            f"coordinate-transformer failed to load (COORD_TRANSFORMS_YAML={_COORD_YAML}). "
            "Install the coordinate-transformer package and point the env var at a valid YAML."
        )

    instance = DagsterInstance.ephemeral()
    checks = []        # one record per asset-check evaluation
    leaves = {}        # leaf node name -> aggregated counters
    run_failures = []  # job executions that errored outright

    def harvest_checks(result, label):
        for ev in result.get_asset_check_evaluations():
            checks.append({
                "run": label,
                "check": ev.check_name,
                "asset": ev.asset_key.to_user_string(),
                "passed": bool(ev.passed),
                "severity": ev.severity.value,
                "metadata": {k: _plain(v) for k, v in (ev.metadata or {}).items()},
            })

    def harvest_leaf(result, node_name):
        if not result.success:
            return
        try:
            out = result.output_for_node(node_name)
        except Exception:
            return
        counts = out.get("counts", {})
        agg = leaves.setdefault(node_name, {
            "seen": 0, "written": 0, "simulated_dry_run": 0,
            "skipped_no_change": 0, "coord_failures": 0,
            "resolution_errors": 0, "partitions": 0, "versions": {},
        })
        for k in ("seen", "written", "simulated_dry_run", "skipped_no_change",
                  "coord_failures", "resolution_errors"):
            agg[k] += int(counts.get(k, 0))
        agg["partitions"] += 1
        for ver, n in (out.get("version_counter") or {}).items():
            agg["versions"][ver] = agg["versions"].get(ver, 0) + n

    def run(job_name, label, run_config, partition_key=None):
        job = defs.resolve_job_def(job_name)
        result = job.execute_in_process(
            instance=instance,
            partition_key=partition_key,
            run_config=run_config,
            raise_on_error=False,
        )
        if not result.success:
            run_failures.append(label)
            print(f"  [FAILED] {label}", flush=True)
        harvest_checks(result, label)
        return result

    # --- MAXIMA raw: enumerate dynamic run keys (read-only) and register them ---
    pairs = []
    if not args.skip_maxima_raw:
        session = requests.Session()
        client = GirderClientWithSession(
            apiUrl=os.environ["GIRDER_API_URL"],
            apiKey=os.environ["GIRDER_API_KEY"],
            session=session,
        )
        run_keys = sorted({
            key
            for dt in MAXIMA_RAW_DATA_TYPES
            for key in fetch_partition_index(client, dt).keys()
        })
        if run_keys:
            instance.add_dynamic_partitions(MAXIMA_RUN_PARTITIONS.name, run_keys)
        pairs = [(dt, rk) for dt in MAXIMA_RAW_DATA_TYPES for rk in run_keys]
        if args.sample is not None:
            pairs = pairs[:args.sample]
        print(f"MAXIMA raw: {len(run_keys)} run keys, sweeping {len(pairs)} partitions", flush=True)
        cfg = _dry_run_config(_MAXIMA_RAW_OPS)
        for i, (dt, rk) in enumerate(pairs, 1):
            pk = MultiPartitionKey({"data_type": dt, "run": rk})
            print(f"  [{i}/{len(pairs)}] {dt} / {rk}", flush=True)
            res = run("coord_enrichment_maxima_raw_partition_job",
                      f"maxima_raw:{dt}/{rk}", cfg, partition_key=pk)
            harvest_leaf(res, "enriched_maxima_raw")

    # --- HELIX ALPSS static partitions ---
    print("HELIX ALPSS: 3 partitions", flush=True)
    for pk in HELIX_ALPSS_KEYS:
        res = run("coord_enrichment_helix_alpss_job",
                  f"helix_alpss:{pk}", _dry_run_config(_HELIX_ALPSS_OPS),
                  partition_key=pk)
        harvest_leaf(res, "enriched_helix_alpss")

    # --- MAXIMA derived single static partition ---
    print("MAXIMA derived: 1 partition", flush=True)
    res = run("coord_enrichment_maxima_derived_job",
              f"maxima_derived:{MAXIMA_DERIVED_KEY}",
              _dry_run_config(_MAXIMA_DERIVED_OPS),
              partition_key=MAXIMA_DERIVED_KEY)
    harvest_leaf(res, "enriched_maxima_derived")

    # --- State report last, so the report asset sees leaf coverage ---
    print("State report", flush=True)
    state_res = run("coord_enrichment_job", "state_report",
                    _dry_run_config(_STATE_REPORT_OPS))
    snapshot = None
    if state_res.success:
        try:
            snap = state_res.output_for_node("coord_transform_config_snapshot")
            snapshot = {
                "yaml_path": snap.yaml_path,
                "yaml_sha256": snap.yaml_sha256,
                "transformer_version": snap.transformer_version,
                "versions_by_instrument": snap.versions_by_instrument,
            }
        except Exception:
            pass

    verdict = _evaluate(checks, leaves, snapshot, run_failures, pre_warnings)
    _write_report(checks, leaves, snapshot, run_failures, pre_warnings, verdict)
    print()
    versions = _version_breakdown(leaves)
    print("Transform versions applied: " + (
        ", ".join(f"{k}={n}" for k, n in sorted(versions.items())) or "none"
    ))
    print(verdict["line"])
    sys.exit(0 if verdict["go"] else 1)


def _evaluate(checks, leaves, snapshot, run_failures, pre_warnings):
    reasons = []

    for label in run_failures:
        reasons.append(f"job execution failed: {label}")

    for c in checks:
        if _is_blocking(c["check"], c["severity"]) and not c["passed"]:
            reasons.append(f"{c['severity']} check failed: {c['check']} ({c['run']})")

    total_written = sum(v["written"] for v in leaves.values())
    total_sim = sum(v["simulated_dry_run"] for v in leaves.values())
    total_skip = sum(v["skipped_no_change"] for v in leaves.values())
    total_res_err = sum(v["resolution_errors"] for v in leaves.values())

    if total_written > 0:
        reasons.append(f"DRY-RUN BREACH: {total_written} items were written (expected 0)")
    if total_res_err > 0:
        reasons.append(f"{total_res_err} resolution errors across leaves")
    if (total_sim + total_skip + total_written) == 0:
        reasons.append("no items resolved in any leaf — nothing was evaluated")

    if snapshot is None or not snapshot.get("yaml_sha256"):
        reasons.append("coordinate-transform snapshot missing yaml_sha256")

    if not os.environ.get("COORD_ENRICHMENT_MANIFEST_ITEM"):
        reasons.append("COORD_ENRICHMENT_MANIFEST_ITEM unset (required for live operation)")

    go = not reasons
    line = "VERDICT: GO" if go else "VERDICT: NO-GO"
    if not go:
        line += " — " + "; ".join(reasons)
    return {"go": go, "reasons": reasons, "line": line}


def _write_report(checks, leaves, snapshot, run_failures, pre_warnings, verdict):
    log_dir = os.path.join(os.path.dirname(__file__), "log")
    os.makedirs(log_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = os.path.join(log_dir, f"readiness_dry_run_{stamp}")

    payload = {
        "timestamp_utc": stamp,
        "verdict": verdict,
        "preflight_warnings": pre_warnings,
        "run_failures": run_failures,
        "snapshot": snapshot,
        "transform_versions": _version_breakdown(leaves),
        "leaves": leaves,
        "checks": checks,
    }
    with open(base + ".json", "w") as f:
        json.dump(payload, f, indent=2, default=str)

    lines = [f"# Readiness dry run — {stamp}", "", f"**{verdict['line']}**", ""]
    if verdict["reasons"]:
        lines.append("## Blocking reasons")
        lines += [f"- {r}" for r in verdict["reasons"]] + [""]
    if pre_warnings:
        lines.append("## Pre-flight warnings")
        lines += [f"- {w}" for w in pre_warnings] + [""]

    lines.append("## Leaf counters (dry-run)")
    lines.append("")
    lines.append("| Leaf | partitions | seen | written | simulated | skipped | coord_fail | res_err | versions |")
    lines.append("|---|--:|--:|--:|--:|--:|--:|--:|---|")
    for name, v in sorted(leaves.items()):
        versions = ", ".join(f"{k}={n}" for k, n in sorted(v["versions"].items())) or "—"
        lines.append(
            f"| {name} | {v['partitions']} | {v['seen']} | {v['written']} | "
            f"{v['simulated_dry_run']} | {v['skipped_no_change']} | "
            f"{v['coord_failures']} | {v['resolution_errors']} | {versions} |"
        )
    lines.append("")

    versions = _version_breakdown(leaves)
    lines.append("## Transform versions applied")
    lines.append("")
    lines.append(
        "Counts of the coordinate transform version actually selected per item "
        "(dry-run still resolves versions). For HELIX these come from "
        "`enriched_helix_alpss` inheriting the parent PDV trace's recorded "
        "version — `HELIX/v1` = pre-2026-04-01 shots, `HELIX/v2` = post-cutover "
        "shots (identity, Station == Sample). A missing HELIX split usually means "
        "parent PDV traces are not yet enriched (see `resolution_errors`)."
    )
    lines.append("")
    lines.append("| Version | count |")
    lines.append("|---|--:|")
    for ver, n in sorted(versions.items()):
        lines.append(f"| {ver} | {n} |")
    if not versions:
        lines.append("| (none resolved) | 0 |")
    lines.append("")

    if snapshot:
        lines += [
            "## Coordinate-transform snapshot",
            "",
            f"- yaml_sha256: `{snapshot.get('yaml_sha256')}`",
            f"- transformer_version: `{snapshot.get('transformer_version')}`",
            f"- instruments: {', '.join(sorted(snapshot.get('versions_by_instrument', {}))) or '—'}",
            "",
        ]

    lines.append("## Asset checks")
    lines.append("")
    lines.append("| Check | severity | blocking | result | run |")
    lines.append("|---|---|:-:|:-:|---|")
    for c in sorted(checks, key=lambda c: (c["severity"], c["check"], c["run"])):
        blocking = "yes" if _is_blocking(c["check"], c["severity"]) else "no"
        result = "PASS" if c["passed"] else "FAIL"
        lines.append(f"| {c['check']} | {c['severity']} | {blocking} | {result} | {c['run']} |")
    lines.append("")

    with open(base + ".md", "w") as f:
        f.write("\n".join(lines))

    print(f"\nReport written to:\n  {base}.md\n  {base}.json")


if __name__ == "__main__":
    main()
