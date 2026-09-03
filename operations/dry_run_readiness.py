#!/usr/bin/env python
"""Read-only production-readiness dry run for the enrichment DAG.

Runs every enrichment flow in dry-run (``dry_run=True`` — no Girder
writes) against the configured Girder instance, collects asset-check
outcomes and per-leaf counters, evaluates them against the go/no-go
rubric, and writes a report to ``operations/log/``.

Three flows are swept, in the same dependency order as a live sweep:

  1. helix_traces — process_helix_assets_job, one dynamic ``helix_pdv_trace``
                    partition per AIMD-L ``<igsn>//<experiment_date>`` key of
                    the PDV traces. The HELIX root: each trace finds the log
                    row naming it and takes its coordinates.
  2. helix_alpss  — coord_enrichment_helix_alpss_job, 3 static partitions.
                    Inherits from the pdv_trace parents enriched by (1).
  3. maxima       — coord_enrichment_maxima_partition_job, one dynamic
                    ``maxima_run`` partition per AIMD-L run. Independent
                    of HELIX.

The state report runs last so it sees leaf coverage.

Dry-run performs no writes. For defense in depth, run this with a
READ-ONLY Girder API key so that any accidental write fails loudly.

See docs/runbooks/readiness_dry_run.md for the rubric and the
UI-driven equivalent.

Usage:
    .venv/bin/python operations/dry_run_readiness.py              # full enumeration
    .venv/bin/python operations/dry_run_readiness.py --sample 2   # quick smoke
    .venv/bin/python operations/dry_run_readiness.py --flows helix_traces,state_report
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone


REQUIRED_ENV = ["GIRDER_API_URL", "GIRDER_API_KEY", "COORD_TRANSFORMS_YAML"]

ALL_FLOWS = ("helix_traces", "helix_alpss", "maxima", "state_report")

HELIX_ALPSS_KEYS = [
    "HELIX/pdv_alpss_output",
    "HELIX/pdv_alpss_result",
    "HELIX/pdv_alpss_results",
]

# process_helix_assets_job has no schedule, so its dry-run op list lives
# here rather than in schedules.py. Only the two Config-consuming assets
# may appear — pdv_log takes no config and Dagster rejects config for it.
_HELIX_SPREADSHEET_OPS = ["pdv_data", "pdv_processing_manifest"]

# A check blocks GO if it is ERROR-severity or its name starts with one of
# these prefixes. Everything else — inventory_nonempty_per_instrument,
# pdv_coverage_above_threshold, pdv_match_rate, igsn_validity_rate — is
# recorded but non-blocking: those measure upstream coverage, which is a
# tuning input, not a correctness gate.
BLOCKING_WARN_PREFIXES = (
    "enrichment_success_rate",
    "no_coord_transform_failures",
    "coord_transform_check",
)


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


def _new_leaf():
    return {
        "seen": 0, "in_scope": 0, "excluded": 0, "excluded_by_reason": {},
        "written": 0, "simulated_dry_run": 0, "skipped_no_change": 0,
        "coord_failures": 0, "resolution_errors": 0,
        "partitions": 0, "versions": {},
    }


def _unresolved(v):
    """In-scope items the leaf neither wrote, simulated, skipped, nor failed.

    Should be ~0 on every leaf: a paired trace is always either written,
    simulated, skipped as unchanged, or recorded as a coord failure.
    """
    return max(
        0,
        v["in_scope"]
        - (v["written"] + v["simulated_dry_run"]
           + v["skipped_no_change"] + v["coord_failures"]),
    )


def _version_breakdown(leaves):
    """Aggregate transform_version counts across all leaves.

    Keys look like ``HELIX/v1``, ``HELIX/v2``, ``MAXIMA/v1``. HELIX counts
    originate at ``pdv_data`` (the root, which passes each shot's
    timestamp) and are inherited by ``enriched_helix_alpss`` from the
    parent PDV trace, so v1 = historical shots, v2 = post-2026-04-01 shots
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
        help="Limit each dynamically-partitioned flow (helix_traces, maxima) "
             "to its first N partitions for a quick smoke pass. "
             "Default: full enumeration.",
    )
    parser.add_argument(
        "--flows", default=",".join(ALL_FLOWS),
        help="Comma-separated subset of flows to sweep. "
             f"Choices: {', '.join(ALL_FLOWS)}. Default: all.",
    )
    args = parser.parse_args()

    flows = [f.strip() for f in args.flows.split(",") if f.strip()]
    unknown = [f for f in flows if f not in ALL_FLOWS]
    if unknown:
        sys.exit(
            f"Unknown flow(s): {', '.join(unknown)}. "
            f"Choices: {', '.join(ALL_FLOWS)}"
        )

    pre_warnings = preflight()

    # Imports deferred until after env preflight: aimdl_coord_enrichment.coordinates
    # loads the coordinate-transformer at import time from COORD_TRANSFORMS_YAML.
    import requests
    from dagster import DagsterInstance

    from aimdl_coord_enrichment import defs
    from aimdl_coord_enrichment.coord_enrichment import MAXIMA_RUN_PARTITIONS
    from aimdl_coord_enrichment.coordinates import _COORD_TRANSFORMER, _COORD_YAML
    from aimdl_coord_enrichment.girder_io import fetch_partition_index
    from aimdl_coord_enrichment.partitions import (
        HELIX_TRACE_DATA_TYPE,
        HELIX_TRACE_PARTITIONS,
    )
    from aimdl_coord_enrichment.resources import GirderClientWithSession
    from aimdl_coord_enrichment.schedules import (
        _HELIX_ALPSS_OPS,
        _MAXIMA_OPS,
        _STATE_REPORT_OPS,
        _dry_run_config,
    )
    from aimdl_coord_enrichment.sensors import _MAXIMA_DISCOVERY_DATA_TYPES

    if _COORD_TRANSFORMER is None:
        sys.exit(
            f"coordinate-transformer failed to load (COORD_TRANSFORMS_YAML={_COORD_YAML}). "
            "Install the coordinate-transformer package and point the env var at a valid YAML."
        )

    instance = DagsterInstance.ephemeral()
    checks = []        # one record per asset-check evaluation
    leaves = {}        # leaf node name -> aggregated counters
    run_failures = []  # job executions that errored outright
    swept = {}         # flow name -> partitions actually swept

    client = GirderClientWithSession(
        apiUrl=os.environ["GIRDER_API_URL"],
        apiKey=os.environ["GIRDER_API_KEY"],
        session=requests.Session(),
    )

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

    def harvest_coord_leaf(result, node_name):
        """Harvest enriched_maxima_run / enriched_helix_alpss output."""
        if not result.success:
            return
        try:
            out = result.output_for_node(node_name)
        except Exception:
            return
        counts = out.get("counts", {})
        excluded = out.get("excluded", {}) or {}
        agg = leaves.setdefault(node_name, _new_leaf())
        for k in ("seen", "written", "simulated_dry_run", "skipped_no_change",
                  "coord_failures", "resolution_errors"):
            agg[k] += int(counts.get(k, 0))
        agg["excluded"] += int(excluded.get("total", 0))
        for reason, n in (excluded.get("by_reason") or {}).items():
            agg["excluded_by_reason"][reason] = (
                agg["excluded_by_reason"].get(reason, 0) + int(n)
            )
        agg["in_scope"] += int(counts.get("seen", 0)) - int(excluded.get("total", 0))
        agg["partitions"] += 1
        for ver, n in (out.get("version_counter") or {}).items():
            agg["versions"][ver] = agg["versions"].get(ver, 0) + n

    def harvest_helix_trace_leaf(result):
        """Harvest pdv_data output, normalized onto the common leaf shape.

        The denominator is the traces in the partition. A trace that finds no
        row, more than one, or a row declaring another sample is unpaired and
        reported by reason — that is this flow's exclusion vocabulary.
        """
        if not result.success:
            return
        try:
            out = result.output_for_node("pdv_data")
        except Exception:
            return
        traces = int(out.get("traces_in_partition", 0))
        paired = int(out.get("paired_count", 0))
        agg = leaves.setdefault("pdv_data", _new_leaf())
        agg["seen"] += traces
        agg["in_scope"] += paired
        agg["excluded"] += traces - paired
        for issue in out.get("pair_issues") or []:
            reason = issue.get("type", "unknown")
            agg["excluded_by_reason"][reason] = (
                agg["excluded_by_reason"].get(reason, 0) + 1
            )
        agg["written"] += int(out.get("written_count", 0))
        agg["simulated_dry_run"] += int(out.get("simulated_count", 0))
        agg["coord_failures"] += int(out.get("coord_failures", 0))
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

    def _limit(keys):
        return keys[:args.sample] if args.sample is not None else keys

    # --- 1. HELIX traces (the root: writes coordinates to pdv_trace items) ---
    if "helix_traces" in flows:
        trace_keys = _limit(sorted(
            fetch_partition_index(client, HELIX_TRACE_DATA_TYPE).keys()
        ))
        if trace_keys:
            instance.add_dynamic_partitions(
                HELIX_TRACE_PARTITIONS.name, trace_keys
            )
        swept["helix_traces"] = len(trace_keys)
        print(f"HELIX traces: sweeping {len(trace_keys)} partitions", flush=True)
        cfg = _dry_run_config(_HELIX_SPREADSHEET_OPS)
        for i, pk in enumerate(trace_keys, 1):
            print(f"  [{i}/{len(trace_keys)}] {pk}", flush=True)
            res = run("process_helix_assets_job", f"helix_traces:{pk}",
                      cfg, partition_key=pk)
            harvest_helix_trace_leaf(res)

    # --- 2. HELIX ALPSS static partitions (inherit from the pdv_trace parents) ---
    if "helix_alpss" in flows:
        swept["helix_alpss"] = len(HELIX_ALPSS_KEYS)
        print(f"HELIX ALPSS: {len(HELIX_ALPSS_KEYS)} partitions", flush=True)
        for pk in HELIX_ALPSS_KEYS:
            res = run("coord_enrichment_helix_alpss_job",
                      f"helix_alpss:{pk}", _dry_run_config(_HELIX_ALPSS_OPS),
                      partition_key=pk)
            harvest_coord_leaf(res, "enriched_helix_alpss")

    # --- 3. MAXIMA run partitions (independent of HELIX) ---
    if "maxima" in flows:
        run_keys = _limit(sorted({
            key
            for dt in _MAXIMA_DISCOVERY_DATA_TYPES
            for key in fetch_partition_index(client, dt).keys()
        }))
        if run_keys:
            instance.add_dynamic_partitions(MAXIMA_RUN_PARTITIONS.name, run_keys)
        swept["maxima"] = len(run_keys)
        print(f"MAXIMA: sweeping {len(run_keys)} run partitions", flush=True)
        cfg = _dry_run_config(_MAXIMA_OPS)
        for i, pk in enumerate(run_keys, 1):
            print(f"  [{i}/{len(run_keys)}] {pk}", flush=True)
            res = run("coord_enrichment_maxima_partition_job",
                      f"maxima:{pk}", cfg, partition_key=pk)
            harvest_coord_leaf(res, "enriched_maxima_run")

    # --- 4. State report last, so the report asset sees leaf coverage ---
    snapshot = None
    if "state_report" in flows:
        print("State report", flush=True)
        state_res = run("coord_enrichment_job", "state_report",
                        _dry_run_config(_STATE_REPORT_OPS))
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

    verdict = _evaluate(checks, leaves, snapshot, run_failures, flows)
    _write_report(checks, leaves, snapshot, run_failures, pre_warnings,
                  verdict, flows, swept)
    print()
    versions = _version_breakdown(leaves)
    print("Transform versions applied: " + (
        ", ".join(f"{k}={n}" for k, n in sorted(versions.items())) or "none"
    ))
    print(verdict["line"])
    sys.exit(0 if verdict["go"] else 1)


def _evaluate(checks, leaves, snapshot, run_failures, flows):
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

    # Exclusion-policy guard. Excluded items are dropped from the
    # success-rate denominator, so a leaf that excluded everything it saw
    # still passes every check. That is a collapsed denominator, not a
    # green run — block on it and make the operator read the reasons.
    for name, v in sorted(leaves.items()):
        if v["seen"] > 0 and v["in_scope"] == 0:
            by_reason = ", ".join(
                f"{k}={n}" for k, n in sorted(v["excluded_by_reason"].items())
            ) or "unattributed"
            reasons.append(
                f"{name}: all {v['seen']} items excluded, nothing in scope "
                f"({by_reason})"
            )

    if "state_report" in flows and (
        snapshot is None or not snapshot.get("yaml_sha256")
    ):
        reasons.append("coordinate-transform snapshot missing yaml_sha256")

    if not os.environ.get("COORD_ENRICHMENT_MANIFEST_ITEM"):
        reasons.append("COORD_ENRICHMENT_MANIFEST_ITEM unset (required for live operation)")

    go = not reasons
    partial = [f for f in ALL_FLOWS if f not in flows]
    if go and partial:
        # A subset run gathers no evidence for the criteria belonging to the
        # flows it skipped. Say so in the verdict itself so a triage run
        # cannot be mistaken for the readiness decision.
        line = f"VERDICT: GO (PARTIAL — did not sweep: {', '.join(partial)})"
    elif go:
        line = "VERDICT: GO"
    else:
        line = "VERDICT: NO-GO — " + "; ".join(reasons)
    return {"go": go, "partial_flows": partial, "reasons": reasons, "line": line}


def _write_report(checks, leaves, snapshot, run_failures, pre_warnings,
                  verdict, flows, swept):
    log_dir = os.path.join(os.path.dirname(__file__), "log")
    os.makedirs(log_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = os.path.join(log_dir, f"readiness_dry_run_{stamp}")

    payload = {
        "timestamp_utc": stamp,
        "verdict": verdict,
        "flows": flows,
        "partitions_swept": swept,
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
    lines.append(
        "Flows swept: " + ", ".join(
            f"{f} ({swept[f]})" if f in swept else f for f in flows
        )
    )
    lines.append("")
    if verdict["reasons"]:
        lines.append("## Blocking reasons")
        lines += [f"- {r}" for r in verdict["reasons"]] + [""]
    if pre_warnings:
        lines.append("## Pre-flight warnings")
        lines += [f"- {w}" for w in pre_warnings] + [""]

    lines.append("## Leaf counters (dry-run)")
    lines.append("")
    lines.append(
        "`in_scope` = `seen` − `excluded`. Excluded items are dropped from "
        "the success-rate denominator by design, so read `in_scope` and "
        "`excluded_by_reason` rather than check colours alone. `unresolved` "
        "is in-scope work the leaf neither wrote, simulated, skipped, nor "
        "recorded as a coord failure, and should be ~0."
    )
    lines.append("")
    lines.append(
        "| Leaf | parts | seen | in_scope | excluded | written | simulated | "
        "skipped | coord_fail | res_err | unresolved | versions |"
    )
    lines.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|")
    for name, v in sorted(leaves.items()):
        versions = ", ".join(f"{k}={n}" for k, n in sorted(v["versions"].items())) or "—"
        lines.append(
            f"| {name} | {v['partitions']} | {v['seen']} | {v['in_scope']} | "
            f"{v['excluded']} | {v['written']} | {v['simulated_dry_run']} | "
            f"{v['skipped_no_change']} | {v['coord_failures']} | "
            f"{v['resolution_errors']} | {_unresolved(v)} | {versions} |"
        )
    lines.append("")

    lines.append("## Exclusions by reason")
    lines.append("")
    any_excl = False
    for name, v in sorted(leaves.items()):
        if v["excluded_by_reason"]:
            any_excl = True
            detail = ", ".join(
                f"{k}={n}" for k, n in sorted(v["excluded_by_reason"].items())
            )
            lines.append(f"- `{name}`: {detail}")
    if not any_excl:
        lines.append("- none")
    lines.append("")

    versions = _version_breakdown(leaves)
    lines.append("## Transform versions applied")
    lines.append("")
    lines.append(
        "Counts of the coordinate transform version actually selected per item "
        "(dry-run still resolves versions). HELIX counts originate at "
        "`pdv_data`, which passes each shot's timestamp, and are inherited by "
        "`enriched_helix_alpss` from the parent PDV trace — `HELIX/v1` = "
        "pre-2026-04-01 shots, `HELIX/v2` = post-cutover shots (identity, "
        "Station == Sample). A missing split on `enriched_helix_alpss` usually "
        "means parent PDV traces are not yet enriched; under the exclusion "
        "policy those items are counted as `parent_not_enriched` rather than "
        "as errors."
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
