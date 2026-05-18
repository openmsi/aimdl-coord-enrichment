#!/usr/bin/env bash
#
# run_live_sweep.sh — one-shot live sweep of the coord_enrichment DAG.
#
# Precondition: dry-run rehearsal was green. See
# docs/runbooks/coord_enrichment_production_sweep.md.
#
# What it does:
#   1. Re-verify env vars and test suite
#   2. Ask for explicit operator confirmation
#   3. Launch each partitioned job per partition key with
#      dry_run=False via `dagster job launch`
#   4. Log every launch command and its output to
#      operations/log/sweep-<timestamp>.log

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LOG_DIR="operations/log"
mkdir -p "$LOG_DIR"
TS="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
LOG="$LOG_DIR/sweep-$TS.log"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

log "=== coord_enrichment live sweep, log $LOG ==="

# ---- Pre-flight -----------------------------------------------------------
required_env=( GIRDER_API_URL GIRDER_API_KEY COORD_TRANSFORMS_YAML
               COORD_ENRICHMENT_MANIFEST_ITEM )
for var in "${required_env[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        log "ERROR: required env var $var is unset. Aborting."
        exit 2
    fi
done
log "env vars: present"
log "  GIRDER_API_URL=$GIRDER_API_URL"
log "  COORD_TRANSFORMS_YAML=$COORD_TRANSFORMS_YAML"
log "  COORD_ENRICHMENT_MANIFEST_ITEM=$COORD_ENRICHMENT_MANIFEST_ITEM"

log "running pytest..."
if ! .venv/bin/pytest tests/ -q >> "$LOG" 2>&1; then
    log "ERROR: pytest failed. Aborting."
    exit 3
fi
log "pytest: green"

# ---- Operator confirmation ------------------------------------------------
cat <<'EOF' | tee -a "$LOG"

You are about to run a LIVE sweep (dry_run=False) against the
configured Girder instance. This will write Station_X/Y,
Sample_X/Y, and coord_provenance to every in-scope item.

Type "LIVE SWEEP" (no quotes) to proceed.
EOF
read -r confirmation
if [[ "$confirmation" != "LIVE SWEEP" ]]; then
    log "operator declined; exiting with no action."
    exit 0
fi
log "operator confirmed"

# ---- Fire each partitioned job per partition key --------------------------
launch() {
    local job="$1"
    local partition="$2"
    local config_yaml
    config_yaml=$(mktemp)
    case "$job" in
        coord_enrichment_maxima_raw_job)
            cat > "$config_yaml" <<EOY
ops:
  provenance_tagged_items: {config: {dry_run: false}}
  enriched_maxima_raw:     {config: {dry_run: false}}
EOY
            ;;
        coord_enrichment_helix_alpss_job)
            cat > "$config_yaml" <<EOY
ops:
  provenance_tagged_items: {config: {dry_run: false}}
  enriched_helix_alpss:    {config: {dry_run: false}}
EOY
            ;;
        coord_enrichment_maxima_derived_job)
            cat > "$config_yaml" <<EOY
ops:
  provenance_tagged_items:  {config: {dry_run: false}}
  enriched_maxima_derived:  {config: {dry_run: false}}
EOY
            ;;
        *)
            log "ERROR: unknown job $job"; exit 4 ;;
    esac

    log "launching $job --partition $partition ..."
    if .venv/bin/dagster job launch \
            -m aimdl_coord_enrichment \
            -j "$job" \
            --partition "$partition" \
            --config "$config_yaml" >> "$LOG" 2>&1; then
        log "  OK  $job / $partition"
    else
        log "  FAIL  $job / $partition (see log)"
    fi
    rm -f "$config_yaml"
}

# Raw first (station-derived leaves)
launch coord_enrichment_maxima_raw_job MAXIMA/xrd_raw
launch coord_enrichment_maxima_raw_job MAXIMA/xrf_raw

# Then inherited leaves (they read parents that were just written)
launch coord_enrichment_helix_alpss_job   HELIX/pdv_alpss_output
launch coord_enrichment_helix_alpss_job   HELIX/pdv_alpss_result
launch coord_enrichment_helix_alpss_job   HELIX/pdv_alpss_results
launch coord_enrichment_maxima_derived_job MAXIMA/xrd_derived

log "=== sweep done; inspect Dagster UI for per-run results ==="
