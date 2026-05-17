> Preserved from `docs/runbooks/coord_enrichment_production_sweep.md`
> as of 2026-04-27, before the section was rewritten to describe a
> UI-driven sweep. Kept here because the operator-confirmation gate
> idea ("Type LIVE SWEEP to proceed") and the launch-log convention
> may be reusable in a future shell-driven sweep tool.

## Live sweep

Once the dry rehearsal is green, run the one-shot live sweep
script:

```
bash operations/run_live_sweep.sh
```

The script:

1. Re-runs the pre-flight check
2. Confirms with the operator that env vars point at production
3. Invokes each partitioned job per partition key with
   `dry_run=False`
4. Writes a launch log to `operations/log/sweep-<timestamp>.log`
