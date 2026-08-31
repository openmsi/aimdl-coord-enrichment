"""Out-of-scope classification for non-standard input files.

Some files in the collection cannot be enriched because of how they were
produced, not because of anything the pipeline does: a run with no
``instructions.txt``, a filename with no ``scan_point_<i>`` index, an item with
no ``experiment_date``. These come from non-standardised lab practice during
testing, and the data associated with them is not of interest.

Policy (decided 2026-08-31): such files are **out of scope**, not failures.
They are counted, grouped by reason, and reported in the asset's
materialization metadata so the volume stays visible — but they do not
increment error counters and they are removed from the success-rate
denominator, so they never turn a partition red.

This is deliberately distinct from a *failure*, which is reserved for
conditions that indicate a problem worth acting on: a Girder write that
errored, or a coordinate transform that returned None.
"""

# Reason codes. Keep stable — they appear in asset metadata and in the
# excluded_by_reason breakdown operators read.
NO_INSTRUCTIONS = "no_instructions"
UNPARSEABLE_NAME = "unparseable_name"
NO_EXPERIMENT_DATE = "no_experiment_date"
SCAN_POINT_OUT_OF_RANGE = "scan_point_out_of_range"
MALFORMED_INSTRUCTIONS = "malformed_instructions"
NO_RESOLVABLE_PARENT = "no_resolvable_parent"
PARENT_NOT_ENRICHED = "parent_not_enriched"

EXCLUSION_REASONS = (
    NO_INSTRUCTIONS,
    UNPARSEABLE_NAME,
    NO_EXPERIMENT_DATE,
    SCAN_POINT_OUT_OF_RANGE,
    MALFORMED_INSTRUCTIONS,
    NO_RESOLVABLE_PARENT,
    PARENT_NOT_ENRICHED,
)


class ExclusionLog:
    """Accumulates excluded items, grouped by reason.

    Records a bounded number of example names per reason so the metadata stays
    readable on a partition where tens of thousands of items are excluded.
    """

    MAX_EXAMPLES_PER_REASON = 5

    def __init__(self):
        self.counts: dict[str, int] = {}
        self.examples: dict[str, list[str]] = {}
        self.total = 0

    def add(self, reason: str, name: str = "", item_id: str | None = None) -> None:
        self.counts[reason] = self.counts.get(reason, 0) + 1
        self.total += 1
        bucket = self.examples.setdefault(reason, [])
        if name and len(bucket) < self.MAX_EXAMPLES_PER_REASON:
            bucket.append(name)

    def summary_text(self) -> str:
        """One-line breakdown, e.g. 'no_instructions=15, unparseable_name=2'."""
        if not self.counts:
            return "none"
        return ", ".join(f"{k}={v}" for k, v in sorted(self.counts.items()))

    def examples_text(self) -> str:
        if not self.examples:
            return "none"
        return "; ".join(
            f"{reason}: {', '.join(names)}"
            for reason, names in sorted(self.examples.items())
            if names
        )

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "by_reason": dict(sorted(self.counts.items())),
            "examples": {k: list(v) for k, v in sorted(self.examples.items())},
        }
