"""Per-run-folder cache for MAXIMA instructions.txt lookups.

The cache lives for the duration of one asset run. Phase 3's
enrichment asset constructs a new cache, uses it throughout the
partition, and discards it at function exit. No cross-run
persistence.
"""

from __future__ import annotations

from helix_dagster.instruments.maxima import (
    fetch_instructions_for_run,
    find_run_folder_id,
)


class InstructionsCache:
    """Memoizes (instructions_item, parsed_json) per run_folder_id.

    Not thread-safe; assumes a single-threaded asset invocation.
    """

    def __init__(self) -> None:
        self._by_run_folder: dict[str, tuple[dict, dict]] = {}

    def get_for_item(self, item: dict, girder) -> tuple[str, dict, dict]:
        """Return (run_folder_id, instructions_item, parsed_json).

        On miss, walks to the run folder and fetches instructions.txt.
        On hit, returns the cached value without any network I/O.
        """
        run_folder_id = find_run_folder_id(item, girder)
        cached = self._by_run_folder.get(run_folder_id)
        if cached is not None:
            instr_item, parsed = cached
            return run_folder_id, instr_item, parsed
        instr_item, parsed = fetch_instructions_for_run(run_folder_id, girder)
        self._by_run_folder[run_folder_id] = (instr_item, parsed)
        return run_folder_id, instr_item, parsed

    def cache_size(self) -> int:
        return len(self._by_run_folder)
