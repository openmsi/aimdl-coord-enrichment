"""Partition definitions for the HELIX flow.

The unit of work is a **PDV trace**, not an experiment-log row. Partitions
are therefore keyed on the AIMD-L logical key served by
``GET /aimdl/partition?dataType=pdv_trace`` — ``"<igsn>//<experiment_date>"``.

A trace carries ``meta.igsn`` and ``meta.data_type`` and nothing else; the
experiment date is supplied by the partition endpoint. Traces that are not
annotated do not appear in the index at all, so "skip unannotated traces" is
enforced by construction rather than by a filter.

The experiment logs are looked up in the *same* key space
(``dataType=pdv_experiment_log``), which is what lets a trace find the log
holding its row.
"""

from dagster import DynamicPartitionsDefinition

HELIX_TRACE_DATA_TYPE = "pdv_trace"
HELIX_EXPERIMENT_LOG_DATA_TYPE = "pdv_experiment_log"

HELIX_TRACE_PARTITIONS = DynamicPartitionsDefinition(name="helix_pdv_trace")
