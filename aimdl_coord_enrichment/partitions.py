"""Partition definitions for the HELIX spreadsheet flow.

Single-dimension dynamic partitions keyed on the AIMD-L logical key
``"<igsn>//<experiment_date>"`` served by
``GET /aimdl/partition?dataType=pdv_experiment_log``. Partition keys are
plain strings, so assets read ``context.partition_key`` directly and the
discovery sensor calls ``build_add_request(sorted(index))``.
"""

from dagster import DynamicPartitionsDefinition

HELIX_EXPERIMENT_LOG_DATA_TYPE = "pdv_experiment_log"

HELIX_EXPERIMENT_LOG_PARTITIONS = DynamicPartitionsDefinition(
    name="helix_experiment_log"
)
