"""Spark job monitoring for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.pipeline_engines.spark.monitor``.
"""

from lakebench.modules.pipeline_engines.spark.monitor import (  # noqa: F401
    JobResult,
    SparkJobMonitor,
)
