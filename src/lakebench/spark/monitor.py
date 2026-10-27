"""Spark job monitoring for Lakebench.

The names the CLI and tests import from ``lakebench.spark.monitor``, so tests
patch them here; the implementation is
``lakebench.modules.pipeline_engines.spark.monitor``.
"""

from lakebench.modules.pipeline_engines.spark.monitor import (  # noqa: F401
    JobResult,
    SparkJobMonitor,
)
