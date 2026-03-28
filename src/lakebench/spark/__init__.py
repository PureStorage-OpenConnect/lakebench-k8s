"""Spark module for Lakebench.

Backward-compat re-exports. Implementations moved to
``lakebench.modules.pipeline_engines.spark.*``.
"""

from lakebench.modules.pipeline_engines.spark.job import (
    SparkJobManager,
    get_executor_count,
    get_job_profile,
)
from lakebench.modules.pipeline_engines.spark.monitor import SparkJobMonitor
from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

__all__ = [
    "SparkOperatorManager",
    "SparkJobManager",
    "SparkJobMonitor",
    "get_executor_count",
    "get_job_profile",
]
