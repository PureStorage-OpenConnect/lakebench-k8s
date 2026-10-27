"""Spark module for Lakebench.

The CLI imports ``SparkOperatorManager`` and ``SparkJobMonitor`` from here
at call time, so tests patch them here (``lakebench.spark.<name>``). The
implementations are ``lakebench.modules.pipeline_engines.spark.*``.
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
