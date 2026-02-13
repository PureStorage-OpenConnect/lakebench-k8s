"""Spark module for Lakebench.

Manages Spark operator installation, job submission, and monitoring.
"""

from .job import SparkJobManager, get_executor_count, get_job_profile
from .monitor import SparkJobMonitor
from .operator import SparkOperatorManager

__all__ = [
    "SparkOperatorManager",
    "SparkJobManager",
    "SparkJobMonitor",
    "get_executor_count",
    "get_job_profile",
]
