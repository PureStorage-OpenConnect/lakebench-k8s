"""Spark job management for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.pipeline_engines.spark.job``.
"""

from lakebench.modules.pipeline_engines.spark.job import (  # noqa: F401
    _FORMAT_VERSION_COMPAT,
    _ICEBERG_RUNTIME_SUFFIX,
    _JOB_PROFILES,
    _MAX_EXECUTORS_SAFE,
    JobState,
    JobStatus,
    JobType,
    SparkJobManager,
    _delta_spark_artifact,
    _parse_spark_major,
    _parse_spark_major_minor,
    _scale_executor_count,
    _scale_partitions,
    _spark_compat,
    _streaming_concurrent_budget,
    get_executor_count,
    get_job_profile,
    resolve_format_version,
    validate_format_version,
)
