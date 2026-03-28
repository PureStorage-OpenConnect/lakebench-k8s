"""Spark Thrift Server deployment for Lakebench.

Backward-compat re-export. Implementation moved to
``lakebench.modules.query_engines.spark_thrift.deployer``.
"""

from lakebench.modules.query_engines.spark_thrift.deployer import SparkThriftDeployer

__all__ = ["SparkThriftDeployer"]
