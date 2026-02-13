"""Trino query benchmark for Lakebench.

Runs a fixed set of analytical queries against the Customer 360
medallion pipeline and computes Queries per Hour (QpH).
"""

from .queries import BENCHMARK_QUERIES, BenchmarkQuery
from .runner import BenchmarkResult, BenchmarkRunner, QueryResult

__all__ = [
    "BENCHMARK_QUERIES",
    "BenchmarkQuery",
    "BenchmarkResult",
    "BenchmarkRunner",
    "QueryResult",
]
