"""Trino query benchmark for Lakebench.

Runs a fixed set of analytical queries against the Customer 360
medallion pipeline and computes Queries per Hour (QpH).
"""

from .queries import (
    BENCHMARK_QUERIES,
    BENCHMARK_QUERIES_BY_DOMAIN,
    BenchmarkQuery,
    get_benchmark_queries,
)
from .runner import BenchmarkResult, BenchmarkRunner, QueryResult

__all__ = [
    "BENCHMARK_QUERIES",
    "BENCHMARK_QUERIES_BY_DOMAIN",
    "BenchmarkQuery",
    "BenchmarkResult",
    "BenchmarkRunner",
    "QueryResult",
    "get_benchmark_queries",
]
