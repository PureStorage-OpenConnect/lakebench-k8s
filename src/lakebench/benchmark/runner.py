"""Benchmark runner for Lakebench.

Executes the query suite against the configured engine and computes QpH.

Supports three modes following TPC methodology:
- **power**: Single sequential query stream (default)
- **throughput**: N concurrent query streams
- **composite**: Power + throughput, geometric mean QpH
"""

from __future__ import annotations

import logging
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .queries import BENCHMARK_QUERIES, BenchmarkQuery

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a single query execution."""

    query: BenchmarkQuery
    elapsed_seconds: float
    rows_returned: int
    success: bool
    error_message: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict."""
        return {
            "name": self.query.name,
            "display_name": self.query.display_name,
            "class": self.query.query_class,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "rows_returned": self.rows_returned,
            "success": self.success,
            "error_message": self.error_message,
        }


@dataclass
class StreamResult:
    """Result of a single query stream within a throughput run."""

    stream_id: int
    queries: list[QueryResult] = field(default_factory=list)
    total_seconds: float = 0.0
    success: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict."""
        return {
            "stream_id": self.stream_id,
            "total_seconds": round(self.total_seconds, 2),
            "success": self.success,
            "queries": [q.to_dict() for q in self.queries],
        }


@dataclass
class BenchmarkResult:
    """Result of a full benchmark run."""

    mode: str  # "power", "throughput", or "composite"
    cache: str  # "hot" or "cold"
    scale: int
    queries: list[QueryResult] = field(default_factory=list)
    total_seconds: float = 0.0
    qph: float = 0.0
    iterations: int = 1
    streams: int = 1
    stream_results: list[StreamResult] = field(default_factory=list)

    def compute_category_qph(self) -> dict[str, float]:
        """Compute QpH per query category.

        Returns a dict mapping category name to QpH for that category.
        """
        from collections import defaultdict

        categories: dict[str, list[float]] = defaultdict(list)
        for r in self.queries:
            categories[r.query.query_class].append(r.elapsed_seconds)

        result: dict[str, float] = {}
        for cat, times in sorted(categories.items()):
            total = sum(times)
            result[cat] = (len(times) / total) * 3600 if total > 0 else 0.0
        return result

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict."""
        d: dict[str, Any] = {
            "benchmark_type": "trino_query",
            "mode": self.mode,
            "cache": self.cache,
            "scale": self.scale,
            "qph": round(self.qph, 1),
            "category_qph": {k: round(v, 1) for k, v in self.compute_category_qph().items()},
            "total_seconds": round(self.total_seconds, 2),
            "iterations": self.iterations,
            "streams": self.streams,
            "queries": [q.to_dict() for q in self.queries],
        }
        if self.stream_results:
            d["stream_results"] = [s.to_dict() for s in self.stream_results]
        return d


class BenchmarkRunner:
    """Runs the query benchmark suite against the configured engine."""

    def __init__(self, config: LakebenchConfig, namespace: str | None = None):
        """Initialize benchmark runner.

        Args:
            config: Lakebench configuration
            namespace: Override namespace (default: from config)
        """
        from .executor import get_executor

        self.config = config
        self.namespace = namespace or config.get_namespace()
        self.executor = get_executor(config, self.namespace)
        self.catalog = self.executor.catalog_name
        self.silver_table = config.architecture.tables.silver
        self.gold_table = config.architecture.tables.gold

    def run(
        self,
        mode: str | None = None,
        cache: str | None = None,
        iterations: int | None = None,
        streams: int | None = None,
        query_class: str | None = None,
    ) -> BenchmarkResult | tuple[BenchmarkResult, BenchmarkResult, BenchmarkResult]:
        """Run benchmark suite. CLI flags override config values.

        Args:
            mode: "power", "throughput", or "composite" (default: from config)
            cache: "hot" or "cold" (default: from config)
            iterations: Per-query iterations (default: from config)
            streams: Concurrent streams for throughput/composite (default: from config)
            query_class: Filter to specific class ("scan", "analytics", "gold")

        Returns:
            BenchmarkResult for power/throughput modes.
            Tuple of (power, throughput, composite) for composite mode.
        """
        bench_cfg = self.config.architecture.benchmark

        # CLI overrides > config values > defaults
        effective_mode = mode or bench_cfg.mode.value
        effective_cache = cache or bench_cfg.cache
        effective_iterations = iterations if iterations is not None else bench_cfg.iterations
        effective_streams = streams if streams is not None else bench_cfg.streams

        # Map legacy mode names for backward compatibility
        if effective_mode in ("standard", "extended"):
            effective_mode = "power"

        if effective_mode == "power":
            return self.run_power(
                cache=effective_cache,
                iterations=effective_iterations,
                query_class=query_class,
            )
        elif effective_mode == "throughput":
            return self.run_throughput(
                streams=effective_streams,
                cache=effective_cache,
                iterations=effective_iterations,
                query_class=query_class,
            )
        elif effective_mode == "composite":
            return self.run_composite(
                streams=effective_streams,
                cache=effective_cache,
                iterations=effective_iterations,
                query_class=query_class,
            )
        else:
            raise ValueError(f"Unknown benchmark mode: {effective_mode}")

    def run_power(
        self,
        cache: str = "hot",
        iterations: int = 1,
        query_class: str | None = None,
    ) -> BenchmarkResult:
        """Run power benchmark (single sequential stream).

        Args:
            cache: "hot" or "cold"
            iterations: Per-query iterations (>1 uses median)
            query_class: Filter to specific class

        Returns:
            BenchmarkResult with mode="power"
        """
        queries = BENCHMARK_QUERIES
        if query_class:
            queries = [q for q in queries if q.query_class == query_class]

        if not queries:
            return BenchmarkResult(
                mode="power",
                cache=cache,
                scale=self.config.architecture.workload.datagen.get_effective_scale(),
            )

        results = self._run_query_stream(queries, cache, iterations)

        total_seconds = sum(r.elapsed_seconds for r in results)
        qph = (len(results) / total_seconds) * 3600 if total_seconds > 0 else 0

        return BenchmarkResult(
            mode="power",
            cache=cache,
            scale=self.config.architecture.workload.datagen.get_effective_scale(),
            queries=results,
            total_seconds=total_seconds,
            qph=qph,
            iterations=iterations,
        )

    def run_throughput(
        self,
        streams: int = 4,
        cache: str = "hot",
        iterations: int = 1,
        query_class: str | None = None,
    ) -> BenchmarkResult:
        """Run throughput benchmark (N concurrent query streams).

        Each stream runs the full query suite independently with shuffled
        query order to reduce correlated cache effects.

        QpH = (total_queries_all_streams / wall_clock_seconds) * 3600

        Args:
            streams: Number of concurrent streams
            cache: "hot" or "cold" (cold flushes once before all streams)
            iterations: Per-query iterations (>1 uses median)
            query_class: Filter to specific class

        Returns:
            BenchmarkResult with mode="throughput" and stream_results
        """
        queries = BENCHMARK_QUERIES
        if query_class:
            queries = [q for q in queries if q.query_class == query_class]

        if not queries:
            return BenchmarkResult(
                mode="throughput",
                cache=cache,
                scale=self.config.architecture.workload.datagen.get_effective_scale(),
                streams=streams,
            )

        # Cold: flush once before all streams start
        if cache == "cold":
            self.executor.flush_cache()

        stream_results: list[StreamResult] = []

        def _run_stream(stream_id: int) -> StreamResult:
            """Execute one complete query stream with shuffled order."""
            stream_queries = list(queries)
            random.shuffle(stream_queries)

            results = self._run_query_stream(
                stream_queries,
                cache="hot",
                iterations=iterations,
            )

            stream_total = sum(r.elapsed_seconds for r in results)
            return StreamResult(
                stream_id=stream_id,
                queries=results,
                total_seconds=stream_total,
                success=all(r.success for r in results),
            )

        wall_start = time.monotonic()

        with ThreadPoolExecutor(max_workers=streams) as executor:
            futures = {executor.submit(_run_stream, i): i for i in range(streams)}
            for future in as_completed(futures):
                stream_results.append(future.result())

        wall_seconds = time.monotonic() - wall_start

        # Sort by stream_id for deterministic output
        stream_results.sort(key=lambda s: s.stream_id)

        # Throughput QpH: total queries across all streams / wall clock
        total_queries = sum(len(s.queries) for s in stream_results)
        throughput_qph = (total_queries / wall_seconds) * 3600 if wall_seconds > 0 else 0

        # Use stream 0's results as the representative query list
        representative_queries = stream_results[0].queries if stream_results else []

        return BenchmarkResult(
            mode="throughput",
            cache=cache,
            scale=self.config.architecture.workload.datagen.get_effective_scale(),
            queries=representative_queries,
            total_seconds=wall_seconds,
            qph=throughput_qph,
            iterations=iterations,
            streams=streams,
            stream_results=stream_results,
        )

    def run_composite(
        self,
        streams: int = 4,
        cache: str = "hot",
        iterations: int = 1,
        query_class: str | None = None,
    ) -> tuple[BenchmarkResult, BenchmarkResult, BenchmarkResult]:
        """Run composite benchmark (power + throughput, geometric mean).

        Args:
            streams: Concurrent streams for throughput phase
            cache: "hot" or "cold"
            iterations: Per-query iterations
            query_class: Filter to specific class

        Returns:
            Tuple of (power_result, throughput_result, composite_result)
        """
        power = self.run_power(
            cache=cache,
            iterations=iterations,
            query_class=query_class,
        )
        throughput = self.run_throughput(
            streams=streams,
            cache=cache,
            iterations=iterations,
            query_class=query_class,
        )

        composite_qph = (
            math.sqrt(power.qph * throughput.qph) if power.qph > 0 and throughput.qph > 0 else 0.0
        )

        composite = BenchmarkResult(
            mode="composite",
            cache=cache,
            scale=self.config.architecture.workload.datagen.get_effective_scale(),
            queries=power.queries,
            total_seconds=power.total_seconds + throughput.total_seconds,
            qph=composite_qph,
            iterations=iterations,
            streams=streams,
            stream_results=throughput.stream_results,
        )

        return power, throughput, composite

    def _run_query_stream(
        self,
        queries: list[BenchmarkQuery],
        cache: str,
        iterations: int,
    ) -> list[QueryResult]:
        """Run a sequence of queries, optionally with multiple iterations.

        Args:
            queries: Ordered list of queries to execute
            cache: "hot" or "cold" (cold flushes before each query)
            iterations: Per-query iterations (>1 uses median)

        Returns:
            List of QueryResult, one per query
        """
        results: list[QueryResult] = []

        for query in queries:
            if cache == "cold":
                self.executor.flush_cache()

            if iterations > 1:
                times: list[float] = []
                last_result: QueryResult | None = None
                for _ in range(iterations):
                    if cache == "cold":
                        self.executor.flush_cache()
                    result = self._execute_single_query(query)
                    times.append(result.elapsed_seconds)
                    last_result = result

                times.sort()
                median_time = times[len(times) // 2]
                assert last_result is not None
                results.append(
                    QueryResult(
                        query=query,
                        elapsed_seconds=median_time,
                        rows_returned=last_result.rows_returned,
                        success=last_result.success,
                        error_message=last_result.error_message,
                    )
                )
            else:
                result = self._execute_single_query(query)
                results.append(result)

        return results

    def _execute_single_query(self, query: BenchmarkQuery) -> QueryResult:
        """Execute a single query via the configured executor.

        Args:
            query: Query to execute

        Returns:
            QueryResult with timing and row count
        """
        sql = query.sql.format(
            catalog=self.catalog,
            silver_table=self.silver_table,
            gold_table=self.gold_table,
        )

        # Adapt SQL for engine-specific dialect (e.g. DuckDB Iceberg syntax)
        sql = self.executor.adapt_query(sql)

        exec_result = self.executor.execute_query(sql, timeout=300)

        return QueryResult(
            query=query,
            elapsed_seconds=exec_result.duration_seconds,
            rows_returned=exec_result.rows_returned,
            success=exec_result.success,
            error_message=exec_result.error or "",
        )
