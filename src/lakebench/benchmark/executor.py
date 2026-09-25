"""Query executor abstraction for Lakebench benchmarks.

Provides a protocol for executing SQL queries against different engines
(Trino, Spark Thrift Server, DuckDB) via ``kubectl exec``.

Usage::

    from lakebench.benchmark.executor import get_executor

    executor = get_executor(config, namespace)
    result = executor.execute_query("SELECT 1", timeout=30)

Executor implementations live in ``lakebench.modules.query_engines.*``.
This module re-exports them for backward compatibility.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig

logger = logging.getLogger(__name__)

# QueryExecutorResult lives in result.py to break circular imports
# (module executors import it, and this file re-imports them).
from lakebench.benchmark.result import QueryExecutorResult  # noqa: E402


class QueryExecutor(Protocol):
    """Protocol for query engine executors."""

    catalog_name: str

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult: ...

    def engine_name(self) -> str: ...

    def health_check(self) -> bool: ...

    def flush_cache(self) -> None: ...

    def adapt_query(self, sql: str) -> str: ...


# -- Backward-compat re-exports from module locations ------------------------

from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor  # noqa: E402
from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor  # noqa: E402
from lakebench.modules.query_engines.trino.executor import TrinoExecutor  # noqa: E402


def get_executor(config: LakebenchConfig, namespace: str | None = None) -> QueryExecutor:
    """Factory: return the appropriate QueryExecutor for the configured engine."""
    ns = namespace or config.get_namespace()
    engine_type = config.architecture.query_engine.type.value
    table_format = config.architecture.table_format.type.value

    if engine_type == "trino":
        catalog = config.architecture.query_engine.trino.catalog_name
        return TrinoExecutor(namespace=ns, catalog_name=catalog, table_format=table_format)
    elif engine_type == "spark-thrift":
        catalog_type = config.architecture.catalog.type.value
        if table_format == "delta" and catalog_type != "unity":
            catalog = "spark_catalog"
        else:
            catalog = config.architecture.query_engine.spark_thrift.catalog_name
        return SparkThriftExecutor(namespace=ns, catalog_name=catalog)
    elif engine_type == "duckdb":
        s3 = config.platform.storage.s3
        catalog = config.architecture.query_engine.duckdb.catalog_name
        catalog_type = config.architecture.catalog.type.value
        tables = config.architecture.tables
        return DuckDBExecutor(
            namespace=ns,
            catalog_name=catalog,
            s3_endpoint=s3.endpoint,
            s3_region=s3.region,
            s3_path_style=s3.path_style,
            s3_buckets={
                "silver": s3.buckets.silver,
                "gold": s3.buckets.gold,
            },
            # Financial FQ3/FQ4/FQ5/FQ8 reference silver_entities /
            # silver_counterparty_edges / silver_account_statements /
            # gold_alerts placeholders; without them in table_names the
            # DuckDB adapter can't rewrite the placeholder to
            # iceberg_scan(...) and every financial FQ that hits an
            # extra table fails on DuckDB. Populate every table name
            # here so the adapter has a full lookup.
            table_names={
                "silver": tables.silver,
                "gold": tables.gold,
                "silver_entities": tables.silver_entities,
                "silver_accounts": tables.silver_accounts,
                "silver_account_statements": tables.silver_account_statements,
                "silver_counterparty_edges": tables.silver_counterparty_edges,
                "gold_alerts": tables.gold_alerts,
                "gold_risk_scores": tables.gold_risk_scores,
                "gold_entity_clusters": tables.gold_entity_clusters,
                "gold_daily_dashboards": tables.gold_daily_dashboards,
                "gold_alert_dispositions": tables.gold_alert_dispositions,
                "gold_cases": tables.gold_cases,
            },
            table_format=table_format,
            catalog_type=catalog_type,
        )
    elif engine_type == "none":
        raise ValueError("Cannot run queries without a query engine (type=none)")
    else:
        raise ValueError(f"Unknown query engine type: {engine_type}")
