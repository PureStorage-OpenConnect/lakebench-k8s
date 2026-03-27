"""Recipe definitions for Lakebench architecture presets.

Each recipe encodes four architecture axes: catalog, table format, pipeline
engine, and query engine.  Everything else (file format, Spark version,
resource sizing) is a YAML override.

Naming convention: ``<catalog>-<format>-<engine>-<query_engine>``

One alias exists: ``default`` = ``hive-iceberg-spark-trino``.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Recipe defaults
# ---------------------------------------------------------------------------
# Each recipe maps 1:1 to a validated entry in _SUPPORTED_COMBINATIONS.
# User-specified values always take precedence over recipe defaults.

RECIPES: dict[str, dict[str, Any]] = {
    "hive-iceberg-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-iceberg-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-iceberg-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "none"},
        },
    },
    "polaris-iceberg-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "trino"},
        },
    },
    "polaris-iceberg-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "polaris-iceberg-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "none"},
        },
    },
    "hive-iceberg-spark-duckdb": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "duckdb", "duckdb": {"cores": 2, "memory": "4g"}},
        },
    },
    "polaris-iceberg-spark-duckdb": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "duckdb", "duckdb": {"cores": 2, "memory": "4g"}},
        },
    },
    # -- Hive + Delta Lake (v1.2) --
    "hive-delta-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta", "delta": {"version": "4.0.0"}},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-delta-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta", "delta": {"version": "4.0.0"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-delta-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta", "delta": {"version": "4.0.0"}},
            "query_engine": {"type": "none"},
        },
    },
    # Unity + Delta excluded from v1.2. See schema.py for rationale.
}

# Alias
RECIPES["default"] = RECIPES["hive-iceberg-spark-trino"]

# Human-readable descriptions for CLI interactive flow
RECIPE_DESCRIPTIONS: dict[str, str] = {
    "default": "Hive + Iceberg + Spark + Trino (recommended)",
    "hive-iceberg-spark-thrift": "Hive + Iceberg + Spark + Spark Thrift",
    "hive-iceberg-spark-duckdb": "Hive + Iceberg + Spark + DuckDB",
    "hive-iceberg-spark-none": "Hive + Iceberg + Spark, no query engine",
    "polaris-iceberg-spark-trino": "Polaris + Iceberg + Spark + Trino",
    "polaris-iceberg-spark-thrift": "Polaris + Iceberg + Spark + Spark Thrift",
    "polaris-iceberg-spark-duckdb": "Polaris + Iceberg + Spark + DuckDB",
    "polaris-iceberg-spark-none": "Polaris + Iceberg + Spark, no query engine",
    "hive-delta-spark-trino": "Hive + Delta + Spark + Trino",
    "hive-delta-spark-thrift": "Hive + Delta + Spark + Spark Thrift",
    "hive-delta-spark-none": "Hive + Delta + Spark, no query engine",
}


def _deep_setdefault(target: dict, defaults: dict) -> None:
    """Recursively merge *defaults* into *target* without overwriting existing keys.

    Only dict values are merged recursively; scalar and list values in *target*
    are never replaced.
    """
    for key, default_value in defaults.items():
        if key not in target:
            target[key] = default_value
        elif isinstance(target[key], dict) and isinstance(default_value, dict):
            _deep_setdefault(target[key], default_value)
