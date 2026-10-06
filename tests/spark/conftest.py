"""Spark-tier harness: one way to find the jars, one way to skip for a
missing jar, one Spark session per module and one way to run a Spark child.

Jars come from ``LB_SPARK_TEST_JARS`` (comma-separated jar files). A test
that needs one says so with ``@pytest.mark.requires_jars("iceberg")`` (or
``"delta"``, or both); without the jar it skips with a reason starting
``LB-JARS missing:``.

CI runs one PySpark line (4.1.1 forward) in two shards via
``pytest-xdist --dist loadfile``, so each file runs in its own worker and
its own JVM. The shard splitter, leak detector, reverse-order mode and
known-bug machinery that lived here are all gone.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SCRIPTS = ROOT / "src" / "lakebench" / "spark" / "scripts"

ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
DRIVER_MEMORY = "3g"
SKIP_PREFIX = "LB-JARS missing:"

JARS: list[Path] = [
    Path(e)
    for e in os.environ.get("LB_SPARK_TEST_JARS", "").split(",")
    if e.strip()
]


def _has(kind: str) -> bool:
    return any(kind in j.name.lower() for j in JARS)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        'requires_jars(*kinds): skip when a named jar kind ("iceberg", "delta") is not provided',
    )
    if JARS:
        os.environ.setdefault(
            "PYSPARK_SUBMIT_ARGS",
            f"--driver-memory {DRIVER_MEMORY} --jars {','.join(str(j) for j in JARS)} pyspark-shell",
        )


def pytest_runtest_setup(item: pytest.Item) -> None:
    for mark in item.iter_markers("requires_jars"):
        missing = [k for k in mark.args if not _has(k)]
        if missing:
            pytest.skip(f"{SKIP_PREFIX} {','.join(missing)}")


def _module_jar_kinds(request: pytest.FixtureRequest) -> set[str]:
    kinds: set[str] = set()
    for item in request.node.iter_markers("requires_jars") if hasattr(request.node, "iter_markers") else []:
        kinds.update(item.args)
    for item in getattr(request, "session", request).items if hasattr(request, "session") else []:
        if item.module is request.module:
            for mark in item.iter_markers("requires_jars"):
                kinds.update(mark.args)
    return kinds


@pytest.fixture(scope="module")
def spark_session(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    conf: dict[str, str] = {
        "spark.master": "local[2]",
        "spark.ui.enabled": "false",
        "spark.driver.memory": DRIVER_MEMORY,
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.warehouse.dir": f"file://{warehouse}",
    }
    if JARS:
        conf["spark.jars"] = ",".join(str(j) for j in JARS)

    kinds = _module_jar_kinds(request)
    extensions: list[str] = []
    if "iceberg" in kinds and _has("iceberg"):
        extensions.append(ICEBERG_EXTENSION)
    if "delta" in kinds and _has("delta"):
        extensions.append(DELTA_EXTENSION)
        conf["spark.sql.catalog.spark_catalog"] = DELTA_CATALOG
    if extensions:
        conf["spark.sql.extensions"] = ",".join(extensions)

    builder = SparkSession.builder
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


class _SparkJars:
    """The jar list currently on the classpath. ``classpath`` is a comma-joined
    string suitable for ``--jars`` or ``spark.jars``, so a child process can be
    given the same classpath as this session."""

    def __init__(self, jars: list[Path]) -> None:
        self.jars = jars
        self.classpath = ",".join(str(j) for j in jars)

    def has(self, kind: str) -> bool:
        return any(kind in j.name.lower() for j in self.jars)


@pytest.fixture(scope="session")
def spark_jars() -> _SparkJars:
    return _SparkJars(list(JARS))


@pytest.fixture(scope="session")
def iceberg_catalog() -> Callable[..., str]:
    def register(spark: Any, name: str, warehouse: Path) -> str:
        spark.conf.set(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{name}.type", "hadoop")
        spark.conf.set(f"spark.sql.catalog.{name}.warehouse", f"file://{warehouse}")
        return name

    return register


@pytest.fixture(scope="session")
def spark_subprocess() -> Callable[..., subprocess.CompletedProcess[str]]:
    def run(
        script: str | Path,
        *args: str | Path,
        env: dict[str, str] | None = None,
        timeout: float = 900,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        child = {
            **os.environ,
            "PYSPARK_PYTHON": sys.executable,
            "PYTHONPATH": os.pathsep.join(
                [str(SCRIPTS), str(HERE), str(ROOT / "src")]
                + [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
            ),
        }
        if JARS:
            child["LB_SPARK_TEST_JARS"] = ",".join(str(j) for j in JARS)
        if env:
            child.update(env)
        proc = subprocess.run(
            [sys.executable, str(script), *(str(a) for a in args)],
            capture_output=True,
            text=True,
            env=child,
            timeout=timeout,
        )
        if check and proc.returncode:
            pytest.fail(f"child exit {proc.returncode}\nSTDOUT:\n{proc.stdout[-2000:]}\nSTDERR:\n{proc.stderr[-2000:]}")
        return proc

    return run
