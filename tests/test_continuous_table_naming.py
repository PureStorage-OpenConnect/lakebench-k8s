"""Continuous c360 table names resolve in a catalog the Spark job defines.

lb16-cs (2026-09-27): hive-delta-spark-trino continuous failed at the reset
with REQUIRES_SINGLE_PART_NAMESPACE. The reset and bronze-ingest named
bronze through CATALOG_NAME, the Trino catalog ("lakehouse"), which for Delta
+ Hive names no Spark catalog; only spark_catalog exists there. These tests
build the real SparkApplication manifest for every recipe and check each
table name the continuous scripts use against the catalogs the manifest's
sparkConf defines. No pyspark needed: the scripts' pyspark import is stubbed.
"""

from __future__ import annotations

import re
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lakebench.config import LakebenchConfig
from lakebench.config.recipes import RECIPES
from lakebench.k8s.client import ClusterCapacity
from lakebench.spark.job import JobType, SparkJobManager

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"

# Every recipe continuous mode can run (all of them: the query engine does
# not change the Spark side).
_RECIPES = sorted(r for r in RECIPES if r != "default")
_STREAM_JOBS = (JobType.BRONZE_VERIFY, JobType.BRONZE_INGEST, JobType.SILVER_STREAM)
_CONTINUOUS_SCRIPTS = (
    "bronze_ingest.py",
    "bronze_ingest_delta.py",
    "silver_stream.py",
    "silver_stream_delta.py",
    "gold_refresh.py",
    "gold_refresh_delta.py",
    "bronze_verify.py",
)


def _config(recipe: str) -> LakebenchConfig:
    return LakebenchConfig(
        name="t",
        recipe=recipe,
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                }
            }
        },
        architecture={
            "pipeline": {"mode": "continuous"},
            **(
                {"catalog": {"polaris": {"client_secret": "s" * 32}}}
                if recipe.startswith("polaris")
                else {}
            ),
        },
    )


def _manifest(recipe: str, job: JobType) -> dict:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=434_000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=434_000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    return SparkJobManager(_config(recipe), k8s)._build_manifest(job)


def _spark_catalogs(manifest: dict) -> set[str]:
    conf = manifest["spec"]["sparkConf"]
    named = {k.split(".")[3] for k in conf if re.fullmatch(r"spark\.sql\.catalog\.[^.]+", k)}
    return named | {"spark_catalog"}


def _env(manifest: dict) -> dict[str, str]:
    return {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}


@pytest.fixture()
def scripts(monkeypatch):
    """Import the pipeline scripts with pyspark stubbed out."""
    monkeypatch.syspath_prepend(str(SCRIPTS))
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()  # type: ignore[attr-defined]
    functions = types.ModuleType("pyspark.sql.functions")
    functions.__getattr__ = lambda name: MagicMock()  # type: ignore[method-assign]
    for name, mod in (
        ("pyspark", pyspark),
        ("pyspark.sql", sql),
        ("pyspark.sql.functions", functions),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    for m in ("common", "bronze_verify", "bronze_ingest_delta"):
        sys.modules.pop(m, None)
    yield
    for m in ("common", "bronze_verify", "bronze_ingest_delta"):
        sys.modules.pop(m, None)


def _use_env(monkeypatch, manifest: dict) -> None:
    for k in ("CATALOG_NAME", "LB_ICEBERG_CATALOG"):
        monkeypatch.delenv(k, raising=False)
    for k, v in _env(manifest).items():
        monkeypatch.setenv(k, v)


@pytest.mark.parametrize("recipe", _RECIPES)
def test_reset_names_every_table_in_a_defined_catalog(recipe, scripts, monkeypatch):
    manifest = _manifest(recipe, JobType.BRONZE_VERIFY)
    _use_env(monkeypatch, manifest)
    import bronze_verify

    tables, _, _ = bronze_verify.continuous_reset_targets()
    catalogs = _spark_catalogs(manifest)
    for t in tables:
        assert t.split(".")[0] in catalogs, (recipe, t, catalogs)
        assert t.count(".") == 2, t  # catalog.namespace.table, one-part namespace


@pytest.mark.parametrize("recipe", [r for r in _RECIPES if "-delta-" in r])
def test_delta_bronze_ingest_target_is_the_table_silver_stream_reads(recipe, scripts, monkeypatch):
    manifest = _manifest(recipe, JobType.BRONZE_INGEST)
    _use_env(monkeypatch, manifest)
    import bronze_ingest_delta

    name, location = bronze_ingest_delta.bronze_target()
    assert name == "spark_catalog.default.bronze_raw"
    assert name.split(".")[0] in _spark_catalogs(manifest)
    # Explicit S3 path: HMS's default database sits on pod-local disk.
    assert location == "s3a://b/warehouse/default.db/bronze_raw"

    # silver-stream (another SparkApplication) reads bronze under this name.
    silver_env = _env(_manifest(recipe, JobType.SILVER_STREAM))
    silver_reads = f"{silver_env['LB_ICEBERG_CATALOG']}.{silver_env['LB_BRONZE_TABLE']}"
    assert silver_reads == name


@pytest.mark.parametrize("recipe", _RECIPES)
def test_every_stream_job_env_names_a_defined_catalog(recipe):
    for job in _STREAM_JOBS + (JobType.GOLD_REFRESH,):
        manifest = _manifest(recipe, job)
        assert _env(manifest)["LB_ICEBERG_CATALOG"] in _spark_catalogs(manifest), (recipe, job)


@pytest.mark.parametrize("script", _CONTINUOUS_SCRIPTS)
def test_continuous_scripts_never_name_tables_through_catalog_name(script):
    """CATALOG_NAME is the Trino catalog. A Spark script that builds a table
    name from it breaks Delta + Hive (it names no Spark catalog there)."""
    src = (SCRIPTS / script).read_text()
    assert not re.search(r"env\(\s*[\"']CATALOG_NAME[\"']", src), script


@pytest.mark.parametrize("recipe", _RECIPES)
def test_reset_clears_the_path_bronze_ingest_creates(recipe, scripts, monkeypatch):
    """The reset's explicit-location list is the Delta bronze target, and
    empty for Iceberg (whose DROP ... PURGE removes the files)."""
    manifest = _manifest(recipe, JobType.BRONZE_VERIFY)
    _use_env(monkeypatch, manifest)
    import bronze_ingest_delta
    import bronze_verify

    got = bronze_verify.continuous_reset_explicit_locations()
    assert got == ([bronze_ingest_delta.bronze_target()] if "-delta-" in recipe else [])
