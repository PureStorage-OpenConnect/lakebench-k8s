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
from types import SimpleNamespace
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
def scripts(monkeypatch, load_script):
    """Import the pipeline scripts with pyspark stubbed out."""
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


def test_delta_bronze_ingest_target_is_the_table_silver_stream_reads(scripts, monkeypatch):
    for recipe in [r for r in _RECIPES if "-delta-" in r]:
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


class _NamespaceCatalog:
    """A Spark session whose namespaces each sit in their own bucket."""

    def sql(self, statement):
        ns = statement.rsplit(" ", 1)[-1].rsplit(".", 1)[-1]
        return SimpleNamespace(
            collect=lambda: [
                {"info_name": "Location", "info_value": f"s3a://lb-{ns}/warehouse/{ns}.db"}
            ]
        )


def test_reset_clears_the_paths_continuous_tables_can_leave(scripts, monkeypatch):
    """The reset's explicit-location list is, for Delta + Hive, the bronze
    target and the managed paths of silver and gold (where a write that
    never registered leaves a log the stream cannot adopt); empty for
    Iceberg (whose DROP ... PURGE removes the files)."""
    for recipe in _RECIPES:
        manifest = _manifest(recipe, JobType.BRONZE_VERIFY)
        _use_env(monkeypatch, manifest)
        import bronze_ingest_delta
        import bronze_verify

        got = bronze_verify.continuous_reset_explicit_locations(_NamespaceCatalog())
        if "-delta-" not in recipe:
            assert got == []
            continue
        assert got[0] == bronze_ingest_delta.bronze_target()
        import os

        for ns in ("silver", "gold"):
            fq = next(t for t, _ in got[1:] if t.split(".")[-2] == ns)
            name = fq.split(".")[-1]
            uri = os.environ[f"LB_{ns.upper()}_URI"].rstrip("/") + "/"
            paths = {loc for t, loc in got if t == fq}
            # The catalog's managed path, the stream's and the batch build's.
            assert paths == {
                f"s3a://lb-{ns}/warehouse/{ns}.db/{name}",
                f"{uri}warehouse/{name}",
                f"{uri}warehouse/{ns}.db/{name}",
            }, (recipe, paths)
