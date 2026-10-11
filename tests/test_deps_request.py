"""DEP-2 request module (SD-2): coordinates, groups, hosts, hashes.

Expected coordinates are the Maven facts the SD-1 offline resolve fetched
from Maven Central on 2026-10-01 (every one resolved), not values read back
from the code under test.
"""

from __future__ import annotations

import pytest

from lakebench.deps import request as req
from tests.conftest import make_config
from tests.fixtures.deps_request_helpers import SPARK40 as SPARK40
from tests.fixtures.deps_request_helpers import _cfg as _cfg

SPARK41 = "apache/spark:4.1.1-python3"
SPARK35 = "apache/spark:3.5.4-python3"


@pytest.mark.parametrize(
    "image,fmt,version,expected",
    [
        (
            SPARK40,
            "iceberg",
            "1.11.0",
            [
                "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
                "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
                "org.apache.hadoop:hadoop-aws:3.4.1",
            ],
        ),
        (
            SPARK41,
            "iceberg",
            "1.11.0",
            [
                "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
                "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
                "org.apache.hadoop:hadoop-aws:3.4.2",
            ],
        ),
        (
            SPARK41,
            "iceberg",
            "1.10.1",
            [
                "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
                "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
                "org.apache.hadoop:hadoop-aws:3.4.2",
            ],
        ),
        (
            SPARK35,
            "iceberg",
            "1.10.1",
            [
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1",
                "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ],
        ),
        (
            SPARK41,
            "delta",
            None,
            ["io.delta:delta-spark_4.1_2.13:4.1.0", "org.apache.hadoop:hadoop-aws:3.4.2"],
        ),
    ],
)
def test_jar_coordinates(image, fmt, version, expected):
    recipe = "hive-delta-spark-trino" if fmt == "delta" else "hive-iceberg-spark-trino"
    assert req.jar_coordinates(_cfg(image, fmt, version, recipe=recipe)) == expected


@pytest.mark.parametrize(
    "recipe,schema,groups",
    [
        ("hive-iceberg-spark-trino", None, ("jars",)),
        ("polaris-iceberg-spark-duckdb", None, ("jars", "duckdb")),
        ("hive-iceberg-spark-trino", "financial", ("jars", "py-reference")),
    ],
)
def test_selected_groups_by_workload_and_engine(recipe, schema, groups):
    over = {"workload": {"schema": schema}} if schema else {}
    assert req.selected_groups(make_config(recipe=recipe, **over)) == groups


def test_pinset_hash_ignores_size_and_resolve_time():
    """The same file triples give the same pinset_sha256 whatever the
    repositories or resolve metadata."""
    groups = {"jars": [{"file": "a.jar", "sha256": "11", "size": 1, "coordinate": "g:a:1"}]}
    assert req.pinset_sha256(groups, ["a.jar"]) == req.pinset_sha256(
        {"jars": [{"file": "a.jar", "sha256": "11", "size": 999, "resolved_at": "x"}]}, ["a.jar"]
    )


def test_request_hash_covers_resolver_image_and_versions():
    cfg = make_config(recipe="hive-iceberg-spark-trino")
    base = req.select_request(cfg, tools_digest="t").request_sha256
    assert req.select_request(cfg, tools_digest="t2").request_sha256 != base
    assert (
        req.select_request(
            _cfg(SPARK40, recipe="hive-iceberg-spark-trino"), tools_digest="t"
        ).request_sha256
        != base
    )
    assert (
        req.select_request(
            _cfg(SPARK41, "iceberg", "1.10.1", recipe="hive-iceberg-spark-trino"), tools_digest="t"
        ).request_sha256
        != base
    )


def test_every_request_field_enters_the_hash():
    """A field that falls out of request_sha256 would let a redeploy keep a
    set resolved for another request (the resolve skips on the hash)."""
    for field, value in [
        ("py_reference", ("numpy==2.2.7",)),
        ("pypi_index", "http://pypi.mirror/simple/"),
        ("duckdb_version", "1.5.6"),
        ("duckdb_extensions", ("httpfs", "iceberg")),
        ("duckdb_image", "python:3.12-slim"),
        ("duckdb_extension_repository", "http://ext.mirror"),
        ("repositories", ("http://nexus/m2/",)),
        ("jar_coordinates", ("g:a:2",)),
        ("spark_image", "apache/spark:4.1.1-python3"),
        ("groups", ("jars",)),
    ]:
        import dataclasses

        aml_duck = make_config(
            recipe="polaris-iceberg-spark-duckdb", workload={"schema": "financial"}
        )
        base = req.select_request(aml_duck, tools_digest="t")
        assert base.py_reference and base.duckdb_version  # both groups selected
        assert dataclasses.replace(base, **{field: value}).request_sha256 != base.request_sha256


def _with_deps(recipe="hive-iceberg-spark-trino", schema=None, **deps):
    s3 = {"endpoint": "http://minio:9000", "access_key": "k", "secret_key": "s"}
    over = {"workload": {"schema": schema}} if schema else {}
    return make_config(recipe=recipe, platform={"storage": {"s3": s3}, "deps": deps}, **over)


@pytest.mark.parametrize(
    "recipe,schema,deps",
    [
        ("hive-iceberg-spark-trino", None, {"maven_repository": "http://nexus/m2/"}),
        ("polaris-iceberg-spark-duckdb", "financial", {"pypi_index": "http://pypi.lab/simple/"}),
        (
            "polaris-iceberg-spark-duckdb",
            "financial",
            {"duckdb_extension_repository": "http://ext.lab"},
        ),
    ],
)
def test_configured_mirror_changes_the_request_hash(recipe, schema, deps):
    """A mirror set in platform.deps alters request_sha256."""
    public = req.select_request(_with_deps(recipe, schema), tools_digest="t")
    mirrored = req.select_request(_with_deps(recipe, schema, **deps), tools_digest="t")
    assert (
        public.request_sha256
        == req.select_request(_with_deps(recipe, schema), tools_digest="t").request_sha256
    )
    assert mirrored.request_sha256 != public.request_sha256


def test_configured_maven_mirror_is_the_only_repository():
    assert req.repositories(_with_deps(maven_repository=" http://nexus/m2 ")) == [
        "http://nexus/m2/"
    ]
    assert req.repositories(_with_deps())[0] == req.MAVEN_CENTRAL


def test_egress_hosts_follow_configured_mirrors():
    cfg = _with_deps(
        "polaris-iceberg-spark-duckdb",
        "financial",
        pypi_index="http://pypi.lab/simple/",
        duckdb_extension_repository="http://ext.lab",
    )
    assert req.egress_hosts(cfg) == sorted(
        {"repo1.maven.org", "maven-central.storage-download.googleapis.com", "pypi.lab", "ext.lab"}
    )
