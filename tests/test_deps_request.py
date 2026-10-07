"""DEP-2 request module (SD-2, ch01 s2.2): coordinates, groups, hosts, hashes.

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
            SPARK40,
            "delta",
            None,
            ["io.delta:delta-spark_2.13:4.0.0", "org.apache.hadoop:hadoop-aws:3.4.1"],
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


def test_selected_groups_by_workload_and_engine():
    assert req.selected_groups(make_config(recipe="hive-iceberg-spark-trino")) == ("jars",)
    assert req.selected_groups(make_config(recipe="polaris-iceberg-spark-duckdb")) == (
        "jars",
        "duckdb",
    )
    aml = make_config(recipe="hive-iceberg-spark-trino", workload={"schema": "financial"})
    assert req.selected_groups(aml) == ("jars", "py-reference")


def test_request_hash_mirror_sensitive_pinset_not(monkeypatch):
    """A mirror change alters request_sha256; the same file triples give the
    same pinset_sha256 whatever the repositories (ch01 s2.2, s2.7)."""
    cfg = make_config(recipe="hive-iceberg-spark-trino")
    public = req.select_request(cfg, tools_digest="t").request_sha256
    assert req.select_request(cfg, tools_digest="t").request_sha256 == public
    monkeypatch.setattr(
        req, "_deps_key", lambda c, k: "http://nexus/m2/" if k == "maven_repository" else None
    )
    mirrored = req.select_request(cfg, tools_digest="t").request_sha256
    assert mirrored != public
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


def test_aml_request_reads_reference_pins_in_place():
    from lakebench.modules.pipeline_engines.spark.job import REFERENCE_PY_DEPS

    aml = make_config(recipe="hive-iceberg-spark-trino", workload={"schema": "financial"})
    assert req.select_request(aml, tools_digest="t").py_reference == tuple(REFERENCE_PY_DEPS)


def test_every_request_field_enters_the_hash():
    """A field that falls out of request_sha256 would let a redeploy keep a
    set resolved for another request (ch01 s2.3 step 1 skips on the hash)."""
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


def _with_deps(**deps):
    s3 = {"endpoint": "http://minio:9000", "access_key": "k", "secret_key": "s"}
    return make_config(platform={"storage": {"s3": s3}, "deps": deps})


def test_deps_key_reads_platform_deps():
    """request.py reads the typed platform.deps keys (SD-4a)."""
    cfg = _with_deps(maven_repository=" http://nexus/m2 ")
    assert req._deps_key(cfg, "maven_repository") == "http://nexus/m2/"
    assert req._deps_key(cfg, "pypi_index") is None
    assert req.repositories(cfg) == ["http://nexus/m2/"]
    assert req.repositories(_with_deps())[0] == req.MAVEN_CENTRAL
    aml = make_config(recipe="polaris-iceberg-spark-duckdb", workload={"schema": "financial"})
    mirrored = aml.model_copy(deep=True)
    mirrored.platform.deps.pypi_index = "http://pypi.lab/simple/"
    mirrored.platform.deps.duckdb_extension_repository = "http://ext.lab"
    r = req.select_request(mirrored, tools_digest="t")
    assert (r.pypi_index, r.duckdb_extension_repository) == (
        "http://pypi.lab/simple/",
        "http://ext.lab",
    )
    assert r.request_sha256 != req.select_request(aml, tools_digest="t").request_sha256
    assert req.egress_hosts(mirrored) == sorted(
        {"repo1.maven.org", "maven-central.storage-download.googleapis.com", "pypi.lab", "ext.lab"}
    )
