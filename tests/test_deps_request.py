"""DEP-2 request module (SD-2, ch01 s2.2): coordinates, groups, hosts, hashes.

Expected coordinates are the Maven facts the SD-1 offline resolve fetched
from Maven Central on 2026-10-01 (every one resolved), not values read back
from the code under test.
"""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path

import pytest

from lakebench.deps import request as req
from tests.conftest import make_config

SPARK40 = "apache/spark:4.0.2-python3"
SPARK41 = "apache/spark:4.1.1-python3"
SPARK35 = "apache/spark:3.5.4-python3"
GOOGLE = "https://maven-central.storage-download.googleapis.com/maven2/"


def _cfg(image=SPARK40, fmt="iceberg", version=None, **over):
    tf: dict = {"type": fmt}
    if version:
        tf[fmt] = {"version": version}
    arch = over.pop("architecture", {})
    arch.setdefault("table_format", tf)
    return make_config(images={"spark": image}, architecture=arch, **over)


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


def test_repositories_default_and_mirror(monkeypatch):
    cfg = make_config(recipe="hive-iceberg-spark-trino")
    assert req.repositories(cfg) == ["https://repo1.maven.org/maven2/", GOOGLE]
    monkeypatch.setattr(
        req,
        "_deps_key",
        lambda c, k: (
            "http://nexus.lb.svc:8081/repository/maven/" if k == "maven_repository" else None
        ),
    )
    assert req.repositories(cfg) == ["http://nexus.lb.svc:8081/repository/maven/"]


def test_egress_hosts_default_per_group():
    c360 = make_config(recipe="hive-iceberg-spark-trino")
    assert req.egress_hosts(c360) == [
        "maven-central.storage-download.googleapis.com",
        "repo1.maven.org",
    ]
    aml = make_config(recipe="hive-iceberg-spark-trino", workload={"schema": "financial"})
    assert req.egress_hosts(aml) == [
        "files.pythonhosted.org",
        "maven-central.storage-download.googleapis.com",
        "pypi.org",
        "repo1.maven.org",
    ]
    duck = make_config(recipe="polaris-iceberg-spark-duckdb")
    assert "extensions.duckdb.org" in req.egress_hosts(duck)
    assert "pypi.org" in req.egress_hosts(duck)


def test_egress_hosts_with_mirrors_name_only_the_mirrors(monkeypatch):
    mirrors = {
        "maven_repository": "http://nexus.lb.svc:8081/repository/maven/",
        "pypi_index": "http://nexus.lb.svc:8081/repository/pypi/simple/",
        "duckdb_extension_repository": "http://ext.mirror.example",
    }
    monkeypatch.setattr(req, "_deps_key", lambda c, k: mirrors.get(k))
    duck = make_config(recipe="polaris-iceberg-spark-duckdb")
    assert req.egress_hosts(duck) == ["ext.mirror.example", "nexus.lb.svc"]


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
    assert req.pinset_sha256(groups) == req.pinset_sha256(
        {"jars": [{"file": "a.jar", "sha256": "11", "size": 999, "resolved_at": "x"}]}
    )


def test_request_hash_covers_resolver_image_and_versions():
    cfg = make_config(recipe="hive-iceberg-spark-trino")
    base = req.select_request(cfg, tools_digest="t").request_sha256
    assert req.select_request(cfg, tools_digest="t2").request_sha256 != base
    assert (
        req.select_request(
            _cfg(SPARK41, recipe="hive-iceberg-spark-trino"), tools_digest="t"
        ).request_sha256
        != base
    )
    assert (
        req.select_request(
            _cfg(SPARK40, "iceberg", "1.10.1", recipe="hive-iceberg-spark-trino"), tools_digest="t"
        ).request_sha256
        != base
    )


def test_c360_request_carries_no_python_or_duckdb_fields():
    r = req.select_request(make_config(recipe="hive-iceberg-spark-trino"), tools_digest="t")
    js = r.canonical_json()
    for key in ("py_reference", "pypi_index", "duckdb_version", "duckdb_image"):
        assert key not in js


def test_aml_request_reads_reference_pins_in_place():
    from lakebench.modules.pipeline_engines.spark.job import REFERENCE_PY_DEPS

    aml = make_config(recipe="hive-iceberg-spark-trino", workload={"schema": "financial"})
    assert req.select_request(aml, tools_digest="t").py_reference == tuple(REFERENCE_PY_DEPS)


def test_pinset_canonical_form():
    """lb_deps.py (stdlib only) must compute the same value: compact JSON of
    the sorted [group, file, sha256] triples."""
    groups = {
        "py-reference": [{"file": "six-1.17.0-py2.py3-none-any.whl", "sha256": "bb"}],
        "jars": [{"file": "b.jar", "sha256": "02"}, {"file": "a.jar", "sha256": "01"}],
    }
    literal = '[["jars","a.jar","01"],["jars","b.jar","02"],["py-reference","six-1.17.0-py2.py3-none-any.whl","bb"]]'
    assert req.pinset_sha256(groups) == hashlib.sha256(literal.encode()).hexdigest()
    changed = {
        **groups,
        "jars": [{"file": "b.jar", "sha256": "03"}, {"file": "a.jar", "sha256": "01"}],
    }
    assert req.pinset_sha256(changed) != req.pinset_sha256(groups)
    moved = {"duckdb-ext": groups["jars"], "py-reference": groups["py-reference"]}
    assert req.pinset_sha256(moved) != req.pinset_sha256(groups)


def test_tools_sha256_raises_when_resolver_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        req.tools_sha256(tmp_path / "lb_deps.py")


def test_one_runtime_reader():
    """Only iceberg_runtime_suffix_for reads _ICEBERG_RUNTIME_SUFFIX (ch01
    s2.10). A second reader is how Thrift and the jobs diverged (UX D2)."""
    name = "_ICEBERG_RUNTIME_SUFFIX"
    job = "modules/pipeline_engines/spark/job.py"
    src = Path(req.__file__).resolve().parents[1]
    offenders = []
    for path in sorted(src.rglob("*.py")):
        rel = path.relative_to(src).as_posix()
        tree = ast.parse(path.read_text())
        allowed: set[int] = set()
        if rel == job:
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    targets = node.targets
                elif isinstance(node, ast.AnnAssign):
                    targets = [node.target]
                else:
                    targets = []
                definition = any(isinstance(t, ast.Name) and t.id == name for t in targets)
                reader = (
                    isinstance(node, ast.FunctionDef) and node.name == "iceberg_runtime_suffix_for"
                )
                if definition or reader:
                    allowed.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
        for node in ast.walk(tree):
            idents = []
            if isinstance(node, ast.Name):
                idents = [node.id]
            elif isinstance(node, ast.Attribute):
                idents = [node.attr]
            elif isinstance(node, ast.ImportFrom):
                idents = [a.name for a in node.names]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                idents = [node.value]  # getattr(job, "_ICEBERG_RUNTIME_SUFFIX")
            if name in idents and node.lineno not in allowed:
                offenders.append(f"{rel}:{node.lineno}")
    assert offenders == []


@pytest.mark.parametrize(
    "field,value",
    [
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
    ],
)
def test_every_request_field_enters_the_hash(field, value):
    """A field that falls out of request_sha256 would let a redeploy keep a
    set resolved for another request (ch01 s2.3 step 1 skips on the hash)."""
    import dataclasses

    aml_duck = make_config(recipe="polaris-iceberg-spark-duckdb", workload={"schema": "financial"})
    base = req.select_request(aml_duck, tools_digest="t")
    assert base.py_reference and base.duckdb_version  # both groups selected
    assert dataclasses.replace(base, **{field: value}).request_sha256 != base.request_sha256


def test_deps_key_reads_platform_deps():
    """The getattr shim must read platform.deps once SD-4a adds it."""
    from types import SimpleNamespace

    cfg = SimpleNamespace(
        platform=SimpleNamespace(deps=SimpleNamespace(maven_repository=" http://nexus/m2/ "))
    )
    assert req._deps_key(cfg, "maven_repository") == "http://nexus/m2/"
    assert req._deps_key(cfg, "pypi_index") is None
    assert req._deps_key(SimpleNamespace(platform=SimpleNamespace()), "maven_repository") is None


def test_pypi_files_host_follows_the_index_host(monkeypatch):
    monkeypatch.setattr(
        req, "_deps_key", lambda c, k: "https://pypi.org/simple" if k == "pypi_index" else None
    )
    aml = make_config(recipe="hive-iceberg-spark-trino", workload={"schema": "financial"})
    assert "files.pythonhosted.org" in req.egress_hosts(aml)


def test_manifest_groups_cover_every_request_group():
    assert req.MANIFEST_GROUPS == {
        "jars": ("jars",),
        "py-reference": ("py-reference",),
        "duckdb": ("duckdb-wheels", "duckdb-ext"),
    }
