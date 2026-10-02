"""What a deployment's dependency set must hold.

This module is the only definition of the jars, Python wheels and DuckDB
files a deployment needs, where they are resolved from, and the two hashes
that name a request and its resolved content:

* ``request_sha256`` names what was asked for. It decides whether a
  redeploy needs a new resolve and is not identity: a mirror change alters
  it without changing what a run means.
* ``pinset_sha256`` names what was resolved: the sorted ``(group, file,
  sha256)`` triples plus the jar order. It is content-addressed, enters the
  Architecture identity group and names the directory the set is served
  from. ``deploy/deps_tools/lb_deps.py`` computes the same value with the
  standard library alone; the two must stay byte-for-byte equal.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig

MAVEN_CENTRAL = "https://repo1.maven.org/maven2/"
PYPI_INDEX = "https://pypi.org/simple/"
# pip download from pypi.org fetches the files from this host.
PYPI_FILES_HOST = "files.pythonhosted.org"
DUCKDB_EXTENSION_REPOSITORY = "http://extensions.duckdb.org"
# Recent DuckDB iceberg releases autoload avro: an iceberg_scan with
# autoinstall off fails without avro.duckdb_extension (checked offline on
# DuckDB 1.5.5 and in a live cluster run, 2026-10-01).
DUCKDB_EXTENSIONS: tuple[str, ...] = ("httpfs", "iceberg", "avro")

GROUP_JARS = "jars"
GROUP_PY_REFERENCE = "py-reference"
GROUP_DUCKDB = "duckdb"

# Request group -> the manifest groups its resolve writes. The stdlib
# lb_deps.py cannot import this module, so it copies the names and a parity
# test pins them to this mapping; the deploy step's check reads the mapping
# directly.
MANIFEST_GROUPS: dict[str, tuple[str, ...]] = {
    GROUP_JARS: ("jars",),
    GROUP_PY_REFERENCE: ("py-reference",),
    GROUP_DUCKDB: ("duckdb-wheels", "duckdb-ext"),
}

# lb_deps.py's exit codes, for the deploy step's messages; the resolver
# copies them and a parity test pins the copy. Its error line carries
# "egress:" when a repository or index could not be reached.
LB_DEPS_EXIT: dict[str, int] = {
    "missing": 3,
    "hash": 4,
    "ux_d2": 5,
    "space": 6,
    "internal": 7,
    "request": 8,
}

# Manifest group -> its directory under the set, which is also its URL
# path below ``<base_url>`` (pip ``--find-links <base_url>/duckdb/wheels/``).
MANIFEST_DIRS: dict[str, str] = {
    "jars": "jars",
    "py-reference": "py-reference",
    "duckdb-wheels": "duckdb/wheels",
    "duckdb-ext": "duckdb-ext",
}

# Jars in a set whose artifactId the stock Spark image also ships in
# /opt/spark/jars at another version, as (artifactId, set version, image
# version). Spark Thrift copies the set onto the system classpath, so a new
# overlap can shadow the image's classes; deploy fails on one not listed
# here. Seeded from a live listing of the stock images (2026-10-01): only
# Delta 4.1.0 on apache/spark:4.1.1 overlaps at another version; today's
# Thrift already loads these files. dlt40's antlr4-runtime 4.13.1 is the
# image's own version and needs no entry.
KNOWN_OVERLAPS: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("jsr305", "3.0.2", "3.0.0"),
        ("log4j-api", "2.25.3", "2.24.3"),
        ("log4j-core", "2.25.3", "2.24.3"),
        ("log4j-slf4j2-impl", "2.25.3", "2.24.3"),
        ("slf4j-api", "2.0.13", "2.0.17"),
    }
)


def unknown_overlaps(overlaps: Iterable[Mapping[str, str]]) -> list[dict[str, str]]:
    """The manifest's overlaps at another version that KNOWN_OVERLAPS does
    not list. Keyed by both versions, so a bump of either side is new."""
    return [
        dict(o)
        for o in overlaps
        if o["version"] != o["image_version"]
        and (o["artifact"], o["version"], o["image_version"]) not in KNOWN_OVERLAPS
    ]


# The resolver shipped to the lb-deps pod. Its sha256 enters the
# request, so a Lakebench upgrade that changes the resolver re-resolves.
TOOLS_PATH = Path(__file__).resolve().parent.parent / "deploy" / "deps_tools" / "lb_deps.py"


def jar_coordinates(cfg: LakebenchConfig) -> list[str]:
    """Maven coordinates every Spark job and Spark Thrift load.

    Moved unchanged from ``SparkJobManager._build_manifest``. The Iceberg
    runtime comes from ``iceberg_runtime_suffix_for``, so the jobs and
    Thrift load one runtime jar (UX D2).
    """
    from lakebench.modules.pipeline_engines.spark.job import (
        _delta_spark_artifact,
        _parse_spark_major,
        _parse_spark_major_minor,
        _spark_compat,
        iceberg_runtime_suffix_for,
    )

    image = cfg.images.spark
    scala_suffix, hadoop_version, aws_sdk_version = _spark_compat(image)
    spark_major = _parse_spark_major(image)
    catalog_type = cfg.architecture.catalog.type.value

    if cfg.architecture.table_format.type.value == "delta":
        packages = [
            _delta_spark_artifact(scala_suffix, cfg.architecture.table_format.delta.version),
            f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
        ]
    else:
        iceberg_version = cfg.architecture.table_format.iceberg.version
        suffix = iceberg_runtime_suffix_for(_parse_spark_major_minor(image), iceberg_version)
        packages = [
            f"org.apache.iceberg:iceberg-spark-runtime-{suffix}{scala_suffix}:{iceberg_version}",
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
            f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
        ]
    if spark_major < 4:
        packages.append(f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}")
    # Unity is not in _SUPPORTED_COMBINATIONS, so this branch is dead on
    # every supported path; kept for parity with the code it replaces.
    if catalog_type == "unity":
        unity_version = cfg.architecture.catalog.unity.spark_connector_version
        packages.append(f"io.unitycatalog:unitycatalog-spark{scala_suffix}:{unity_version}")
    return packages


def _deps_key(cfg: LakebenchConfig, key: str) -> str | None:
    """``platform.deps.<key>``. The keys arrive with the deploy step's config
    block, which replaces this getattr with direct access to str-typed
    fields; until then every key reads None and the public repositories
    apply."""
    block = getattr(cfg.platform, "deps", None)
    value = getattr(block, key, None) if block is not None else None
    if value is None:
        return None
    return str(value).strip() or None


def repositories(cfg: LakebenchConfig) -> list[str]:
    """Maven repositories for the lb-deps resolve, in chain order.

    The resolve writes them into an explicit ``spark.jars.ivySettings``
    chain (ch01 s2.3 step 3), so these are the only resolvers it uses and a
    configured mirror is the only one. Without a mirror: Maven Central, then
    the Google mirror. Today's runtime ``--packages`` chain is Central,
    ``repos.spark-packages.org``, then the ``spark.jars.repositories`` entry
    (Spark ``MavenUtils.createRepoResolvers``); spark-packages serves none of
    Lakebench's coordinates, and an offline check found the two chains
    resolve byte-identical sets for the four release-matrix rows.
    """
    from lakebench.modules.pipeline_engines.spark.job import _MAVEN_MIRROR_REPOS

    mirror = _deps_key(cfg, "maven_repository")
    if mirror:
        return [mirror]
    return [MAVEN_CENTRAL, _MAVEN_MIRROR_REPOS]


def pypi_index(cfg: LakebenchConfig) -> str:
    return _deps_key(cfg, "pypi_index") or PYPI_INDEX


def duckdb_extension_repository(cfg: LakebenchConfig) -> str:
    return _deps_key(cfg, "duckdb_extension_repository") or DUCKDB_EXTENSION_REPOSITORY


def selected_groups(cfg: LakebenchConfig) -> tuple[str, ...]:
    """The groups a deployment needs (ch01 s2.2 table).

    ``py-reference`` serves the AML reference detector's driver. The design
    also selects it for ``ml_loop.enabled``; the ML loop and its config key
    moved to v1.8, which adds that condition with the key.
    """
    groups = [GROUP_JARS]
    if cfg.architecture.workload.schema_type.value == "financial":
        groups.append(GROUP_PY_REFERENCE)
    if cfg.architecture.query_engine.type.value == "duckdb":
        groups.append(GROUP_DUCKDB)
    return tuple(groups)


def egress_hosts(cfg: LakebenchConfig) -> list[str]:
    """Hosts the lb-deps resolve contacts for the selected groups, sorted.

    Once every consumer reads the set, the resolve is the only fetch from outside the
    deployment. pip downloading from pypi.org fetches the files from
    ``files.pythonhosted.org`` too; a configured index names only its own
    host, because where it redirects is the mirror's business.
    """
    groups = selected_groups(cfg)
    hosts = {_host(r) for r in repositories(cfg)}
    if GROUP_PY_REFERENCE in groups or GROUP_DUCKDB in groups:
        index = pypi_index(cfg)
        hosts.add(_host(index))
        if _host(index) == _host(PYPI_INDEX):
            hosts.add(PYPI_FILES_HOST)
    if GROUP_DUCKDB in groups:
        hosts.add(_host(duckdb_extension_repository(cfg)))
    return sorted(hosts)


def _host(url: str) -> str:
    host = urlparse(url).hostname
    if not host:
        raise ValueError(f"not a URL with a host: {url!r}")
    return host


@dataclass(frozen=True)
class DepsRequest:
    """Everything the resolve is asked for. Its canonical JSON is hashed."""

    groups: tuple[str, ...]
    jar_coordinates: tuple[str, ...]
    repositories: tuple[str, ...]
    spark_image: str
    tools_sha256: str
    py_reference: tuple[str, ...] = ()
    pypi_index: str = ""
    duckdb_version: str = ""
    duckdb_extensions: tuple[str, ...] = ()
    duckdb_image: str = ""
    duckdb_extension_repository: str = ""

    def canonical_json(self) -> str:
        """The bytes of ``request.json`` in the ``lb-deps-tools`` ConfigMap.
        lb_deps.py hashes the file and refuses it unless the sha256 equals
        the pod's ``LB_DEPS_REQUEST_SHA256``, so write exactly this."""
        # Unselected groups leave their fields empty; dropping empties keeps
        # a C360 request's hash free of AML and DuckDB fields.
        data: dict[str, Any] = {k: v for k, v in asdict(self).items() if v not in ("", ())}
        return _canonical(data)

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()


def tools_sha256(path: Path = TOOLS_PATH) -> str:
    """sha256 of the shipped resolver. Raises when it is missing."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def select_request(cfg: LakebenchConfig, *, tools_digest: str | None = None) -> DepsRequest:
    """The request for this deployment. ``tools_digest`` overrides the
    shipped resolver's hash (tests)."""
    from lakebench.modules.pipeline_engines.spark.job import REFERENCE_PY_DEPS

    groups = selected_groups(cfg)
    kwargs: dict[str, Any] = {}
    if GROUP_PY_REFERENCE in groups or GROUP_DUCKDB in groups:
        kwargs["pypi_index"] = pypi_index(cfg)
    if GROUP_PY_REFERENCE in groups:
        # Read in place, not moved (C12): the frozen scorer's pins.
        kwargs["py_reference"] = tuple(REFERENCE_PY_DEPS)
    if GROUP_DUCKDB in groups:
        kwargs["duckdb_version"] = cfg.architecture.query_engine.duckdb.version
        kwargs["duckdb_extensions"] = DUCKDB_EXTENSIONS
        kwargs["duckdb_image"] = cfg.images.duckdb
        kwargs["duckdb_extension_repository"] = duckdb_extension_repository(cfg)
    return DepsRequest(
        groups=groups,
        jar_coordinates=tuple(jar_coordinates(cfg)),
        repositories=tuple(repositories(cfg)),
        spark_image=cfg.images.spark,
        tools_sha256=tools_digest if tools_digest is not None else tools_sha256(),
        **kwargs,
    )


def pinset_sha256(
    groups: Mapping[str, Iterable[Mapping[str, Any]]], jar_order: Iterable[str]
) -> str:
    """Content hash of a resolved set: the sorted ``[group, file, sha256]``
    triples and the jar order, as compact sorted-key JSON
    ``{"files": [...], "jar_order": [...]}``. The order is Spark's own
    ``--packages`` order, which the jobs keep in ``spark.jars``; it decides
    which of two jars holding the same class wins, so the same files in
    another order are another set. Size, timestamps, hosts and repositories
    do not enter it. ``jar_order`` must name each file of ``jars`` once."""
    entries = {g: list(es) for g, es in groups.items()}  # iterables are read twice
    triples = sorted([g, e["file"], e["sha256"]] for g, es in entries.items() for e in es)
    order = list(jar_order)
    jars = sorted(e["file"] for e in entries.get("jars", ()))
    if sorted(order) != jars or len(set(order)) != len(order):
        raise ValueError(f"jar_order {order} is not an ordering of the jars group {jars}")
    return hashlib.sha256(
        json.dumps(
            {"files": triples, "jar_order": order}, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def _canonical(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
