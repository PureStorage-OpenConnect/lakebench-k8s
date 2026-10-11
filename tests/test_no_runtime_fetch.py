"""After deploy, no pod fetches a dependency from outside the deployment
(DEP-2).

Renders every Spark job manifest for every supported recipe, workload and
mode, the Spark Thrift and DuckDB Deployments and the datagen Job, and fails
on any runtime resolve: Spark package or Ivy settings, ``--packages``, a pip
install that is not ``--no-index --require-hashes`` against the config's own
lb-deps host, a DuckDB ``INSTALL``, a jar URL on another host, or any URL
naming a host the resolve itself contacts (``egress_hosts``). The resolver's
own pod (templates/deps, deploy/deps_tools) and local mode (local_job.py,
out of scope) are excluded by name.
"""

from __future__ import annotations

import itertools
import re
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config.recipes import RECIPES
from lakebench.deploy.engine import DeploymentEngine, TemplateRenderer
from lakebench.deps import manifest as m
from lakebench.deps.request import egress_hosts
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config

FORBIDDEN_KEYS = (
    "spark.jars.packages",
    "spark.jars.repositories",
    "spark.jars.ivy",
    "spark.jars.ivySettings",
)


def _cases():
    for recipe, schema, mode in itertools.product(
        sorted(r for r in RECIPES if r != "default"),
        ("customer360", "financial"),
        ("batch", "continuous"),
    ):
        try:
            cfg = make_config(
                recipe=recipe,
                workload={"schema": schema},
                architecture={"pipeline": {"mode": mode}},
            )
        except ValidationError:  # not a supported combination
            continue
        yield f"{recipe}-{schema}-{mode}", cfg


CASES = list(_cases())


def test_every_recipe_is_rendered():
    covered = {name.split("-customer360-")[0].split("-financial-")[0] for name, _ in CASES}
    assert covered == {r for r in RECIPES if r != "default"}


def _handle(cfg):
    """The placeholder set, served from the config's own lb-deps host."""
    h = m.placeholder_handle(cfg)
    return m.DepsHandle(
        h.pinset_sha256,
        h.request_sha256,
        m.base_url(cfg.get_namespace(), h.pinset_sha256),
        "",
        h.manifest,
    )


def _rendered(cfg) -> list[tuple[str, str]]:
    """(what, YAML text) for everything the deployment runs."""
    out: list[tuple[str, str]] = []
    handle = _handle(cfg)
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(cfg, k8s)
    mgr.deps = handle
    financial = cfg.architecture.workload.schema_type.value == "financial"
    for jt in JobType:
        if jt.value == "score-financial-reference" and not financial:
            continue  # the reference job needs the AML set's wheels
        out.append((jt.value, yaml.safe_dump(mgr._build_manifest(jt))))
    engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
    ctx = {**engine.context, **m.consumer_context(handle)}
    engine_type = cfg.architecture.query_engine.type.value
    templates = {
        "spark-thrift": "spark-thrift/sparkapplication.yaml.j2",
        "duckdb": "duckdb/deployment.yaml.j2",
    }
    if engine_type in templates:
        out.append((engine_type, TemplateRenderer().render(templates[engine_type], ctx)))
    if engine_type == "duckdb":
        from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor

        ex = DuckDBExecutor.__new__(DuckDBExecutor)
        ex.table_format = cfg.architecture.table_format.type.value
        ex.s3_endpoint, ex.s3_region, ex.s3_path_style = "http://s3:80", "us-east-1", True
        script = ex._build_python_script("SELECT 1")
        out.append(("duckdb-query", yaml.safe_dump({"script": script})))
    from lakebench.deploy.datagen import DatagenDeployer

    dg = DatagenDeployer(engine)
    dctx = {**ctx, **dg._build_datagen_context()}
    out.append(("datagen", TemplateRenderer().render("datagen/job.yaml.j2", dctx)))
    return out


def _strings(node) -> list[str]:
    """Every string a manifest carries; a command list as one string."""
    if isinstance(node, dict):
        return [s for v in node.values() for s in _strings(v)]
    if isinstance(node, list):
        if node and all(isinstance(x, str) for x in node):
            return [" ".join(node)]
        return [s for v in node for s in _strings(v)]
    return [node] if isinstance(node, str) else []


def _problems(cfg, what: str, text: str) -> list[str]:
    """What a rendered manifest (YAML text, comments ignored) fetches."""
    problems = []
    host = f"lb-deps.{cfg.get_namespace()}.svc.cluster.local"
    docs = [d for d in yaml.safe_load_all(text) if d is not None]
    strings = [s for d in docs for s in _strings(d)]
    keys = set()

    def walk_keys(node):
        if isinstance(node, dict):
            for k, v in node.items():
                keys.add(k)
                walk_keys(v)
        elif isinstance(node, list):
            for v in node:
                walk_keys(v)

    walk_keys(docs)
    for key in FORBIDDEN_KEYS:
        if key in keys or any(key + "=" in s for s in strings):
            problems.append(f"{what}: {key}")
    for d in docs:
        spec_deps = ((d.get("spec") or {}).get("deps") if isinstance(d, dict) else None) or {}
        for k in ("packages", "jars", "repositories", "files", "pyFiles"):
            if spec_deps.get(k):
                problems.append(f"{what}: spec.deps.{k}")
    for s in strings:
        if re.search(r"--(packages|jars|repositories)\b", s):
            problems.append(f"{what}: --packages/--jars/--repositories")
        for cmd in re.findall(r"pip3?\b[^;&\n]*?\binstall\b.*?(?=;|&&|\n|$)", s):
            pinned = "--no-index" in cmd and "--require-hashes" in cmd
            if not (pinned and f"--trusted-host {host}" in cmd):
                problems.append(f"{what}: unpinned pip install: {cmd[:120]}")
            for link in re.findall(r"--find-links\s+(\S+)", cmd):
                if urlsplit(link).hostname != host:
                    problems.append(f"{what}: pip --find-links {link}")
        if re.search(r"(?i)\bINSTALL\s+['\"]?\w+|install_extension\(", s):
            problems.append(f"{what}: DuckDB INSTALL")
        for egress in egress_hosts(cfg):
            if egress in s:
                problems.append(f"{what}: names {egress}")
    for d in docs:
        conf = (d.get("spec") or {}).get("sparkConf") or {} if isinstance(d, dict) else {}
        for key in ("spark.jars", "spark.submit.pyFiles", "spark.files", "spark.archives"):
            for url in filter(None, (conf.get(key) or "").split(",")):
                if urlsplit(url).hostname != host:
                    problems.append(f"{what}: {key} on {urlsplit(url).hostname}")
    return problems


@pytest.mark.parametrize("name,cfg", CASES, ids=[n for n, _ in CASES])
def test_no_runtime_fetch(name, cfg):
    problems = [p for what, text in _rendered(cfg) for p in _problems(cfg, what, text)]
    assert problems == []
