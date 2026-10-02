"""After deploy, no pod fetches a dependency from outside the deployment
(DEP-2, ch01 s2.10).

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
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest
import yaml

from lakebench.config.recipes import RECIPES
from lakebench.deploy.engine import DeploymentEngine, TemplateRenderer
from lakebench.deps import manifest as m
from lakebench.deps.request import egress_hosts
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config

SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"
EXCLUDED = ("templates/deps/", "deploy/deps_tools/", "local_job.py")
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
        except Exception:  # noqa: BLE001 -- not a supported combination
            continue
        yield f"{recipe}-{schema}-{mode}", cfg


CASES = list(_cases())


def _rendered(cfg) -> list[tuple[str, str]]:
    """(what, YAML text) for everything the deployment runs."""
    out: list[tuple[str, str]] = []
    handle = m.placeholder_handle(cfg)
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(cfg, k8s)
    mgr.deps = handle
    for jt in JobType:
        try:
            out.append((jt.value, yaml.safe_dump(mgr._build_manifest(jt))))
        except m.DepsSetMissing:
            continue  # the reference job on a workload without its wheels
    engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
    ctx = {**engine.context, **m.consumer_context(handle)}
    engine_type = cfg.architecture.query_engine.type.value
    templates = {
        "spark-thrift": "spark-thrift/sparkapplication.yaml.j2",
        "duckdb": "duckdb/deployment.yaml.j2",
    }
    if engine_type in templates:
        out.append((engine_type, TemplateRenderer().render(templates[engine_type], ctx)))
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
    host = urlsplit(m.placeholder_handle(cfg).base_url).hostname
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
    for s in strings:
        if re.search(r"--packages\b", s):
            problems.append(f"{what}: --packages")
        for cmd in re.findall(r"pip3? install.*?(?=;|&&|\n|$)", s):
            pinned = "--no-index" in cmd and "--require-hashes" in cmd
            if not (pinned and f"--trusted-host {host}" in cmd):
                problems.append(f"{what}: unpinned pip install: {cmd[:120]}")
        if re.search(r"\bINSTALL\s+\w+", s):
            problems.append(f"{what}: DuckDB INSTALL")
        for egress in egress_hosts(cfg):
            if egress in s:
                problems.append(f"{what}: names {egress}")
    for d in docs:
        conf = (d.get("spec") or {}).get("sparkConf") or {} if isinstance(d, dict) else {}
        for key in ("spark.jars", "spark.submit.pyFiles"):
            for url in filter(None, (conf.get(key) or "").split(",")):
                if urlsplit(url).hostname != host:
                    problems.append(f"{what}: {key} on {urlsplit(url).hostname}")
    return problems


@pytest.mark.parametrize("name,cfg", CASES, ids=[n for n, _ in CASES])
def test_no_runtime_fetch(name, cfg):
    problems = [p for what, text in _rendered(cfg) for p in _problems(cfg, what, text)]
    assert problems == []


def test_the_guard_catches_each_form():
    cfg = make_config()
    host = urlsplit(m.placeholder_handle(cfg).base_url).hostname
    bad = [
        "sparkConf: {spark.jars.packages: x}",
        "command: spark-submit --packages a:b:c",
        "command: pip install duckdb==1.5.5",
        f"command: pip install --no-index --trusted-host {host} x",
        "command: python -c \"c.execute('INSTALL iceberg')\"",
        "url: https://repo1.maven.org/maven2/",
    ]
    for text in bad:
        assert _problems(cfg, "t", text), text
    assert _problems(cfg, "t", "spec: {sparkConf: {spark.jars: 'http://elsewhere/x.jar'}}")
    assert not _problems(
        cfg,
        "t",
        f"command: pip install --no-index --require-hashes --trusted-host {host} -r r.txt",
    )


def test_no_shipped_template_resolves_at_runtime():
    """The static half: no template outside the resolver's own runs a
    package resolve or an unpinned install."""
    bad = []
    for path in sorted((SRC / "templates").rglob("*.j2")):
        rel = path.relative_to(SRC).as_posix()
        if any(rel.startswith(e) for e in EXCLUDED):
            continue
        text = path.read_text()
        if re.search(r"--packages\b|spark\.jars\.ivy|INSTALL \w+", text):
            bad.append(rel)
        for cmd in re.findall(r"pip3? install[^\n]*", text):
            if "--no-index" not in cmd or "--require-hashes" not in cmd:
                bad.append(f"{rel}: {cmd[:80]}")
    assert bad == []
