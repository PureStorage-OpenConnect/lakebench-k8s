"""Datagen Pushgateway env injection: the datagen Job gets LB_PUSHGATEWAY_URL +
LB_RUN_ID only when observability + the pushgateway are enabled, and no-ops
otherwise. See docs/internal/observability-pushgateway.md.
"""

from __future__ import annotations

import yaml

from lakebench.deploy.datagen import DatagenDeployer
from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config


def _engine(observability_enabled: bool) -> DeploymentEngine:
    cfg = make_config(
        architecture={"workload": {"schema": "financial", "datagen": {"seed": 43}}},
        observability={"enabled": observability_enabled},
    )
    return DeploymentEngine(cfg, dry_run=True)


def test_context_has_pushgateway_when_enabled(monkeypatch):
    monkeypatch.setenv("LB_RUN_ID", "run-abc123")
    engine = _engine(observability_enabled=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    assert ctx["pushgateway_url"].startswith("http://lakebench-pushgateway.")
    assert ctx["pushgateway_url"].endswith(".svc:9091")
    assert ctx["run_id"] == "run-abc123"


def test_context_omits_pushgateway_when_disabled():
    engine = _engine(observability_enabled=False)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    assert "pushgateway_url" not in ctx


def test_job_renders_pushgateway_env_when_enabled(monkeypatch):
    monkeypatch.setenv("LB_RUN_ID", "run-abc123")
    engine = _engine(observability_enabled=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    job = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))
    env = {
        e["name"]: e.get("value") for e in job["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["LB_PUSHGATEWAY_URL"].startswith("http://lakebench-pushgateway.")
    assert env["LB_RUN_ID"] == "run-abc123"


def test_job_omits_pushgateway_env_when_disabled():
    engine = _engine(observability_enabled=False)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    job = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))
    env_names = {e["name"] for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert "LB_PUSHGATEWAY_URL" not in env_names
