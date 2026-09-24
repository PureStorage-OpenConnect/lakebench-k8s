"""Contract: the args lakebench renders into the datagen Job must be accepted
by the image's entrypoint, and must not pin the generator to one thread.

Regressions found by the 2026-09-24 audit:
- The autosizer resolves `auto` to `continuous` above scale 10 and the Job
  passed `--mode continuous`, which the entrypoint rejected (exit 2), so every
  scale > 10 generate crash-looped.
- Batch mode forced generators=1, rendered as `--workers 1`, which overrode
  the pod's CPU count: one rayon thread per pod.
- The Job set S3_REGION, but the Rust S3 sink reads AWS_REGION.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]


def _entrypoint():
    spec = importlib.util.spec_from_file_location(
        "datagen_entrypoint", REPO / "datagen_rs" / "entrypoint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _render(schema: str, scale: float) -> dict:
    from lakebench.config import LakebenchConfig
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    cfg = LakebenchConfig(
        name="contract",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://s3:80",
                    "access_key": "k",
                    "secret_key": "s",
                    "region": "eu-west-9",
                }
            }
        },
        architecture={"workload": {"schema": schema, "datagen": {"scale": scale}}},
    )
    resolve_auto_sizing(cfg)
    engine = DeploymentEngine(cfg, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    return yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))


def _container(job: dict) -> dict:
    return job["spec"]["template"]["spec"]["containers"][0]


@pytest.mark.parametrize("schema", ["customer360", "financial"])
@pytest.mark.parametrize("scale", [1, 100])
def test_rendered_args_are_accepted_and_use_pod_cpu(schema, scale, monkeypatch):
    job = _render(schema, scale)
    c = _container(job)
    ep = _entrypoint()
    monkeypatch.setenv("CPU_LIMIT", str(c["resources"]["requests"]["cpu"]))
    captured: dict = {}

    def _fake_exec(_path, cmd):
        captured["cmd"] = cmd
        raise SystemExit(0)

    with patch.object(sys, "argv", ["entrypoint.py", *[str(a) for a in c["args"]]]):
        with patch.object(ep.os, "execvp", _fake_exec):
            with pytest.raises(SystemExit) as exc:
                ep.main()
    assert exc.value.code == 0, "entrypoint rejected the rendered args"
    cmd = captured["cmd"]
    threads = int(cmd[cmd.index("--threads") + 1])
    assert threads >= 4, f"generator pinned to {threads} thread(s)"


@pytest.mark.parametrize("schema", ["customer360", "financial"])
def test_datagen_job_sets_aws_region(schema):
    env = {e["name"]: e.get("value") for e in _container(_render(schema, 1))["env"]}
    assert env.get("AWS_REGION") == "eu-west-9"


def test_user_datagen_cpu_and_memory_are_honoured():
    from lakebench.config import LakebenchConfig
    from lakebench.config.autosizer import resolve_auto_sizing

    cfg = LakebenchConfig(
        name="contract",
        platform={
            "storage": {"s3": {"endpoint": "http://s3:80", "access_key": "k", "secret_key": "s"}}
        },
        architecture={"workload": {"datagen": {"scale": 10, "cpu": "16", "memory": "32Gi"}}},
    )
    resolve_auto_sizing(cfg)
    assert cfg.architecture.workload.datagen.cpu == "16"
    assert cfg.architecture.workload.datagen.memory == "32Gi"
