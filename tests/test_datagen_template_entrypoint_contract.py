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
    # No cluster: the engine would otherwise load a kubeconfig, which exists
    # on dev machines but not in CI.
    with patch("lakebench.k8s.get_k8s_client"):
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


def test_entrypoint_memory_model_matches_autosizer():
    """The image's thread cap and lakebench's memory default use one model."""
    from lakebench.config import autosizer as a

    ep = _entrypoint()
    assert ep.PER_THREAD_FILE_MULTIPLIER == a.DATAGEN_PER_THREAD_FILE_MULTIPLIER
    assert ep.WORLD_BYTES_PER_ENTITY_NODE0 == a.DATAGEN_WORLD_BYTES_PER_ENTITY_NODE0
    assert ep.ENTITIES_PER_SCALE == a.DATAGEN_ENTITIES_PER_SCALE
    assert ep.BASE_GIB == a.DATAGEN_BASE_GIB
    assert ep.HEADROOM == a.DATAGEN_HEADROOM


@pytest.mark.parametrize("schema", ["customer360", "financial"])
@pytest.mark.parametrize("scale", [1, 10, 100])
def test_default_memory_fits_default_threads(schema, scale):
    """Regression: 8 threads at 512 MB files needed 12-26 GiB against an
    8-12 GiB limit (measured with the real binary). The autosized memory must
    fit the autosized CPU's threads, so the entrypoint never has to cap them."""
    job = _render(schema, scale)
    c = _container(job)
    ep = _entrypoint()
    cpu = int(str(c["resources"]["limits"]["cpu"]))
    mem_gib = int(str(c["resources"]["limits"]["memory"]).removesuffix("Gi"))
    args = [str(a) for a in c["args"]]
    file_mb = int(args[args.index("--file-size-mb") + 1])
    cap = ep.max_threads_for_memory(schema, float(scale), file_mb, True, mem_gib * 2**30)
    assert cap >= cpu, f"{schema} scale {scale}: {cpu} threads but memory fits {cap}"
    assert args[args.index("--workers") + 1] == "0"


def test_thread_cap_under_tight_memory():
    ep = _entrypoint()
    # 512 MB files, 8 GiB limit, c360: about 1.5 GiB per thread -> 4 threads.
    assert ep.max_threads_for_memory("customer360", 1.0, 512, True, 8 * 2**30) == 4
    # Financial scale 500 on node 0 does not fit 8 GiB at all -> floor of 1.
    assert ep.max_threads_for_memory("financial", 500.0, 64, True, 8 * 2**30) == 1


@pytest.mark.parametrize("schema", ["customer360", "financial"])
@pytest.mark.parametrize("cycle", [0, 3])
def test_cycle_is_forwarded_only_when_nonzero(schema, cycle, monkeypatch):
    """WORKPLAN B4: --cycle reaches the binary for n > 0; cycle 0 keeps the
    single-run argv unchanged."""
    ep = _entrypoint()
    monkeypatch.setenv("CPU_LIMIT", "8")
    captured: dict = {}

    def _fake_exec(_path, cmd):
        captured["cmd"] = cmd
        raise SystemExit(0)

    argv = ["entrypoint.py", "--schema", schema, "--bucket", "b"]
    if cycle:
        argv += ["--cycle", str(cycle)]
    with patch.object(sys, "argv", argv):
        with patch.object(ep.os, "execvp", _fake_exec):
            with pytest.raises(SystemExit):
                ep.main()
    cmd = captured["cmd"]
    if cycle:
        assert cmd[cmd.index("--cycle") + 1] == str(cycle)
    else:
        assert "--cycle" not in cmd


def test_negative_cycle_is_rejected(monkeypatch):
    ep = _entrypoint()
    monkeypatch.setenv("CPU_LIMIT", "8")
    with patch.object(sys, "argv", ["entrypoint.py", "--bucket", "b", "--cycle", "-1"]):
        with patch.object(ep.os, "execvp", lambda *_: pytest.fail("exec'd")):
            assert ep.main() == 2
