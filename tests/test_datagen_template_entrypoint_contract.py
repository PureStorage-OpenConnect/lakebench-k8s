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
import math
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


@pytest.mark.parametrize("delivery", ["batch", "continuous", "auto"])
def test_delivery_mode_always_reaches_the_generator(delivery):
    """LB-196: the Rust default is continuous, so batch must be forwarded
    explicitly or every batch request silently runs continuous."""
    ep = _entrypoint()
    captured: dict = {}

    def _fake_exec(_path, cmd):
        captured["cmd"] = cmd
        raise SystemExit(0)

    argv = [
        "entrypoint.py",
        "--schema",
        "financial",
        "--scale",
        "1",
        "--seed",
        "777011",
        "--bucket",
        "b",
        "--delivery-mode",
        delivery,
    ]
    with patch.object(sys, "argv", argv), patch.object(ep.os, "execvp", _fake_exec):
        with pytest.raises(SystemExit):
            ep.main()
    cmd = captured["cmd"]
    expected = "continuous" if delivery == "auto" else delivery
    assert cmd[cmd.index("--delivery-mode") + 1] == expected


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
    assert ep.BASE_GIB == a.DATAGEN_BASE_GIB
    assert ep.GIB_PER_SCALE == a.DATAGEN_GIB_PER_SCALE
    assert ep.GIB_PER_EXTRA_THREAD == a.DATAGEN_GIB_PER_EXTRA_THREAD
    assert ep.BASE_THREADS == a.DATAGEN_BASE_THREADS
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
    assert args[args.index("--file-size-mb") + 1] == "64"
    cap = ep.max_threads_for_memory(schema, float(scale), mem_gib * 2**30)
    assert cap >= cpu, f"{schema} scale {scale}: {cpu} threads but memory fits {cap}"
    assert args[args.index("--workers") + 1] == "0"


@pytest.mark.parametrize("schema", ["customer360", "financial"])
@pytest.mark.parametrize("scale", [1, 10, 100])
@pytest.mark.parametrize("cpu", [2, 4, 8, 16])
def test_request_and_cap_agree_off_the_default_grid(schema, scale, cpu):
    """LB-199 review F4: the autosizer request and the entrypoint cap use one
    model, so for any (schema, scale, cpu) the request must admit the cpu
    threads. Catches a worker-term added to the cap but omitted from the request
    (the mismatch that was invisible at scale 1/10 with the old grid)."""
    from lakebench.config import autosizer as a

    ep = _entrypoint()
    threads = cpu
    req = math.ceil(a.datagen_memory_gib(schema, float(scale), threads))
    cap = ep.max_threads_for_memory(schema, float(scale), req * 2**30)
    assert cap >= threads, f"{schema} s{scale} cpu{cpu}: request {req}Gi caps to {cap} < {threads}"


def test_thread_cap_under_tight_memory():
    ep = _entrypoint()
    # Below the 8-thread peak: one thread fewer per per-thread cost short.
    # s300 peak 5.35 + 0.0087*300 = 7.96 GiB; 7 GiB / 1.25 = 5.6 -> 2.36 short -> 5 fewer.
    assert ep.max_threads_for_memory("financial", 300.0, 7 * 2**30) == 3
    # Far below: never under 1.
    assert ep.max_threads_for_memory("financial", 300.0, 1 * 2**30) == 1
    # Spare memory above the 8-thread peak buys extra threads.
    peak = (ep.BASE_GIB["financial"] + ep.GIB_PER_SCALE["financial"] * 100) * ep.HEADROOM
    extra = ep.GIB_PER_EXTRA_THREAD["financial"] * ep.HEADROOM
    assert (
        ep.max_threads_for_memory("financial", 100.0, int((peak + 2 * extra + 0.01) * 2**30)) == 10
    )


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

    argv = ["entrypoint.py", "--schema", schema, "--bucket", "b", "--seed", "7777"]
    if cycle:
        argv += ["--cycle", str(cycle), "--cycles", "5"]
    with patch.object(sys, "argv", argv):
        with patch.object(ep.os, "execvp", _fake_exec):
            with pytest.raises(SystemExit):
                ep.main()
    cmd = captured["cmd"]
    if cycle:
        assert cmd[cmd.index("--cycle") + 1] == str(cycle)
        assert cmd[cmd.index("--cycles") + 1] == "5"
    else:
        assert "--cycle" not in cmd and "--cycles" not in cmd


def test_negative_cycle_is_rejected(monkeypatch):
    ep = _entrypoint()
    monkeypatch.setenv("CPU_LIMIT", "8")
    for bad in (["--cycle", "-1"], ["--cycle", "3", "--cycles", "3"], ["--cycles", "0"]):
        with patch.object(sys, "argv", ["entrypoint.py", "--bucket", "b", *bad]):
            with patch.object(ep.os, "execvp", lambda *_: pytest.fail("exec'd")):
                assert ep.main() == 2
