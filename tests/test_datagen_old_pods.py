"""A fresh generate must not start while an earlier datagen Job's pods still run.

``_delete_existing_job`` deletes the old Job with Background propagation, so
its pods get SIGTERM and keep running for their grace period. A pod that
flushes a ``part-*`` file in that window, after the cycle-0 clear, puts an
old file in the new corpus that silver reads as this run's rows, and nothing
refuses. The fake world below models one such pod: it stops ``stop_after_s``
seconds after the Job delete and writes its last file as it stops.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lakebench.deploy.datagen import DatagenDeployer
from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
from tests.conftest import make_config

PREFIX = "customer/interactions"


class _World:
    """Clock, one old datagen pod, and the bronze bucket's keys."""

    def __init__(self, stop_after_s: float | None, phase: str = "Running"):
        self.now = 1_000.0
        self.keys: set[str] = {f"{PREFIX}/part-000000-old.parquet"}
        self.pod_alive = True
        self.phase = phase
        self.stop_after_s = stop_after_s
        self.deleted_at: float | None = None
        self.applied: list[dict] = []
        self.list_calls = 0
        self.list_error: Exception | None = None

    def tick(self) -> None:
        if (
            self.pod_alive
            and self.deleted_at is not None
            and self.stop_after_s is not None
            and self.now >= self.deleted_at + self.stop_after_s
        ):
            # SIGTERM flush: the old pod's last file lands as it stops.
            self.keys.add(f"{PREFIX}/part-000007-old-flush.parquet")
            self.pod_alive = False

    def advance(self, s: float) -> None:
        self.now += s
        self.tick()


class _Time:
    def __init__(self, world: _World):
        self.w = world

    def time(self) -> float:
        return self.w.now

    def monotonic(self) -> float:
        return self.w.now

    def sleep(self, s: float) -> None:
        self.w.advance(s)


def _install(monkeypatch, world: _World, *, owned: bool = True):
    import kubernetes.client as kc

    class _Batch:
        def delete_namespaced_job(self, name, namespace, body=None, **kw):
            world.deleted_at = world.now

    class _Core:
        def list_namespaced_pod(self, namespace, label_selector="", **kw):
            world.list_calls += 1
            if world.list_error is not None:
                raise world.list_error
            assert "lakebench-datagen" in label_selector
            items = []
            if world.pod_alive:
                items.append(
                    SimpleNamespace(
                        metadata=SimpleNamespace(name="lakebench-datagen-0-abcde"),
                        status=SimpleNamespace(phase=world.phase),
                    )
                )
            return SimpleNamespace(items=items)

    class _S3:
        _init_error = None
        #: The corpus series marker the deployer writes after its clear
        #: (kept apart from the part files in ``world.keys``).
        markers: dict = {}

        def __init__(self, **kw):
            pass

        @property
        def raw_client(self):
            from tests.fixtures.memory_s3 import MemoryBoto

            return MemoryBoto(_S3.markers)

        def bucket_exists(self, bucket):
            return True

        def has_user_objects(self, bucket, prefix=""):
            return any(k.startswith(prefix) for k in world.keys)

        def delete_prefix(self, bucket, prefix, *, abort_multipart=False):
            gone = {k for k in world.keys if k.startswith(prefix.rstrip("/") + "/")}
            world.keys -= gone
            return len(gone)

    monkeypatch.setattr(kc, "BatchV1Api", _Batch)
    monkeypatch.setattr(kc, "CoreV1Api", _Core)
    monkeypatch.setattr("lakebench.s3.S3Client", _S3)
    monkeypatch.setattr("lakebench.deploy.datagen.time", _Time(world))
    monkeypatch.setattr("lakebench.deploy.datagen.deployment_may_empty", lambda *a, **k: owned)
    monkeypatch.setattr(
        DatagenDeployer,
        "_build_datagen_context",
        lambda self: {
            "datagen_path_prefix": PREFIX,
            "datagen_parallelism": 1,
            "datagen_target_tb": "0.001",
        },
    )


def _deployer(world: _World, **kw) -> DatagenDeployer:
    cfg = make_config(architecture={"workload": {"datagen": {"seed": 43}}})
    d = DatagenDeployer(DeploymentEngine(cfg, dry_run=False), **kw)

    class _K8s:
        def apply_manifest(self, manifest, namespace=None):
            world.applied.append(manifest)

    class _Renderer:
        def render(self, name, context):
            return "kind: Job\nmetadata:\n  name: lakebench-datagen\n"

    d.k8s = _K8s()
    d.renderer = _Renderer()
    return d


def _old_files(world: _World) -> set[str]:
    return {k for k in world.keys if "-old" in k}


@pytest.mark.parametrize("cycle", [None, 0])
def test_terminating_pod_cannot_write_into_the_cleared_prefix(monkeypatch, cycle):
    """Owned bucket, an old pod that stops 20 s after the Job delete."""
    world = _World(stop_after_s=20)
    _install(monkeypatch, world)
    d = _deployer(world)
    r = d.deploy() if cycle is None else d.deploy_cycle(0, 2)
    assert r.status is DeploymentStatus.SUCCESS, r.message
    world.advance(60)  # the world goes on after deploy() returns
    assert not world.pod_alive
    assert len(world.applied) == 1
    # The new Job was applied only after the old pod stopped, and the clear
    # ran after its last write, so no old file is left in the new corpus.
    assert _old_files(world) == set()


def test_new_job_waits_for_the_old_pod_then_clears(monkeypatch):
    world = _World(stop_after_s=20)
    _install(monkeypatch, world)
    d = _deployer(world)
    assert d.deploy().status is DeploymentStatus.SUCCESS
    # Cleared after the old pod's last write, and before the new Job.
    assert not world.pod_alive and world.keys == set()
    assert world.now >= world.deleted_at + 20


@pytest.mark.parametrize("phase", ["Succeeded", "Failed"])
def test_finished_pods_do_not_hold_up_a_generate(monkeypatch, phase):
    """A pod whose containers exited writes nothing; only the delete's GC is pending."""
    world = _World(stop_after_s=None, phase=phase)
    _install(monkeypatch, world)
    d = _deployer(world)
    start = world.now
    assert d.deploy().status is DeploymentStatus.SUCCESS
    assert world.now - start < 10  # the 2 s delete settle, no pod wait


def test_a_pod_that_never_stops_refuses_without_clearing(monkeypatch):
    from lakebench.exit_codes import REFUSAL_DETAIL

    world = _World(stop_after_s=None)  # stuck Terminating
    _install(monkeypatch, world)
    d = _deployer(world)
    r = d.deploy()
    assert r.status is DeploymentStatus.FAILED
    assert r.details == {REFUSAL_DETAIL: "datagen.pods_live"}
    assert "lakebench-datagen-0-abcde" in r.message
    assert "still running 300s after the Job was deleted" in r.message
    ns = d.config.get_namespace()
    assert f"kubectl get pods -n {ns} -l app=lakebench-datagen" in r.message
    assert world.applied == []  # no new Job
    assert world.keys == {f"{PREFIX}/part-000000-old.parquet"}  # nothing cleared
    r = d.deploy_cycle(0, 3)
    assert r.details == {REFUSAL_DETAIL: "datagen.pods_live"} and world.applied == []


def test_the_hint_names_the_config_context(monkeypatch):
    world = _World(stop_after_s=None)
    _install(monkeypatch, world)
    d = _deployer(world)
    d.config.platform.kubernetes.context = "lab-ocp"
    r = d.deploy()
    assert "kubectl get pods --context lab-ocp -n " in r.message


def test_pods_that_cannot_be_listed_fail_closed(monkeypatch):
    world = _World(stop_after_s=None)
    world.list_error = RuntimeError("apiserver timeout")
    _install(monkeypatch, world)
    d = _deployer(world)
    r = d.deploy()
    assert r.status is DeploymentStatus.FAILED
    assert r.details == {}  # could not check: an ordinary failure (exit 1), not a refusal
    assert "could not list the datagen pods" in r.message and "apiserver timeout" in r.message
    assert world.applied == [] and world.list_calls > 1  # retried until the deadline


def test_a_listing_error_is_retried(monkeypatch):
    world = _World(stop_after_s=None)
    world.pod_alive = False
    world.list_error = RuntimeError("blip")
    _install(monkeypatch, world)
    calls = {"n": 0}
    import kubernetes.client as kc

    real = kc.CoreV1Api

    class _Flaky(real):  # type: ignore[misc,valid-type]
        def list_namespaced_pod(self, *a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("blip")
            world.list_error = None
            return super().list_namespaced_pod(*a, **k)

    monkeypatch.setattr(kc, "CoreV1Api", _Flaky)
    d = _deployer(world)
    assert d.deploy().status is DeploymentStatus.SUCCESS
    assert calls["n"] == 2


def test_stale_bronze_refusal_carries_its_exit_path(monkeypatch):
    from lakebench.exit_codes import REFUSAL_DETAIL

    world = _World(stop_after_s=None)
    world.pod_alive = False
    _install(monkeypatch, world, owned=False)
    d = _deployer(world)
    r = d.deploy()
    assert r.status is DeploymentStatus.FAILED
    assert r.details == {REFUSAL_DETAIL: "run.bronze_nonempty"}
    assert world.applied == []


# --- CLI order: the earlier Job's pods stop before the bronze gate looks -----


_RUN = ["--skip-preflight", "--skip-benchmark", "--skip-maintenance", "--yes"]


def _cli_order(monkeypatch, tmp_path, command, argv, *, gate_module, cycles=1):
    """Run the CLI until the bronze gate; return the stop/gate order."""
    import typer
    from typer.testing import CliRunner

    from lakebench.cli import app
    from tests import test_datagen_timeout_and_regenerate as dg

    monkeypatch.chdir(tmp_path)
    (dg._stub_run_deps if command == "generate" else dg._stub_full_run)(monkeypatch)
    monkeypatch.setattr("lakebench.s3.S3Client", dg._FakeS3)
    order: list[str] = []
    monkeypatch.setattr(
        "lakebench.deploy.datagen.stop_previous_datagen", lambda c: order.append("stop")
    )

    def gate(*_a, **_k):
        order.append("gate")
        raise typer.Exit(9)

    monkeypatch.setattr(f"lakebench.cli.{gate_module}.enforce_bronze_gate", gate)
    extras = {"architecture": f"{{pipeline: {{cycles: {cycles}}}}}"} if cycles > 1 else {}
    cfg = dg._write_cfg(tmp_path, **extras)
    res = CliRunner().invoke(app, [command, str(cfg), *argv])
    assert res.exit_code == 9, res.output[-2000:]
    return order


def test_generate_stops_old_pods_before_the_gate(monkeypatch, tmp_path):
    order = _cli_order(monkeypatch, tmp_path, "generate", ["--yes"], gate_module="_generate")
    assert order == ["stop", "gate"]


def test_run_generate_stops_old_pods_before_the_gate(monkeypatch, tmp_path):
    order = _cli_order(monkeypatch, tmp_path, "run", ["--generate", *_RUN], gate_module="_run")
    assert order == ["stop", "gate"]


def test_multi_cycle_run_stops_old_pods_before_the_gate(monkeypatch, tmp_path):
    order = _cli_order(monkeypatch, tmp_path, "run", _RUN, gate_module="_run", cycles=2)
    assert order == ["stop", "gate"]


def test_generate_refusal_from_live_pods_exits_3_before_the_gate(monkeypatch, tmp_path):
    """The stop refuses before the gate lists or clears anything."""
    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.deploy.datagen import DatagenPodsStillRunning
    from tests import test_datagen_timeout_and_regenerate as dg

    monkeypatch.chdir(tmp_path)
    dg._stub_run_deps(monkeypatch)

    def stop(c):
        raise DatagenPodsStillRunning("datagen pod(s) x are still running")

    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", stop)
    gate_calls: list[int] = []
    monkeypatch.setattr(
        "lakebench.cli._generate.enforce_bronze_gate", lambda *a, **k: gate_calls.append(1)
    )
    res = CliRunner().invoke(app, ["generate", str(dg._write_cfg(tmp_path)), "--yes"])
    assert res.exit_code == 3, res.output
    assert gate_calls == []
    assert "still running" in res.output


@pytest.mark.parametrize("command", ["generate", "run"])
def test_deployer_takes_the_gates_decision_not_the_flag(monkeypatch, tmp_path, command):
    """--allow-stale-bronze on a prefix the gate found empty: the deployer is
    built without it, so objects that appear after the gate are refused
    rather than written over with no stale-bronze record."""
    from types import SimpleNamespace as NS

    from typer.testing import CliRunner

    from lakebench.cli import app
    from tests import test_datagen_timeout_and_regenerate as dg

    monkeypatch.chdir(tmp_path)
    (dg._stub_run_deps if command == "generate" else dg._stub_full_run)(monkeypatch)
    monkeypatch.setattr("lakebench.s3.S3Client", dg._FakeS3)
    gate_module = "_generate" if command == "generate" else "_run"
    monkeypatch.setattr(
        f"lakebench.cli.{gate_module}.enforce_bronze_gate",
        lambda *a, **k: NS(stale_allowed=False, record=lambda: None),
    )
    built: list[bool] = []

    class _Refusing:
        def __init__(self, engine, allow_stale_bronze=False, **kw):
            built.append(allow_stale_bronze)

        def deploy(self):
            from lakebench.deploy.engine import DeploymentResult
            from lakebench.exit_codes import REFUSAL_DETAIL

            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message="holds objects",
                details={REFUSAL_DETAIL: "run.bronze_nonempty"},
            )

    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", _Refusing)
    argv = ["--yes"] if command == "generate" else ["--generate", *_RUN]
    cfg = dg._write_cfg(tmp_path)
    res = CliRunner().invoke(app, [command, str(cfg), *argv, "--allow-stale-bronze"])
    assert built == [False]
    assert res.exit_code == 3, res.output[-2000:]


def test_generate_with_an_unreachable_cluster_still_exits_4(monkeypatch, tmp_path):
    """Pods that cannot be listed before anything ran: the prerequisite exit
    an unreachable cluster had before the stop existed (k8s.unreachable)."""
    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.deploy.datagen import DatagenPodsUnknown
    from tests import test_datagen_timeout_and_regenerate as dg

    monkeypatch.chdir(tmp_path)
    dg._stub_run_deps(monkeypatch)

    def stop(c):
        raise DatagenPodsUnknown("could not delete the earlier lakebench-datagen Job")

    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", stop)
    res = CliRunner().invoke(app, ["generate", str(dg._write_cfg(tmp_path)), "--yes"])
    assert res.exit_code == 4, res.output


@pytest.mark.parametrize("gate_allowed", [False, True])
def test_multi_cycle_deployer_takes_the_gates_decision_not_the_flag(
    monkeypatch, tmp_path, gate_allowed
):
    """--allow-stale-bronze on a multi-cycle run whose cycle-0 gate found the
    prefix empty: the cycle deployers are built without it, so objects that
    land after the gate are refused (exit 3) rather than written over with no
    stale-bronze record. --generate is refused on a multi-cycle run, so this
    gate is the only one before the cycles."""
    from types import SimpleNamespace as NS

    from typer.testing import CliRunner

    from lakebench.cli import app
    from tests import test_datagen_timeout_and_regenerate as dg

    monkeypatch.chdir(tmp_path)
    dg._stub_full_run(monkeypatch)
    monkeypatch.setattr("lakebench.s3.S3Client", dg._FakeS3)
    monkeypatch.setattr(
        "lakebench.cli._run.enforce_bronze_gate",
        lambda *a, **k: NS(stale_allowed=gate_allowed, record=lambda: None),
    )
    built: list[bool] = []

    class _Refusing:
        def __init__(self, engine, allow_stale_bronze=False, **kw):
            built.append(allow_stale_bronze)

        def deploy_cycle(self, cycle_index, total_cycles):
            from lakebench.deploy.engine import DeploymentResult
            from lakebench.exit_codes import REFUSAL_DETAIL

            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message="holds objects",
                details={REFUSAL_DETAIL: "run.bronze_nonempty"},
            )

    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", _Refusing)
    cfg = dg._write_cfg(tmp_path, architecture="{pipeline: {cycles: 2}}")
    res = CliRunner().invoke(app, ["run", str(cfg), *_RUN, "--allow-stale-bronze"])
    assert built == [gate_allowed], res.output[-2000:]  # what the gate allowed reaches cycle 0
    assert res.exit_code == 3, res.output[-2000:]
