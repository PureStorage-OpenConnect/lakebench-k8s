"""A4 (v1.6): datagen timeout and --regenerate.

Covers three related fixes on the datagen run path.

- Batch ``run --generate`` no longer prints "Datagen completed" when the
  wait loop's timeout expired. The run exits 1 (CLI-1; 5 in 1.6) and the
  record's ``verdict.reasons`` says "datagen timed out", the datagen Job is
  deleted, and every
  leftover streaming SparkApplication (``bronze-ingest``, ``silver-stream``,
  ``gold-refresh``) is deleted so a timed-out generate does not leave
  orphan compute behind.
- ``lakebench generate`` and ``lakebench run --generate`` refuse a
  non-empty bronze prefix (exit 3, refused; 2 in 1.6) unless ``--regenerate`` is passed;
  with the flag, on a bucket this deployment may empty, the datagen prefix
  is cleared (``S3Client.delete_prefix``) before datagen submits. SAF-9
  (v1.7) replaced the whole-bucket empty; tests/test_bronze_gate.py covers
  the unowned rows.
- The deployer's cycle path (``deploy_cycle``) does not call the CLI gate;
  the multi-cycle loop in ``run`` does, before cycle 0.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import typer

from lakebench.cli._helpers import enforce_bronze_gate
from lakebench.cli._run import DATAGEN_TIMED_OUT, _handle_datagen_timeout
from lakebench.cli._sustained import _STREAM_APPS
from lakebench.exit_codes import ExitCode
from lakebench.s3.client import BucketInfo
from tests.conftest import make_config

# --- Helpers ----------------------------------------------------------------


class _FakeS3:
    """Records get_bucket_size / empty_bucket calls and returns canned data.

    Configured per-instance so a test can inject its own scenario without
    reaching into boto3.
    """

    _next_info: BucketInfo | None = None
    _next_init_error: str | None = None
    instances: list[_FakeS3] = []  # noqa: F821 -- forward ref via type hints below

    def __init__(self, **_kw: object) -> None:
        self.kw = _kw
        self._init_error = _FakeS3._next_init_error
        self.empty_calls: list[str] = []
        self.prefix_calls: list[tuple[str, str]] = []
        self.get_calls: list[tuple[str, str]] = []
        _FakeS3.instances.append(self)

    def _info(self, bucket: str) -> BucketInfo:
        info = _FakeS3._next_info
        if info is None:
            return BucketInfo(name=bucket, exists=True, object_count=0, size_bytes=0)
        return info

    def get_bucket_size(self, bucket: str, prefix: str = "") -> BucketInfo:
        self.get_calls.append((bucket, prefix))
        return self._info(bucket)

    def bucket_exists(self, bucket: str) -> bool:
        return self._info(bucket).exists

    def has_user_objects(self, bucket: str, prefix: str = "") -> bool:
        self.get_calls.append((bucket, prefix))
        return bool(self._info(bucket).object_count)

    def empty_bucket(self, bucket: str, **_kw: object) -> int:
        self.empty_calls.append(bucket)
        return 42

    def delete_prefix(self, bucket: str, prefix: str, **_kw: object) -> int:
        # The gate clears only the datagen prefix (SAF-9); recorded as an empty.
        self.empty_calls.append(bucket)
        self.prefix_calls.append((bucket, prefix))
        return 42


@pytest.fixture(autouse=True)
def _owned_bronze(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests are about the owned-bucket rows; tests/test_bronze_gate.py
    covers ownership itself."""
    monkeypatch.setattr("lakebench.deploy.datagen.deployment_may_empty", lambda *a, **k: True)


@pytest.fixture(autouse=True)
def _reset_fake_s3() -> None:
    """Isolate _FakeS3 state between tests."""
    _FakeS3.instances = []
    _FakeS3._next_info = None
    _FakeS3._next_init_error = None
    yield
    _FakeS3.instances = []
    _FakeS3._next_info = None
    _FakeS3._next_init_error = None


def _cfg(schema: str = "customer360"):
    return make_config(
        architecture={"workload": {"schema": schema, "datagen": {"seed": 43}}},
    )


# --- _handle_datagen_timeout ------------------------------------------------


class TestHandleDatagenTimeout:
    """The A4 timeout handler: fail with a distinct exit code, stop the
    datagen Job, delete leftover SparkApplications."""

    def test_fails_the_run(self) -> None:
        deployer = MagicMock()
        job_manager = MagicMock()
        with pytest.raises(typer.Exit) as exc_info:
            _handle_datagen_timeout(
                datagen_deployer=deployer,
                job_manager=job_manager,
                namespace="ns-a4",
                timeout_s=1200,
                elapsed_s=1201.0,
            )
        # CLI-1: 1 like any failed run; the record keeps the reason.
        assert exc_info.value.exit_code == ExitCode.FAILED

    def test_stops_datagen_job_and_orphan_sparkapps(self) -> None:
        deployer = MagicMock()
        job_manager = MagicMock()
        with pytest.raises(typer.Exit):
            _handle_datagen_timeout(
                datagen_deployer=deployer,
                job_manager=job_manager,
                namespace="ns-a4",
                timeout_s=1200,
                elapsed_s=1201.0,
            )
        # Cleanup calls pass a request_timeout so a hung K8s API cannot
        # block the exit indefinitely.
        deployer._delete_existing_job.assert_called_once_with("ns-a4", request_timeout=15)
        # Every stream app that could be consuming the trickle is deleted,
        # each with the same request_timeout cap.
        deleted = [call.args[0] for call in job_manager._delete_job.call_args_list]
        assert set(deleted) == set(_STREAM_APPS)
        for call in job_manager._delete_job.call_args_list:
            assert call.kwargs.get("request_timeout") == 15

    def test_cleanup_errors_do_not_prevent_exit(self) -> None:
        # Best-effort cleanup: a failure deleting the Job or a SparkApplication
        # must not swallow the exit -- the run still fails with the timeout
        # code, and the operator sees the warning in the logs.
        deployer = MagicMock()
        deployer._delete_existing_job.side_effect = RuntimeError("API down")
        job_manager = MagicMock()
        job_manager._delete_job.side_effect = RuntimeError("API down")
        with pytest.raises(typer.Exit) as exc_info:
            _handle_datagen_timeout(
                datagen_deployer=deployer,
                job_manager=job_manager,
                namespace="ns-a4",
                timeout_s=60,
                elapsed_s=61.0,
            )
        assert exc_info.value.exit_code == ExitCode.FAILED
        # Delete of each stream app was still attempted.
        assert job_manager._delete_job.call_count == len(_STREAM_APPS)


# --- enforce_bronze_gate ----------------------------------------------------


class TestEnforceBronzeRegenerate:
    """The A4 CLI-level gate on ``run --generate`` and ``generate``."""

    def test_empty_bronze_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=0, size_bytes=0)
        enforce_bronze_gate(_cfg(), regenerate=False)
        # Nothing was emptied.
        assert _FakeS3.instances[0].empty_calls == []

    def test_missing_bucket_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Fresh deploy: bronze does not exist yet, no refusal.
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=False)
        enforce_bronze_gate(_cfg(), regenerate=False)
        assert _FakeS3.instances[0].empty_calls == []

    def test_non_empty_without_regenerate_refuses_with_exit_3(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(
            name="b", exists=True, object_count=17, size_bytes=42 * (1024**3)
        )
        with pytest.raises(typer.Exit) as exc_info:
            enforce_bronze_gate(_cfg(), regenerate=False)
        assert exc_info.value.exit_code == ExitCode.REFUSED  # run.bronze_nonempty
        # empty_bucket must NOT be called on a refusal.
        assert _FakeS3.instances[0].empty_calls == []

    def test_non_empty_with_regenerate_clears_the_datagen_prefix(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(
            name="b", exists=True, object_count=17, size_bytes=42 * (1024**3)
        )
        cfg = _cfg()
        enforce_bronze_gate(cfg, regenerate=True)
        fake = _FakeS3.instances[0]
        bronze = cfg.platform.storage.s3.buckets.bronze
        assert fake.prefix_calls == [(bronze, "customer/interactions")]

    def test_financial_prefix_checked(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # AML on the C360 default template writes under ``pacs008`` -- the gate
        # must check the same prefix as the deployer writes to.
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=0, size_bytes=0)
        enforce_bronze_gate(_cfg(schema="financial"), regenerate=False)
        fake = _FakeS3.instances[0]
        assert fake.get_calls[0][1].startswith("pacs008")

    def test_s3_init_error_refuses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Cannot check safely: refuse rather than proceed and (under
        # --regenerate) wipe something we cannot even list.
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_init_error = "endpoint unreachable"
        with pytest.raises(typer.Exit) as exc_info:
            enforce_bronze_gate(_cfg(), regenerate=True)
        assert exc_info.value.exit_code == ExitCode.PREREQUISITE  # S3 cannot be read


# --- CLI: `generate --regenerate` -------------------------------------------


def _write_cfg(tmp_path, **extras: str) -> object:
    """Write a minimal Lakebench yaml the CLI can load."""
    body = (
        "name: a4-datagen\n"
        "platform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
        "      access_key: x\n      secret_key: y\n"
    )
    for k, v in extras.items():
        body += f"{k}: {v}\n"
    p = tmp_path / "c.yaml"
    p.write_text(body)
    return p


def _stub_run_deps(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    """Stub everything the CLI touches upstream of the datagen block."""
    from lakebench.k8s import ClusterCapacity

    k8s_stub = MagicMock()
    k8s_stub.get_cluster_capacity.return_value = ClusterCapacity(
        434_000, 8 * 432 * 1024**3, 8, 54_000, 432 * 1024**3
    )
    monkeypatch.setattr("lakebench.cli._generate.get_k8s_client", lambda **kw: k8s_stub)
    return {"k8s": k8s_stub}


class TestGenerateStandaloneRegenerate:
    """`lakebench generate --regenerate` (and the refusal without it)."""

    def test_refuses_non_empty_bronze_without_flag(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typer.testing import CliRunner

        from lakebench.cli import app

        _stub_run_deps(monkeypatch)
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=5, size_bytes=1_000_000)
        # DeploymentEngine must NOT be reached; the gate exits first.
        deploy_engine_seen = {"called": False}

        def _boom(*_a, **_kw):
            deploy_engine_seen["called"] = True
            raise AssertionError("DeploymentEngine should not be constructed after refusal")

        monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _boom)
        cfg_file = _write_cfg(tmp_path)
        res = CliRunner().invoke(app, ["generate", str(cfg_file), "--yes"])
        assert res.exit_code == ExitCode.REFUSED, res.output
        assert "refusing to generate over it" in res.output.lower()
        assert "--regenerate" in res.output
        assert deploy_engine_seen["called"] is False

    def test_with_regenerate_empties_then_refills(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typer.testing import CliRunner

        from lakebench.cli import app

        _stub_run_deps(monkeypatch)
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=5, size_bytes=1_000_000)
        # After empty_bucket, DeploymentEngine is constructed and we stop
        # there via SystemExit(7) so we can assert both the empty happened
        # and the deploy path was reached in that order.
        monkeypatch.setattr(
            "lakebench.deploy.DeploymentEngine", MagicMock(side_effect=SystemExit(7))
        )
        cfg_file = _write_cfg(tmp_path)
        res = CliRunner().invoke(app, ["generate", str(cfg_file), "--yes", "--regenerate"])
        # SystemExit(7) surfaces as the CLI exit code.
        assert res.exit_code == 7, (res.output, repr(res.exception))
        # empty_bucket was called before the deploy path.
        assert _FakeS3.instances[0].empty_calls == [_FakeS3.instances[0].get_calls[0][0]]


# --- CLI: `run --generate [--regenerate]` -----------------------------------


def _stub_full_run(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    """Stub the run command up to the datagen block.

    Includes: prerequisites, infra readiness, K8s client, Spark Operator
    manager, Spark job manager (SparkJobMonitor), and the DeploymentEngine
    used to build the DatagenDeployer.
    """
    from lakebench.k8s import ClusterCapacity

    # Cluster capacity read from ``_run_prerequisites`` and inside the run.
    k8s_stub = MagicMock()
    k8s_stub.namespace_exists.return_value = True
    k8s_stub.get_cluster_capacity.return_value = ClusterCapacity(
        434_000, 8 * 432 * 1024**3, 8, 54_000, 432 * 1024**3
    )
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: k8s_stub)

    # Prerequisites: everything passes so the datagen block is reached.
    class _PassingReport:
        checks: list = []
        all_passed = True

    monkeypatch.setattr(
        "lakebench.cli._prerequisites.run_prerequisites",
        lambda cfg, **kw: _PassingReport(),
    )

    # Spark Operator manager: ready and watches the namespace.
    op = MagicMock()
    op.check_status.return_value = MagicMock(
        ready=True, installed=True, version="2.5.1", message="ok"
    )
    op.ensure_namespace_watched.return_value = MagicMock(watching_namespace=True, message="ok")
    monkeypatch.setattr("lakebench.spark.SparkOperatorManager", lambda **kw: op)

    # Job manager (SparkJobManager) via get_engine: deploy_scripts_configmap
    # succeeds; _delete_job records calls (used by the timeout handler).
    job_manager = MagicMock()
    job_manager.deploy_scripts_configmap.return_value = True
    monkeypatch.setattr("lakebench.engine.get_engine", lambda cfg, k8s: job_manager)

    # Monitor is not exercised in the datagen block; give it something.
    monkeypatch.setattr("lakebench.spark.SparkJobMonitor", lambda *a, **kw: MagicMock())

    # DeploymentEngine is only used to build the DatagenDeployer here.
    monkeypatch.setattr(
        "lakebench.deploy.DeploymentEngine", lambda cfg, **kw: MagicMock(config=cfg)
    )
    return {"k8s": k8s_stub, "op": op, "job_manager": job_manager}


class _FakeDatagenDeployer:
    """Records _delete_existing_job / deploy / get_progress; stays running."""

    def __init__(self, engine: object, **_kw: object) -> None:
        self.engine = engine
        self.deploy_calls = 0
        self.delete_calls: list[tuple[str, int | None]] = []
        self.progress_polls = 0

    def deploy(self):
        self.deploy_calls += 1
        return MagicMock(
            status=MagicMock(value="success"),
            details={"parallelism": 4, "target_tb": 0.01},
        )

    def get_progress(self):
        self.progress_polls += 1
        # Always running: never finishes so the wait loop must time out.
        return {"running": True, "succeeded": 0, "completions": 4}

    def _delete_existing_job(self, namespace: str, *, request_timeout: int | None = None) -> None:
        self.delete_calls.append((namespace, request_timeout))


class TestRunGenerateTimeout:
    """`run --generate` exits with the timeout code and cleans up orphans."""

    def test_timeout_fails_run_and_kills_orphan_sparkapp(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typer.testing import CliRunner

        from lakebench.cli import app

        monkeypatch.chdir(tmp_path)  # the run record lands under tmp_path
        stubs = _stub_full_run(monkeypatch)
        # Bronze is empty so the --regenerate gate does not fire.
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=0, size_bytes=0)

        deployers: list[_FakeDatagenDeployer] = []

        def _make_deployer(engine, **_kw):
            d = _FakeDatagenDeployer(engine)
            deployers.append(d)
            return d

        monkeypatch.setattr("lakebench.deploy.DatagenDeployer", _make_deployer)

        # A fake monotonic clock so the wait loop's timeout expires without
        # sleeping in real time.
        import time as _time

        clock = {"t": 1_000.0}

        def _now() -> float:
            return clock["t"]

        def _sleep(seconds: float) -> None:
            clock["t"] += max(float(seconds), 0.001)

        monkeypatch.setattr(_time, "time", _now)
        monkeypatch.setattr(_time, "sleep", _sleep)

        cfg_file = _write_cfg(tmp_path)
        res = CliRunner().invoke(
            app,
            [
                "run",
                str(cfg_file),
                "--generate",
                "--skip-preflight",
                "--skip-benchmark",
                "--skip-maintenance",
                "--timeout",
                "60",
                "--yes",
            ],
        )
        assert res.exit_code == ExitCode.FAILED, (
            res.output[-2000:],
            repr(res.exception),
        )
        # The record keeps the timeout distinct (CLI-1).
        import json

        records = sorted(tmp_path.glob("lakebench-output/runs/run-*/metrics.json"))
        assert len(records) == 1, records
        verdict = json.loads(records[0].read_text())["verdict"]
        assert verdict["status"] == "FAILED"
        assert DATAGEN_TIMED_OUT in verdict["reasons"]
        # A re-rendered report recomputes the verdict from the loaded record.
        from lakebench.metrics import MetricsStorage
        from lakebench.metrics.verdict import compute_verdict

        run_id = json.loads(records[0].read_text())["run_id"]
        loaded = MetricsStorage().load_run(run_id)
        assert loaded is not None and loaded.failure_reasons == [DATAGEN_TIMED_OUT]
        assert DATAGEN_TIMED_OUT in compute_verdict(loaded).reasons
        assert "Datagen completed" not in res.output
        assert "wait budget" in res.output.lower() or "timed out" in res.output.lower()
        # datagen Job was stopped and every stream app was deleted.
        assert deployers and deployers[0].delete_calls
        deleted = [call.args[0] for call in stubs["job_manager"]._delete_job.call_args_list]
        assert set(deleted) == set(_STREAM_APPS)


class TestRunGenerateRegenerate:
    """`run --generate` refuses non-empty bronze without --regenerate."""

    def test_refuses_non_empty_bronze_without_flag(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typer.testing import CliRunner

        from lakebench.cli import app

        _stub_full_run(monkeypatch)
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=9, size_bytes=1024)

        # DatagenDeployer construction must not be reached.
        seen = {"deploy": False}

        def _boom(engine, **_kw):
            seen["deploy"] = True
            raise AssertionError("DatagenDeployer should not be constructed after refusal")

        monkeypatch.setattr("lakebench.deploy.DatagenDeployer", _boom)
        cfg_file = _write_cfg(tmp_path)
        res = CliRunner().invoke(
            app,
            [
                "run",
                str(cfg_file),
                "--generate",
                "--skip-preflight",
                "--skip-benchmark",
                "--skip-maintenance",
                "--yes",
            ],
        )
        assert res.exit_code == ExitCode.REFUSED, (res.output[-2000:], repr(res.exception))
        assert "--regenerate" in res.output
        assert seen["deploy"] is False

    def test_regenerate_empties_before_datagen(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typer.testing import CliRunner

        from lakebench.cli import app

        _stub_full_run(monkeypatch)
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=9, size_bytes=1024)

        # Stop after DatagenDeployer.deploy() so we can assert ordering: the
        # empty happened first, then deploy() was called.
        call_order: list[str] = []
        orig_empty = _FakeS3.delete_prefix

        def _empty_wrap(self, bucket, prefix, **kw):
            call_order.append("empty")
            return orig_empty(self, bucket, prefix, **kw)

        monkeypatch.setattr(_FakeS3, "delete_prefix", _empty_wrap)

        class _StopAfterDeploy(_FakeDatagenDeployer):
            def deploy(self):
                call_order.append("deploy")
                raise SystemExit(9)

        monkeypatch.setattr("lakebench.deploy.DatagenDeployer", _StopAfterDeploy)
        cfg_file = _write_cfg(tmp_path)
        res = CliRunner().invoke(
            app,
            [
                "run",
                str(cfg_file),
                "--generate",
                "--regenerate",
                "--skip-preflight",
                "--skip-benchmark",
                "--skip-maintenance",
                "--yes",
            ],
        )
        assert res.exit_code == 9, (res.output[-2000:], repr(res.exception))
        assert call_order == ["empty", "deploy"], call_order


# --- Multi-cycle is not touched --------------------------------------------


class TestMultiCycleNotTouched:
    """The deployer's cycle path (``deploy_cycle``) never calls the CLI gate
    itself; ``run``'s multi-cycle loop calls it once, before cycle 0."""

    def test_deploy_cycle_does_not_call_enforce(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # If the multi-cycle path were routed through the gate, this
        # sentinel would fire. It must not.
        called = {"enforce": False}

        def _boom(*_a, **_kw):
            called["enforce"] = True
            raise AssertionError("deploy_cycle must not call enforce_bronze_gate")

        monkeypatch.setattr("lakebench.cli._helpers.enforce_bronze_gate", _boom)
        # The deployer's cycle path only touches the K8s / S3 / template
        # layers, all mocked here; we just prove it does not import or
        # invoke the CLI gate.
        from lakebench.deploy.datagen import DatagenDeployer
        from lakebench.deploy.engine import DeploymentEngine

        cfg = _cfg()
        deployer = DatagenDeployer(DeploymentEngine(cfg, dry_run=True))
        # dry_run short-circuits the actual K8s apply -- but the gate would
        # still have run if it were wired in.
        result = deployer.deploy_cycle(cycle_index=1, total_cycles=3)
        assert result.status.value == "success"
        assert called["enforce"] is False
