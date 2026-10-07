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
from tests.fixtures.datagen_timeout_helpers import _FakeDatagenDeployer as _FakeDatagenDeployer
from tests.fixtures.datagen_timeout_helpers import _FakeS3 as _FakeS3
from tests.fixtures.datagen_timeout_helpers import _stub_full_run as _stub_full_run
from tests.fixtures.datagen_timeout_helpers import _stub_run_deps as _stub_run_deps
from tests.fixtures.datagen_timeout_helpers import _write_cfg as _write_cfg

# --- Helpers ----------------------------------------------------------------


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
    _FakeS3.store = {}
    yield
    _FakeS3.instances = []
    _FakeS3._next_info = None
    _FakeS3._next_init_error = None
    _FakeS3.store = {}


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

    def test_missing_bucket_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Fresh deploy: bronze does not exist yet, no refusal.
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_info = BucketInfo(name="b", exists=False)
        enforce_bronze_gate(_cfg(), regenerate=False)
        assert _FakeS3.instances[0].empty_calls == []

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
