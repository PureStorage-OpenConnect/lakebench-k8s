"""Datagen timeout and --regenerate.

- Batch ``run --generate`` does not report datagen complete when the wait
  loop's timeout expired. The run exits 1 and the record's
  ``verdict.reasons`` says "datagen timed out", the datagen Job is deleted,
  and every leftover streaming SparkApplication (``bronze-ingest``,
  ``silver-stream``, ``gold-refresh``) is deleted so a timed-out generate
  does not leave orphan compute behind.
- ``lakebench generate`` and ``lakebench run --generate`` refuse a non-empty
  bronze prefix (exit 3) unless ``--regenerate`` is passed; with the flag, on
  a bucket this deployment may empty, the datagen prefix is cleared before
  datagen submits. tests/test_bronze_gate.py covers the unowned rows.
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
    """The timeout handler: fail the run, stop the datagen Job, delete leftover
    SparkApplications, whatever the cleanup does."""

    @pytest.mark.parametrize("cleanup_fails", [False, True])
    def test_run_fails_and_every_stream_app_is_tried(self, cleanup_fails: bool) -> None:
        # Best-effort cleanup: a failure deleting the Job or a SparkApplication
        # must not swallow the exit or stop the remaining deletes.
        deployer = MagicMock()
        job_manager = MagicMock()
        if cleanup_fails:
            deployer._delete_existing_job.side_effect = RuntimeError("API down")
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
        deleted = [call.args[0] for call in job_manager._delete_job.call_args_list]
        assert set(deleted) == set(_STREAM_APPS)


# --- enforce_bronze_gate ----------------------------------------------------


class TestEnforceBronzeRegenerate:
    """The A4 CLI-level gate on ``run --generate`` and ``generate``."""

    @pytest.mark.parametrize(
        ("schema", "held_prefix", "refused"),
        [
            ("financial", "pacs008", True),
            ("financial", "customer/interactions", False),
            ("customer360", "customer/interactions", True),
            ("customer360", "pacs008", False),
            # Fresh deploy: bronze does not exist yet, no refusal.
            ("customer360", None, False),
        ],
    )
    def test_gate_checks_the_prefix_datagen_writes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        schema: str,
        held_prefix: str | None,
        refused: bool,
    ) -> None:
        class _PrefixS3(_FakeS3):
            held = held_prefix

            def bucket_exists(self, bucket: str) -> bool:
                return self.held is not None

            def has_user_objects(self, bucket: str, prefix: str = "") -> bool:
                return self.held is not None and prefix.startswith(self.held)

            def get_bucket_size(self, bucket: str, prefix: str = "", exclude_prefix: str = ""):
                return BucketInfo(name=bucket, exists=True, object_count=3, size_bytes=1024)

        monkeypatch.setattr("lakebench.s3.S3Client", _PrefixS3)
        if refused:
            with pytest.raises(typer.Exit) as exc_info:
                enforce_bronze_gate(_cfg(schema=schema), regenerate=False)
            assert exc_info.value.exit_code == ExitCode.REFUSED
        else:
            enforce_bronze_gate(_cfg(schema=schema), regenerate=False)
        assert _FakeS3.instances[0].empty_calls == []

    def test_s3_init_error_refuses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Cannot check safely: refuse rather than proceed and (under
        # --regenerate) wipe something we cannot even list.
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
        _FakeS3._next_init_error = "endpoint unreachable"
        with pytest.raises(typer.Exit) as exc_info:
            enforce_bronze_gate(_cfg(), regenerate=True)
        assert exc_info.value.exit_code == ExitCode.PREREQUISITE  # S3 cannot be read


# --- CLI: refusal without --regenerate --------------------------------------


@pytest.mark.parametrize("command", ["generate", "run"])
def test_non_empty_bronze_is_refused_without_the_flag(
    command: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typer.testing import CliRunner

    from lakebench.cli import app

    if command == "generate":
        _stub_run_deps(monkeypatch)
        # The gate exits first: the deploy engine must not be reached.
        downstream = "lakebench.deploy.DeploymentEngine"
        argv = ["generate", "--yes"]
    else:
        _stub_full_run(monkeypatch)
        downstream = "lakebench.deploy.DatagenDeployer"
        argv = ["run", "--generate", "--skip-preflight", "--skip-benchmark"]
        argv += ["--skip-maintenance", "--yes"]
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    _FakeS3._next_info = BucketInfo(name="b", exists=True, object_count=5, size_bytes=1_000_000)
    reached = {"called": False}

    def _boom(*_a, **_kw):
        reached["called"] = True
        raise AssertionError("datagen reached after refusal")

    monkeypatch.setattr(downstream, _boom)
    cfg_file = _write_cfg(tmp_path)
    res = CliRunner().invoke(app, [argv[0], str(cfg_file), *argv[1:]])
    assert res.exit_code == ExitCode.REFUSED, (res.output[-2000:], repr(res.exception))
    assert reached["called"] is False


# --- CLI: `generate --regenerate` -------------------------------------------


class TestGenerateStandaloneRegenerate:
    """`lakebench generate --regenerate` (and the refusal without it)."""

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
        # datagen Job was stopped and every stream app was deleted.
        assert deployers and deployers[0].delete_calls
        deleted = [call.args[0] for call in stubs["job_manager"]._delete_job.call_args_list]
        assert set(deleted) == set(_STREAM_APPS)


class TestRunGenerateRegenerate:
    """`run --generate` refuses non-empty bronze without --regenerate."""

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
