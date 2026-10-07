"""Shared test helpers moved from tests/test_datagen_timeout_and_regenerate.py (imported by several test files)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.s3.client import BucketInfo


class _FakeS3:
    """Records get_bucket_size / empty_bucket calls and returns canned data.

    Configured per-instance so a test can inject its own scenario without
    reaching into boto3.
    """

    _next_info: BucketInfo | None = None
    _next_init_error: str | None = None
    instances: list[_FakeS3] = []  # noqa: F821 -- forward ref via type hints below
    #: Objects behind ``raw_client`` (the corpus series marker), shared by
    #: every instance in a test.
    store: dict[tuple[str, str], bytes] = {}

    @property
    def raw_client(self):
        from tests.fixtures.memory_s3 import MemoryBoto

        return MemoryBoto(_FakeS3.store)

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
    # No earlier datagen Job to stop (tests/test_datagen_old_pods.py covers it).
    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", lambda c: None)
    # A bronze bucket of this test's own: no series marker another test wrote.
    monkeypatch.setattr(_FakeS3, "store", {})
    return {"k8s": k8s_stub}


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
    # No earlier datagen Job to stop (tests/test_datagen_old_pods.py covers it).
    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", lambda c: None)
    # A run that reuses bronze reads its corpus series marker first: an empty
    # bucket in memory, unless the test installed its own S3 fake already.
    import lakebench.s3 as _s3

    if _s3.S3Client.__module__ == "lakebench.s3.client":
        monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    monkeypatch.setattr(_FakeS3, "store", {})
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
