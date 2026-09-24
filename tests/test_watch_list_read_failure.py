"""A failed watch-list read must never look like an empty watch list.

Regression (2026-09-24 audit): `_get_watched_namespaces` returned [] on any
helm failure. Strict removal then saw "namespace not in []" and reported
success, so destroy deleted a namespace the operator still watched (the
operator crash-loops for every tenant). The add path wrote `[namespace]`,
dropping every other deployment from the list.
"""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from lakebench.modules.pipeline_engines.spark.operator import (
    SparkOperatorManager,
    WatchListMutationError,
    _WatchListReadError,
)


def _mgr() -> SparkOperatorManager:
    return SparkOperatorManager(namespace="spark-operator", version="2.5.1")


def _helm(returncode: int, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(
        args=["helm"], returncode=returncode, stdout=stdout, stderr=stderr
    )


def test_read_failure_raises():
    with patch(
        "subprocess.run", return_value=_helm(1, stderr="Error: Kubernetes cluster unreachable")
    ):
        with pytest.raises(_WatchListReadError):
            _mgr()._get_watched_namespaces()


def test_helm_missing_raises():
    with patch("subprocess.run", side_effect=FileNotFoundError("helm")):
        with pytest.raises(_WatchListReadError):
            _mgr()._get_watched_namespaces()


def test_release_not_found_and_no_controller_means_nothing_watched():
    runs = [
        _helm(1, stderr="Error: release: not found"),
        _helm(
            1,
            stderr='Error from server (NotFound): deployments.apps "spark-operator-controller" not found',
        ),
    ]
    with patch("subprocess.run", side_effect=runs):
        assert _mgr()._get_watched_namespaces() == []


def test_release_not_found_but_controller_present_raises():
    runs = [_helm(1, stderr="Error: release: not found"), _helm(0, stdout="deployment exists")]
    with patch("subprocess.run", side_effect=runs):
        with pytest.raises(_WatchListReadError):
            _mgr()._get_watched_namespaces()


def test_release_not_found_and_probe_fails_raises():
    runs = [
        _helm(1, stderr="Error: release: not found"),
        _helm(1, stderr="Unable to connect to the server"),
    ]
    with patch("subprocess.run", side_effect=runs):
        with pytest.raises(_WatchListReadError):
            _mgr()._get_watched_namespaces()


def test_kube_context_is_passed_to_helm_and_kubectl():
    mgr = SparkOperatorManager(namespace="spark-operator", version="2.5.1", kube_context="ctx-a")
    assert mgr._with_context(["helm", "get", "values", "x"])[:3] == [
        "helm",
        "--kube-context",
        "ctx-a",
    ]
    assert mgr._with_context(["kubectl", "get", "ns"])[:3] == ["kubectl", "--context", "ctx-a"]
    assert _mgr()._with_context(["helm", "list"]) == ["helm", "list"]


def test_non_strict_remove_fails_on_unreadable_list():
    with patch.object(
        SparkOperatorManager, "_get_watched_namespaces", side_effect=_WatchListReadError("x")
    ):
        assert _mgr().remove_namespace_from_watch("lb-a", strict=False) is False


def test_strict_remove_raises_on_unreadable_list():
    from contextlib import contextmanager

    @contextmanager
    def _lock(*_a, **_k):
        yield

    with (
        patch.object(
            SparkOperatorManager, "_get_watched_namespaces", side_effect=_WatchListReadError("x")
        ),
        patch("kubernetes.client.CoreV1Api"),
        patch("lakebench.deploy.cluster_lock.cluster_lock", _lock),
    ):
        with pytest.raises(WatchListMutationError):
            _mgr().remove_namespace_from_watch("lb-a", strict=True)


def test_add_refuses_on_unreadable_list_and_never_upgrades():
    with (
        patch.object(
            SparkOperatorManager, "_get_watched_namespaces", side_effect=_WatchListReadError("x")
        ),
        patch("subprocess.run") as run,
    ):
        assert _mgr()._add_namespace_to_watch_impl("lb-new") is False
    assert not any("upgrade" in (c.args[0] if c.args else []) for c in run.call_args_list)
