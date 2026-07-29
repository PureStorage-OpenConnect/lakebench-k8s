"""Tests for DuckDB readiness failure diagnostics.

DuckDB installs itself at container startup, so `_wait_for_ready` waits 900s.
Readiness alone cannot say *why* a pod is not ready: a slow pip install, a
crash-looping container, an unschedulable pod, and a Deployment that was never
created all present as `ready_replicas: 0` and then raised the same bare
timeout message. That turns any DuckDB failure into a 15-minute wait followed
by no information (seen live in UAT u01).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.modules.query_engines.duckdb.deployer import DuckDBDeployer

# The selector must match templates/duckdb/deployment.yaml.j2. A stale
# selector makes every failure report "no pod was created", which is a
# confidently wrong answer rather than a missing one.
_EXPECTED_SELECTOR = "app.kubernetes.io/name=lakebench,app.kubernetes.io/component=duckdb"


def _deployer() -> DuckDBDeployer:
    engine = MagicMock()
    return DuckDBDeployer(engine)


def _pod(name: str, phase: str = "Running", **container_state):
    pod = MagicMock()
    pod.metadata.name = name
    pod.status.phase = phase
    if container_state.get("no_statuses"):
        pod.status.container_statuses = None
        return pod

    cs = MagicMock()
    cs.ready = container_state.get("ready", False)
    cs.restart_count = container_state.get("restart_count", 0)
    cs.state.waiting = container_state.get("waiting")
    cs.state.terminated = container_state.get("terminated")
    pod.status.container_statuses = [cs]
    return pod


def _waiting(reason: str, message: str | None = None):
    w = MagicMock()
    w.reason = reason
    w.message = message
    return w


def _terminated(reason: str, exit_code: int):
    t = MagicMock()
    t.reason = reason
    t.exit_code = exit_code
    return t


class _CoreApi:
    """Stands in for CoreV1Api, capturing the selector it was queried with."""

    def __init__(self, pods=None, raises: Exception | None = None):
        self._pods = pods or []
        self._raises = raises
        self.selector: str | None = None

    def list_namespaced_pod(self, namespace, label_selector):
        self.selector = label_selector
        if self._raises:
            raise self._raises
        result = MagicMock()
        result.items = self._pods
        return result


def _describe(pods=None, raises: Exception | None = None):
    """Run _describe_not_ready against a fake API, returning (text, api)."""
    api = _CoreApi(pods, raises)
    with patch("kubernetes.client.CoreV1Api", return_value=api):
        return _deployer()._describe_not_ready("u01"), api


class TestSelectorMatchesTemplate:
    def test_queries_the_template_labels(self):
        """Regression: an `app=lakebench-duckdb` selector matches nothing."""
        _, api = _describe([_pod("lakebench-duckdb-abc", ready=False)])
        assert api.selector == _EXPECTED_SELECTOR


class TestDiagnosis:
    def test_no_pod_points_at_scheduling_or_scc(self):
        text, _ = _describe([])
        assert "No DuckDB pod was created" in text
        assert "scheduled" in text or "security context" in text

    def test_image_pull_failure_is_named(self):
        text, _ = _describe([_pod("d-1", waiting=_waiting("ImagePullBackOff", "quota exceeded"))])
        assert "ImagePullBackOff" in text
        assert "quota exceeded" in text

    def test_waiting_without_message_still_reports(self):
        text, _ = _describe([_pod("d-1", waiting=_waiting("CrashLoopBackOff"))])
        assert "CrashLoopBackOff" in text
        assert "no detail" in text

    def test_terminated_reports_reason_and_exit_code(self):
        text, _ = _describe([_pod("d-1", terminated=_terminated("OOMKilled", 137))])
        assert "OOMKilled" in text
        assert "137" in text

    def test_running_but_failing_probe_reports_restarts(self):
        """The distinction that matters: slow start versus probe killing it."""
        text, _ = _describe([_pod("d-1", ready=False, restart_count=4)])
        assert "4 restart(s)" in text
        assert "startup probe" in text

    def test_multiple_pods_are_all_reported(self):
        text, _ = _describe(
            [
                _pod("d-1", waiting=_waiting("ImagePullBackOff")),
                _pod("d-2", ready=False, restart_count=2),
            ]
        )
        assert "d-1" in text and "d-2" in text


class TestDiagnosticsNeverMaskTheTimeout:
    def test_api_error_degrades_to_a_note(self):
        text, _ = _describe(raises=RuntimeError("connection refused"))
        assert "Could not inspect pods" in text
        assert "connection refused" in text

    def test_timeout_message_carries_the_diagnosis(self):
        deployer = _deployer()
        apps = MagicMock()
        dep = MagicMock()
        dep.status.ready_replicas = 0
        dep.spec.replicas = 1
        apps.read_namespaced_deployment.return_value = dep

        with (
            patch("kubernetes.client.AppsV1Api", return_value=apps),
            patch.object(
                DuckDBDeployer,
                "_describe_not_ready",
                return_value="d-1: terminated (OOMKilled, exit 137)",
            ),
            patch("time.sleep"),
            pytest.raises(RuntimeError) as exc,
        ):
            deployer._wait_for_ready("u01", timeout_seconds=0)

        msg = str(exc.value)
        assert "did not become ready" in msg
        assert "OOMKilled" in msg, "the timeout must carry the reason, not just the fact"
