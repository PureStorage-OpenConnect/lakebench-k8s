"""LB-091: wait_for_deployment_ready must fail fast on terminal Waiting
reasons and surface pod state on timeout.

The pre-fix behaviour only checked ``ready_replicas >= desired``, so an
ImagePullBackOff or CrashLoopBackOff burned the full 600s timeout and
returned a bare "not ready (0/1 replicas)" -- exactly the LB-070 shape
that masked a Helm crash loop as an OpenShift SCC rollout timeout.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lakebench.k8s.wait import (
    WaitStatus,
    WaitTerminal,
    _describe_deployment_pods,
    _pod_has_terminal_waiting,
    _selector_from_match_labels,
    wait_for_deployment_ready,
    wait_for_statefulset_ready,
)


def _pod(name, waiting=None, terminated=None, ready=True, restart_count=0, phase="Pending"):
    state = SimpleNamespace(
        waiting=waiting,
        terminated=terminated,
        running=None,
    )
    cs = SimpleNamespace(state=state, ready=ready, restart_count=restart_count)
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(phase=phase, container_statuses=[cs]),
    )


def _deployment(match_labels=None, desired=1, ready=0):
    return SimpleNamespace(
        spec=SimpleNamespace(
            replicas=desired,
            selector=SimpleNamespace(match_labels=match_labels or {"app": "x"}),
        ),
        status=SimpleNamespace(ready_replicas=ready),
    )


class TestSelectorFromMatchLabels:
    def test_none(self):
        assert _selector_from_match_labels(None) is None

    def test_empty(self):
        assert _selector_from_match_labels({}) is None

    def test_multi_key_sorted(self):
        # Sorted so the string is stable regardless of dict ordering.
        s = _selector_from_match_labels({"b": "2", "a": "1"})
        assert s == "a=1,b=2"


class TestDescribeDeploymentPods:
    def test_no_selector_is_explicit(self):
        assert "no label selector" in _describe_deployment_pods("ns", None)

    def test_no_pods_names_the_scheduling_failure(self):
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=[])
            note = _describe_deployment_pods("ns", "app=x")
        assert "no pods matched" in note

    def test_waiting_reason_in_note(self):
        pods = [_pod("p", waiting=SimpleNamespace(reason="ImagePullBackOff", message="not found"))]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            note = _describe_deployment_pods("ns", "app=x")
        assert "ImagePullBackOff" in note
        assert "not found" in note

    def test_terminated_exit_code_in_note(self):
        pods = [
            _pod("p", terminated=SimpleNamespace(reason="OOMKilled", exit_code=137)),
        ]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            note = _describe_deployment_pods("ns", "app=x")
        assert "OOMKilled" in note
        assert "137" in note

    def test_running_but_not_ready_names_restart_count(self):
        pods = [_pod("p", ready=False, restart_count=5, phase="Running")]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            note = _describe_deployment_pods("ns", "app=x")
        # Restart count distinguishes a slow start from a probe repeatedly killing.
        assert "5 restart" in note

    def test_diagnostic_never_raises(self):
        with patch("kubernetes.client.CoreV1Api", side_effect=RuntimeError("api down")):
            note = _describe_deployment_pods("ns", "app=x")
        assert "could not inspect" in note.lower()


class TestPodHasTerminalWaiting:
    def test_none_when_no_selector(self):
        assert _pod_has_terminal_waiting("ns", None) is None

    def test_none_when_no_pods_in_terminal_state(self):
        pods = [_pod("p", waiting=SimpleNamespace(reason="ContainerCreating", message=""))]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            assert _pod_has_terminal_waiting("ns", "app=x") is None

    def test_flags_image_pull_backoff(self):
        pods = [_pod("p", waiting=SimpleNamespace(reason="ImagePullBackOff", message="notfound"))]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            note = _pod_has_terminal_waiting("ns", "app=x")
        assert note is not None
        assert "ImagePullBackOff" in note

    def test_flags_crash_loop_after_debounce_threshold(self):
        """After LB-091 follow-up, CrashLoopBackOff is only terminal once
        restart_count reaches `_CRASH_LOOP_TERMINAL_RESTART_COUNT` (=3).
        See TestCrashLoopDebounce for the not-yet-terminal case."""
        pods = [
            _pod(
                "p",
                waiting=SimpleNamespace(reason="CrashLoopBackOff", message=""),
                restart_count=5,
            )
        ]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            note = _pod_has_terminal_waiting("ns", "app=x")
        assert note is not None
        assert "restarts=5" in note

    def test_ignores_transient_container_creating(self):
        """ContainerCreating and PodInitializing are transient. Blocking on
        them would break the normal cold-start path."""
        pods = [_pod("p", waiting=SimpleNamespace(reason="PodInitializing", message=""))]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            assert _pod_has_terminal_waiting("ns", "app=x") is None

    def test_read_failure_keeps_caller_waiting(self):
        # Diagnostics must not fail the wait; returning None means "keep polling".
        with patch("kubernetes.client.CoreV1Api", side_effect=RuntimeError("api down")):
            assert _pod_has_terminal_waiting("ns", "app=x") is None


class TestWaitForDeploymentReadyFailFast:
    def test_ready_returns_immediately(self):
        dep = _deployment(desired=1, ready=1)
        with patch("kubernetes.client.AppsV1Api") as apps:
            apps.return_value.read_namespaced_deployment.return_value = dep
            result = wait_for_deployment_ready(
                client=MagicMock(), name="d", namespace="ns", timeout_seconds=1
            )
        assert result.status is WaitStatus.READY

    def test_fails_fast_on_terminal_waiting(self):
        """The core of LB-091: an ImagePullBackOff must not consume the
        whole timeout budget."""
        dep = _deployment(desired=1, ready=0)
        pods = [_pod("p", waiting=SimpleNamespace(reason="ImagePullBackOff", message="notfound"))]
        with (
            patch("kubernetes.client.AppsV1Api") as apps,
            patch("kubernetes.client.CoreV1Api") as core,
        ):
            apps.return_value.read_namespaced_deployment.return_value = dep
            core.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            with patch("lakebench.k8s.wait.time") as mock_time:
                # Make sure we did not run out the clock -- we should get
                # FAILED on the first attempt, well before timeout.
                mock_time.time.side_effect = [0.0, 0.0, 0.1]
                mock_time.sleep = MagicMock()
                result = wait_for_deployment_ready(
                    client=MagicMock(),
                    name="d",
                    namespace="ns",
                    timeout_seconds=600,
                    poll_interval=5,
                )
        assert result.status is WaitStatus.FAILED
        assert "ImagePullBackOff" in result.message
        # LB-070 shape: the terminal-state message must actually name what
        # went wrong; the bare "not ready" is what we are fixing.
        assert "not ready" not in result.message.lower()

    def test_timeout_surfaces_pod_state(self):
        """On genuine timeout the message must carry pod-state diagnostics,
        not just 'not ready (0/1)'."""
        dep = _deployment(desired=1, ready=0)
        pods = [_pod("p", ready=False, restart_count=3, phase="Running")]
        with (
            patch("kubernetes.client.AppsV1Api") as apps,
            patch("kubernetes.client.CoreV1Api") as core,
        ):
            apps.return_value.read_namespaced_deployment.return_value = dep
            core.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            with patch("lakebench.k8s.wait.time") as mock_time:
                mock_time.time.side_effect = [0.0, 0.0, 100.0]
                mock_time.sleep = MagicMock()
                result = wait_for_deployment_ready(
                    client=MagicMock(),
                    name="d",
                    namespace="ns",
                    timeout_seconds=1,
                    poll_interval=1,
                )
        assert result.status is WaitStatus.TIMEOUT
        assert "Pod state:" in result.message
        assert "3 restart" in result.message

    def test_wait_terminal_is_exception(self):
        assert issubclass(WaitTerminal, Exception)


class TestCrashLoopDebounce:
    """LB-091 follow-up: CrashLoopBackOff on the first observation is not
    terminal -- a container that crashes once because a dependency is
    still coming up (Trino before Hive, Polaris before Postgres) can
    still recover. Only after N sustained restarts do we give up.
    """

    def test_crash_loop_with_one_restart_is_not_terminal(self):
        pods = [
            _pod(
                "p",
                waiting=SimpleNamespace(reason="CrashLoopBackOff", message="back off 10s"),
                restart_count=1,
            )
        ]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            assert _pod_has_terminal_waiting("ns", "app=x") is None, (
                "restart_count=1 must not fail the wait -- dep may still come up"
            )

    def test_crash_loop_after_three_restarts_is_terminal(self):
        pods = [
            _pod(
                "p",
                waiting=SimpleNamespace(reason="CrashLoopBackOff", message="back off"),
                restart_count=3,
            )
        ]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            note = _pod_has_terminal_waiting("ns", "app=x")
        assert note is not None
        assert "CrashLoopBackOff" in note
        assert "restarts=3" in note

    def test_image_pull_backoff_is_always_terminal(self):
        """ImagePullBackOff has no debounce -- a wrong image tag is
        never going to fix itself, and it has no restart_count anyway."""
        pods = [
            _pod(
                "p",
                waiting=SimpleNamespace(reason="ImagePullBackOff", message="not found"),
                restart_count=0,
            )
        ]
        with patch("kubernetes.client.CoreV1Api") as mock_api:
            mock_api.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            assert _pod_has_terminal_waiting("ns", "app=x") is not None


class TestStatefulSetFailFast:
    """LB-091 parity for StatefulSet -- Postgres (LB-054 area) and Trino
    workers both use StatefulSets. Before this pass, a bad Postgres
    image tag burned the full wait timeout with a bare "not ready"."""

    def _statefulset(self, match_labels=None, desired=1, ready=0):
        return SimpleNamespace(
            spec=SimpleNamespace(
                replicas=desired,
                selector=SimpleNamespace(match_labels=match_labels or {"app": "x"}),
            ),
            status=SimpleNamespace(ready_replicas=ready),
        )

    def test_ready_returns_immediately(self):
        sts = self._statefulset(desired=1, ready=1)
        with patch("kubernetes.client.AppsV1Api") as apps:
            apps.return_value.read_namespaced_stateful_set.return_value = sts
            result = wait_for_statefulset_ready(
                client=MagicMock(), name="s", namespace="ns", timeout_seconds=1
            )
        assert result.status is WaitStatus.READY

    def test_fails_fast_on_terminal_waiting(self):
        sts = self._statefulset(desired=1, ready=0)
        pods = [_pod("p", waiting=SimpleNamespace(reason="ImagePullBackOff", message="notfound"))]
        with (
            patch("kubernetes.client.AppsV1Api") as apps,
            patch("kubernetes.client.CoreV1Api") as core,
        ):
            apps.return_value.read_namespaced_stateful_set.return_value = sts
            core.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            with patch("lakebench.k8s.wait.time") as mock_time:
                mock_time.time.side_effect = [0.0, 0.0, 0.1]
                mock_time.sleep = MagicMock()
                result = wait_for_statefulset_ready(
                    client=MagicMock(),
                    name="s",
                    namespace="ns",
                    timeout_seconds=600,
                    poll_interval=5,
                )
        assert result.status is WaitStatus.FAILED
        assert "ImagePullBackOff" in result.message

    def test_timeout_surfaces_pod_state(self):
        sts = self._statefulset(desired=1, ready=0)
        pods = [_pod("p", ready=False, restart_count=2, phase="Running")]
        with (
            patch("kubernetes.client.AppsV1Api") as apps,
            patch("kubernetes.client.CoreV1Api") as core,
        ):
            apps.return_value.read_namespaced_stateful_set.return_value = sts
            core.return_value.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
            with patch("lakebench.k8s.wait.time") as mock_time:
                mock_time.time.side_effect = [0.0, 0.0, 100.0]
                mock_time.sleep = MagicMock()
                result = wait_for_statefulset_ready(
                    client=MagicMock(),
                    name="s",
                    namespace="ns",
                    timeout_seconds=1,
                    poll_interval=1,
                )
        assert result.status is WaitStatus.TIMEOUT
        assert "Pod state:" in result.message
        assert "2 restart" in result.message
