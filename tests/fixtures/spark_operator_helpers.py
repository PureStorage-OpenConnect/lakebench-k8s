"""Shared setup for tests that drive SparkOperatorManager's watch-list edits."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

RUN = "lakebench.modules.pipeline_engines.spark.operator.subprocess.run"
SLEEP = "lakebench.modules.pipeline_engines.spark.operator.time.sleep"
INSTALLED = "2.5.1"

_UNSET = object()


def operator_patcher(monkeypatch) -> Iterator[Callable[..., MagicMock]]:
    """Generator body of the test modules' ``operator_run`` fixture: yields a
    factory that patches the manager's collaborators and returns the
    ``subprocess.run`` mock.

    ``watched`` is the namespace list the operator reports (``None`` means it
    watches all); ``watched_reads`` replaces it with one answer per read.
    ``verify`` is what the post-upgrade watch check returns, or a list of
    answers; leave it ``None`` to keep the real check. ``filter_existing``
    passes every namespace through as live. ``sleep`` skips retry back-off.
    """
    # The lease is covered elsewhere; these tests exercise the read-modify-write.
    monkeypatch.setattr(SparkOperatorManager, "_bypass_cluster_lock", True, raising=False)
    stack = ExitStack()

    def _setup(
        watched=("u01",),
        *,
        watched_reads=_UNSET,
        verify=True,
        restart=True,
        openshift=False,
        filter_existing=True,
        sleep=False,
    ) -> MagicMock:
        cls = SparkOperatorManager
        stack.enter_context(patch.object(cls, "_namespace_is_terminating", return_value=False))
        stack.enter_context(patch.object(cls, "_get_helm_version", return_value=INSTALLED))
        if watched_reads is _UNSET:
            stack.enter_context(
                patch.object(
                    cls,
                    "_get_watched_namespaces",
                    return_value=None if watched is None else list(watched),
                )
            )
        else:
            stack.enter_context(
                patch.object(cls, "_get_watched_namespaces", side_effect=watched_reads)
            )
        if verify is not None:
            kw = {"side_effect": verify} if isinstance(verify, list) else {"return_value": verify}
            stack.enter_context(patch.object(cls, "_verify_namespace_watched", **kw))
        stack.enter_context(patch.object(cls, "_restart_operator", return_value=restart))
        stack.enter_context(patch.object(cls, "_is_openshift", return_value=openshift))
        if filter_existing:
            stack.enter_context(
                patch.object(cls, "_filter_existing_namespaces", side_effect=lambda ns: ns)
            )
        if sleep:
            stack.enter_context(patch(SLEEP))
        return stack.enter_context(patch(RUN))

    yield _setup
    stack.close()
