"""Destroy must un-watch a namespace before deleting it (LB-066).

The Spark Operator crash-loops on a watched namespace that does not exist --
it cannot establish a Pod watch, its cache never syncs, and SparkApplication
reconciliation stops for every namespace on the cluster. Ordering is the whole
point: removing the namespace after deleting it leaves exactly the window the
fix exists to close.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.deploy.destroy import destroy_all
from lakebench.deploy.engine import DeploymentStatus


def _engine(create_namespace: bool = True) -> MagicMock:
    engine = MagicMock()
    cfg = engine.config
    cfg.get_namespace.return_value = "u02"
    cfg.platform.kubernetes.create_namespace = create_namespace
    cfg.platform.compute.spark.operator.namespace = "spark-operator"
    cfg.platform.compute.spark.operator.version = "2.4.0"
    return engine


def _run_destroy(engine, manager_cls) -> list:
    """Run destroy with everything but the namespace step neutralised."""
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    # PR-1's Step-0 identity check would otherwise reach the API server;
    # this test is about un-watch ordering, so we bypass identity with a
    # clean MATCH verdict.
    match_report = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name="u02",
        expected_deployment="u02",
        found_deployment="u02",
    )
    with (
        patch("lakebench.spark.SparkOperatorManager", manager_cls),
        patch(
            "lakebench.deploy.ownership.verify_namespace_identity",
            return_value=match_report,
        ),
        patch(
            "lakebench.deploy.ownership.verify_bucket_ownership",
            return_value=match_report,
        ),
        patch("kubernetes.client.CoreV1Api"),
        # The earlier destroy stages talk to a cluster we do not have. They
        # report their own failures and are not what this test is about.
        patch("lakebench.deploy.destroy.logger"),
    ):
        # Buckets are not under test here; a failed bucket step (the mocked
        # S3 config cannot init) now keeps the namespace by design.
        return destroy_all(engine, clean_buckets=False)


class TestOrdering:
    def test_unwatch_happens_before_namespace_delete(self):
        calls: list[str] = []

        manager = MagicMock()
        manager.remove_namespace_from_watch.side_effect = lambda ns, **_kw: (
            calls.append(f"unwatch:{ns}") or True
        )
        manager_cls = MagicMock(return_value=manager)

        engine = _engine()
        engine.k8s.delete_namespace.side_effect = lambda ns, **_kw: calls.append(f"delete:{ns}")

        _run_destroy(engine, manager_cls)

        assert "unwatch:u02" in calls, "destroy never un-watched the namespace"
        assert "delete:u02" in calls
        assert calls.index("unwatch:u02") < calls.index("delete:u02"), (
            f"un-watch must precede delete, got {calls}"
        )

    def test_operator_is_addressed_with_its_own_namespace_and_version(self):
        manager_cls = MagicMock(return_value=MagicMock())
        engine = _engine()

        _run_destroy(engine, manager_cls)

        kwargs = manager_cls.call_args.kwargs
        assert kwargs["namespace"] == "spark-operator"
        assert kwargs["version"] == "2.4.0", "removal must not drift the pinned chart"
        assert kwargs["job_namespace"] == "u02"


class TestDestroyIsNotBlocked:
    def test_unwatch_failure_now_blocks_namespace_delete(self):
        """ADR-F1: with strict watch-list, a failed drop MUST prevent
        the namespace delete -- deleting a namespace the operator still
        watches crash-loops the operator globally, which is the exact
        failure PR-2 was written to prevent. Destroy records the
        failure loudly and points at ``admin repair-operator``.
        """
        from lakebench.modules.pipeline_engines.spark.operator import (
            WatchListMutationError,
        )

        manager = MagicMock()
        manager.remove_namespace_from_watch.side_effect = WatchListMutationError("helm unreachable")
        manager_cls = MagicMock(return_value=manager)

        engine = _engine()
        results = _run_destroy(engine, manager_cls)

        engine.k8s.delete_namespace.assert_not_called()
        ns_results = [r for r in results if r.component == "namespace"]
        assert ns_results
        assert ns_results[-1].status == DeploymentStatus.SKIPPED
        assert "admin repair-operator" in ns_results[-1].message
        watch_results = [r for r in results if r.component == "spark-operator-watch"]
        assert watch_results and watch_results[-1].status == DeploymentStatus.FAILED

    def test_no_unwatch_when_namespace_is_not_ours_to_delete(self):
        """create_namespace=False means we never owned it -- leave it alone."""
        manager_cls = MagicMock(return_value=MagicMock())
        engine = _engine(create_namespace=False)

        _run_destroy(engine, manager_cls)

        manager_cls.assert_not_called()
        engine.k8s.delete_namespace.assert_not_called()
