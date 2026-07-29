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
    with (
        patch("lakebench.spark.SparkOperatorManager", manager_cls),
        # The earlier destroy stages talk to a cluster we do not have. They
        # report their own failures and are not what this test is about.
        patch("lakebench.deploy.destroy.logger"),
    ):
        return destroy_all(engine)


class TestOrdering:
    def test_unwatch_happens_before_namespace_delete(self):
        calls: list[str] = []

        manager = MagicMock()
        manager.remove_namespace_from_watch.side_effect = lambda ns: (
            calls.append(f"unwatch:{ns}") or True
        )
        manager_cls = MagicMock(return_value=manager)

        engine = _engine()
        engine.k8s.delete_namespace.side_effect = lambda ns: calls.append(f"delete:{ns}")

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
    def test_unwatch_failure_does_not_stop_the_delete(self):
        """A shared operator we cannot reconfigure must not block a destroy."""
        manager = MagicMock()
        manager.remove_namespace_from_watch.side_effect = RuntimeError("helm unreachable")
        manager_cls = MagicMock(return_value=manager)

        engine = _engine()
        results = _run_destroy(engine, manager_cls)

        engine.k8s.delete_namespace.assert_called_once_with("u02")
        ns_results = [r for r in results if r.component == "namespace"]
        assert ns_results and ns_results[-1].status == DeploymentStatus.SUCCESS

    def test_no_unwatch_when_namespace_is_not_ours_to_delete(self):
        """create_namespace=False means we never owned it -- leave it alone."""
        manager_cls = MagicMock(return_value=MagicMock())
        engine = _engine(create_namespace=False)

        _run_destroy(engine, manager_cls)

        manager_cls.assert_not_called()
        engine.k8s.delete_namespace.assert_not_called()
