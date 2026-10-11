"""Build a dry-run DeploymentEngine and the template context the deployers inject."""

from __future__ import annotations

from unittest.mock import patch

from lakebench.deploy.datagen import DatagenDeployer
from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config
from tests.fixtures.deploy_helpers import _mock_k8s as _mock_k8s


def _make_engine(cfg=None, **config_overrides) -> DeploymentEngine:
    """Build a DeploymentEngine with mocked K8s + OpenShift detection."""
    if cfg is None:
        cfg = make_config(**config_overrides)
    with patch(
        "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
        return_value=False,
    ):
        return DeploymentEngine(config=cfg, k8s_client=_mock_k8s(), dry_run=True)


def _enrich_context(engine: DeploymentEngine) -> dict:
    """Build the full template context, including deployer-specific variables.

    The base _build_context() omits variables that individual deployers inject.
    This helper adds those so every template renders without undefined variables.
    """
    ctx = dict(engine.context)
    cfg = engine.config
    # The Thrift and DuckDB deployers add where the dependency set is served
    # (deploy.deps.consumer_context); offline, the placeholder set.
    from lakebench.deps.manifest import consumer_context, placeholder_handle

    ctx.update(consumer_context(placeholder_handle(cfg)))

    # The secrets step injects the per-deployment Hive DB password.
    ctx.setdefault("postgres_password", "test-hive-db-password")
    ctx.setdefault("prometheus_retention", cfg.observability.retention)
    ctx.setdefault("prometheus_storage", cfg.observability.storage)
    # Both grafana and prometheus templates use ``pull_policy``.
    ctx.setdefault("pull_policy", cfg.images.pull_policy.value)

    # The datagen variables come from the deployer's own context builder.
    for key, value in DatagenDeployer(engine)._build_datagen_context().items():
        ctx.setdefault(key, value)

    return ctx
