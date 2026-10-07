"""Shared test helpers moved from tests/test_functional_templates.py (imported by several test files)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.deploy.engine import DeploymentEngine
from tests.conftest import make_config


def _mock_k8s() -> MagicMock:
    """Create a mock K8sClient for engine construction."""
    k8s = MagicMock()
    k8s.namespace_exists.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.get_cluster_capacity.return_value = None
    return k8s


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

    The base _build_context() omits variables that individual deployers inject
    (e.g. grafana_image, prometheus_image). This helper adds those so every
    template can be rendered without encountering undefined variables.
    """
    ctx = dict(engine.context)
    cfg = engine.config
    # The Thrift and DuckDB deployers add where the dependency set is served
    # (deploy.deps.consumer_context); offline, the placeholder set.
    from lakebench.deps.manifest import consumer_context, placeholder_handle

    ctx.update(consumer_context(placeholder_handle(cfg)))

    # The Prometheus and Grafana deployers that injected image, retention and
    # storage-class variables are gone (the kube-prometheus-stack chart
    # deploys both); no template reads those variables.
    # The secrets step injects the per-deployment Hive DB password (SAF-8)
    ctx.setdefault("postgres_password", "test-hive-db-password")
    ctx.setdefault("prometheus_retention", cfg.observability.retention)
    ctx.setdefault("prometheus_storage", cfg.observability.storage)
    # Both grafana and prometheus templates use ``pull_policy`` (not image_pull_policy)
    ctx.setdefault("pull_policy", cfg.images.pull_policy.value)

    # Datagen deployer injects these (see datagen.py _build_datagen_context)
    ctx.setdefault("datagen_target_tb", "0.010000")
    ctx.setdefault("datagen_file_size_mb", 512)
    # datagen_payload_kb removed 2026-09-28; template no longer renders it.
    ctx.setdefault("datagen_path_prefix", "customer/interactions/")
    ctx.setdefault("datagen_seed", 42)
    ctx.setdefault("datagen_cpu", "2")
    ctx.setdefault("datagen_memory", "4Gi")
    ctx.setdefault("datagen_mode", "batch")
    ctx.setdefault("datagen_workers", 4)
    ctx.setdefault("datagen_dirty_ratio", 0.08)
    ctx.setdefault("datagen_image", cfg.images.datagen)
    ctx.setdefault("datagen_timestamp_start", None)
    ctx.setdefault("datagen_timestamp_end", None)

    return ctx
