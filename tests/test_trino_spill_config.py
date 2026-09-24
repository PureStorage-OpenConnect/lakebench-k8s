"""Coordinator and workers must agree on spill-enabled (live AML run
2026-09-24: workers-only spill made 3 of 8 queries fail with "spillable not
yet set", because the coordinator plans joins without the spillable flag)."""

from __future__ import annotations

import yaml

from lakebench.deploy.engine import TemplateRenderer


def _props(ctx):
    from unittest.mock import MagicMock, patch

    from lakebench.deploy.engine import DeploymentEngine
    from tests.conftest import make_config

    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    with patch.object(DeploymentEngine, "_detect_openshift", return_value=False):
        engine = DeploymentEngine(make_config(), k8s_client=k8s)
    full = {**engine.context, **ctx}
    doc = yaml.safe_load(TemplateRenderer().render("trino/configmap.yaml.j2", full))

    def parse(text):
        return dict(
            line.split("=", 1)
            for line in text.splitlines()
            if "=" in line and not line.startswith("#")
        )

    return parse(doc["data"]["config.properties.coordinator"]), parse(
        doc["data"]["config.properties.worker"]
    )


def test_spill_settings_match_when_enabled():
    coord, worker = _props({"trino_worker_spill_enabled": True})
    for key in ("spill-enabled", "spiller-spill-path", "max-spill-per-node"):
        assert coord.get(key) == worker.get(key), key
    assert coord["spill-enabled"] == "true"


def test_spill_absent_on_both_when_disabled():
    coord, worker = _props({"trino_worker_spill_enabled": False})
    assert "spill-enabled" not in coord and "spill-enabled" not in worker
