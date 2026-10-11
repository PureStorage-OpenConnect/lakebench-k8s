"""Coordinator and workers render the same spill settings."""

from __future__ import annotations

from tests.fixtures.trino_configmap import configmap, engine, props


def _props(ctx):
    full = {**engine().context, **ctx}
    cm = configmap(full)
    return props(cm["config.properties.coordinator"]), props(cm["config.properties.worker"])


def test_spill_settings_match_when_enabled():
    coord, worker = _props({"trino_worker_spill_enabled": True})
    for key in ("spill-enabled", "spiller-spill-path", "max-spill-per-node"):
        assert coord.get(key) == worker.get(key), key
    assert coord["spill-enabled"] == "true"
