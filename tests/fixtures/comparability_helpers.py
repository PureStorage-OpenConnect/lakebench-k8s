"""Shared test helpers moved from tests/test_comparability.py (imported by several test files)."""

from __future__ import annotations

import copy

from tests.fixtures.corpus_identity_helpers import observe, two_nodes
from tests.fixtures.corpus_identity_helpers import series as series_body
from tests.fixtures.experiment_helpers import _cfg, _metrics

SYSID = {
    "type": "cluster",
    "version": 2,
    "fingerprint": "f" * 16,
    "partial": False,
    "parts": {"api_server_ca": "c" * 12, "kubernetes": "v1.31.6"},
}


def _fresh(*, markers: bool = True, system: bool = True):
    """A v1.7 run (identity_version 2 at run start), with or without the
    v2 inputs."""
    run = _metrics(_cfg())
    inputs = run.config_snapshot["experiment_inputs"]
    assert inputs["identity_version"] == 2
    if markers:
        obs, _ = observe(two_nodes(), series_body())
        inputs["corpus_observation"] = obs
    if system:
        inputs["system_identity"] = copy.deepcopy(SYSID)
    return run
