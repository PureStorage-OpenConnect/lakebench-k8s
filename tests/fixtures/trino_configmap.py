"""Render the Trino configmap from a real engine context, and parse it."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import yaml

from lakebench.deploy.engine import DeploymentEngine, TemplateRenderer
from tests.conftest import make_config

_UNITS = {"B": 1, "kB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}


def engine(**overrides) -> DeploymentEngine:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    with patch.object(DeploymentEngine, "_detect_openshift", return_value=False):
        return DeploymentEngine(make_config(**overrides), k8s_client=k8s)


def render(name: str, ctx: dict) -> list[dict]:
    return [d for d in yaml.safe_load_all(TemplateRenderer().render(name, ctx)) if d]


def configmap(ctx: dict) -> dict[str, str]:
    return render("trino/configmap.yaml.j2", ctx)[0]["data"]


def props(text: str) -> dict[str, str]:
    return dict(
        line.strip().split("=", 1)
        for line in text.splitlines()
        if "=" in line and not line.strip().startswith("#")
    )


def size_bytes(value: str) -> int:
    """Parse a Trino DataSize (airlift: MB and GB are binary)."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(B|kB|MB|GB|TB)", value.strip())
    assert m, value
    return int(float(m.group(1)) * _UNITS[m.group(2)])


def xmx_bytes(jvm_config: str) -> int:
    m = re.search(r"^\s*-Xmx(\d+)([mMgG])\s*$", jvm_config, re.MULTILINE)
    assert m, jvm_config
    return int(m.group(1)) * (2**20 if m.group(2) in "mM" else 2**30)
