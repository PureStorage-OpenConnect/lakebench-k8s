"""Shared test helpers moved from tests/test_saf2_deploy_state.py (imported by several test files)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from kubernetes.client.rest import ApiException

from lakebench.config import deploy_state as ds
from lakebench.deploy.ownership import (
    ANNOTATION_CREATED_BUCKETS,
    ANNOTATION_DEPLOY_NONCE,
    ANNOTATION_DEPLOYMENT_NAME,
)
from lakebench.deploy.ownership import ANNOTATION_STATE_SCHEMA as ANNOTATION_STATE_SCHEMA

NAME = "lb-x"


BUCKETS = f"{NAME}-bronze,{NAME}-gold,{NAME}-silver"


class FakeCore:
    """Namespaces by name: {"uid": str, "annotations": dict}."""

    def __init__(self) -> None:
        self.namespaces: dict[str, dict[str, Any]] = {}
        self.fail_reads = False
        self.reads = 0
        self.patches: list[tuple[str, dict]] = []

    def add(self, name: str, uid: str = "u1", **annotations: str) -> None:
        self.namespaces[name] = {"uid": uid, "annotations": dict(annotations)}

    def read_namespace(self, name: str, **kwargs: Any):
        self.reads += 1
        if self.fail_reads:
            raise ApiException(status=500, reason="boom")
        ns = self.namespaces.get(name)
        if ns is None:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace(
            metadata=SimpleNamespace(uid=ns["uid"], annotations=dict(ns["annotations"]))
        )

    def patch_namespace(self, name: str, body: dict) -> None:
        self.patches.append((name, body))
        self.namespaces[name]["annotations"].update(body["metadata"]["annotations"])


def _v16_namespace(core: FakeCore, nonce: str = "n16", **extra: str) -> None:
    core.add(
        NAME,
        **{
            ANNOTATION_DEPLOYMENT_NAME: NAME,
            ANNOTATION_DEPLOY_NONCE: nonce,
            ANNOTATION_CREATED_BUCKETS: BUCKETS,
            **extra,
        },
    )


def _nameless(d: Path, fname: str = "a.yaml") -> Path:
    p = d / fname
    p.write_text("recipe: hive-iceberg-spark-trino\n")
    return p


def _legacy_state(d: Path, name: str = NAME) -> None:
    (d / ".lakebench").mkdir(exist_ok=True)
    (d / ".lakebench" / "state.json").write_text(json.dumps({"name": name, "created": "x"}))


def _v17_state(cfg_path: Path, nonces: list[tuple[str, str]], **over: Any) -> Path:
    st = ds.new_state(cfg_path, NAME, NAME)
    st.nonces = [ds.NonceEntry(n, s, "t") for n, s in nonces]  # type: ignore[arg-type]
    for k, v in over.items():
        setattr(st, k, v)
    path = ds.state_path(cfg_path, NAME)
    ds.write_state(path, st)
    return path


def _named(d: Path) -> Path:
    p = d / "named.yaml"
    p.write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    return p
