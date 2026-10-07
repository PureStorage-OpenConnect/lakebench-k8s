"""Shared test helpers moved from tests/test_scripts_maps.py (imported by several test files)."""

from __future__ import annotations

from typing import Any

from lakebench.config import LakebenchConfig

LEGACY = "lakebench-spark-scripts"


class FakeK8s:
    """Records applies; reads back what was applied (or ``overrides``).

    Holds a v1.6 legacy map owned by ``legacy_owner`` (None: no legacy map).
    """

    def __init__(
        self,
        fail_apply_of: str | None = None,
        overrides: dict | None = None,
        legacy_owner: str | None = "sd8",
    ):
        self.applied: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.store: dict[str, dict[str, Any]] = {}
        self.fail_apply_of = fail_apply_of
        self.overrides = overrides or {}
        if legacy_owner is not None:
            self.store[LEGACY] = {
                "labels": {"app.kubernetes.io/instance": legacy_owner},
                "annotations": {},
                "data": {"common.py": "# 1.6"},
            }

    def get_cluster_capacity(self):
        return None

    def apply_manifest(self, manifest, namespace=None):
        name = manifest["metadata"]["name"]
        if name == self.fail_apply_of:
            return False
        self.applied.append(manifest)
        md = manifest["metadata"]
        self.store[name] = {
            "labels": dict(md.get("labels", {})),
            "annotations": dict(md.get("annotations", {})),
            "data": dict(manifest["data"]),
        }
        return True

    def get_configmap(self, name, namespace=None):
        if name in self.overrides:
            return self.overrides[name]
        return self.store.get(name)

    def delete_configmap(self, name, namespace=None):
        self.deleted.append(name)
        return self.store.pop(name, None) is not None


def _cfg(schema: str = "customer360", fmt: str = "iceberg") -> LakebenchConfig:
    return LakebenchConfig(
        name="sd8",
        workload={"schema": schema},
        architecture={"table_format": {"type": fmt}},
    )
