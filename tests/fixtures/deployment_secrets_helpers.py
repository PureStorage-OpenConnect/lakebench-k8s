"""Shared test helpers moved from tests/test_deployment_secrets.py (imported by several test files)."""

from __future__ import annotations

import base64
from types import SimpleNamespace

from kubernetes.client.rest import ApiException


class FakeCore:
    """CoreV1Api subset: Secrets and PVCs per namespace."""

    def __init__(self, secrets: dict | None = None, pvcs: set | None = None):
        # {(ns, name): {key: plaintext}}
        self.secrets: dict[tuple[str, str], dict[str, str]] = dict(secrets or {})
        self.pvcs: set[tuple[str, str]] = set(pvcs or set())
        self.creates: list[dict] = []
        self.replaces: list[dict] = []

    def read_namespaced_secret(self, name, ns):
        if (ns, name) not in self.secrets:
            raise ApiException(status=404)
        data = {
            k: base64.b64encode(v.encode()).decode() for k, v in self.secrets[(ns, name)].items()
        }
        return SimpleNamespace(data=data)

    def create_namespaced_secret(self, ns, body):
        name = body["metadata"]["name"]
        if (ns, name) in self.secrets:
            raise ApiException(status=409)
        self.creates.append(body)
        self.secrets[(ns, name)] = dict(body["stringData"])

    def replace_namespaced_secret(self, name, ns, body):
        self.replaces.append(body)
        self.secrets[(ns, name)] = dict(body["stringData"])

    def connect_get_namespaced_pod_exec(self, *a, **k):  # only passed to stream()
        raise AssertionError("exec goes through the patched stream")

    def read_namespaced_persistent_volume_claim(self, name, ns):
        if (ns, name) not in self.pvcs:
            raise ApiException(status=404)
        return SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp=None))
