"""Shared test helpers moved from tests/test_cli_cluster_ops.py (imported by several test files)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml
from kubernetes.client.rest import ApiException

from lakebench.cli import _cluster_ops as ops


class _Resp:
    def __init__(self, data: bytes):
        self.data = data
        self.released = False

    def stream(self, amt):
        for i in range(0, len(self.data), 3):  # small chunks: split lines and code points
            yield self.data[i : i + 3]

    def release_conn(self):
        self.released = True


class FakeCore:
    def __init__(self, pods=(), logs=None, list_error=None, read_errors=None):
        self.ns_exists = True
        self.ns_error: BaseException | None = None
        self.pods = list(pods)
        self.logs = logs or {}
        self.list_error = list_error
        self.read_errors = read_errors or {}
        self.selectors: list[str] = []
        self.reads: list[tuple[str, dict]] = []
        self.responses: list[_Resp] = []

    def read_namespace(self, name, _request_timeout=None):
        assert _request_timeout == ops.API_TIMEOUT
        if self.ns_error is not None:
            raise self.ns_error
        if not self.ns_exists:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace(metadata=SimpleNamespace(name=name))

    def list_namespaced_pod(self, namespace, label_selector="", _request_timeout=None):
        assert _request_timeout == ops.API_TIMEOUT
        self.selectors.append(label_selector)
        if self.list_error is not None:
            raise self.list_error
        return SimpleNamespace(items=list(self.pods))

    def read_namespaced_pod_log(self, name, namespace, **kwargs):
        self.reads.append((name, kwargs))
        if name in self.read_errors:
            raise self.read_errors[name]
        resp = _Resp(self.logs.get(name, b""))
        self.responses.append(resp)
        return resp


class FakeApps:
    def __init__(self, objects=None, errors=None):
        self.objects = objects or {}
        self.errors = errors or {}

    def _read(self, name):
        if name in self.errors:
            raise self.errors[name]
        if name not in self.objects:
            raise ApiException(status=404, reason="Not Found")
        ready, desired = self.objects[name]
        return SimpleNamespace(
            status=SimpleNamespace(ready_replicas=ready),
            spec=SimpleNamespace(replicas=desired),
        )

    def read_namespaced_stateful_set(self, name, namespace, _request_timeout=None):
        assert _request_timeout == ops.API_TIMEOUT
        return self._read(name)

    def read_namespaced_deployment(self, name, namespace, _request_timeout=None):
        assert _request_timeout == ops.API_TIMEOUT
        return self._read(name)


class FakeBatch:
    def __init__(self, job=False, read_error=None, delete_error=None, finished=""):
        self.job = job
        self.read_error = read_error
        self.delete_error = delete_error
        self.finished = finished  # "", "Complete" or "Failed"
        self.deleted: list[tuple[str, Any]] = []

    def read_namespaced_job(self, name, namespace, _request_timeout=None):
        assert _request_timeout == ops.API_TIMEOUT
        if self.read_error is not None:
            raise self.read_error
        if not self.job:
            raise ApiException(status=404, reason="Not Found")
        conditions = [SimpleNamespace(type=self.finished, status="True")] if self.finished else []
        return SimpleNamespace(
            status=SimpleNamespace(active=1, succeeded=0, conditions=conditions),
            spec=SimpleNamespace(completions=1),
        )

    def delete_namespaced_job(self, name, namespace, body=None, _request_timeout=None):
        assert _request_timeout == ops.API_TIMEOUT
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append((name, body))


class FakeCustom:
    def __init__(self, apps=(), list_error=None, delete_errors=None, states=None):
        self.apps = list(apps)
        self.list_error = list_error
        self.delete_errors = delete_errors or {}
        self.states = states or {}  # name -> applicationState.state
        self.deleted: list[str] = []

    def list_namespaced_custom_object(
        self, group, version, namespace, plural, _request_timeout=None
    ):
        assert (group, version, plural) == ("sparkoperator.k8s.io", "v1beta2", "sparkapplications")
        assert _request_timeout == ops.API_TIMEOUT
        if self.list_error is not None:
            raise self.list_error
        items = []
        for n in self.apps:
            item: dict = {"metadata": {"name": n}}
            if n in self.states:
                item["status"] = {"applicationState": {"state": self.states[n]}}
            items.append(item)
        return {"items": items}

    def delete_namespaced_custom_object(
        self, group, version, namespace, plural, name, _request_timeout=None
    ):
        assert _request_timeout == ops.API_TIMEOUT
        if name in self.delete_errors:
            raise self.delete_errors[name]
        self.deleted.append(name)


class FakeK8s:
    """The pinned client the commands build; `pre_stop` receives it."""

    def namespace_exists(self, name):
        raise AssertionError("the commands read the namespace through CoreV1Api")


def _config(tmp_path: Path, recipe: str = "hive-iceberg-spark-trino") -> Path:
    p = tmp_path / "lakebench.yaml"
    p.write_text(yaml.safe_dump({"name": "ops", "recipe": recipe}))
    return p


_TRINO_HIVE = {
    "lakebench-postgres": (1, 1),
    "lakebench-hive-metastore-default": (1, 1),
    "lakebench-trino-coordinator": (1, 1),
    "lakebench-trino-worker": (2, 2),
}
