"""A Service an operator creates between apply's read and create is updated,
not a failed deploy (the Stackable Hive operator creates
``<cluster>-metastore`` while lakebench applies the same name)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

from lakebench.k8s.client import K8sClient


class _Core:
    """Services held in a dict; ``appear_on_create`` makes the first create
    lose the race to another writer."""

    def __init__(self, appear_on_create: bool):
        self.services: dict[str, dict] = {}
        self.appear_on_create = appear_on_create

    def read_namespaced_service(self, name, namespace):
        if name not in self.services:
            raise ApiException(status=404)
        return SimpleNamespace(spec=SimpleNamespace(cluster_ip=self.services[name]["clusterIP"]))

    def create_namespaced_service(self, namespace, manifest):
        name = manifest["metadata"]["name"]
        if self.appear_on_create:
            self.appear_on_create = False
            self.services[name] = {"clusterIP": "172.30.0.9", "owner": "operator"}
            raise ApiException(status=409)
        self.services[name] = {"clusterIP": "172.30.0.1", "owner": "lakebench"}

    def replace_namespaced_service(self, name, namespace, manifest):
        self.services[name] = {"clusterIP": manifest["spec"]["clusterIP"], "owner": "lakebench"}


@pytest.mark.parametrize("race", [False, True])
def test_service_apply_survives_a_concurrent_create(race):
    client = K8sClient.__new__(K8sClient)
    client._core_v1 = _Core(appear_on_create=race)
    manifest = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "lakebench-hive-metastore"},
        "spec": {"ports": [{"port": 9083}]},
    }

    assert client.apply_manifest(manifest, namespace="ns-a") is True

    svc = client._core_v1.services["lakebench-hive-metastore"]
    assert svc["owner"] == "lakebench"
    # The update keeps the clusterIP of the Service that won the race.
    assert svc["clusterIP"] == ("172.30.0.9" if race else "172.30.0.1")
