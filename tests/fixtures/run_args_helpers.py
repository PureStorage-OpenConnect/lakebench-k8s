"""Shared test helpers moved from tests/test_run_args.py (imported by several test files)."""

from __future__ import annotations

import pytest

CONFIG = """\
name: runargs
recipe: hive-iceberg-spark-trino
platform:
  kubernetes:
    namespace: runargs
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: x
      secret_key: y
architecture:
  pipeline:
    mode: batch
workload:
  schema: customer360
  datagen:
    scale: 1
"""


@pytest.fixture
def no_cluster(monkeypatch):
    """Every way ``run`` reaches a cluster, S3 or a child process records
    the call and raises."""
    import subprocess

    import boto3
    import kubernetes.client
    import kubernetes.config

    from lakebench.k8s.client import K8sClient

    fired: list[str] = []

    def stop(name):
        def call(*a, **k):
            fired.append(name)
            raise AssertionError(f"cluster call: {name}")

        return call

    monkeypatch.setattr(kubernetes.config, "load_kube_config", stop("load_kube_config"))
    monkeypatch.setattr(kubernetes.config, "load_incluster_config", stop("load_incluster_config"))
    monkeypatch.setattr(kubernetes.client.ApiClient, "__init__", stop("ApiClient"))
    monkeypatch.setattr(K8sClient, "__init__", stop("K8sClient"))
    monkeypatch.setattr(subprocess, "run", stop("subprocess.run"))
    monkeypatch.setattr(subprocess, "Popen", stop("subprocess.Popen"))
    monkeypatch.setattr(boto3, "client", stop("boto3.client"))
    return fired
