"""The destroy-safety oracle flags what it exists to flag.

Every destroy and shared-component test ends in ``assert_clean()``; if the
recorder stopped classifying a call as foreign, those tests would pass on an
unsafe destroy. Each row drives one call through the installed fakes.
"""

from __future__ import annotations

import boto3
import kubernetes.client as kc
import pytest
from kubernetes.client.models import V1ClusterRole, V1ConfigMap, V1ObjectMeta

from lakebench.deploy.cluster_lock import cluster_lock
from tests.fixtures.recording_k8s import (
    REASON_FOREIGN_BUCKET,
    REASON_FOREIGN_DELETE,
    REASON_UNLEASED,
    TAG_DEPLOYMENT,
    recording,
)


def _cm(name: str) -> V1ConfigMap:
    return V1ConfigMap(metadata=V1ObjectMeta(name=name))


def _cluster_role() -> None:
    kc.RbacAuthorizationV1Api().create_cluster_role(V1ClusterRole(metadata=V1ObjectMeta(name="x")))


def _foreign_delete():
    kc.CoreV1Api().delete_namespaced_config_map("cm", "lb-b")


def _leased_shared_mutation():
    with cluster_lock(kc.CoreV1Api(), ttl_seconds=60, timeout=1):
        _cluster_role()


def _put(bucket: str):
    return lambda: boto3.client("s3").put_object(Bucket=bucket, Key="k", Body=b"x")


def _own_work():
    core = kc.CoreV1Api()
    core.create_namespaced_config_map("lb-a", _cm("m"))
    core.delete_namespaced_config_map("m", "lb-a")
    boto3.client("s3").put_object(Bucket="a-bronze", Key="k", Body=b"x")


@pytest.mark.parametrize(
    ("act", "reason"),
    [
        pytest.param(_foreign_delete, REASON_FOREIGN_DELETE, id="delete-in-another-namespace"),
        pytest.param(_cluster_role, REASON_UNLEASED, id="shared-mutation-without-lease"),
        pytest.param(_put("b-bronze"), REASON_FOREIGN_BUCKET, id="write-to-unlisted-bucket"),
        pytest.param(_put("a-silver"), REASON_FOREIGN_BUCKET, id="write-to-bucket-tagged-by-b"),
        pytest.param(_leased_shared_mutation, None, id="shared-mutation-under-lease"),
        pytest.param(_own_work, None, id="own-namespace-and-own-bucket"),
    ],
)
def test_assert_clean_flags_each_unsafe_call(act, reason):
    with recording("lb-a", buckets=["a-bronze", "a-silver"], deployment="a") as rec:
        rec.add_namespace("lb-a")
        rec.add_namespace("lb-b")
        rec.add("configmaps", _cm("cm"), namespace="lb-b")
        rec.add_bucket("a-bronze", tags={TAG_DEPLOYMENT: "a"})
        rec.add_bucket("a-silver", tags={TAG_DEPLOYMENT: "b"})
        rec.add_bucket("b-bronze", tags={TAG_DEPLOYMENT: "b"})
        act()
        if reason is None:
            rec.assert_clean()
        else:
            with pytest.raises(AssertionError, match=reason):
                rec.assert_clean()
