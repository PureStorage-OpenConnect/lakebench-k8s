"""SAF-10: the cluster fingerprint in bucket ownership (SD-18a, DESIGN ch01 section 4).

One case per row of the verdict matrix, on a tagged backend (the
``lakebench.cluster`` tag) and a tagless one (FlashBlade: the
``.lakebench/owner.json`` marker), then what deploy and destroy do with each
verdict, on the recording fake.

The hole this closes: cluster Y deploys the same deployment name while
cluster X's bucket is still empty; before, Y adopted it and Y's destroy
later emptied X's data.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy.ownership import (
    OWNER_MARKER_KEY,
    TAG_CLUSTER,
    TAG_DEPLOYMENT_NAME,
    IdentityVerdict,
    verify_bucket_ownership,
)
from tests.fixtures.recording_k8s import K8sRecorder, recording

NS = "u01"
FP = "fp-here"
OTHER_FP = "fp-there"
B = "u01-bronze"


def _boto():
    import boto3

    return boto3.client("s3")


def _seed(rec: K8sRecorder, *, tagged: bool, deployment: str | None, cluster: str | None) -> None:
    """One bucket stamped with ``deployment`` and ``cluster`` (None: not stamped)."""
    rec.s3_tagging = tagged
    if tagged:
        tags = {}
        if deployment:
            tags[TAG_DEPLOYMENT_NAME] = deployment
        if cluster:
            tags[TAG_CLUSTER] = cluster
        rec.add_bucket(B, tags=tags or None)
    else:
        objects = {}
        if deployment:
            marker = {"deployment": deployment}
            if cluster:
                marker["cluster"] = cluster
            objects[OWNER_MARKER_KEY] = json.dumps(marker).encode()
        rec.add_bucket(B, objects)


ROWS = [
    # (row, stamp deployment, stamp cluster, expected cluster, in record, verdict tagged, tagless)
    ("1", NS, FP, FP, False, IdentityVerdict.MATCH, IdentityVerdict.MATCH),
    (
        "2",
        NS,
        OTHER_FP,
        FP,
        False,
        IdentityVerdict.FOREIGN_CLUSTER,
        IdentityVerdict.FOREIGN_CLUSTER,
    ),
    ("3", NS, None, FP, True, IdentityVerdict.LEGACY_PROVEN, IdentityVerdict.LEGACY_PROVEN),
    ("4", NS, None, FP, False, IdentityVerdict.LEGACY_UNPROVEN, IdentityVerdict.LEGACY_UNPROVEN),
    # 5: the CA rotated, so this cluster's fingerprint changed under its own stamp.
    (
        "5",
        NS,
        FP,
        "fp-after-rotation",
        True,
        IdentityVerdict.FOREIGN_CLUSTER,
        IdentityVerdict.FOREIGN_CLUSTER,
    ),
    ("6", "u02", FP, FP, False, IdentityVerdict.MISMATCH, IdentityVerdict.MISMATCH),
    ("7", None, None, FP, False, IdentityVerdict.ABSENT, IdentityVerdict.UNSUPPORTED),
    (
        "8",
        NS,
        FP,
        None,
        True,
        IdentityVerdict.UNVERIFIED_CLUSTER,
        IdentityVerdict.UNVERIFIED_CLUSTER,
    ),
]


@pytest.mark.parametrize("tagged", [True, False], ids=["tagged", "tagless"])
@pytest.mark.parametrize(
    ("row", "deployment", "cluster", "mine", "recorded", "want_tagged", "want_tagless"),
    ROWS,
    ids=[f"row{r[0]}" for r in ROWS],
)
def test_matrix_row(row, deployment, cluster, mine, recorded, want_tagged, want_tagless, tagged):
    with recording(NS) as rec:
        _seed(rec, tagged=tagged, deployment=deployment, cluster=cluster)
        v = verify_bucket_ownership(
            _boto(), B, NS, expected_cluster=mine, created_record={B} if recorded else set()
        )
        assert v.verdict is (want_tagged if tagged else want_tagless), v.hint
        assert v.tagged is tagged
        if row == "5":
            assert "CA changed" in (v.hint or "")


def test_tagless_marker_without_stamp_in_record_is_legacy_proven():
    """A FlashBlade bucket 1.6 created: no marker, in the created record."""
    with recording(NS) as rec:
        _seed(rec, tagged=False, deployment=None, cluster=None)
        v = verify_bucket_ownership(_boto(), B, NS, expected_cluster=FP, created_record={B})
        assert v.verdict is IdentityVerdict.LEGACY_PROVEN and not v.tagged


def test_corrupt_marker_is_never_read_as_no_claim():
    from lakebench.deploy.ownership import BucketOwnershipError

    with recording(NS) as rec:
        rec.s3_tagging = False
        rec.add_bucket(B, {OWNER_MARKER_KEY: b"[1, 2]"})
        with pytest.raises(BucketOwnershipError):
            verify_bucket_ownership(_boto(), B, NS, expected_cluster=FP, created_record=())


# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------


def _deploy(rec: K8sRecorder, *, fp: str | None = FP, force_legacy: bool = False):
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s.client import K8sClient
    from tests.conftest import make_config

    cfg = make_config(name=NS)
    rec.for_config(cfg)
    if ("namespaces", None, NS) not in rec.store:
        rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
    engine = DeploymentEngine(cfg, k8s_client=K8sClient(namespace=NS))
    with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value=fp):
        return engine._deploy_buckets(force_legacy=force_legacy)


class TestDeploy:
    def test_new_buckets_carry_the_cluster_stamp(self):
        with recording(NS) as rec:
            result = _deploy(rec)
            assert result.status.value == "success", result.message
            for b in ("u01-bronze", "u01-silver", "u01-gold"):
                assert rec.tags_store[b][TAG_CLUSTER] == FP

    def test_refuses_without_a_fingerprint(self):
        with recording(NS) as rec:
            result = _deploy(rec, fp=None)
            assert result.status.value == "failed"
            assert "cannot compute this cluster's fingerprint" in result.message
            assert not rec.buckets_store or all(
                TAG_CLUSTER not in rec.tags_store.get(b, {}) for b in rec.buckets_store
            )

    def test_refuses_another_clusters_bucket(self):
        """Row 2: fails reverted (the name tag alone read MATCH)."""
        with recording(NS) as rec:
            rec.add_bucket(B, tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: OTHER_FP})
            result = _deploy(rec)
            assert result.status.value == "failed"
            assert "another cluster" in result.message
            assert rec.tags_store[B][TAG_CLUSTER] == OTHER_FP
            # A refusal (exit 3), not an unclassified failure (exit 1).
            assert result.details.get("refusal") == "deploy.identity_foreign"

    def test_stamps_a_recorded_legacy_bucket(self):
        """Row 3: a bucket this namespace's record lists gets this cluster's stamp."""
        with recording(NS) as rec:
            rec.add_namespace(
                NS,
                annotations={
                    "lakebench.deployment/name": NS,
                    "lakebench.deployment/created-buckets": B,
                },
            )
            rec.add_bucket(B, tags={TAG_DEPLOYMENT_NAME: NS, "lakebench.created": "true"})
            result = _deploy(rec)
            assert result.status.value == "success", result.message
            assert rec.tags_store[B][TAG_CLUSTER] == FP
            assert rec.tags_store[B]["lakebench.created"] == "true"

    def test_never_stamps_an_unproven_legacy_bucket(self):
        """Row 4: used, but no claim is added for this cluster."""
        with recording(NS) as rec:
            rec.add_bucket(B, ["data/part-0"], tags={TAG_DEPLOYMENT_NAME: NS})
            result = _deploy(rec)
            assert result.status.value == "success", result.message
            assert TAG_CLUSTER not in rec.tags_store[B]

    @pytest.mark.parametrize("force_legacy", [False, True])
    def test_tagless_empty_unmarked_bucket_is_adopted_only_with_force_legacy(self, force_legacy):
        """Row 7, the cross-cluster hole: an empty, unmarked FlashBlade bucket
        may be another cluster's not yet written. Fails reverted (it was
        recorded as adopted while empty)."""
        from lakebench.deploy.ownership import ANNOTATION_ADOPTED_EMPTY_BUCKETS

        with recording(NS) as rec:
            rec.s3_tagging = False
            rec.add_bucket(B)
            result = _deploy(rec, force_legacy=force_legacy)
            assert result.status.value == "success", result.message
            anns = rec.store[("namespaces", None, NS)].metadata.annotations or {}
            adopted = anns.get(ANNOTATION_ADOPTED_EMPTY_BUCKETS, "")
            assert (B in adopted) is force_legacy


# ---------------------------------------------------------------------------
# Destroy
# ---------------------------------------------------------------------------


def _destroy(
    rec: K8sRecorder,
    *,
    fp: str | None = FP,
    annotations: dict | None = None,
    delete_buckets: bool = True,
) -> list:
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport
    from lakebench.k8s.client import K8sClient
    from tests.conftest import make_config

    cfg = make_config(name=NS)
    cfg.platform.kubernetes.create_namespace = False
    rec.for_config(cfg)
    rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS, **(annotations or {})})
    rec.add_spark_operator(watched=[NS])
    rec.add_stackable()
    engine = MagicMock()
    engine.config = cfg
    engine.k8s = K8sClient(namespace=NS)
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name=NS,
        expected_deployment=NS,
        found_deployment=NS,
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.api_server_fingerprint", return_value=fp),
        patch("lakebench.deploy.destroy._sleep", lambda s: None),
    ):
        return destroy_all(engine, clean_buckets=True, delete_buckets=delete_buckets)


def _bucket_result(results: list):
    return next(r for r in results if r.component == "s3-buckets")


class TestDestroy:
    def test_another_clusters_bucket_is_kept(self):
        """Row 2: fails reverted (destroy emptied it on the name tag)."""
        with recording() as rec:
            rec.add_bucket(
                B, ["data/part-0"], tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: OTHER_FP}
            )
            results = _destroy(rec)
            assert rec.buckets_store[B] == {"data/part-0": b"x"}
            res = _bucket_result(results)
            assert res.status.value == "failed"
            assert "claimed from another cluster" in res.message

    def test_unproven_legacy_bucket_is_kept(self):
        with recording() as rec:
            rec.add_bucket(B, ["data/part-0"], tags={TAG_DEPLOYMENT_NAME: NS})
            results = _destroy(rec)
            assert rec.buckets_store[B] == {"data/part-0": b"x"}
            assert "reclaim-bucket" in _bucket_result(results).message

    def test_destroy_without_fingerprint_keeps_buckets(self):
        """Row 8: no bucket is emptied or deleted; the refusal carries its exit path."""
        with recording() as rec:
            rec.add_bucket(
                B,
                ["data/part-0"],
                tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP, "lakebench.created": "true"},
            )
            results = _destroy(
                rec, fp=None, annotations={"lakebench.deployment/created-buckets": B}
            )
            assert B in rec.buckets_store
            assert rec.buckets_store[B] == {"data/part-0": b"x"}
            res = _bucket_result(results)
            assert res.status.value == "failed"
            assert "Destroy NOT completed: this cluster has no fingerprint" in res.message
            assert res.details.get("refusal") == "destroy.unverified_cluster"

    def test_own_stamped_bucket_is_emptied_and_deleted(self):
        with recording() as rec:
            rec.add_bucket(
                B,
                ["data/part-0"],
                tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP, "lakebench.created": "true"},
            )
            results = _destroy(rec, annotations={"lakebench.deployment/created-buckets": B})
            assert _bucket_result(results).status.value == "success", _bucket_result(
                results
            ).message
            assert B not in rec.buckets_store

    def test_tagless_own_marker_is_emptied_and_deleted_when_created(self):
        with recording() as rec:
            rec.s3_tagging = False
            marker = json.dumps({"deployment": NS, "cluster": FP}).encode()
            rec.add_bucket(B, {"data/part-0": b"x", OWNER_MARKER_KEY: marker})
            results = _destroy(rec, annotations={"lakebench.deployment/created-buckets": B})
            assert _bucket_result(results).status.value == "success", _bucket_result(
                results
            ).message
            assert B not in rec.buckets_store

    def test_tagless_foreign_cluster_marker_is_kept(self):
        """Row 2 on FlashBlade: the other cluster's marker keeps its data safe even
        when this namespace's record lists the name."""
        with recording() as rec:
            rec.s3_tagging = False
            marker = json.dumps({"deployment": NS, "cluster": OTHER_FP}).encode()
            rec.add_bucket(B, {"data/part-0": b"x", OWNER_MARKER_KEY: marker})
            results = _destroy(rec, annotations={"lakebench.deployment/created-buckets": B})
            assert "data/part-0" in rec.buckets_store[B]
            assert _bucket_result(results).status.value == "failed"


# ---------------------------------------------------------------------------
# The continuous reset and clean apply the same verdicts
# ---------------------------------------------------------------------------


def test_continuous_reset_applies_the_matrix():
    for tags, problem in [
        ({TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP}, False),
        ({TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: OTHER_FP}, True),
        ({TAG_DEPLOYMENT_NAME: NS}, True),  # row 4, not in the record
    ]:
        from kubernetes import client

        from lakebench.cli._sustained import _bucket_ownership_problem
        from tests.conftest import make_config

        cfg = make_config(name=NS)
        with recording() as rec:
            rec.for_config(cfg)
            rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
            for b in ("u01-bronze", "u01-silver", "u01-gold"):
                rec.add_bucket(b, tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP})
            rec.tags_store[B] = dict(tags)
            with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value=FP):
                got = _bucket_ownership_problem(cfg, client.CoreV1Api())
            assert (got is not None) is problem, got
            if problem:
                assert B in got


# ---------------------------------------------------------------------------
# Review round: the marker on kept buckets, row 3 in destroy, no fingerprint
# ---------------------------------------------------------------------------

MINE = json.dumps({"deployment": NS, "cluster": FP}).encode()
CREATED = {"lakebench.deployment/created-buckets": B}


class TestDestroyKeepsTheClaim:
    def test_keep_buckets_keeps_the_marker(self):
        """A bucket destroy keeps stays this deployment's: the marker stays."""
        with recording() as rec:
            rec.s3_tagging = False
            rec.add_bucket(B, {"data/part-0": b"x", OWNER_MARKER_KEY: MINE})
            results = _destroy(rec, annotations=CREATED, delete_buckets=False)
            assert _bucket_result(results).status.value == "success"
            assert rec.buckets_store[B] == {OWNER_MARKER_KEY: MINE}

    def test_marker_bucket_not_created_is_emptied_and_kept_with_its_marker(self):
        with recording() as rec:
            rec.s3_tagging = False
            rec.add_bucket(B, {"data/part-0": b"x", OWNER_MARKER_KEY: MINE})
            _destroy(rec)
            assert rec.buckets_store[B] == {OWNER_MARKER_KEY: MINE}

    def test_a_kept_recorded_legacy_bucket_is_stamped(self):
        """Row 3 with --keep-buckets: stamped before the record goes with the
        namespace, so the next deploy reads row 1, not row 4."""
        with recording() as rec:
            rec.add_bucket(
                B, ["data/part-0"], tags={TAG_DEPLOYMENT_NAME: NS, "lakebench.created": "true"}
            )
            _destroy(rec, annotations=CREATED, delete_buckets=False)
            assert rec.tags_store[B][TAG_CLUSTER] == FP
            assert rec.tags_store[B]["lakebench.created"] == "true"
            assert rec.buckets_store[B] == {}

    def test_a_kept_recorded_tagless_bucket_gets_its_marker(self):
        with recording() as rec:
            rec.s3_tagging = False
            rec.add_bucket(B, ["u01-bronze-data/part-0"])
            _destroy(rec, annotations=CREATED, delete_buckets=False)
            marker = json.loads(rec.buckets_store[B][OWNER_MARKER_KEY])
            assert (marker["deployment"], marker["cluster"]) == (NS, FP)

    def test_no_fingerprint_keeps_an_unstamped_recorded_bucket(self):
        """Row 3 needs this cluster's fingerprint too: no bucket is emptied."""
        with recording() as rec:
            rec.add_bucket(
                B, ["data/part-0"], tags={TAG_DEPLOYMENT_NAME: NS, "lakebench.created": "true"}
            )
            results = _destroy(rec, fp=None, annotations=CREATED)
            assert rec.buckets_store[B] == {"data/part-0": b"x"}
            assert _bucket_result(results).status.value == "failed"


class TestNo16AdoptionProof:
    def test_deploy_does_not_claim_a_16_adopted_tagless_bucket(self):
        """1.6 recorded another cluster's empty bucket as adopted; 1.7 must not
        turn that record into a marker claim."""
        with recording(NS) as rec:
            rec.s3_tagging = False
            rec.add_namespace(
                NS,
                annotations={
                    "lakebench.deployment/name": NS,
                    "lakebench.deployment/adopted-empty-buckets": B,
                },
            )
            rec.add_bucket(B, ["their/part-0"])
            result = _deploy(rec)
            assert result.status.value == "success", result.message
            assert OWNER_MARKER_KEY not in rec.buckets_store[B]

    def test_marker_write_mode_is_recorded(self):
        from lakebench.deploy.ownership import ANNOTATION_MARKER_WRITE

        with recording(NS) as rec:
            rec.s3_tagging = False
            _deploy(rec)
            anns = rec.store[("namespaces", None, NS)].metadata.annotations
            assert anns[ANNOTATION_MARKER_WRITE] == "conditional"

    def test_preprovisioned_claim_lost_is_not_recorded(self):
        from lakebench.deploy.engine import DeploymentEngine
        from lakebench.deploy.ownership import ANNOTATION_ADOPTED_EMPTY_BUCKETS
        from lakebench.k8s.client import K8sClient
        from tests.conftest import make_config

        cfg = make_config(name=NS)
        cfg.platform.storage.s3.create_buckets = False
        with recording(NS) as rec:
            rec.for_config(cfg)
            rec.s3_tagging = False
            rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
            for b in ("u01-bronze", "u01-silver", "u01-gold"):
                rec.add_bucket(b)
            other = json.dumps({"deployment": NS, "cluster": OTHER_FP}).encode()

            def racing_claim(bucket, key):
                # Another cluster's marker replaces ours on bronze before our
                # read-back (the race the marker decides).
                if bucket == B and key == OWNER_MARKER_KEY:
                    rec.buckets_store[B][OWNER_MARKER_KEY] = other

            rec.after_put = racing_claim
            engine = DeploymentEngine(cfg, k8s_client=K8sClient(namespace=NS))
            with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value=FP):
                engine._deploy_buckets(force_legacy=True)
            anns = rec.store[("namespaces", None, NS)].metadata.annotations
            adopted = set(anns.get(ANNOTATION_ADOPTED_EMPTY_BUCKETS, "").split(","))
            assert B not in adopted
            assert {"u01-silver", "u01-gold"} <= adopted


def test_has_user_objects_survives_a_backend_ignoring_start_after():
    from lakebench.s3.client import has_user_objects

    class IgnoresStartAfter:
        def __init__(self, keys):
            self.keys = sorted(keys)

        def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000, **_kw):  # noqa: N803
            ks = [k for k in self.keys if k.startswith(Prefix)][:MaxKeys]
            return {"Contents": [{"Key": k} for k in ks], "KeyCount": len(ks)}

        def get_paginator(self, _op):
            outer = self

            class P:
                def paginate(self, Bucket, Prefix="", **_kw):  # noqa: N803
                    yield outer.list_objects_v2(Bucket, Prefix)

            return P()

    assert has_user_objects(IgnoresStartAfter([OWNER_MARKER_KEY, "zz/data"]), B) is True
    assert has_user_objects(IgnoresStartAfter([OWNER_MARKER_KEY]), B) is False


def test_stale_bronze_note_outlives_generate_for_a_later_run(tmp_path, monkeypatch):
    from lakebench.cli._helpers import load_stale_bronze, record_stale_bronze
    from tests.conftest import make_config

    monkeypatch.setattr("lakebench._constants.DEFAULT_OUTPUT_DIR", str(tmp_path))
    cfg = make_config(name=NS)
    note = {"allowed": True, "objects_before": 3, "bucket": B, "prefix": "customer/interactions"}
    record_stale_bronze(cfg, note)
    assert load_stale_bronze(cfg)["objects_before"] == 3
    # A note for another bronze bucket (the config changed) does not count.
    cfg.platform.storage.s3.buckets.bronze = "u01-bronze-2"
    assert load_stale_bronze(cfg) is None
    cfg.platform.storage.s3.buckets.bronze = B
    record_stale_bronze(cfg, None)  # a later clean generate
    assert load_stale_bronze(cfg) is None
