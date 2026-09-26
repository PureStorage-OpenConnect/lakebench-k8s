"""Destroy deletes the emptied buckets it owns, and only those (LB-159).

Destroy used to empty buckets and stop there; 136 empty ov-* buckets piled up
on FlashBlade. A bucket is deleted only when this deployment provably owns it
(ownership tag, or the name-prefix claim on backends without tagging) and
lakebench manages bucket lifecycle (``create_buckets``). Another deployment's
bucket, a --force-legacy bucket, and a pre-provisioned bucket are never
deleted.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from lakebench.deploy import destroy as destroy_mod
from lakebench.deploy.engine import DeploymentStatus
from lakebench.s3.client import S3BucketError, S3BucketVanished, S3Client


def _err(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "DeleteBucket")


class FakeBoto:
    """Enough of boto3 for empty_bucket + delete_bucket."""

    def __init__(self, buckets: dict[str, list[str]], lag: int = 0):
        self.buckets = {k: list(v) for k, v in buckets.items()}
        self.lag = lag  # DeleteBucket answers BucketNotEmpty this many times
        self.delete_bucket_calls: list[str] = []
        self.vanish: set[str] = set()  # buckets deleted by "another destroy" when listed

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    def get_paginator(self, op):
        fake = self

        class _P:
            def paginate(self, Bucket, **_kw):
                if Bucket in fake.vanish:
                    fake.buckets.pop(Bucket, None)
                    raise ClientError({"Error": {"Code": "NoSuchBucket"}}, "ListObjectsV2")
                keys = fake.buckets.get(Bucket, [])
                if op == "list_objects_v2":
                    return [{"Contents": [{"Key": k} for k in keys], "KeyCount": len(keys)}]
                return [{"Uploads": []}]

        return _P()

    def delete_objects(self, Bucket, Delete):
        drop = {o["Key"] for o in Delete["Objects"]}
        self.buckets[Bucket] = [k for k in self.buckets[Bucket] if k not in drop]
        return {}

    def delete_bucket(self, Bucket):
        self.delete_bucket_calls.append(Bucket)
        if Bucket not in self.buckets:
            raise _err("NoSuchBucket")
        if self.lag > 0:
            self.lag -= 1
            raise _err("BucketNotEmpty")
        if self.buckets[Bucket]:
            raise _err("BucketNotEmpty")
        del self.buckets[Bucket]


def _s3(boto) -> S3Client:
    c = S3Client.__new__(S3Client)
    c._client = boto
    c._init_error = None
    return c


@pytest.fixture(autouse=True)
def _no_real_sleep():
    with patch("time.sleep"):
        yield


class TestS3DeleteBucket:
    def test_deletes_empty_bucket(self):
        boto = FakeBoto({"a-bronze": []})
        assert _s3(boto).delete_bucket("a-bronze") is True
        assert "a-bronze" not in boto.buckets

    def test_already_gone(self):
        assert _s3(FakeBoto({})).delete_bucket("a-bronze") is False

    def test_listing_lag_is_retried(self):
        boto = FakeBoto({"a-bronze": []}, lag=2)
        assert _s3(boto).delete_bucket("a-bronze") is True
        assert boto.delete_bucket_calls == ["a-bronze"] * 3

    def test_refilled_bucket_is_not_re_emptied(self):
        """Data that reappears after the verified empty may be a redeploy's."""
        boto = FakeBoto({"a-bronze": ["new-deploy/part-0.parquet"]})
        with (
            patch("time.monotonic", side_effect=[0.0, 5.0, 1000.0]),
            pytest.raises(S3BucketError, match="BucketNotEmpty"),
        ):
            _s3(boto).delete_bucket("a-bronze", max_wait=10)
        assert boto.buckets["a-bronze"] == ["new-deploy/part-0.parquet"]

    def test_never_empty_raises_at_the_bound(self):
        boto = FakeBoto({"a-bronze": []}, lag=10**6)
        with patch("time.monotonic", side_effect=[0.0] + [1000.0] * 10):
            with pytest.raises(S3BucketError, match="BucketNotEmpty"):
                _s3(boto).delete_bucket("a-bronze", max_wait=120)

    def test_bucket_deleted_mid_empty_raises_vanished(self):
        """S-P4: the other destroy deletes the bucket while this one lists it."""
        boto = FakeBoto({"a-bronze": ["x"]})

        def vanish(op):
            raise ClientError({"Error": {"Code": "NoSuchBucket"}}, "ListObjectsV2")

        boto.get_paginator = vanish
        with pytest.raises(S3BucketVanished):
            _s3(boto).empty_bucket("a-bronze")

    def test_other_errors_raise(self):
        boto = FakeBoto({"a-bronze": []})
        boto.delete_bucket = MagicMock(side_effect=_err("AccessDenied"))
        with pytest.raises(S3BucketError, match="AccessDenied"):
            _s3(boto).delete_bucket("a-bronze")


class TestDeleteOwnedBuckets:
    def test_owned_deleted_unproven_kept_gone_reported(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "legacy-gold": []})
        notes, failed = destroy_mod._delete_owned_buckets(
            _s3(boto),
            ["a-bronze", "a-silver", "legacy-gold", "a-gone"],
            deletable={"a-bronze", "a-silver", "a-gone"},
            enabled=True,
            create_buckets=True,
        )
        assert not failed
        assert set(boto.buckets) == {"legacy-gold"}, "an unproven bucket must survive"
        text = " | ".join(notes)
        assert "deleted buckets: a-bronze, a-silver" in text
        assert "already gone: a-gone" in text
        assert "kept (ownership not proven" in text and "legacy-gold" in text

    def test_absent_bucket_reported_as_gone_not_kept(self):
        notes, _ = destroy_mod._delete_owned_buckets(
            _s3(FakeBoto({})),
            ["a-bronze"],
            deletable=set(),
            enabled=True,
            create_buckets=True,
            absent={"a-bronze"},
        )
        assert notes == ["already gone: a-bronze"]

    def test_pre_provisioned_buckets_are_never_deleted(self):
        boto = FakeBoto({"shared-bronze": []})
        notes, failed = destroy_mod._delete_owned_buckets(
            _s3(boto),
            ["shared-bronze"],
            deletable={"shared-bronze"},
            enabled=True,
            create_buckets=False,
        )
        assert not failed and boto.delete_bucket_calls == []
        assert "create_buckets=false" in notes[0]

    def test_keep_buckets_flag(self):
        boto = FakeBoto({"a-bronze": []})
        notes, _ = destroy_mod._delete_owned_buckets(
            _s3(boto), ["a-bronze"], deletable={"a-bronze"}, enabled=False, create_buckets=True
        )
        assert boto.delete_bucket_calls == [] and "--keep-buckets" in notes[0]

    def test_delete_failure_is_reported(self):
        boto = FakeBoto({"a-bronze": []})
        boto.delete_bucket = MagicMock(side_effect=_err("AccessDenied"))
        notes, failed = destroy_mod._delete_owned_buckets(
            _s3(boto), ["a-bronze"], deletable={"a-bronze"}, enabled=True, create_buckets=True
        )
        assert failed
        assert "emptied but NOT deleted" in " ".join(notes)


class TestDestroyAllBuckets:
    """End to end through destroy_all's bucket step with verdicts per bucket."""

    def _run_layers(self, boto, bronze, silver, gold):
        self._layers = (bronze, silver, gold)
        try:
            return self._run(boto, dict.fromkeys(self._layers, "MATCH"))
        finally:
            self._layers = None

    _layers: tuple[str, str, str] | None = None

    def _run(self, boto, verdicts, *, create_buckets=True, force_legacy=False, other=()):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        engine = MagicMock()
        cfg = engine.config
        cfg.name = "a"
        cfg.get_namespace.return_value = "a"
        cfg.platform.kubernetes.create_namespace = False
        cfg.platform.kubernetes.context = ""
        cfg.observability.enabled = False
        s3_cfg = cfg.platform.storage.s3
        bronze, silver, gold = self._layers or ("a-bronze", "a-silver", "a-gold")
        s3_cfg.buckets.bronze = bronze
        s3_cfg.buckets.silver = silver
        s3_cfg.buckets.gold = gold
        s3_cfg.create_buckets = create_buckets
        engine.k8s.namespace_exists.return_value = True

        def verify(_boto, bucket, _name):
            return IdentityReport(
                verdict=getattr(IdentityVerdict, verdicts[bucket]),
                resource_name=bucket,
                expected_deployment="a",
                hint=f"{bucket} verdict {verdicts[bucket]}",
            )

        ns_match = IdentityReport(
            verdict=IdentityVerdict.MATCH, resource_name="a", expected_deployment="a"
        )
        with (
            patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=ns_match),
            patch("lakebench.deploy.ownership.verify_bucket_ownership", side_effect=verify),
            patch(
                "lakebench.deploy.ownership.list_lakebench_deployment_names",
                return_value=list(other),
            ),
            patch("lakebench.k8s.get_k8s_client"),
            patch("kubernetes.client.CoreV1Api") as core,
            patch("kubernetes.client.CustomObjectsApi"),
            patch("lakebench.deploy.destroy.logger"),
            patch("lakebench.s3.S3Client", return_value=_s3(boto)),
        ):
            core.return_value.list_namespace.return_value.items = []
            results = destroy_mod.destroy_all(engine, clean_buckets=True, force_legacy=force_legacy)
        self._results = results
        return [r for r in results if r.component == "s3-buckets"][-1]

    def test_owned_by_tag_and_prefix_are_deleted(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(boto, {"a-bronze": "MATCH", "a-silver": "UNSUPPORTED", "a-gold": "MATCH"})
        assert r.status is DeploymentStatus.SUCCESS
        assert boto.buckets == {}
        assert "deleted buckets: a-bronze, a-silver, a-gold" in r.message

    def test_bucket_shared_by_two_layers_is_deleted_once(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-gold": []})
        boto_calls = boto.delete_bucket_calls
        r = self._run_layers(boto, bronze="a-bronze", silver="a-bronze", gold="a-gold")
        assert r.status is DeploymentStatus.SUCCESS
        assert boto_calls == ["a-bronze", "a-gold"]
        assert "already gone" not in r.message

    def test_bucket_vanishing_mid_empty_stops_the_bucket_step(self):
        """S-P4: the other destroy deleted a bucket; do not touch the rest.

        A redeploy may re-create the names before this slow run gets to them.
        """
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": ["y"], "a-gold": ["z"]})
        boto.vanish = {"a-bronze"}
        r = self._run(boto, {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "MATCH"})
        assert r.status is DeploymentStatus.FAILED, "unemptied buckets must not read as success"
        assert "concurrent destroy" in r.message
        assert self._results[-1].component == "namespace"
        assert "Stopped before infrastructure teardown" in self._results[-1].message
        assert boto.buckets == {"a-silver": ["y"], "a-gold": ["z"]}
        assert boto.delete_bucket_calls == []

    def test_foreign_bucket_refuses_and_nothing_is_deleted(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(boto, {"a-bronze": "MATCH", "a-silver": "MISMATCH", "a-gold": "MATCH"})
        assert r.status is DeploymentStatus.FAILED
        assert boto.delete_bucket_calls == []
        assert boto.buckets["a-bronze"] == ["x"], "a refused step must not empty anything"

    def test_force_legacy_buckets_are_emptied_but_kept(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "ABSENT", "a-silver": "MATCH", "a-gold": "MATCH"},
            force_legacy=True,
        )
        assert r.status is DeploymentStatus.SUCCESS
        assert set(boto.buckets) == {"a-bronze"} and boto.buckets["a-bronze"] == []
        assert "kept (ownership not proven" in r.message

    def test_pre_provisioned_buckets_are_emptied_but_kept(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "MATCH"},
            create_buckets=False,
        )
        assert boto.delete_bucket_calls == []
        assert "create_buckets=false" in r.message

    def test_prefix_claim_lost_to_longer_prefix_is_not_deleted(self):
        """Another deployment 'a-silver...' has a longer claim; refuse, delete nothing."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "UNSUPPORTED", "a-silver": "UNSUPPORTED", "a-gold": "UNSUPPORTED"},
            other=["a-silver"],
        )
        assert r.status is DeploymentStatus.FAILED
        assert boto.delete_bucket_calls == []
