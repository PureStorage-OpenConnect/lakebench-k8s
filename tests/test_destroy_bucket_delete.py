"""Destroy deletes the emptied buckets it owns, and only those.

A bucket is deleted only when this deployment provably owns it (ownership
tag, or the name-prefix claim on backends without tagging) and lakebench
manages bucket lifecycle (``create_buckets``). Another deployment's bucket, a
--force-legacy bucket, a pre-provisioned bucket and an adopted bucket are
emptied at most, never deleted.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from lakebench.deploy import destroy as destroy_mod
from lakebench.deploy.engine import DeploymentStatus
from lakebench.s3.client import S3BucketError, S3BucketVanished
from tests.fixtures.destroy_bucket_helpers import (
    DestroyAllBucketsHarness as DestroyAllBucketsHarness,
)
from tests.fixtures.destroy_bucket_helpers import FakeBoto as FakeBoto
from tests.fixtures.destroy_bucket_helpers import _err as _err
from tests.fixtures.destroy_bucket_helpers import _s3 as _s3


@pytest.fixture(autouse=True)
def _no_real_sleep():
    with patch("time.sleep"):
        yield


class FakeClock:
    """One clock for time.monotonic and time.sleep: sleeping advances it."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, seconds)


@pytest.fixture
def clock():
    c = FakeClock()
    with patch("time.monotonic", c.monotonic), patch("time.sleep", c.sleep):
        yield c


class TestS3DeleteBucket:
    def test_deletes_empty_bucket(self):
        boto = FakeBoto({"a-bronze": []})
        assert _s3(boto).delete_bucket("a-bronze") is True
        assert "a-bronze" not in boto.buckets

    def test_already_gone(self):
        assert _s3(FakeBoto({})).delete_bucket("a-bronze") is False

    def test_listing_lag_is_retried_until_the_bucket_is_deleted(self, clock):
        boto = FakeBoto({"a-bronze": []}, lag=2)
        assert _s3(boto).delete_bucket("a-bronze") is True
        assert boto.buckets == {}

    def test_refilled_bucket_is_not_re_emptied(self, clock):
        """Data that reappears after the verified empty may be a redeploy's."""
        boto = FakeBoto({"a-bronze": ["new-deploy/part-0.parquet"]})
        with pytest.raises(S3BucketError, match="BucketNotEmpty"):
            _s3(boto).delete_bucket("a-bronze", max_wait=10)
        assert boto.buckets["a-bronze"] == ["new-deploy/part-0.parquet"]

    def test_never_empty_raises_at_the_bound(self, clock):
        boto = FakeBoto({"a-bronze": []}, lag=10**6)
        with pytest.raises(S3BucketError, match="BucketNotEmpty"):
            _s3(boto).delete_bucket("a-bronze", max_wait=120)
        assert boto.buckets == {"a-bronze": []}

    def test_bucket_deleted_mid_empty_raises_vanished(self):
        """The other destroy deletes the bucket while this one lists it."""
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


class TestDestroyAllBuckets(DestroyAllBucketsHarness):
    """End to end through destroy_all's bucket step with verdicts per bucket."""

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

    def test_foreign_bucket_is_left_alone_and_the_rest_still_cleaned(self):
        """One refusal no longer stops the deployment's own buckets."""
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": ["theirs"], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MISMATCH", "a-gold": "MATCH"},
            create_namespace=True,
        )
        assert r.status is DeploymentStatus.FAILED
        assert boto.buckets == {"a-silver": ["theirs"]}, "only the refused bucket survives"
        assert "a-silver" not in boto.delete_bucket_calls
        assert "Bucket ownership refused" in r.message
        assert r.details["refusal"] == "deploy.identity_foreign"  # exit 3 (CLI-1)
        # a-silver was on the record (the harness default) but another
        # deployment's tag proves it is not ours now: it leaves the record and
        # the namespace can go.
        assert ["a-silver"] in [c.args[2] for c in self.forget.call_args_list]
        assert self.engine.k8s.delete_namespace.called

    def test_force_legacy_buckets_are_emptied_but_kept(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "ABSENT", "a-silver": "MATCH", "a-gold": "MATCH"},
            force_legacy=True,
        )
        assert r.status is DeploymentStatus.SUCCESS
        assert set(boto.buckets) == {"a-bronze"} and boto.buckets["a-bronze"] == []
        assert "provenance unknown" in r.message

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
        """Another deployment 'a-silver' has the longer claim on a-silver only."""
        boto = FakeBoto({"a-bronze": [], "a-silver": ["theirs"], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "UNSUPPORTED", "a-silver": "UNSUPPORTED", "a-gold": "UNSUPPORTED"},
            other=["a-silver"],
        )
        assert r.status is DeploymentStatus.FAILED
        assert "a-silver" not in boto.delete_bucket_calls
        assert boto.buckets == {"a-silver": ["theirs"]}

    def test_adopted_bucket_is_emptied_but_kept(self):
        """MATCH by tag (e.g. adopted with deploy --force-legacy) is not creation."""
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "MATCH"},
            created={"a-silver"},
        )
        assert r.status is DeploymentStatus.SUCCESS
        assert set(boto.buckets) == {"a-bronze", "a-gold"}
        assert boto.buckets["a-bronze"] == [], "adopted buckets are still emptied"
        assert "provenance unknown" in r.message

    # -- Tagless backends (FlashBlade): the name alone is not ownership -------

    def test_tagless_unrecorded_bucket_with_matching_name_is_not_emptied(self):
        """A user's own bucket named like a lakebench one (a-bronze), adopted by
        deploy on a backend without tagging, must survive destroy: nothing but
        the name says lakebench made it. A tagged backend would have demanded
        --force-legacy at deploy; this one never asked."""
        boto = FakeBoto({"a-bronze": ["user data"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "UNSUPPORTED"),
            created={"a-silver", "a-gold"},
        )
        assert boto.buckets == {"a-bronze": ["user data"]}
        assert "a-bronze" not in boto.delete_bucket_calls
        assert r.status is DeploymentStatus.FAILED
        assert "lists neither as created nor as adopted while empty" in r.message
        assert "a-bronze" in r.message

    def test_tagless_bucket_in_a_16_adopted_empty_record_is_kept(self):
        """SAF-10: 1.6 recorded a bucket as adopted while empty even when it was
        another cluster's bucket not yet written (the cross-cluster hole), so
        that record proves nothing now. A 1.7 adoption carries an owner marker
        instead (tests/test_bucket_fingerprint_matrix.py). Before 1.7 this
        bucket was emptied."""
        boto = FakeBoto({"a-bronze": ["theirs"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "UNSUPPORTED"),
            created={"a-silver", "a-gold"},
            adopted_empty={"a-bronze"},
        )
        assert boto.buckets == {"a-bronze": ["theirs"]}
        assert "a-bronze" not in boto.delete_bucket_calls
        assert r.status is DeploymentStatus.FAILED
        assert "a-bronze" in r.message

    def test_tagless_unrecorded_bucket_is_emptied_only_on_force_legacy_never_deleted(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "UNSUPPORTED"),
            created={"a-silver", "a-gold"},
            force_legacy=True,
        )
        assert boto.buckets == {"a-bronze": []}
        assert "a-bronze" not in boto.delete_bucket_calls
        assert r.status is DeploymentStatus.SUCCESS, r.message

    def test_tagless_sibling_with_longer_prefix_keeps_its_recorded_buckets(self):
        """lb16 vs lb16-base near miss: lb16's config (or a stale record) names
        lb16-base-bronze, which lb16-base, live on the cluster, owns. Even a
        created-record entry for it must not let lb16 empty or delete it."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "a-base-bronze": ["B"]})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold", "a-base-bronze"], "UNSUPPORTED"),
            created={"a-bronze", "a-silver", "a-gold", "a-base-bronze"},
            other=["a-base"],
            create_namespace=True,
        )
        assert boto.buckets == {"a-base-bronze": ["B"]}
        assert "a-base-bronze" not in boto.delete_bucket_calls
        assert r.status is DeploymentStatus.FAILED

    def test_tagless_unrecorded_with_unreadable_record_keeps_the_namespace(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "UNSUPPORTED"),
            created_error=RuntimeError("apiserver 503"),
            create_namespace=True,
        )
        assert boto.buckets["a-bronze"] == ["x"]
        self.engine.k8s.delete_namespace.assert_not_called()

    def test_created_tag_marks_a_bucket_for_deletion(self):
        from lakebench.deploy.ownership import TAG_CREATED_BY_LAKEBENCH, TAG_DEPLOYMENT_NAME

        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        boto.get_bucket_tagging = lambda Bucket: {
            "TagSet": [{"Key": TAG_DEPLOYMENT_NAME, "Value": "a"}]
            + ([{"Key": TAG_CREATED_BY_LAKEBENCH, "Value": "true"}] if Bucket == "a-gold" else [])
        }
        self._run(boto, dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"), created=set())
        assert boto.delete_bucket_calls == ["a-gold"]

    # -- UID re-check around bucket data (silent data loss guard) ---------

    def test_redeploy_before_emptying_leaves_its_buckets_alone(self):
        """D1 finished, R redeployed and wrote bronze; slow D2 must not empty it."""
        boto = FakeBoto({"a-bronze": ["r/bronze-0"], "a-silver": [], "a-gold": []})
        uids = iter(["uid-1"] * self.PRE)
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, "uid-2"),
        )
        assert boto.buckets["a-bronze"] == ["r/bronze-0"]
        assert boto.delete_bucket_calls == []
        assert "newer deployment" in r.message
        assert "newer deployment" in self._results[-1].message
        assert r.details["refusal"] == "destroy.redeployed"

    def test_redeploy_between_buckets_stops_mid_step(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": ["r/new"], "a-gold": ["r/new"]})
        # pre-bucket reads, before-loop, before bronze, bronze batch; then R.
        uids = iter(["uid-1"] * (self.PRE + 3))
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, "uid-2"),
        )
        assert boto.buckets["a-bronze"] == []
        assert boto.buckets["a-silver"] == ["r/new"] and boto.buckets["a-gold"] == ["r/new"]
        assert boto.delete_bucket_calls == []

    def test_redeploy_while_a_bucket_is_being_emptied_stops_before_the_next_batch(self):
        """A large bucket empties over many batches; R can land mid-bucket."""
        boto = FakeBoto({"a-bronze": ["r/new"], "a-silver": [], "a-gold": []})
        # pre-bucket reads, before-loop, before bronze; R before the first batch.
        uids = iter(["uid-1"] * (self.PRE + 2))
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, "uid-2"),
        )
        assert boto.buckets["a-bronze"] == ["r/new"]
        assert boto.delete_bucket_calls == []

    def test_redeploy_before_bucket_delete_stops_deletes(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        # pre-bucket reads, before-loop, three empties; R lands before deletes.
        uids = iter(["uid-1"] * (self.PRE + 4))
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, "uid-2"),
        )
        assert boto.delete_bucket_calls == []
        assert set(boto.buckets) == {"a-bronze", "a-silver", "a-gold"}

    def test_absent_then_present_namespace_stops_force_legacy_wipe(self):
        """Namespace absent at start (--force-legacy by name), then R creates it."""
        boto = FakeBoto({"a-bronze": ["r/new"], "a-silver": [], "a-gold": []})
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            force_legacy=True,
            namespace_present=False,
            uid=lambda _ns: "uid-r",
        )
        assert boto.buckets["a-bronze"] == ["r/new"]
        assert boto.delete_bucket_calls == []

    def test_unreadable_uid_during_bucket_step_keeps_everything(self):
        calls = {"n": 0}

        def uid(_ns):
            calls["n"] += 1
            if calls["n"] >= self.PRE + 1:
                raise RuntimeError("apiserver 503")
            return "uid-1"

        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(boto, dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"), uid=uid)
        assert r.status is DeploymentStatus.FAILED
        assert boto.buckets["a-bronze"] == ["x"]
        assert self._results[-1].status is DeploymentStatus.FAILED

    def test_failed_bucket_delete_keeps_the_namespace(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        boto.delete_bucket = MagicMock(side_effect=_err("AccessDenied"))
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            create_namespace=True,
        )
        assert r.status is DeploymentStatus.FAILED
        assert "--force-legacy" not in r.message
        self.engine.k8s.delete_namespace.assert_not_called()
        ns = [x for x in self._results if x.component == "namespace"][-1]
        assert "NOT deleted" in ns.message

    # -- CLI-1: a refusal exits 3 only when nothing else in the step failed --

    def test_refusal_with_a_retryable_failure_is_not_a_refusal(self):
        """A foreign bucket plus an owned bucket whose delete failed: re-running
        destroy can fix the second, so the step must not exit 3."""
        boto = FakeBoto({"a-bronze": [], "a-silver": ["theirs"], "a-gold": []})
        boto.delete_bucket = MagicMock(side_effect=_err("AccessDenied"))
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MISMATCH", "a-gold": "MATCH"},
            create_namespace=True,
        )
        assert r.status is DeploymentStatus.FAILED
        assert "Bucket ownership refused" in r.message
        assert "refusal" not in r.details
        assert self._exit_code_of_bucket_and_namespace() is None  # exit 1

    def test_refused_untagged_recorded_bucket_keeps_the_namespace_as_a_refusal(self):
        """The namespace kept as a refused bucket's record follows the refusal."""
        from lakebench.exit_codes import ExitCode

        boto = FakeBoto({"a-bronze": [], "a-silver": ["x"], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "ABSENT", "a-gold": "MATCH"},
            create_namespace=True,
        )
        assert r.details.get("refusal") == "deploy.identity_foreign", r.message
        ns = [x for x in self._results if x.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED and "NOT deleted" in ns.message
        assert ns.details.get("follows_refusal") is True
        assert self._exit_code_of_bucket_and_namespace() == ExitCode.REFUSED

    # -- second full review ------------------------------------------------

    def test_deleted_buckets_are_dropped_from_the_created_record(self):
        """A namespace that outlives destroy must stop claiming deleted names,
        or a bucket later pre-provisioned under one is deleted next time."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        self._run(boto, dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"))
        self.forget.assert_called_once()
        assert self.forget.call_args.args[1:] == ("a", ["a-bronze", "a-silver", "a-gold"])

    def test_unreadable_created_record_fails_and_keeps_the_namespace(self):
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "UNSUPPORTED"),
            created_error=RuntimeError("apiserver 503"),
            create_namespace=True,
        )
        assert r.status is DeploymentStatus.FAILED
        assert "record unreadable" in r.message
        assert set(boto.buckets) == {"a-bronze", "a-silver", "a-gold"}
        self.engine.k8s.delete_namespace.assert_not_called()

    def test_namespace_deleted_mid_bucket_step_is_failed_not_success(self):
        """Nothing proves another destroy finished the rest (kubectl delete ns
        looks the same) and the ownership record is gone."""
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": ["y"], "a-gold": ["z"]})
        # pre-bucket reads, before-loop, before bronze, bronze batch; then gone.
        uids = iter(["uid-1"] * (self.PRE + 3))
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, ""),
        )
        assert r.status is DeploymentStatus.FAILED
        assert "may still hold data" in r.message
        assert any(x.component == "secretclass" for x in self._results), (
            "the skipped cluster-scoped SecretClasses must be named"
        )
        assert boto.buckets["a-silver"] == ["y"] and boto.buckets["a-gold"] == ["z"]
        assert boto.delete_bucket_calls == []

    def test_created_record_is_not_edited_after_a_redeploy(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        # start, before-loop, three empties, three deletes; R lands before
        # the record update.
        uids = iter(["uid-1"] * (self.PRE + 7))
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, "uid-2"),
        )
        assert boto.buckets == {}
        self.forget.assert_not_called()

    # -- third review: guards before steps 1-3 and the deploy nonce --------

    def test_redeploy_before_step_1_keeps_its_spark_jobs(self):
        boto = FakeBoto({"a-bronze": ["r/new"], "a-silver": [], "a-gold": []})
        uids = iter(["uid-1"])  # start; R lands before step 1
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            uid=lambda _ns: next(uids, "uid-2"),
        )
        assert not self.custom.return_value.delete_namespaced_custom_object.called
        assert boto.buckets["a-bronze"] == ["r/new"]
        assert r.status is DeploymentStatus.FAILED
        assert "NOT completed" in r.message

    def test_redeploy_during_table_cleanup_stops_before_the_next_drop(self):
        """DROP TABLE must not run against R."""
        boto = FakeBoto({"a-bronze": ["r/new"], "a-silver": [], "a-gold": []})
        state = {"uid": "uid-1"}
        ran: list[str] = []

        def on_sql(sql):
            ran.append(sql)
            state["uid"] = "uid-2"  # R lands while the first statement runs

        self._on_sql = on_sql
        try:
            r = self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                uid=lambda _ns: state["uid"],
                maint=("trino", "trino-coordinator-0", "lakehouse"),
            )
        finally:
            self._on_sql = None
        assert ran == [self.UNREG_SILVER]
        assert boto.buckets["a-bronze"] == ["r/new"]
        assert r.status is DeploymentStatus.FAILED

    def test_redeploy_into_the_same_namespace_is_seen_by_the_nonce(self):
        """create_namespace=false (or a still-Active namespace): the UID never
        changes, only the deploy nonce does. R's buckets must survive."""
        boto = FakeBoto({"a-bronze": ["r/new"], "a-silver": [], "a-gold": []})
        reads = {"n": 0}

        def nonce(_ns, _key):
            reads["n"] += 1
            return "n-1" if reads["n"] <= self.PRE else "n-2"

        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            nonce=nonce,
        )
        assert boto.buckets["a-bronze"] == ["r/new"]
        assert boto.delete_bucket_calls == []

    def test_failed_record_cleanup_is_reported_failed(self):
        """A stale created-buckets record outlives destroy when the namespace
        is kept (create_namespace=false); that must not read as success."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
            forget_error=RuntimeError("apiserver 503"),
        )
        assert boto.buckets == {}
        assert r.status is DeploymentStatus.FAILED
        assert "still listed as created" in r.message

    # -- fourth review ----------------------------------------------------

    def test_rerun_forgets_recorded_buckets_that_are_already_gone(self):
        """After a failed record update the buckets are gone; a re-run must
        still take them off the record (create_namespace=false keeps it)."""
        boto = FakeBoto({"a-gold": []})
        self._run(
            boto,
            {"a-bronze": "NOT_FOUND", "a-silver": "NOT_FOUND", "a-gold": "MATCH"},
            created={"a-bronze", "a-silver", "a-gold"},
        )
        self.forget.assert_called_once()
        assert sorted(self.forget.call_args.args[2]) == ["a-bronze", "a-gold", "a-silver"]

    def test_unreadable_uid_before_record_update_is_failed(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        calls = {"n": 0}

        def uid(_ns):
            calls["n"] += 1
            # pre-bucket reads, before-loop, three empties, three deletes;
            # the read guarding the record update fails.
            if calls["n"] == self.PRE + 8:
                raise RuntimeError("apiserver 503")
            return "uid-1"

        r = self._run(boto, dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"), uid=uid)
        assert boto.buckets == {}
        self.forget.assert_not_called()
        assert r.status is DeploymentStatus.FAILED
        assert "still listed as created" in r.message

    def test_failed_table_statements_fail_the_step(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})

        def fail_drop(sql):
            if sql.startswith("CALL"):
                raise RuntimeError("Query failed: coordinator connection reset")

        self._on_sql = fail_drop
        try:
            self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                maint=("trino", "trino-coordinator-0", "lakehouse"),
            )
        finally:
            self._on_sql = None
        tables = [x for x in self._results if x.component == "table-cleanup"][-1]
        assert tables.status is DeploymentStatus.FAILED
        assert "2 failed statement" in tables.message
        assert boto.buckets == {}, "later steps still run"

    # -- final review -------------------------------------------------------

    def test_partial_deployment_with_missing_tables_is_a_clean_teardown(self):
        """Deploy-only or failed-before-gold: the tables were never written."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})

        def missing(sql):
            # Real Trino CLI output, as exec_sql wraps it.
            if "gold" in sql and sql.startswith("DROP"):
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 20260926_101010_00002_abcde failed: "
                    "line 1:1: Schema 'gold' does not exist"
                )
            if "gold" in sql:
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 20260926_101010_00001_abcde failed: "
                    "line 1:13: Table 'lakehouse.gold.t' does not exist"
                )

        self._on_sql = missing
        try:
            self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                maint=("trino", "trino-coordinator-0", "lakehouse"),
            )
        finally:
            self._on_sql = None
        tables = [x for x in self._results if x.component == "table-cleanup"][-1]
        assert tables.status is DeploymentStatus.SUCCESS, tables.message
        # Maintenance on a large table runs for minutes; 30 s killed it.
        assert self.sql_timeouts and min(self.sql_timeouts) >= 600

    def test_stale_absent_verdict_is_not_forgotten(self):
        """The tag read said NoSuchBucket but the bucket is there: keep it on
        the record (and the namespace) so a re-run deletes it."""
        boto = FakeBoto({"a-bronze": ["x"], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "NOT_FOUND", "a-silver": "NOT_FOUND", "a-gold": "MATCH"},
            created={"a-bronze", "a-silver", "a-gold"},
            create_namespace=True,
        )
        assert sorted(self.forget.call_args.args[2]) == ["a-gold", "a-silver"]
        assert "a-bronze" in boto.buckets
        assert r.status is DeploymentStatus.FAILED
        assert "not confirmed gone" in r.message
        self.engine.k8s.delete_namespace.assert_not_called()

    def test_missing_catalog_fails_table_cleanup(self):
        """Only table/schema-missing is a clean skip; a missing catalog is not."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})

        def no_catalog(sql):
            raise RuntimeError(
                "exec_sql failed (rc=1): Query 20260926_101010_00003_abcde failed: "
                "line 1:13: Catalog 'lakehouse' not found"
            )

        self._on_sql = no_catalog
        try:
            self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                maint=("trino", "trino-coordinator-0", "lakehouse"),
            )
        finally:
            self._on_sql = None
        tables = [x for x in self._results if x.component == "table-cleanup"][-1]
        assert tables.status is DeploymentStatus.FAILED

    # -- full review of f9dfda6: bound the table step ------------------------

    def test_first_statement_timeout_stops_the_table_step(self):
        from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

        ran: list[str] = []

        def hang(sql):
            ran.append(sql)
            raise ExecSqlTimeout("exec_sql timed out after 600s (may still be running)")

        tables = self._run_tables(hang)
        assert ran == [self.UNREG_SILVER]
        assert tables.status is DeploymentStatus.FAILED
        assert "not attempted" in tables.message and "1 statement(s)" in tables.message

    def test_table_step_has_an_overall_cap(self, monkeypatch):
        clock = {"t": 0.0}

        def tick():
            clock["t"] += 1000.0  # each statement "takes" 1000 s
            return clock["t"]

        monkeypatch.setattr(destroy_mod, "_monotonic", tick)
        ran: list[str] = []
        tables = self._run_tables(ran.append)
        assert ran == [self.UNREG_SILVER]
        assert tables.status is DeploymentStatus.FAILED
        assert "cap" in tables.message

    def test_combined_statement_is_labelled_by_its_operative_statement(self):
        assert destroy_mod._operative_sql("SET SESSION x = '0s'; CALL c.system.vacuum()") == "CALL"
        assert destroy_mod._operative_sql("SET a=b; VACUUM t RETAIN 0 HOURS") == "VACUUM"
        assert destroy_mod._operative_sql("DROP TABLE IF EXISTS t") == "DROP"

    def test_iceberg_destroy_skips_snapshot_and_orphan_maintenance(self):
        """Trino only unregisters (a DROP deletes every referenced
        file); no maintenance statement runs."""
        ran: list[str] = []
        tables = self._run_tables(ran.append, table_format="iceberg")
        assert ran == [self.UNREG_SILVER, self.UNREG_GOLD]
        assert tables.status is DeploymentStatus.SUCCESS
        assert "maintenance skipped" in tables.message

    def test_delta_destroy_missing_table_is_a_clean_teardown(self):
        def missing(sql):
            if "'gold'" in sql:
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 20260926_101010_00001_abcde failed: "
                    "line 1:7: Table 'lakehouse.gold.t' does not exist"
                )

        tables = self._run_tables(missing, table_format="delta")
        assert tables.status is DeploymentStatus.SUCCESS, tables.message

    def test_delta_destroy_skips_vacuum_and_only_drops(self):
        """Policy 2026-09-26: the buckets are emptied and deleted right after."""
        ran: list[str] = []
        tables = self._run_tables(ran.append, table_format="delta")
        assert ran == [self.UNREG_SILVER, self.UNREG_GOLD]
        assert tables.status is DeploymentStatus.SUCCESS
        assert "maintenance skipped" in tables.message

    # -- Polaris via Trino: DROP always requests purge, Polaris refuses it ----

    def test_polaris_via_trino_skips_drops_and_succeeds(self):
        """Live lb16-aml-batch: all 17 drops failed ("Failed to drop table")
        because Trino drops with purge and Polaris refuses it; destroy exited
        1 although everything was removed. No statement may be sent (enabling
        purge would delete add_files-registered datagen files)."""
        ran: list[str] = []

        def refuse(sql):
            ran.append(sql)
            if sql.startswith(("DROP", "CALL")):
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 20260927_043036_00175_hj3h7 failed: "
                    "Failed to drop table 'pacs008_raw'"
                )

        _r, tables, boto = self._run_catalog(
            "polaris", ("trino", "trino-coordinator-0", "lakehouse"), refuse
        )
        assert ran == []
        assert tables.status is DeploymentStatus.SUCCESS, tables.message
        assert "not unregistered" in tables.message and "Polaris" in tables.message
        assert boto.buckets == {}, "the bucket step still runs"

    def test_polaris_kept_namespace_still_unregisters(self):
        """create_namespace=false keeps the namespace and the PostgreSQL PVC,
        so the catalog entries would outlive destroy: they are unregistered
        (a non-purge drop, never a purge) and a refusal must fail."""
        ran: list[str] = []

        def refuse(sql):
            ran.append(sql)
            raise RuntimeError("exec_sql failed (rc=1): Failed to drop table 't'")

        _r, tables, _b = self._run_catalog(
            "polaris",
            ("trino", "trino-coordinator-0", "lakehouse"),
            refuse,
            create_namespace=False,
        )
        assert ran == [self.UNREG_SILVER, self.UNREG_GOLD]
        assert tables.status is DeploymentStatus.FAILED

    def test_polaris_via_spark_thrift_still_drops(self):
        """Spark's plain DROP does not purge, so Polaris accepts it."""
        ran: list[str] = []
        _r, tables, _b = self._run_catalog(
            "polaris", ("spark-thrift", "thrift-0", "lakehouse"), ran.append
        )
        assert ran == ["DROP lakehouse.silver.t", "DROP lakehouse.gold.t"]
        assert tables.status is DeploymentStatus.SUCCESS

    def test_hive_via_trino_drop_failures_still_fail_the_step(self):
        def fail(sql):
            raise RuntimeError("exec_sql failed (rc=1): Failed to drop table 't'")

        _r, tables, _b = self._run_catalog(
            "hive", ("trino", "trino-coordinator-0", "lakehouse"), fail
        )
        assert tables.status is DeploymentStatus.FAILED

    # -- Config buckets UNION recorded buckets ------------------------

    def test_recorded_bucket_from_an_earlier_config_is_deleted(self):
        """Live: ov-sp-a-bronze recorded, config bronze was ov-sp-a-shared."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "a-old": []})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold", "a-old"], "UNSUPPORTED"),
            created={"a-bronze", "a-silver", "a-gold", "a-old"},
        )
        assert r.status is DeploymentStatus.SUCCESS, r.message
        assert boto.buckets == {}
        assert "a-old" in self.forget.call_args.args[2]

    def test_non_empty_recorded_only_bucket_is_never_emptied_on_name_alone(self):
        """Review: on FlashBlade another deployment may be using it now."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "a-old": ["B data"]})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold", "a-old"], "UNSUPPORTED"),
            created={"a-bronze", "a-silver", "a-gold", "a-old"},
            create_namespace=True,
        )
        assert boto.buckets == {"a-old": ["B data"]}
        assert r.status is DeploymentStatus.FAILED
        assert "not named by this one, and not empty" in r.message
        self.engine.k8s.delete_namespace.assert_not_called()

    def test_recorded_bucket_with_a_matching_tag_is_emptied_and_deleted(self):
        """With tagging the tag is authoritative: recorded-only MATCH is ours."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "a-old": ["x"]})
        r = self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold", "a-old"], "MATCH"),
            created={"a-bronze", "a-silver", "a-gold", "a-old"},
        )
        assert r.status is DeploymentStatus.SUCCESS, r.message
        assert boto.buckets == {}

    def test_force_legacy_never_covers_a_recorded_only_bucket(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "shared-x": ["other"]})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "MATCH", "shared-x": "ABSENT"},
            created={"a-bronze", "a-silver", "a-gold", "shared-x"},
            force_legacy=True,
        )
        assert boto.buckets == {"shared-x": ["other"]}
        assert r.status is DeploymentStatus.FAILED
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "shared-y": ["other"]})
        self._run(
            boto,
            {
                "a-bronze": "MATCH",
                "a-silver": "MATCH",
                "a-gold": "MATCH",
                "shared-y": "UNSUPPORTED",
            },
            created={"a-bronze", "a-silver", "a-gold", "shared-y"},
            force_legacy=True,
        )
        assert boto.buckets == {"shared-y": ["other"]}

    def test_recorded_bucket_now_tagged_to_another_deployment_leaves_the_record(self):
        """MISMATCH is proof it is someone else's now: drop it from the record
        rather than keep the namespace forever."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "a-old": ["theirs"]})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "MATCH", "a-old": "MISMATCH"},
            created={"a-bronze", "a-silver", "a-gold", "a-old"},
            create_namespace=True,
        )
        assert boto.buckets == {"a-old": ["theirs"]}
        assert r.status is DeploymentStatus.FAILED
        assert "dropped from this deployment's record: a-old" in r.message
        forgotten = [c.args[2] for c in self.forget.call_args_list]
        assert ["a-old"] in forgotten
        assert self.engine.k8s.delete_namespace.called

    def test_refused_bucket_not_in_the_record_does_not_keep_the_namespace(self):
        """S-P6: B's bronze is A's shared bucket; B never created it."""
        boto = FakeBoto({"a-bronze": ["A data"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "MISMATCH", "a-silver": "MATCH", "a-gold": "MATCH"},
            created={"a-silver", "a-gold"},
            create_namespace=True,
        )
        assert r.status is DeploymentStatus.FAILED
        assert boto.buckets == {"a-bronze": ["A data"]}
        assert self.engine.k8s.delete_namespace.called

    def test_recorded_bucket_is_only_considered_when_the_namespace_exists(self):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": [], "a-old": ["x"]})
        self._run(
            boto,
            dict.fromkeys(["a-bronze", "a-silver", "a-gold", "a-old"], "MATCH"),
            created={"a-old"},
            force_legacy=True,
            namespace_present=False,
            uid=lambda _ns: "",
        )
        assert boto.buckets["a-old"] == ["x"]
