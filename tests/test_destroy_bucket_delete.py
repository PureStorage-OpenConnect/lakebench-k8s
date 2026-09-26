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

    def get_bucket_tagging(self, Bucket):
        # Owned but not marked created (adopted); tests override as needed.
        return {"TagSet": [{"Key": "lakebench.deployment", "Value": "a"}]}

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

    def test_default_retry_budget_covers_flashblade_ghost_counts(self):
        import inspect

        assert inspect.signature(S3Client.delete_bucket).parameters["max_wait"].default >= 300

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
        assert "provenance unknown" in text and "legacy-gold" in text

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

    _on_sql = None
    sql_timeouts: list[int] = []

    def _exec(self, _engine, _k8s, _pod, _ns, sql, timeout=30):
        self.sql_timeouts.append(timeout)
        if self._on_sql:
            self._on_sql(sql)

    # Incarnation reads before the bucket step: start, then the guards before
    # step 1 (Spark jobs), step 2 (pods), step 2b (datagen), step 3 (tables).
    PRE = 5

    def _run_layers(self, boto, bronze, silver, gold):
        self._layers = (bronze, silver, gold)
        try:
            return self._run(boto, dict.fromkeys(self._layers, "MATCH"))
        finally:
            self._layers = None

    _layers: tuple[str, str, str] | None = None

    def _run(
        self,
        boto,
        verdicts,
        *,
        create_buckets=True,
        force_legacy=False,
        other=(),
        created=None,
        uid=None,
        namespace_present=True,
        create_namespace=False,
        created_error=None,
        nonce=None,
        maint=None,
        forget_error=None,
    ):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        self.sql_timeouts = []
        engine = MagicMock()
        cfg = engine.config
        cfg.name = "a"
        cfg.get_namespace.return_value = "a"
        cfg.platform.kubernetes.create_namespace = create_namespace
        cfg.platform.compute.spark.operator.namespace = "spark-operator"
        cfg.platform.compute.spark.operator.version = "2.5.1"
        cfg.platform.kubernetes.context = ""
        cfg.observability.enabled = False
        s3_cfg = cfg.platform.storage.s3
        bronze, silver, gold = self._layers or ("a-bronze", "a-silver", "a-gold")
        s3_cfg.buckets.bronze = bronze
        s3_cfg.buckets.silver = silver
        s3_cfg.buckets.gold = gold
        s3_cfg.create_buckets = create_buckets
        engine.k8s.namespace_exists.return_value = namespace_present
        engine.k8s.get_namespace_uid.side_effect = uid or (lambda _ns: "uid-1")
        engine.k8s.get_namespace_annotation.side_effect = nonce or (lambda _ns, _k: "n-1")
        cfg.architecture.tables.workload_tables.return_value = ["silver.t", "gold.t"]
        self.engine = engine
        # Default: deploy recorded all three as created (LB-159 marker).
        created_record = set(verdicts) if created is None else set(created)

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
            patch("kubernetes.client.CustomObjectsApi") as custom,
            patch("lakebench.deploy.destroy.logger"),
            patch("lakebench.s3.S3Client", return_value=_s3(boto)),
            patch(
                "lakebench.deploy.ownership.read_created_buckets",
                **(
                    {"side_effect": created_error}
                    if created_error
                    else {"return_value": created_record}
                ),
            ),
            patch(
                "lakebench.deploy.ownership.forget_created_buckets", side_effect=forget_error
            ) as forget,
            patch("lakebench.spark.SparkOperatorManager"),
            patch(
                "lakebench.deploy.iceberg.find_maintenance_engine",
                return_value=(maint or (None, None, None)),
            ),
            patch(
                "lakebench.deploy.iceberg.build_maintenance_sql",
                side_effect=lambda _e, _c, t, _r: [f"EXPIRE {t}", f"ORPHANS {t}"],
            ),
            patch(
                "lakebench.deploy.iceberg.build_drop_table_sql",
                side_effect=lambda _e, t: f"DROP {t}",
            ),
            patch("lakebench.deploy.iceberg.exec_sql", side_effect=self._exec) as exec_sql,
        ):
            self.exec_sql = exec_sql
            core.return_value.list_namespace.return_value.items = []
            custom.return_value.list_namespaced_custom_object.return_value = {
                "items": [{"metadata": {"name": "spark-job"}}]
            }
            self.custom = custom
            results = destroy_mod.destroy_all(engine, clean_buckets=True, force_legacy=force_legacy)
            self.forget = forget
        self._results = results
        buckets_results = [r for r in results if r.component == "s3-buckets"]
        return buckets_results[-1] if buckets_results else results[-1]

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
        """Another deployment 'a-silver...' has a longer claim; refuse, delete nothing."""
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "UNSUPPORTED", "a-silver": "UNSUPPORTED", "a-gold": "UNSUPPORTED"},
            other=["a-silver"],
        )
        assert r.status is DeploymentStatus.FAILED
        assert boto.delete_bucket_calls == []

    # -- LB-159 owner decision: delete only what lakebench created --------

    def test_adopted_bucket_is_emptied_but_kept(self):
        """MATCH by tag (e.g. adopted with deploy --force-legacy) is not creation."""
        boto = FakeBoto({"a-bronze": ["x"], "a-silver": [], "a-gold": []})
        r = self._run(
            boto,
            {"a-bronze": "MATCH", "a-silver": "MATCH", "a-gold": "UNSUPPORTED"},
            created={"a-silver"},
        )
        assert r.status is DeploymentStatus.SUCCESS
        assert set(boto.buckets) == {"a-bronze", "a-gold"}
        assert boto.buckets["a-bronze"] == [], "adopted buckets are still emptied"
        assert "provenance unknown" in r.message

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

    def test_redeploy_during_table_maintenance_stops_before_the_next_statement(self):
        """Maintenance at 0s retention and DROP TABLE must not run against R."""
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
        assert ran == ["EXPIRE lakehouse.silver.t"]
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
            if sql.startswith("DROP"):
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

    def _run_tables(self, on_sql, boto=None):
        boto = boto or FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        self._on_sql = on_sql
        try:
            self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                maint=("trino", "trino-coordinator-0", "lakehouse"),
            )
        finally:
            self._on_sql = None
        return [x for x in self._results if x.component == "table-cleanup"][-1]

    def test_first_statement_timeout_stops_the_table_step(self):
        from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

        ran: list[str] = []

        def hang(sql):
            ran.append(sql)
            raise ExecSqlTimeout("exec_sql timed out after 600s (may still be running)")

        tables = self._run_tables(hang)
        assert ran == ["EXPIRE lakehouse.silver.t"]
        assert tables.status is DeploymentStatus.FAILED
        assert "not attempted" in tables.message and "5 statement(s)" in tables.message

    def test_table_step_has_an_overall_cap(self, monkeypatch):
        clock = {"t": 0.0}

        def tick():
            clock["t"] += 700.0  # each statement "takes" 700 s
            return clock["t"]

        monkeypatch.setattr(destroy_mod, "_monotonic", tick)
        ran: list[str] = []
        tables = self._run_tables(ran.append)
        assert 0 < len(ran) < 6
        assert tables.status is DeploymentStatus.FAILED
        assert "cap" in tables.message

    def test_combined_statement_is_labelled_by_vacuum_and_skips_are_not_listed(self):
        from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

        assert destroy_mod._operative_sql("SET SESSION x = '0s'; CALL c.system.vacuum()") == "CALL"
        assert destroy_mod._operative_sql("SET a=b; VACUUM t RETAIN 0 HOURS") == "VACUUM"

        def run(sql):
            if "silver" in sql:
                raise RuntimeError(
                    "exec_sql failed (rc=1): Query 1 failed: Table 'lakehouse.silver.t' "
                    "does not exist"
                )
            raise ExecSqlTimeout("timed out")

        tables = self._run_tables(run)
        # silver maintenance skipped (missing), gold EXPIRE timed out; the
        # remaining are gold ORPHANS and the two drops, not silver's ORPHANS.
        assert "3 statement(s) not attempted" in tables.message
        assert "ORPHANS lakehouse.silver.t" not in tables.message.split("not attempted")[1]
