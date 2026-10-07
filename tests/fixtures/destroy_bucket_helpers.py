"""Shared test helpers moved from tests/test_destroy_bucket_delete.py (imported by several test files)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from lakebench.deploy import destroy as destroy_mod
from lakebench.s3.client import S3Client


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

    def list_objects_v2(self, Bucket, MaxKeys=1000, Prefix="", StartAfter=""):
        keys = sorted(
            k for k in self.buckets.get(Bucket, []) if k.startswith(Prefix) and k > StartAfter
        )[:MaxKeys]
        return {"KeyCount": len(keys), "Contents": [{"Key": k} for k in keys]}

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


class DestroyAllBucketsHarness:
    """destroy_all driven with per-bucket ownership verdicts on FakeBoto."""

    _on_sql = None

    sql_timeouts: list[int] = []

    def _exec(self, _engine, _k8s, _pod, _ns, sql, timeout=30):
        self.sql_timeouts.append(timeout)
        if self._on_sql:
            self._on_sql(sql)

    UNREG_SILVER = (
        "CALL lakehouse.system.unregister_table(schema_name => 'silver', table_name => 't')"
    )

    UNREG_GOLD = "CALL lakehouse.system.unregister_table(schema_name => 'gold', table_name => 't')"

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

    _tables: tuple[str, ...] = ("silver.t", "gold.t")

    def _run(
        self,
        boto,
        verdicts,
        *,
        create_buckets=True,
        force_legacy=False,
        other=(),
        created=None,
        adopted_empty=(),
        uid=None,
        namespace_present=True,
        create_namespace=False,
        created_error=None,
        nonce=None,
        maint=None,
        forget_error=None,
        table_format=None,
        delete_buckets=True,
        catalog_type=None,
        clean_buckets=True,
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
        cfg.architecture.tables.workload_tables.return_value = list(self._tables)
        if table_format:
            cfg.architecture.table_format.type.value = table_format
        cfg.architecture.catalog.type.value = catalog_type or "hive"
        self.engine = engine
        # Default: deploy recorded all three as created (the created-buckets marker).
        created_record = set(verdicts) if created is None else set(created)

        def verify(_boto, bucket, _name, **_kw):
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
                "lakebench.deploy.ownership.read_adopted_empty_buckets",
                return_value=set(adopted_empty),
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
            results = destroy_mod.destroy_all(
                engine,
                clean_buckets=clean_buckets,
                force_legacy=force_legacy,
                delete_buckets=delete_buckets,
            )
            self.forget = forget
        self._results = results
        buckets_results = [r for r in results if r.component == "s3-buckets"]
        return buckets_results[-1] if buckets_results else results[-1]

    def _exit_code_of_bucket_and_namespace(self):
        from lakebench.cli._exit import refused_result_code

        steps = [x for x in self._results if x.component in ("s3-buckets", "namespace")]
        return refused_result_code(steps)

    def _run_tables(self, on_sql, boto=None, table_format=None):
        boto = boto or FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        self._on_sql = on_sql
        try:
            self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                maint=("trino", "trino-coordinator-0", "lakehouse"),
                table_format=table_format,
            )
        finally:
            self._on_sql = None
        return [x for x in self._results if x.component == "table-cleanup"][-1]

    def _run_catalog(self, catalog_type, maint, on_sql, create_namespace=True):
        boto = FakeBoto({"a-bronze": [], "a-silver": [], "a-gold": []})
        self._on_sql = on_sql
        try:
            r = self._run(
                boto,
                dict.fromkeys(["a-bronze", "a-silver", "a-gold"], "MATCH"),
                maint=maint,
                table_format="iceberg",
                catalog_type=catalog_type,
                create_namespace=create_namespace,
            )
        finally:
            self._on_sql = None
        tables = [x for x in self._results if x.component == "table-cleanup"][-1]
        return r, tables, boto
