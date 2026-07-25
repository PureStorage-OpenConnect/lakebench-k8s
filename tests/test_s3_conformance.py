"""S3 backend conformance suite.

Lakebench runs against several S3 implementations (FlashBlade, Garage, AWS,
MinIO). They are not interchangeable, and the differences are invisible to
mocked unit tests. Two real examples motivated this suite:

- SeaweedFS returns an empty ``ListAllMyBucketsResult`` while ``head_bucket``
  returns 200. ``test_s3_connectivity()`` reports success with ``buckets: []``,
  so the failure is silent and every mocked test still passes.
- S3A signs with a default region when ``fs.s3a.endpoint.region`` is unset.
  FlashBlade ignores the region; Garage rejects the signature (LB-052).

These tests state what lakebench actually requires of a backend, so a store is
either supported or provably not.

Marked ``integration``: they need a live endpoint and are deselected by default.

Run against a backend by setting the endpoint and credentials::

    LB_S3_ENDPOINT=http://10.21.227.93:80 \\
    LB_S3_ACCESS_KEY=... LB_S3_SECRET_KEY=... \\
    pytest tests/test_s3_conformance.py -m integration -v
"""

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

ENDPOINT = os.environ.get("LB_S3_ENDPOINT", "")
ACCESS_KEY = os.environ.get("LB_S3_ACCESS_KEY", "")
SECRET_KEY = os.environ.get("LB_S3_SECRET_KEY", "")
REGION = os.environ.get("LB_S3_REGION", "us-east-1")

pytest_skip = pytest.mark.skipif(
    not (ENDPOINT and ACCESS_KEY and SECRET_KEY),
    reason="Set LB_S3_ENDPOINT, LB_S3_ACCESS_KEY, LB_S3_SECRET_KEY to run",
)


@pytest.fixture(scope="module")
def raw_client():
    """A boto3 client configured the way lakebench configures one."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 2}),
    )


@pytest.fixture
def temp_bucket(raw_client):
    """A disposable bucket, removed even if the test fails."""
    name = f"lb-conformance-{uuid.uuid4().hex[:10]}"
    raw_client.create_bucket(Bucket=name)
    try:
        yield name
    finally:
        try:
            for page in raw_client.get_paginator("list_objects_v2").paginate(Bucket=name):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    raw_client.delete_objects(Bucket=name, Delete={"Objects": keys})
            for up in raw_client.list_multipart_uploads(Bucket=name).get("Uploads", []):
                raw_client.abort_multipart_upload(
                    Bucket=name, Key=up["Key"], UploadId=up["UploadId"]
                )
            raw_client.delete_bucket(Bucket=name)
        except Exception:
            pass


@pytest_skip
class TestBucketOperations:
    """Bucket lifecycle. lakebench's deploy and destroy paths depend on these."""

    def test_create_and_head_bucket(self, raw_client, temp_bucket):
        resp = raw_client.head_bucket(Bucket=temp_bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_buckets_includes_created_bucket(self, raw_client, temp_bucket):
        """The check that disqualified SeaweedFS.

        SeaweedFS returns an empty bucket list while head_bucket returns 200,
        so connectivity checks report success while enumeration sees nothing.
        """
        names = [b["Name"] for b in raw_client.list_buckets()["Buckets"]]
        assert temp_bucket in names, (
            f"Backend does not enumerate buckets. {temp_bucket} exists "
            f"(head_bucket returns 200) but list_buckets returned {names}. "
            "Lakebench requires working bucket enumeration."
        )

    def test_delete_bucket(self, raw_client):
        name = f"lb-conformance-{uuid.uuid4().hex[:10]}"
        raw_client.create_bucket(Bucket=name)
        raw_client.delete_bucket(Bucket=name)
        assert name not in [b["Name"] for b in raw_client.list_buckets()["Buckets"]]


@pytest_skip
class TestObjectOperations:
    """Object read/write/list. The datagen and Spark paths depend on these."""

    def test_put_get_roundtrip(self, raw_client, temp_bucket):
        raw_client.put_object(Bucket=temp_bucket, Key="a/b/c.parquet", Body=b"payload")
        got = raw_client.get_object(Bucket=temp_bucket, Key="a/b/c.parquet")["Body"].read()
        assert got == b"payload"

    def test_list_objects_with_prefix(self, raw_client, temp_bucket):
        """Prefix listing backs get_bucket_size() and Iceberg metadata scans."""
        for key in ("warehouse/t/data/1.parquet", "warehouse/t/metadata/v1.json", "other/x"):
            raw_client.put_object(Bucket=temp_bucket, Key=key, Body=b"x")
        keys = [
            o["Key"]
            for o in raw_client.list_objects_v2(Bucket=temp_bucket, Prefix="warehouse/").get(
                "Contents", []
            )
        ]
        assert len(keys) == 2
        assert all(k.startswith("warehouse/") for k in keys)

    def test_delete_objects_batch(self, raw_client, temp_bucket):
        """empty_bucket() deletes in batches."""
        keys = [f"batch/{i}" for i in range(5)]
        for k in keys:
            raw_client.put_object(Bucket=temp_bucket, Key=k, Body=b"x")
        raw_client.delete_objects(
            Bucket=temp_bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )
        assert raw_client.list_objects_v2(Bucket=temp_bucket).get("Contents", []) == []


@pytest_skip
class TestMultipartUpload:
    """Multipart support. empty_bucket() cannot clean a bucket without it."""

    def test_multipart_lifecycle_and_abort(self, raw_client, temp_bucket):
        upload_id = raw_client.create_multipart_upload(Bucket=temp_bucket, Key="big.parquet")[
            "UploadId"
        ]
        raw_client.upload_part(
            Bucket=temp_bucket,
            Key="big.parquet",
            PartNumber=1,
            UploadId=upload_id,
            Body=b"y" * (5 * 1024 * 1024),
        )

        in_progress = raw_client.list_multipart_uploads(Bucket=temp_bucket).get("Uploads", [])
        assert len(in_progress) == 1, (
            "Backend does not report in-progress multipart uploads. "
            "empty_bucket() relies on list_multipart_uploads to detect them."
        )

        raw_client.abort_multipart_upload(Bucket=temp_bucket, Key="big.parquet", UploadId=upload_id)
        assert raw_client.list_multipart_uploads(Bucket=temp_bucket).get("Uploads", []) == []


@pytest_skip
class TestSignatureBehaviour:
    """Region-scope strictness. Documents backend divergence (LB-052)."""

    def test_documents_region_strictness(self, temp_bucket):
        """Record whether the backend validates the sigv4 region scope.

        Not a pass/fail requirement: both behaviours are legitimate. It is
        recorded because it determines whether omitting
        ``fs.s3a.endpoint.region`` breaks Spark against this backend.
        FlashBlade accepts any region; Garage rejects a mismatch.
        """
        import boto3
        import botocore
        from botocore.config import Config

        wrong = "eu-west-1" if REGION != "eu-west-1" else "us-east-2"
        client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=wrong,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
        )
        try:
            client.head_bucket(Bucket=temp_bucket)
            strict = False
        except botocore.exceptions.ClientError:
            strict = True

        print(
            f"\n  Backend region-scope strictness: {'STRICT' if strict else 'PERMISSIVE'}"
            f"\n  (strict backends require spark.hadoop.fs.s3a.endpoint.region)"
        )
        assert strict in (True, False)


@pytest_skip
class TestLakebenchS3Client:
    """The wrapper lakebench actually uses, not just raw boto3."""

    def test_connectivity_check_reports_buckets(self, temp_bucket):
        """Guards the SeaweedFS failure mode at the lakebench API level.

        A backend that cannot enumerate buckets still returns
        overall_success=True with an empty list, which reads as a pass.
        """
        from lakebench.s3 import test_s3_connectivity

        result = test_s3_connectivity(
            endpoint=ENDPOINT,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region=REGION,
            path_style=True,
        )
        assert result["overall_success"] is True
        assert result["buckets"], (
            "test_s3_connectivity() reported success with an empty bucket list. "
            "This is the SeaweedFS failure mode: enumeration is broken but the "
            "check still passes."
        )

    def test_empty_bucket_clears_objects_and_multipart(self, raw_client, temp_bucket):
        """The destroy path, exercised against a real backend."""
        from lakebench.s3.client import S3Client

        for i in range(3):
            raw_client.put_object(Bucket=temp_bucket, Key=f"data/{i}.parquet", Body=b"x" * 512)
        upload_id = raw_client.create_multipart_upload(Bucket=temp_bucket, Key="partial")[
            "UploadId"
        ]
        raw_client.upload_part(
            Bucket=temp_bucket,
            Key="partial",
            PartNumber=1,
            UploadId=upload_id,
            Body=b"z" * (5 * 1024 * 1024),
        )

        client = S3Client(
            endpoint=ENDPOINT,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region=REGION,
            path_style=True,
        )
        client.empty_bucket(temp_bucket, max_wait=120)

        assert raw_client.list_objects_v2(Bucket=temp_bucket).get("Contents", []) == []
        assert raw_client.list_multipart_uploads(Bucket=temp_bucket).get("Uploads", []) == []
