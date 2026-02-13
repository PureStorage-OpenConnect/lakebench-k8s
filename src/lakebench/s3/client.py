"""S3 client for Lakebench."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError


class S3Error(Exception):
    """Base exception for S3 errors."""

    pass


class S3ConnectionError(S3Error):
    """Raised when S3 endpoint is unreachable."""

    pass


class S3AuthError(S3Error):
    """Raised when S3 authentication fails."""

    pass


class S3BucketError(S3Error):
    """Raised when bucket operations fail."""

    pass


@dataclass
class S3Credentials:
    """S3 credentials container."""

    access_key: str
    secret_key: str


@dataclass
class S3ConnectionInfo:
    """S3 connection information."""

    endpoint: str
    region: str
    path_style: bool
    credentials: S3Credentials


@dataclass
class BucketInfo:
    """Information about an S3 bucket."""

    name: str
    exists: bool
    object_count: int | None = None
    size_bytes: int | None = None


class S3Client:
    """S3 client for bucket operations and connectivity testing.

    This client wraps boto3 and provides high-level operations for:
    - Testing endpoint connectivity
    - Validating credentials
    - Creating and checking buckets
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        path_style: bool = True,
    ):
        """Initialize S3 client.

        Args:
            endpoint: S3 endpoint URL (e.g., http://your-s3-endpoint:80)
            access_key: S3 access key
            secret_key: S3 secret key
            region: AWS region for signature compatibility
            path_style: Use path-style addressing (required for FlashBlade, MinIO)
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.path_style = path_style

        # Configure boto client
        boto_config = BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path" if path_style else "virtual"},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=10,
            read_timeout=30,
        )

        try:
            self._client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                config=boto_config,
            )
            self._init_error: str | None = None
        except (ValueError, Exception) as e:
            self._client = None  # type: ignore[assignment]
            self._init_error = str(e)

    @classmethod
    def from_connection_info(cls, info: S3ConnectionInfo) -> S3Client:
        """Create client from connection info dataclass."""
        return cls(
            endpoint=info.endpoint,
            access_key=info.credentials.access_key,
            secret_key=info.credentials.secret_key,
            region=info.region,
            path_style=info.path_style,
        )

    def test_endpoint_reachable(self) -> tuple[bool, str]:
        """Test if S3 endpoint is reachable.

        Returns:
            Tuple of (success, message)
        """
        if self._init_error:
            return False, f"Invalid endpoint: {self._init_error}"

        parsed = urlparse(self.endpoint)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)

        if not host:
            return False, f"Invalid endpoint URL: {self.endpoint}"

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                return True, f"Endpoint {host}:{port} is reachable"
            else:
                return False, f"Cannot connect to {host}:{port} (connection refused)"
        except socket.gaierror:
            return False, f"Cannot resolve hostname: {host}"
        except TimeoutError:
            return False, f"Connection to {host}:{port} timed out"
        except Exception as e:
            return False, f"Connection error: {e}"

    def test_credentials(self) -> tuple[bool, str]:
        """Test if S3 credentials are valid.

        Returns:
            Tuple of (success, message)
        """
        try:
            # Try to list buckets - requires valid credentials
            self._client.list_buckets()
            return True, "Credentials are valid"
        except NoCredentialsError:
            return False, "No credentials provided"
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = e.response.get("Error", {}).get("Message", str(e))

            if error_code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
                return False, f"Invalid credentials: {error_msg}"
            elif error_code == "AccessDenied":
                return False, f"Access denied: {error_msg}"
            else:
                return False, f"S3 error ({error_code}): {error_msg}"
        except EndpointConnectionError as e:
            return False, f"Cannot connect to endpoint: {e}"
        except Exception as e:
            return False, f"Unexpected error: {e}"

    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists.

        Args:
            bucket_name: Name of the bucket

        Returns:
            True if bucket exists, False otherwise
        """
        try:
            self._client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                return False
            # Re-raise other errors (e.g., access denied)
            raise S3BucketError(f"Error checking bucket {bucket_name}: {e}")  # noqa: B904

    def create_bucket(self, bucket_name: str) -> bool:
        """Create a bucket if it doesn't exist.

        Args:
            bucket_name: Name of the bucket to create

        Returns:
            True if bucket was created, False if it already existed
        """
        if self.bucket_exists(bucket_name):
            return False

        try:
            # For non-us-east-1 regions, need to specify location
            if self.region and self.region != "us-east-1":
                self._client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            else:
                self._client.create_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_msg = e.response.get("Error", {}).get("Message", str(e))

            if error_code == "BucketAlreadyOwnedByYou":
                return False
            raise S3BucketError(f"Failed to create bucket {bucket_name}: {error_msg}")  # noqa: B904

    def list_buckets(self) -> list[str]:
        """List all buckets.

        Returns:
            List of bucket names
        """
        try:
            response = self._client.list_buckets()
            return [b["Name"] for b in response.get("Buckets", [])]
        except ClientError as e:
            raise S3Error(f"Failed to list buckets: {e}")  # noqa: B904

    def get_bucket_info(self, bucket_name: str) -> BucketInfo:
        """Get information about a bucket.

        Args:
            bucket_name: Name of the bucket

        Returns:
            BucketInfo with bucket details
        """
        exists = self.bucket_exists(bucket_name)

        if not exists:
            return BucketInfo(name=bucket_name, exists=False)

        # Try to get object count (may fail on large buckets)
        object_count = None
        size_bytes = None

        try:
            # List first 1000 objects to estimate
            response = self._client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000)
            objects = response.get("Contents", [])
            object_count = len(objects)
            size_bytes = sum(obj.get("Size", 0) for obj in objects)

            # If truncated, we only have a sample
            if response.get("IsTruncated", False):
                object_count = None  # Unknown, too many
                size_bytes = None
        except ClientError:
            pass  # Can't get details, but bucket exists

        return BucketInfo(
            name=bucket_name,
            exists=True,
            object_count=object_count,
            size_bytes=size_bytes,
        )

    def get_bucket_size(self, bucket_name: str, prefix: str = "") -> BucketInfo:
        """Get accurate bucket size by paginating all objects.

        Unlike :meth:`get_bucket_info`, this paginates through ALL
        objects to return an accurate total size and count.

        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix to filter by

        Returns:
            BucketInfo with accurate ``object_count`` and ``size_bytes``
        """
        if not self.bucket_exists(bucket_name):
            return BucketInfo(name=bucket_name, exists=False)

        total_objects = 0
        total_bytes = 0

        try:
            paginator = self._client.get_paginator("list_objects_v2")
            paginate_args: dict[str, Any] = {"Bucket": bucket_name}
            if prefix:
                paginate_args["Prefix"] = prefix

            for page in paginator.paginate(**paginate_args):
                objects = page.get("Contents", [])
                total_objects += len(objects)
                total_bytes += sum(obj.get("Size", 0) for obj in objects)

            return BucketInfo(
                name=bucket_name,
                exists=True,
                object_count=total_objects,
                size_bytes=total_bytes,
            )
        except ClientError as e:
            raise S3BucketError(  # noqa: B904
                f"Failed to get bucket size for {bucket_name}: {e}"
            )

    def empty_bucket(self, bucket_name: str, max_wait: int = 300) -> int:
        """Delete all objects and abort incomplete multipart uploads in a bucket.

        Cleanup pattern: delete, abort multipart uploads, then
        retry until the bucket is truly empty (FlashBlade may lag on cleanup).

        Args:
            bucket_name: Name of the bucket to empty
            max_wait: Maximum seconds to wait for bucket to be fully empty

        Returns:
            Number of objects deleted
        """
        import time

        if not self.bucket_exists(bucket_name):
            return 0

        deleted_count = 0
        start = time.monotonic()

        try:
            while True:
                batch_deleted = 0

                # 1. Delete all completed objects
                paginator = self._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket_name):
                    objects = page.get("Contents", [])
                    if not objects:
                        continue
                    delete_objects = [{"Key": obj["Key"]} for obj in objects]
                    self._client.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": delete_objects},
                    )
                    batch_deleted += len(delete_objects)

                # 2. Abort all incomplete multipart uploads
                mp_paginator = self._client.get_paginator("list_multipart_uploads")
                for page in mp_paginator.paginate(Bucket=bucket_name):
                    for upload in page.get("Uploads", []):
                        self._client.abort_multipart_upload(
                            Bucket=bucket_name,
                            Key=upload["Key"],
                            UploadId=upload["UploadId"],
                        )
                        batch_deleted += 1

                deleted_count += batch_deleted

                # 3. Verify empty -- recount objects and uploads
                obj_count = 0
                for page in self._client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket_name, MaxKeys=1
                ):
                    obj_count += page.get("KeyCount", 0)

                mp_count = 0
                for page in self._client.get_paginator("list_multipart_uploads").paginate(
                    Bucket=bucket_name
                ):
                    mp_count += len(page.get("Uploads", []))

                if obj_count == 0 and mp_count == 0:
                    return deleted_count

                # Not yet empty -- FlashBlade may need time to sync
                if time.monotonic() - start > max_wait:
                    return deleted_count

                time.sleep(3)

        except ClientError as e:
            raise S3BucketError(f"Failed to empty bucket {bucket_name}: {e}")  # noqa: B904

    def ensure_buckets(self, bucket_names: list[str]) -> dict[str, bool]:
        """Ensure all specified buckets exist, creating if necessary.

        Args:
            bucket_names: List of bucket names to ensure

        Returns:
            Dict mapping bucket name to whether it was created (True) or existed (False)
        """
        results = {}
        for name in bucket_names:
            results[name] = self.create_bucket(name)
        return results


def test_s3_connectivity(
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str = "us-east-1",
    path_style: bool = True,
) -> dict[str, Any]:
    """Test S3 connectivity and credentials.

    This is a convenience function for validation that returns all test results.

    Args:
        endpoint: S3 endpoint URL
        access_key: S3 access key
        secret_key: S3 secret key
        region: AWS region
        path_style: Use path-style addressing

    Returns:
        Dict with test results:
        {
            "endpoint_reachable": bool,
            "endpoint_message": str,
            "credentials_valid": bool,
            "credentials_message": str,
            "buckets": list[str] | None,
            "overall_success": bool,
        }
    """
    result: dict[str, Any] = {
        "endpoint_reachable": False,
        "endpoint_message": "",
        "credentials_valid": False,
        "credentials_message": "",
        "buckets": None,
        "overall_success": False,
    }

    # Test endpoint
    try:
        client = S3Client(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            path_style=path_style,
        )

        reachable, msg = client.test_endpoint_reachable()
        result["endpoint_reachable"] = reachable
        result["endpoint_message"] = msg

        if not reachable:
            return result

        # Test credentials
        valid, msg = client.test_credentials()
        result["credentials_valid"] = valid
        result["credentials_message"] = msg

        if not valid:
            return result

        # List buckets
        result["buckets"] = client.list_buckets()
        result["overall_success"] = True

    except Exception as e:
        result["credentials_message"] = f"Unexpected error: {e}"

    return result


# Prevent pytest from collecting this as a test function
test_s3_connectivity.__test__ = False  # type: ignore[attr-defined]
