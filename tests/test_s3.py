"""Tests for S3 client functionality."""

from lakebench.s3 import (
    BucketInfo,
    S3Client,
    S3ConnectionInfo,
    S3Credentials,
    test_s3_connectivity,
)


class TestS3Credentials:
    """Tests for S3Credentials dataclass."""

    def test_create_credentials(self):
        creds = S3Credentials(access_key="key", secret_key="secret")
        assert creds.access_key == "key"
        assert creds.secret_key == "secret"


class TestS3ConnectionInfo:
    """Tests for S3ConnectionInfo dataclass."""

    def test_create_connection_info(self):
        creds = S3Credentials(access_key="key", secret_key="secret")
        info = S3ConnectionInfo(
            endpoint="http://localhost:9000",
            region="us-east-1",
            path_style=True,
            credentials=creds,
        )
        assert info.endpoint == "http://localhost:9000"
        assert info.path_style is True


class TestBucketInfo:
    """Tests for BucketInfo dataclass."""

    def test_bucket_exists(self):
        info = BucketInfo(name="test-bucket", exists=True, object_count=100)
        assert info.exists is True
        assert info.object_count == 100

    def test_bucket_not_exists(self):
        info = BucketInfo(name="test-bucket", exists=False)
        assert info.exists is False
        assert info.object_count is None


class TestS3ClientInit:
    """Tests for S3Client initialization."""

    def test_create_client(self):
        """Test creating an S3 client (no actual connection)."""
        client = S3Client(
            endpoint="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            region="us-east-1",
            path_style=True,
        )
        assert client.endpoint == "http://localhost:9000"
        assert client.path_style is True

    def test_create_from_connection_info(self):
        """Test creating client from connection info."""
        creds = S3Credentials(access_key="key", secret_key="secret")
        info = S3ConnectionInfo(
            endpoint="http://localhost:9000",
            region="us-east-1",
            path_style=True,
            credentials=creds,
        )
        client = S3Client.from_connection_info(info)
        assert client.endpoint == "http://localhost:9000"


class TestS3ClientEndpointCheck:
    """Tests for endpoint reachability checking."""

    def test_unreachable_endpoint(self):
        """Test checking an unreachable endpoint."""
        client = S3Client(
            endpoint="http://192.0.2.1:9000",  # TEST-NET, should be unreachable
            access_key="key",
            secret_key="secret",
        )
        reachable, msg = client.test_endpoint_reachable()
        assert reachable is False
        # Message should indicate connection issue
        assert "192.0.2.1" in msg or "cannot" in msg.lower() or "timed out" in msg.lower()

    def test_invalid_endpoint_url(self):
        """Test with invalid endpoint URL."""
        client = S3Client(
            endpoint="not-a-url",
            access_key="key",
            secret_key="secret",
        )
        reachable, msg = client.test_endpoint_reachable()
        assert reachable is False
        assert "invalid" in msg.lower() or "cannot" in msg.lower()


class TestS3ConnectivityHelper:
    """Tests for the test_s3_connectivity helper function."""

    def test_connectivity_with_unreachable_endpoint(self):
        """Test connectivity check with unreachable endpoint."""
        results = test_s3_connectivity(
            endpoint="http://192.0.2.1:9000",
            access_key="key",
            secret_key="secret",
        )
        assert results["endpoint_reachable"] is False
        assert results["overall_success"] is False
        # Credentials shouldn't be tested if endpoint is unreachable
        assert results["credentials_valid"] is False


class TestGetBucketSize:
    """Tests for paginated bucket size measurement."""

    def _make_client(self):
        """Create a test S3Client (no connection needed)."""
        return S3Client(
            endpoint="http://localhost:9000",
            access_key="key",
            secret_key="secret",
        )

    def test_nonexistent_bucket(self):
        """get_bucket_size returns exists=False for missing bucket."""
        from unittest.mock import patch

        client = self._make_client()
        with patch.object(client, "bucket_exists", return_value=False):
            info = client.get_bucket_size("no-such-bucket")
        assert info.exists is False
        assert info.object_count is None

    def test_empty_bucket(self):
        """get_bucket_size returns 0 objects/bytes for empty bucket."""
        from unittest.mock import MagicMock, patch

        client = self._make_client()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]

        with (
            patch.object(client, "bucket_exists", return_value=True),
            patch.object(client._client, "get_paginator", return_value=mock_paginator),
        ):
            info = client.get_bucket_size("empty-bucket")

        assert info.exists is True
        assert info.object_count == 0
        assert info.size_bytes == 0

    def test_single_page(self):
        """get_bucket_size handles single-page listing."""
        from unittest.mock import MagicMock, patch

        client = self._make_client()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "file1.parquet", "Size": 1000},
                    {"Key": "file2.parquet", "Size": 2000},
                    {"Key": "file3.parquet", "Size": 3000},
                ]
            }
        ]

        with (
            patch.object(client, "bucket_exists", return_value=True),
            patch.object(client._client, "get_paginator", return_value=mock_paginator),
        ):
            info = client.get_bucket_size("test-bucket")

        assert info.object_count == 3
        assert info.size_bytes == 6000

    def test_multi_page(self):
        """get_bucket_size paginates across multiple pages."""
        from unittest.mock import MagicMock, patch

        client = self._make_client()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "file1.parquet", "Size": 1000},
                    {"Key": "file2.parquet", "Size": 2000},
                ]
            },
            {
                "Contents": [
                    {"Key": "file3.parquet", "Size": 3000},
                ]
            },
        ]

        with (
            patch.object(client, "bucket_exists", return_value=True),
            patch.object(client._client, "get_paginator", return_value=mock_paginator),
        ):
            info = client.get_bucket_size("test-bucket")

        assert info.object_count == 3
        assert info.size_bytes == 6000

    def test_with_prefix(self):
        """get_bucket_size passes prefix to paginator."""
        from unittest.mock import MagicMock, patch

        client = self._make_client()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "data/file1.parquet", "Size": 5000},
                ]
            },
        ]

        with (
            patch.object(client, "bucket_exists", return_value=True),
            patch.object(client._client, "get_paginator", return_value=mock_paginator),
        ):
            info = client.get_bucket_size("test-bucket", prefix="data/")

        mock_paginator.paginate.assert_called_once_with(Bucket="test-bucket", Prefix="data/")
        assert info.object_count == 1
        assert info.size_bytes == 5000


# Note: Integration tests requiring a real S3/MinIO endpoint
# would go in a separate test file (test_s3_integration.py)
# and be marked with @pytest.mark.integration
