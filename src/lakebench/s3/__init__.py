"""S3 client module for Lakebench."""

from .client import (
    BucketInfo,
    S3AuthError,
    S3BucketError,
    S3Client,
    S3ConnectionError,
    S3ConnectionInfo,
    S3Credentials,
    S3Error,
    test_s3_connectivity,
)
from .conformance import (
    KNOWN_BACKENDS,
    CheckResult,
    CheckStatus,
    ConformanceReport,
    Severity,
    detect_backend,
    run_conformance,
)

__all__ = [
    "KNOWN_BACKENDS",
    "CheckResult",
    "CheckStatus",
    "ConformanceReport",
    "Severity",
    "detect_backend",
    "run_conformance",
    "S3Client",
    "S3Credentials",
    "S3ConnectionInfo",
    "BucketInfo",
    "S3Error",
    "S3ConnectionError",
    "S3AuthError",
    "S3BucketError",
    "test_s3_connectivity",
]
