"""S3 client module for Lakebench."""

from .client import (
    BucketInfo,
    S3AuthError,
    S3BucketError,
    S3BucketVanished,
    S3Client,
    S3Error,
    s3_client_from_config,
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
    "BucketInfo",
    "S3Error",
    "S3AuthError",
    "S3BucketError",
    "S3BucketVanished",
    "s3_client_from_config",
    "test_s3_connectivity",
]
