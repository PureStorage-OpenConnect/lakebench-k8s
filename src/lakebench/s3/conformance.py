"""S3 backend conformance checks.

Lakebench runs against several S3 implementations and they are not
interchangeable. The differences live in backend behaviour, not in lakebench
code, so mocked unit tests cannot see them. Two real cases motivate this
module:

- SeaweedFS returns an empty bucket list while ``head_bucket`` returns 200.
  ``test_s3_connectivity()`` reports success with no buckets, so the failure
  is silent and ``empty_bucket()`` cannot clean up on destroy.
- S3A signs with a default region when ``fs.s3a.endpoint.region`` is unset.
  FlashBlade ignores the region; strict backends reject the signature.

Checks are graded, not pass/fail as a block:

- ``REQUIRED``  -- lakebench is broken without it. Enumeration, multipart abort.
- ``ADVISORY``  -- affects behaviour but is handled. Region strictness.
- ``INFO``      -- recorded for diagnostics only.

This module performs no gating. It reports what a backend does. Callers decide
what to do with the result.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    """How much a failed check matters."""

    REQUIRED = "required"
    ADVISORY = "advisory"
    INFO = "info"


class CheckStatus(str, Enum):
    """Outcome of a single check."""

    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class CheckResult:
    """Outcome of one conformance check."""

    name: str
    status: CheckStatus
    severity: Severity
    message: str
    impact: str = ""

    @property
    def blocking(self) -> bool:
        """True when this failure breaks a lakebench code path."""
        return self.status is CheckStatus.FAIL and self.severity is Severity.REQUIRED


@dataclass
class ConformanceReport:
    """Result of running the conformance checks against a backend."""

    endpoint: str
    backend: str = "unknown"
    checks: list[CheckResult] = field(default_factory=list)
    # Recorded properties that change how lakebench configures itself.
    properties: dict[str, Any] = field(default_factory=dict)
    degraded: bool = False
    degraded_reason: str = ""

    @property
    def passed(self) -> bool:
        """True when no REQUIRED check failed.

        Advisory failures do not make a backend unusable.
        """
        return not any(c.blocking for c in self.checks)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.blocking]

    @property
    def advisories(self) -> list[CheckResult]:
        return [
            c
            for c in self.checks
            if c.status is CheckStatus.FAIL and c.severity is Severity.ADVISORY
        ]

    def summary(self) -> str:
        p = sum(1 for c in self.checks if c.status is CheckStatus.PASS)
        f = sum(1 for c in self.checks if c.status is CheckStatus.FAIL)
        s = sum(1 for c in self.checks if c.status is CheckStatus.SKIP)
        return f"{p} passed, {f} failed, {s} skipped"


# Backends whose behaviour has been verified against this suite.
# Presence here is a fast path, not a gate: an unlisted backend is checked
# at runtime rather than refused.
KNOWN_BACKENDS: dict[str, dict[str, Any]] = {
    "flashblade": {
        "label": "Pure Storage FlashBlade",
        "verified": "2026-07-25",
        "region_strict": False,
        "notes": "Reference platform. Multipart ghost objects are async GC'd; "
        "empty_bucket() retries until list_objects_v2 and "
        "list_multipart_uploads both return empty.",
    },
    "garage": {
        "label": "Garage",
        "verified": "2026-07-25",
        "region_strict": True,
        "notes": "Local/laptop default. Requires spark.hadoop.fs.s3a.endpoint.region "
        "because it validates the sigv4 region scope.",
    },
    "aws": {
        "label": "AWS S3",
        "verified": "",
        "region_strict": True,
        "notes": "Not validated against this suite. Uses virtual-hosted addressing "
        "by default (set path_style: false).",
    },
    "seaweedfs": {
        "label": "SeaweedFS",
        "verified": "2026-07-25",
        "region_strict": False,
        "unsupported": True,
        "notes": "NOT SUPPORTED. Returns an empty ListAllMyBucketsResult while "
        "head_bucket returns 200, so bucket enumeration is silently broken "
        "and destroy cannot clean up reliably.",
    },
}


def _mib(n: int) -> bytes:
    return b"y" * (n * 1024 * 1024)


class ConformanceRunner:
    """Runs the conformance checks against a live S3 endpoint.

    Creates a temporary bucket when permitted, and removes it afterwards.
    When bucket creation is not permitted, falls back to a degraded read-only
    run against an existing bucket rather than reporting a false failure.
    """

    TEMP_PREFIX = "lb-conformance-"

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        path_style: bool = True,
        existing_bucket: str = "",
        allow_create_bucket: bool = True,
    ) -> None:
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.path_style = path_style
        self.existing_bucket = existing_bucket
        self.allow_create_bucket = allow_create_bucket
        self._client: Any = None
        self._bucket: str = ""
        self._owns_bucket = False

    # -- client -------------------------------------------------------------

    def _make_client(self, region: str | None = None) -> Any:
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=region or self.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path" if self.path_style else "virtual"},
                retries={"max_attempts": 2, "mode": "standard"},
                connect_timeout=10,
                read_timeout=30,
            ),
        )

    # -- lifecycle ----------------------------------------------------------

    def _acquire_bucket(self, report: ConformanceReport) -> bool:
        """Create a temp bucket, or fall back to an existing one."""
        if self.allow_create_bucket:
            name = f"{self.TEMP_PREFIX}{uuid.uuid4().hex[:10]}"
            try:
                self._client.create_bucket(Bucket=name)
                self._bucket = name
                self._owns_bucket = True
                return True
            except Exception as e:
                logger.debug("Could not create temp bucket: %s", e)

        if self.existing_bucket:
            try:
                self._client.head_bucket(Bucket=self.existing_bucket)
                self._bucket = self.existing_bucket
                self._owns_bucket = False
                report.degraded = True
                cause = (
                    "Bucket creation was not attempted (--no-full)"
                    if not self.allow_create_bucket
                    else "No permission to create buckets"
                )
                report.degraded_reason = (
                    f"{cause}. Ran read-only checks against existing bucket "
                    f"'{self.existing_bucket}'. Write and multipart checks "
                    f"were skipped."
                )
                return True
            except Exception as e:
                logger.debug("Existing bucket unusable: %s", e)

        report.degraded = True
        report.degraded_reason = (
            "Could not create a temporary bucket and no usable existing bucket "
            "was supplied. Only connectivity was checked."
        )
        return False

    def _release_bucket(self) -> None:
        if not (self._bucket and self._owns_bucket):
            return
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    self._client.delete_objects(Bucket=self._bucket, Delete={"Objects": keys})
            for up in self._client.list_multipart_uploads(Bucket=self._bucket).get("Uploads", []):
                self._client.abort_multipart_upload(
                    Bucket=self._bucket, Key=up["Key"], UploadId=up["UploadId"]
                )
            self._client.delete_bucket(Bucket=self._bucket)
        except Exception as e:
            logger.warning("Could not fully clean up temp bucket %s: %s", self._bucket, e)

    # -- checks -------------------------------------------------------------

    def _check_connectivity(self, report: ConformanceReport) -> bool:
        try:
            self._client.list_buckets()
            report.checks.append(
                CheckResult(
                    "connectivity",
                    CheckStatus.PASS,
                    Severity.REQUIRED,
                    "Endpoint reachable and credentials accepted",
                )
            )
            return True
        except Exception as e:
            report.checks.append(
                CheckResult(
                    "connectivity",
                    CheckStatus.FAIL,
                    Severity.REQUIRED,
                    f"Cannot reach endpoint or credentials rejected: {e}",
                    impact="Nothing else can run.",
                )
            )
            return False

    def _check_enumeration(self, report: ConformanceReport) -> None:
        """The check that disqualified SeaweedFS."""
        try:
            names = [b["Name"] for b in self._client.list_buckets()["Buckets"]]
            if self._bucket in names:
                report.checks.append(
                    CheckResult(
                        "bucket-enumeration",
                        CheckStatus.PASS,
                        Severity.REQUIRED,
                        "list_buckets returns created buckets",
                    )
                )
            else:
                report.checks.append(
                    CheckResult(
                        "bucket-enumeration",
                        CheckStatus.FAIL,
                        Severity.REQUIRED,
                        f"Bucket '{self._bucket}' exists but list_buckets did not return it",
                        impact=(
                            "Bucket enumeration is silently broken. Connectivity checks "
                            "report success with an empty bucket list, and destroy cannot "
                            "reliably clean up. This is the SeaweedFS failure mode."
                        ),
                    )
                )
        except Exception as e:
            report.checks.append(
                CheckResult(
                    "bucket-enumeration",
                    CheckStatus.FAIL,
                    Severity.REQUIRED,
                    f"list_buckets failed: {e}",
                    impact="Deploy cannot verify buckets; destroy cannot clean up.",
                )
            )

    def _check_object_ops(self, report: ConformanceReport) -> None:
        key = "lb-conformance/probe.parquet"
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=b"payload")
            got = self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()
            if got != b"payload":
                raise ValueError("round-trip mismatch")
            listed = [
                o["Key"]
                for o in self._client.list_objects_v2(
                    Bucket=self._bucket, Prefix="lb-conformance/"
                ).get("Contents", [])
            ]
            if key not in listed:
                raise ValueError("prefix listing did not return the object")
            self._client.delete_objects(Bucket=self._bucket, Delete={"Objects": [{"Key": key}]})
            report.checks.append(
                CheckResult(
                    "object-operations",
                    CheckStatus.PASS,
                    Severity.REQUIRED,
                    "put, get, prefix-list, and batch-delete all work",
                )
            )
        except Exception as e:
            report.checks.append(
                CheckResult(
                    "object-operations",
                    CheckStatus.FAIL,
                    Severity.REQUIRED,
                    f"Object operations failed: {e}",
                    impact="Datagen cannot write and Spark cannot read.",
                )
            )

    def _check_multipart(self, report: ConformanceReport) -> None:
        """empty_bucket() cannot clean a bucket without abort support."""
        key = "lb-conformance/multipart.parquet"
        upload_id = ""
        try:
            upload_id = self._client.create_multipart_upload(Bucket=self._bucket, Key=key)[
                "UploadId"
            ]
            self._client.upload_part(
                Bucket=self._bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=_mib(5)
            )
            in_progress = self._client.list_multipart_uploads(Bucket=self._bucket).get(
                "Uploads", []
            )
            if not in_progress:
                raise ValueError("list_multipart_uploads did not report the in-progress upload")
            self._client.abort_multipart_upload(Bucket=self._bucket, Key=key, UploadId=upload_id)
            upload_id = ""
            remaining = self._client.list_multipart_uploads(Bucket=self._bucket).get("Uploads", [])
            if remaining:
                raise ValueError("upload still listed after abort")
            report.checks.append(
                CheckResult(
                    "multipart-upload",
                    CheckStatus.PASS,
                    Severity.REQUIRED,
                    "create, list, and abort all work",
                )
            )
        except Exception as e:
            report.checks.append(
                CheckResult(
                    "multipart-upload",
                    CheckStatus.FAIL,
                    Severity.REQUIRED,
                    f"Multipart lifecycle failed: {e}",
                    impact=(
                        "empty_bucket() relies on list_multipart_uploads and "
                        "abort_multipart_upload. Destroy will leave incomplete "
                        "uploads behind and buckets will not empty."
                    ),
                )
            )
            if upload_id:
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._bucket, Key=key, UploadId=upload_id
                    )
                except Exception:
                    pass

    def _check_region_strictness(self, report: ConformanceReport) -> None:
        """Record whether the backend validates the sigv4 region scope.

        Not a defect either way. It determines whether Spark needs
        ``fs.s3a.endpoint.region`` set explicitly (LB-052).
        """
        wrong = "eu-west-1" if self.region != "eu-west-1" else "us-east-2"
        try:
            self._make_client(region=wrong).head_bucket(Bucket=self._bucket)
            strict = False
        except Exception:
            strict = True

        report.properties["region_strict"] = strict
        if strict:
            report.checks.append(
                CheckResult(
                    "region-strictness",
                    CheckStatus.PASS,
                    Severity.ADVISORY,
                    "Backend validates the sigv4 region scope (strict)",
                    impact=(
                        "Spark must set spark.hadoop.fs.s3a.endpoint.region. "
                        "Lakebench sets this automatically."
                    ),
                )
            )
        else:
            report.checks.append(
                CheckResult(
                    "region-strictness",
                    CheckStatus.PASS,
                    Severity.INFO,
                    "Backend accepts any region (permissive)",
                )
            )

    # -- entry point --------------------------------------------------------

    def run(self) -> ConformanceReport:
        """Run all checks and return a report. Never raises."""
        report = ConformanceReport(endpoint=self.endpoint, backend=detect_backend(self.endpoint))

        try:
            self._client = self._make_client()
        except Exception as e:
            report.checks.append(
                CheckResult(
                    "connectivity",
                    CheckStatus.FAIL,
                    Severity.REQUIRED,
                    f"Could not build S3 client: {e}",
                )
            )
            return report

        if not self._check_connectivity(report):
            return report

        if not self._acquire_bucket(report):
            return report

        try:
            self._check_enumeration(report)
            if self._owns_bucket:
                self._check_object_ops(report)
                self._check_multipart(report)
            else:
                for name in ("object-operations", "multipart-upload"):
                    report.checks.append(
                        CheckResult(
                            name,
                            CheckStatus.SKIP,
                            Severity.REQUIRED,
                            "Skipped: read-only run against an existing bucket",
                            impact="Run with create-bucket permission for full validation.",
                        )
                    )
            self._check_region_strictness(report)
        finally:
            self._release_bucket()

        return report


def detect_backend(endpoint: str) -> str:
    """Best-effort backend identification from the endpoint.

    Advisory only. An unrecognised endpoint is checked at runtime, never
    refused, so a wrong guess costs nothing.
    """
    ep = (endpoint or "").lower()
    if "amazonaws.com" in ep:
        return "aws"
    return "unknown"


def run_conformance(
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str = "us-east-1",
    path_style: bool = True,
    existing_bucket: str = "",
    allow_create_bucket: bool = True,
) -> ConformanceReport:
    """Run the S3 conformance checks against a backend.

    Reports what the backend does. Performs no gating.

    Set ``allow_create_bucket=False`` to skip creating a temporary bucket and
    run read-only checks against ``existing_bucket`` instead.
    """
    return ConformanceRunner(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        path_style=path_style,
        existing_bucket=existing_bucket,
        allow_create_bucket=allow_create_bucket,
    ).run()
