"""Unit tests for the S3 conformance runner.

These are mocked and run in the default suite. The live counterpart is
``tests/test_s3_conformance.py`` (integration-marked).
"""

from unittest import mock

import pytest

from lakebench.s3.conformance import (
    KNOWN_BACKENDS,
    CheckResult,
    CheckStatus,
    ConformanceReport,
    ConformanceRunner,
    Severity,
    detect_backend,
)


class TestGrading:
    """Severity determines whether a failure blocks, not the failure itself."""

    def test_required_failure_blocks(self):
        c = CheckResult("x", CheckStatus.FAIL, Severity.REQUIRED, "broken")
        assert c.blocking is True

    def test_advisory_failure_does_not_block(self):
        c = CheckResult("x", CheckStatus.FAIL, Severity.ADVISORY, "quirk")
        assert c.blocking is False

    def test_passing_required_check_does_not_block(self):
        c = CheckResult("x", CheckStatus.PASS, Severity.REQUIRED, "ok")
        assert c.blocking is False

    def test_report_passes_when_only_advisory_fails(self):
        r = ConformanceReport(endpoint="http://x")
        r.checks = [
            CheckResult("a", CheckStatus.PASS, Severity.REQUIRED, ""),
            CheckResult("b", CheckStatus.FAIL, Severity.ADVISORY, ""),
        ]
        assert r.passed is True
        assert len(r.advisories) == 1

    def test_report_fails_on_required(self):
        r = ConformanceReport(endpoint="http://x")
        r.checks = [CheckResult("a", CheckStatus.FAIL, Severity.REQUIRED, "")]
        assert r.passed is False
        assert len(r.blocking_failures) == 1

    def test_summary_counts(self):
        r = ConformanceReport(endpoint="http://x")
        r.checks = [
            CheckResult("a", CheckStatus.PASS, Severity.REQUIRED, ""),
            CheckResult("b", CheckStatus.FAIL, Severity.REQUIRED, ""),
            CheckResult("c", CheckStatus.SKIP, Severity.REQUIRED, ""),
        ]
        assert r.summary() == "1 passed, 1 failed, 1 skipped"


class TestKnownBackends:
    """The registry is advisory metadata, not an allowlist."""

    def test_flashblade_and_garage_are_verified(self):
        for name in ("flashblade", "garage"):
            assert KNOWN_BACKENDS[name]["verified"], f"{name} should carry a verified date"

    def test_seaweedfs_marked_unsupported_with_reason(self):
        sw = KNOWN_BACKENDS["seaweedfs"]
        assert sw.get("unsupported") is True
        assert "enumeration" in sw["notes"].lower() or "listallmybuckets" in sw["notes"].lower()

    def test_region_strictness_recorded(self):
        assert KNOWN_BACKENDS["flashblade"]["region_strict"] is False
        assert KNOWN_BACKENDS["garage"]["region_strict"] is True


class TestDetectBackend:
    def test_detects_aws(self):
        assert detect_backend("https://s3.us-east-1.amazonaws.com") == "aws"

    def test_unknown_endpoint_is_not_refused(self):
        """An unrecognised endpoint is checked at runtime, never rejected."""
        assert detect_backend("http://10.1.2.3:80") == "unknown"

    def test_handles_empty(self):
        assert detect_backend("") == "unknown"


def _runner(client):
    r = ConformanceRunner("http://x", "ak", "sk")
    r._make_client = mock.MagicMock(return_value=client)  # type: ignore[method-assign]
    return r


class TestRunnerBehaviour:
    def test_connectivity_failure_stops_early(self):
        client = mock.MagicMock()
        client.list_buckets.side_effect = RuntimeError("refused")
        report = _runner(client).run()
        assert report.passed is False
        assert len(report.checks) == 1
        assert report.checks[0].name == "connectivity"

    def test_enumeration_failure_is_blocking(self):
        """The SeaweedFS failure mode, reproduced with mocks."""
        client = mock.MagicMock()
        client.list_buckets.return_value = {"Buckets": []}  # never lists the created bucket
        client.list_multipart_uploads.return_value = {"Uploads": []}
        client.get_paginator.return_value.paginate.return_value = []
        report = _runner(client).run()
        enum = next(c for c in report.checks if c.name == "bucket-enumeration")
        assert enum.status is CheckStatus.FAIL
        assert enum.blocking is True
        assert report.passed is False

    def test_degraded_mode_when_bucket_creation_denied(self):
        """No create-bucket permission must not read as a backend defect."""
        client = mock.MagicMock()
        client.create_bucket.side_effect = RuntimeError("AccessDenied")
        client.list_buckets.return_value = {"Buckets": [{"Name": "existing"}]}
        client.head_bucket.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        client.list_multipart_uploads.return_value = {"Uploads": []}

        runner = ConformanceRunner("http://x", "ak", "sk", existing_bucket="existing")
        runner._make_client = mock.MagicMock(return_value=client)  # type: ignore[method-assign]
        report = runner.run()

        assert report.degraded is True
        assert "create buckets" in report.degraded_reason
        skipped = [c.name for c in report.checks if c.status is CheckStatus.SKIP]
        assert "object-operations" in skipped
        assert "multipart-upload" in skipped

    def test_no_full_does_not_attempt_bucket_creation(self):
        """--no-full must actually skip creation, not just pass a fallback."""
        client = mock.MagicMock()
        client.list_buckets.return_value = {"Buckets": [{"Name": "existing"}]}
        client.head_bucket.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        client.list_multipart_uploads.return_value = {"Uploads": []}

        runner = ConformanceRunner(
            "http://x", "ak", "sk", existing_bucket="existing", allow_create_bucket=False
        )
        runner._make_client = mock.MagicMock(return_value=client)  # type: ignore[method-assign]
        report = runner.run()

        assert not client.create_bucket.called, "--no-full must not create a bucket"
        assert report.degraded is True
        assert "--no-full" in report.degraded_reason
        assert report.passed is True, "skipped checks are not failures"

    def test_no_bucket_and_no_fallback_is_degraded_not_crash(self):
        client = mock.MagicMock()
        client.create_bucket.side_effect = RuntimeError("AccessDenied")
        client.list_buckets.return_value = {"Buckets": []}
        report = _runner(client).run()
        assert report.degraded is True
        assert "Only connectivity" in report.degraded_reason

    def test_temp_bucket_removed_after_run(self):
        client = mock.MagicMock()
        client.list_buckets.return_value = {"Buckets": [{"Name": "ignored"}]}
        client.list_multipart_uploads.return_value = {"Uploads": []}
        client.get_paginator.return_value.paginate.return_value = []
        _runner(client).run()
        assert client.delete_bucket.called, "temp bucket must be cleaned up"

    def test_runner_never_raises_on_backend_errors(self):
        client = mock.MagicMock()
        client.list_buckets.return_value = {"Buckets": []}
        client.create_bucket.side_effect = RuntimeError("boom")
        report = _runner(client).run()
        assert isinstance(report, ConformanceReport)


class TestRegionStrictnessIsRecordedNotGraded:
    """Both behaviours are legitimate; the property drives Spark config."""

    def test_strict_backend_recorded_as_advisory(self):
        client = mock.MagicMock()
        client.list_buckets.return_value = {"Buckets": [{"Name": "b"}]}
        client.list_multipart_uploads.return_value = {"Uploads": []}
        client.get_paginator.return_value.paginate.return_value = []

        strict = mock.MagicMock()
        strict.head_bucket.side_effect = RuntimeError("SignatureDoesNotMatch")

        runner = ConformanceRunner("http://x", "ak", "sk")
        calls = {"n": 0}

        def make(region=None):
            calls["n"] += 1
            return strict if region else client

        runner._make_client = make  # type: ignore[method-assign]
        report = runner.run()

        assert report.properties["region_strict"] is True
        check = next(c for c in report.checks if c.name == "region-strictness")
        assert check.status is CheckStatus.PASS
        assert check.severity is Severity.ADVISORY


class TestSparkSetsRegion:
    """LB-052: the region must actually reach the Spark conf."""

    def test_manifest_sets_s3a_endpoint_region(self):
        from lakebench.config import load_config
        from lakebench.spark.job import JobType, SparkJobManager

        cfg = load_config("examples/hive-iceberg-spark-trino.yaml")
        manifest = SparkJobManager(cfg, mock.MagicMock())._build_manifest(JobType.BRONZE_VERIFY)
        conf = manifest["spec"]["sparkConf"]
        assert conf["spark.hadoop.fs.s3a.endpoint.region"] == cfg.platform.storage.s3.region


@pytest.mark.parametrize("backend", ["flashblade", "garage", "aws", "seaweedfs"])
def test_registry_entries_are_well_formed(backend):
    entry = KNOWN_BACKENDS[backend]
    assert entry["label"]
    assert "notes" in entry
    assert isinstance(entry["region_strict"], bool)
