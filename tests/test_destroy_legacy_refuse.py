"""Destroy refuses on legacy (annotation-less / tag-less) resources.

Regression against the roleplay P0: the design doc claimed destroy
refuses on `IdentityVerdict.ABSENT`, but the code was warn-and-proceed.
A destroy pointed at a legacy annotationless namespace would happily
proceed to empty its buckets -- the exact silent-corruption class the
ownership invariant was written to prevent.

Post-fix contract:

- ABSENT namespace annotations without ``--force-legacy``: refuse and
  return with ``ownership-check`` FAILED, no namespaced resources
  touched.
- ABSENT bucket tag without ``--force-legacy``: refuse and return with
  ``s3-buckets`` FAILED, no ``empty_bucket`` call.
- ABSENT + ``--force-legacy``: proceed loudly with a WARN in logs and
  the destroy summary.
- MISMATCH always refuses, ``--force-legacy`` does not override.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _bare_engine() -> MagicMock:
    engine = MagicMock()
    cfg = engine.config
    cfg.get_namespace.return_value = "lb-test"
    cfg.name = "lb-test"
    cfg.platform.kubernetes.create_namespace = True
    cfg.platform.kubernetes.context = None
    cfg.platform.compute.spark.operator.namespace = "spark-operator"
    cfg.platform.compute.spark.operator.version = "2.5.1"
    cfg.platform.storage.s3.buckets.bronze = "lb-test-bronze"
    cfg.platform.storage.s3.buckets.silver = "lb-test-silver"
    cfg.platform.storage.s3.buckets.gold = "lb-test-gold"
    return engine


def _healthy_s3() -> MagicMock:
    s3 = MagicMock()
    s3._init_error = None
    s3.empty_bucket.return_value = 0
    return s3


def _run(engine, force_legacy: bool, ns_verdict, bucket_verdict):
    """Run destroy_all with mocked identity/bucket checks and neutered
    K8s client access. Returns (results, reports) where reports is a
    list of (component, status, message) captured from the progress
    callback -- ownership-check emits via report(), not results.append,
    so tests need the callback trail."""
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport

    ns_report = IdentityReport(
        verdict=ns_verdict,
        resource_name="lb-test",
        expected_deployment="lb-test",
        hint="test hint",
    )
    bucket_report = IdentityReport(
        verdict=bucket_verdict,
        resource_name="bkt",
        expected_deployment="lb-test",
        hint="test bucket hint",
    )
    reports: list[tuple[str, str, str]] = []

    def _capture(component, status, message):
        reports.append((component, str(status), message))

    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=ns_report),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=bucket_report),
        patch("kubernetes.client.CoreV1Api"),
        patch("kubernetes.client.CustomObjectsApi"),
        patch("kubernetes.client.RbacAuthorizationV1Api"),
        patch("kubernetes.client.StorageV1Api"),
        patch("kubernetes.client.BatchV1Api"),
        patch("lakebench.deploy.destroy.logger"),
        # A healthy S3 client: an unmocked one fails init on the MagicMock
        # config, and destroy (correctly) keeps the namespace after a failed
        # bucket step, which is not what these tests are about.
        patch("lakebench.s3.S3Client", return_value=_healthy_s3()),
    ):
        results = destroy_all(engine, force_legacy=force_legacy, progress_callback=_capture)
    return results, reports


class TestNamespaceAbsent:
    """ABSENT namespace annotations must refuse unless --force-legacy."""

    def test_refuse_without_force_legacy(self):
        from lakebench.deploy.ownership import IdentityVerdict

        engine = _bare_engine()
        engine.k8s.namespace_exists.return_value = True
        results, reports = _run(
            engine,
            force_legacy=False,
            ns_verdict=IdentityVerdict.ABSENT,
            bucket_verdict=IdentityVerdict.MATCH,
        )
        # Results carries a FAILED ownership-check row (refuse path).
        oc_results = [r for r in results if r.component == "ownership-check"]
        assert oc_results and oc_results[-1].status.value.lower() == "failed"
        assert "admin migrate-deployment" in oc_results[-1].message
        assert "--force-legacy" in oc_results[-1].message
        # Destroy must NOT have proceeded to delete the namespace.
        engine.k8s.delete_namespace.assert_not_called()
        # Only one report emitted -- the FAILED ownership-check.
        oc_reports = [r for r in reports if r[0] == "ownership-check"]
        assert oc_reports and "FAILED" in oc_reports[-1][1].upper()

    def test_proceed_with_force_legacy(self):
        """With --force-legacy on ABSENT, destroy proceeds all the way
        through to the namespace delete step, and emits a WARN-flavoured
        ownership-check SUCCESS via the progress callback."""
        from lakebench.deploy.ownership import IdentityVerdict

        engine = _bare_engine()
        engine.k8s.namespace_exists.return_value = True
        _results, reports = _run(
            engine,
            force_legacy=True,
            ns_verdict=IdentityVerdict.ABSENT,
            bucket_verdict=IdentityVerdict.ABSENT,
        )
        # Ownership-check reported SUCCESS with the --force-legacy note.
        oc_reports = [r for r in reports if r[0] == "ownership-check"]
        assert oc_reports
        _, status_str, msg = oc_reports[-1]
        assert "SUCCESS" in status_str.upper()
        assert "--force-legacy" in msg
        # And crucially, destroy actually proceeded to the namespace
        # delete -- the whole point of the escape hatch.
        engine.k8s.delete_namespace.assert_called_once_with("lb-test")

    def test_mismatch_refuses_regardless_of_force_legacy(self):
        """A foreign annotation is never bypassable by --force-legacy."""
        from lakebench.deploy.ownership import IdentityVerdict

        engine = _bare_engine()
        engine.k8s.namespace_exists.return_value = True
        results, _reports = _run(
            engine,
            force_legacy=True,
            ns_verdict=IdentityVerdict.MISMATCH,
            bucket_verdict=IdentityVerdict.MATCH,
        )
        oc = [r for r in results if r.component == "ownership-check"]
        assert oc and oc[-1].status.value.lower() == "failed"
        engine.k8s.delete_namespace.assert_not_called()


class TestBucketAbsent:
    """ABSENT bucket tag must refuse unless --force-legacy."""

    def _run_with_s3(self, force_legacy: bool, bucket_verdict):
        from lakebench.deploy.destroy import destroy_all
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        engine = _bare_engine()
        engine.k8s.namespace_exists.return_value = True
        s3_cfg = engine.config.platform.storage.s3
        s3_cfg.endpoint = "http://s3:9000"
        s3_cfg.buckets.bronze = "bronze"
        s3_cfg.buckets.silver = "silver"
        s3_cfg.buckets.gold = "gold"

        ns_match = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="lb-test",
            expected_deployment="lb-test",
        )
        bucket_report = IdentityReport(
            verdict=bucket_verdict,
            resource_name="bkt",
            expected_deployment="lb-test",
            hint="bucket hint",
        )
        mock_s3 = MagicMock()
        mock_s3._init_error = None
        mock_s3.raw_client = MagicMock()
        with (
            patch(
                "lakebench.deploy.ownership.verify_namespace_identity",
                return_value=ns_match,
            ),
            patch(
                "lakebench.deploy.ownership.verify_bucket_ownership",
                return_value=bucket_report,
            ),
            patch("kubernetes.client.CoreV1Api"),
            patch("kubernetes.client.CustomObjectsApi"),
            patch("kubernetes.client.RbacAuthorizationV1Api"),
            patch("kubernetes.client.StorageV1Api"),
            patch("kubernetes.client.BatchV1Api"),
            patch("lakebench.s3.S3Client", return_value=mock_s3),
            patch("lakebench.deploy.destroy.logger"),
        ):
            results = destroy_all(engine, force_legacy=force_legacy)
        return results, mock_s3

    def test_refuse_untagged_without_force_legacy(self):
        from lakebench.deploy.ownership import IdentityVerdict

        results, s3 = self._run_with_s3(force_legacy=False, bucket_verdict=IdentityVerdict.ABSENT)
        s3_res = [r for r in results if r.component == "s3-buckets"]
        assert s3_res and s3_res[-1].status.value.lower() == "failed"
        assert "no lakebench ownership tag" in s3_res[-1].message
        assert "--force-legacy" in s3_res[-1].message
        # empty_bucket must not have been called.
        s3.empty_bucket.assert_not_called()

    def test_proceed_untagged_with_force_legacy(self):
        from lakebench.deploy.ownership import IdentityVerdict

        results, s3 = self._run_with_s3(force_legacy=True, bucket_verdict=IdentityVerdict.ABSENT)
        s3_res = [r for r in results if r.component == "s3-buckets"]
        # Proceeded through -- SUCCESS on the bucket-empty step.
        assert s3_res and s3_res[-1].status.value.lower() == "success"
        assert s3.empty_bucket.call_count == 3  # bronze / silver / gold

    def test_mismatch_never_bypassable(self):
        """Even with --force-legacy, a foreign-tagged bucket refuses."""
        from lakebench.deploy.ownership import IdentityVerdict

        results, s3 = self._run_with_s3(force_legacy=True, bucket_verdict=IdentityVerdict.MISMATCH)
        s3_res = [r for r in results if r.component == "s3-buckets"]
        assert s3_res and s3_res[-1].status.value.lower() == "failed"
        assert "owned by another deployment" in s3_res[-1].message
        s3.empty_bucket.assert_not_called()


class TestCliFlag:
    """The --force-legacy flag is exposed on the destroy CLI."""

    def test_flag_registered(self):
        from typer.testing import CliRunner

        from lakebench.cli import app

        runner = CliRunner()
        r = runner.invoke(app, ["destroy", "--help"])
        assert r.exit_code == 0
        assert "--force-legacy" in r.output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
