"""`lakebench clean` ownership and confirmation regressions (2026-09-24 audit).

1. On a backend without bucket tagging (FlashBlade) every bucket reports
   UNSUPPORTED, and clean fell straight through to empty_bucket with no name
   check. It must apply the same longest-prefix rule destroy uses, and refuse
   when that cannot be checked.
2. Answering "no" to the running-jobs prompt was swallowed (click.Abort is a
   RuntimeError caught by a broad except), so the clean went ahead.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
import typer

CFG = (
    "name: my-clean\n"
    "platform:\n"
    "  storage:\n"
    "    s3:\n"
    "      endpoint: http://minio:9000\n"
    "      access_key: k\n"
    "      secret_key: s\n"
    "      buckets:\n"
    "        bronze: my-clean-bronze\n"
    "        silver: my-clean-silver\n"
    "        gold: my-clean-gold\n"
)


def _cfg(tmp_path: Path) -> Path:
    p = tmp_path / "c.yaml"
    p.write_text(CFG)
    return p


def _unsupported(bucket="b"):
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    return IdentityReport(
        verdict=IdentityVerdict.UNSUPPORTED,
        resource_name=bucket,
        expected_deployment="my-clean",
        hint="no tagging",
    )


def _s3():
    s3 = MagicMock()
    s3._init_error = None
    s3.empty_bucket.return_value = 0
    return s3


def _verified_namespace():
    """Patches that make the deployment's namespace present and verified,
    so these tests exercise the bucket-level checks after the gate."""
    from contextlib import ExitStack

    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    stack = ExitStack()
    stack.enter_context(patch("kubernetes.client.CoreV1Api"))
    stack.enter_context(
        patch(
            "lakebench.deploy.ownership.verify_namespace_identity",
            return_value=IdentityReport(
                verdict=IdentityVerdict.MATCH,
                resource_name="lakebench",
                expected_deployment="my-clean",
                hint="",
            ),
        )
    )
    stack.enter_context(patch("lakebench.deploy.ownership.build_identity_from_config"))
    return stack


def _clean(cfg, **kw):
    from lakebench.cli._clean import clean

    args = {
        "target": "data",
        "config_file": cfg,
        "file_option": None,
        "force": True,
        "force_legacy": False,
        "metrics_dir": Path("/tmp/nonexistent-metrics"),
    }
    args.update(kw)
    with _verified_namespace():
        return clean(**args)


@patch("lakebench.k8s.get_k8s_client")
@patch("kubernetes.client.CustomObjectsApi")
@patch("kubernetes.client.BatchV1Api")
@patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=None)
@patch("lakebench.deploy.ownership.verify_bucket_ownership")
@patch("lakebench.s3.S3Client")
def test_unsupported_refuses_when_siblings_unknown(
    s3_cls, verify, _names, _batch, _crd, _k8s, tmp_path
):
    s3 = _s3()
    s3_cls.return_value = s3
    verify.side_effect = lambda _c, b, _n: _unsupported(b)
    with pytest.raises(typer.Exit) as exc:
        _clean(_cfg(tmp_path))
    assert exc.value.exit_code == 1
    assert s3.empty_bucket.call_count == 0


@patch("lakebench.k8s.get_k8s_client")
@patch("kubernetes.client.CustomObjectsApi")
@patch("kubernetes.client.BatchV1Api")
@patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=["my"])
@patch("lakebench.deploy.ownership.verify_bucket_ownership")
@patch("lakebench.s3.S3Client")
def test_unsupported_refuses_when_other_deployment_has_prefix_claim(
    s3_cls, verify, _names, _batch, _crd, _k8s, tmp_path
):
    # Another deployment named "my-clean-x" would win buckets it prefixes;
    # here the bucket name does not match "my-clean" at all.
    s3 = _s3()
    s3_cls.return_value = s3
    verify.side_effect = lambda _c, b, _n: _unsupported(b)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        CFG.replace("my-clean-bronze", "other-bronze")
        .replace("my-clean-silver", "other-silver")
        .replace("my-clean-gold", "other-gold")
    )
    with pytest.raises(typer.Exit):
        _clean(cfg)
    assert s3.empty_bucket.call_count == 0


@patch("lakebench.k8s.get_k8s_client")
@patch("kubernetes.client.CustomObjectsApi")
@patch("kubernetes.client.BatchV1Api")
@patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=[])
@patch("lakebench.deploy.ownership.verify_bucket_ownership")
@patch("lakebench.s3.S3Client")
def test_unsupported_cleans_on_prefix_match(s3_cls, verify, _names, _batch, _crd, _k8s, tmp_path):
    s3 = _s3()
    s3_cls.return_value = s3
    verify.side_effect = lambda _c, b, _n: _unsupported(b)
    _clean(_cfg(tmp_path))
    assert s3.empty_bucket.call_count == 3


@patch("lakebench.k8s.get_k8s_client")
@patch("kubernetes.client.CustomObjectsApi")
@patch("kubernetes.client.BatchV1Api")
@patch("lakebench.deploy.ownership.verify_bucket_ownership")
@patch("lakebench.s3.S3Client")
def test_declining_running_jobs_prompt_aborts(s3_cls, verify, batch, _crd, _k8s, tmp_path):
    s3 = _s3()
    s3_cls.return_value = s3
    batch.return_value.read_namespaced_job.return_value.status.active = 2

    # Answer yes to the general "are you sure" prompt and no to the
    # running-jobs prompt, so the test exercises the latter.
    def _confirm(text, *a, **k):
        if "still writing" in text.lower() or "running" in text.lower():
            raise click.Abort()
        return True

    with patch("typer.confirm", side_effect=_confirm):
        with pytest.raises(click.Abort):
            _clean(_cfg(tmp_path), force=False)
    assert s3.empty_bucket.call_count == 0
    verify.assert_not_called()


@patch("lakebench.k8s.get_k8s_client")
@patch("lakebench.s3.S3Client")
def test_clean_refuses_when_namespace_absent(s3_cls, _k8s, tmp_path):
    """Same rule as destroy: a missing namespace (stale or wrong context)
    means ownership cannot be proven, so buckets are not touched."""
    from kubernetes.client.rest import ApiException

    from lakebench.cli._clean import clean

    s3 = _s3()
    s3_cls.return_value = s3
    with patch("kubernetes.client.CoreV1Api") as core:
        core.return_value.read_namespace.side_effect = ApiException(status=404)
        core.return_value.list_namespace.return_value.items = []
        with pytest.raises(typer.Exit):
            clean(
                target="data",
                config_file=_cfg(tmp_path),
                file_option=None,
                force=True,
                force_legacy=False,
                metrics_dir=Path("/tmp/nonexistent-metrics"),
            )
    s3.empty_bucket.assert_not_called()
