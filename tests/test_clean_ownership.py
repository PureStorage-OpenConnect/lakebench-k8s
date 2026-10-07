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
    # No engine pod: clean empties the buckets and leaves the catalog, with a
    # warning (tests/test_clean_unregister.py covers the unregister step).
    stack.enter_context(
        patch(
            "lakebench.modules.table_formats.iceberg.maintenance.find_maintenance_engine",
            return_value=(None, None, None),
        )
    )
    return stack


def _clean(cfg, **kw):
    from lakebench.cli._clean import clean

    args = {
        "target": "silver",
        "config_file": cfg,
        "file_option": None,
        "force": True,
        "force_legacy": False,
    }
    args.update(kw)
    with _verified_namespace():
        return clean(**args)


@pytest.mark.parametrize(
    ("siblings", "tagless_ours", "other_buckets", "cleaned"),
    [
        (None, True, False, False),  # sibling list unreadable: refuse
        (["my"], True, True, False),  # the bucket names do not match this deployment
        ([], True, False, True),  # prefix match and contents recorded as ours
        ([], False, False, False),  # prefix match but no record: a user's bucket
    ],
)
def test_tagless_bucket_is_cleaned_only_when_provably_ours(
    tmp_path, siblings, tagless_ours, other_buckets, cleaned
):
    s3 = _s3()
    if other_buckets:
        cfg = tmp_path / "c.yaml"
        cfg.write_text(
            CFG.replace("my-clean-bronze", "other-bronze")
            .replace("my-clean-silver", "other-silver")
            .replace("my-clean-gold", "other-gold")
        )
    else:
        cfg = _cfg(tmp_path)
    with (
        patch("lakebench.s3.S3Client", return_value=s3),
        patch(
            "lakebench.deploy.ownership.verify_bucket_ownership",
            side_effect=lambda _c, b, _n, **_k: _unsupported(b),
        ),
        patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=siblings),
        patch("kubernetes.client.BatchV1Api"),
        patch("kubernetes.client.CustomObjectsApi"),
        patch("lakebench.k8s.get_k8s_client"),
        patch("lakebench.deploy.ownership.tagless_contents_are_ours", return_value=tagless_ours),
    ):
        if cleaned:
            _clean(cfg)
        else:
            with pytest.raises(typer.Exit) as exc:
                _clean(cfg)
            assert exc.value.exit_code != 0
    assert s3.empty_bucket.call_count == (1 if cleaned else 0)  # clean silver: one bucket


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
                target="silver",
                config_file=_cfg(tmp_path),
                file_option=None,
                force=True,
                force_legacy=False,
            )
    s3.empty_bucket.assert_not_called()
