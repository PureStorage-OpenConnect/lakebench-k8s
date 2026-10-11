"""Shared harness for `lakebench clean` tests: a deployment config and a
present, verified namespace."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from unittest.mock import patch

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


@contextmanager
def verified_namespace(bucket_report=None):
    """Make the deployment's namespace present and verified.

    `bucket_report` is what verify_bucket_ownership returns. Leave it None
    when the test patches that check itself (for example UNSUPPORTED, the
    FlashBlade case).
    """
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    with ExitStack() as stack:
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
        if bucket_report is not None:
            stack.enter_context(
                patch(
                    "lakebench.deploy.ownership.verify_bucket_ownership", return_value=bucket_report
                )
            )
        yield
