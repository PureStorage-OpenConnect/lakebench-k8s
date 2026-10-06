"""The CLI side of the nameless-config check.

A nameless config acts only on a deployment it can prove is its own.

``destroy``, ``stop``, ``status`` and ``logs`` call :func:`guard_nameless`
right after loading the config and before their first cluster step. A config
with a ``name:`` passes without any cluster call; the checks themselves are
``config.deploy_state.check_nameless_target``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

#: Help text of the ``--name`` option on the commands that take one.
NAME_OPTION_HELP = (
    "The deployment name, for a config with no name: in a directory with "
    "several nameless configs, or a v1.6 directory (only .lakebench/state.json). "
    "Must equal the config's own name when it has one."
)


def _core_v1_factory(cfg: Any):
    def make() -> Any:
        from kubernetes import client

        from lakebench.k8s import get_k8s_client

        # Pins the process to the config's context before the read, so the
        # check reads the right cluster.
        get_k8s_client(context=cfg.platform.kubernetes.context, namespace=cfg.get_namespace())
        return client.CoreV1Api()

    return make


def _bucket_owned_factory(cfg: Any):
    """``bucket -> bool``: the bucket's ownership tag names this deployment.

    Any failure (no credentials, no tagging on the backend, unreachable)
    answers False, so an unproven bucket refuses the nameless teardown.
    """
    state: dict[str, Any] = {}

    def owned(bucket: str) -> bool:
        try:
            from lakebench.deploy.ownership import IdentityVerdict, verify_bucket_ownership
            from lakebench.s3 import S3Client

            if "client" not in state:
                s3 = cfg.platform.storage.s3
                state["client"] = S3Client(
                    endpoint=s3.endpoint,
                    access_key=s3.access_key,
                    secret_key=s3.secret_key,
                    region=s3.region,
                    path_style=s3.path_style,
                    ca_cert=s3.ca_cert,
                    verify_ssl=s3.verify_ssl,
                )
            client = state["client"]
            if client._init_error:
                return False
            if "cluster" not in state:
                from lakebench.deploy.ownership import api_server_fingerprint

                state["cluster"] = api_server_fingerprint(cfg.platform.kubernetes.context or "")
            # Only this deployment's and this cluster's stamp is proof (a
            # name tag alone reads the same on another cluster), so no
            # created-buckets record is passed: a legacy bucket is not MATCH.
            report = verify_bucket_ownership(
                client.raw_client,
                bucket,
                cfg.name,
                expected_cluster=state["cluster"],
                created_record=(),
            )
            return report.verdict is IdentityVerdict.MATCH
        except Exception:  # noqa: BLE001 -- unproven is refused, never assumed
            return False

    return owned


def guard_nameless(cfg: Any, config_file: Path, *, allow_absent: bool) -> str | None:
    """Run the nameless-config checks for ``cfg``; see ``check_nameless_target``.

    Returns the verified namespace incarnation (``uid#nonce``) or None.
    Raises ``SafetyRefusal`` (exit 3) or ``PrerequisiteError`` (exit 4).
    """
    from lakebench.config.deploy_state import check_nameless_target

    return check_nameless_target(
        cfg,
        _core_v1_factory(cfg),
        config_path=config_file,
        allow_absent=allow_absent,
        bucket_owned=_bucket_owned_factory(cfg),
    )
