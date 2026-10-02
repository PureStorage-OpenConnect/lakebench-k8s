"""Per-deployment secrets.

A new deployment gets its own Hive metastore DB password, Polaris DB password
and Polaris client secret, each generated once and stored in a Secret in the
deployment's namespace (Category 1). An existing deployment keeps its own:

- **Hive metastore DB** (Secret ``lakebench-postgres-secret``, key
  ``password``). Postgres reads it only at initdb, so the stored value wins;
  with no Secret but an existing Postgres PVC the data was initialised with
  the v1.6 default; otherwise a new password is generated.
- **Polaris DB** (Secret ``lakebench-polaris-db``). With no Secret, an
  existing ``polaris`` role means a v1.6 deployment, which keeps the v1.6
  password; otherwise a new one is generated before ``CREATE USER``.
- **Polaris client secret** (Secret ``lakebench-polaris-client``). A config
  value wins (and is stored). With no config value the stored one is used;
  deploy generates it only for a fresh Polaris, because a realm bootstrapped
  earlier only accepts the secret it was bootstrapped with. ``run``,
  ``benchmark`` and ``destroy`` read it back, so separate invocations agree
  (a value generated at config load would differ per invocation).

Values are never logged.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig

logger = logging.getLogger(__name__)

HIVE_DB_SECRET = "lakebench-postgres-secret"
HIVE_DB_KEY = "password"
POLARIS_DB_SECRET = "lakebench-polaris-db"
POLARIS_DB_KEY = "password"
POLARIS_CLIENT_SECRET = "lakebench-polaris-client"
POLARIS_CLIENT_KEY = "clientSecret"
POSTGRES_PVC = "data-lakebench-postgres-0"

# The fixed v1.6 values. Used only to keep a deployment v1.6 created working.
LEGACY_HIVE_DB_PASSWORD = "lakebench-hive-2024"  # noqa: S105
LEGACY_POLARIS_DB_PASSWORD = "lakebench-polaris-2024"  # noqa: S105


class DeploymentSecretError(Exception):
    """A per-deployment secret cannot be resolved safely. One line with the fix."""


def _generate(nbytes: int) -> str:
    return secrets.token_urlsafe(nbytes)


def read_secret_key(core_v1: Any, namespace: str, name: str, key: str) -> str | None:
    """The decoded value of ``key`` in Secret ``name``; None when the Secret is
    absent. A Secret without the key, or any other API error, raises."""
    from kubernetes.client.rest import ApiException

    try:
        sec = core_v1.read_namespaced_secret(name, namespace)
    except ApiException as e:
        if e.status == 404:
            return None
        raise
    data = getattr(sec, "data", None) or {}
    if key not in data:
        raise DeploymentSecretError(
            f"Secret {name} in namespace {namespace} has no key {key!r}; restore it or destroy "
            "the deployment"
        )
    return base64.b64decode(data[key]).decode("utf-8")


def _labels(cfg: LakebenchConfig, component: str) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": "lakebench",
        "app.kubernetes.io/instance": cfg.name,
        "app.kubernetes.io/component": component,
        "app.kubernetes.io/managed-by": "lakebench",
        "lakebench.io/deployment": cfg.name,
    }


def create_secret(
    core_v1: Any,
    cfg: LakebenchConfig,
    namespace: str,
    name: str,
    key: str,
    value: str,
    component: str,
) -> str:
    """Create Secret ``name`` holding ``value``; returns the stored value. If
    another writer created it first (409), its value is returned instead, so
    two racing deploys of one deployment end with one value."""
    from kubernetes.client.rest import ApiException

    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace, "labels": _labels(cfg, component)},
        "type": "Opaque",
        "stringData": {key: value},
    }
    try:
        core_v1.create_namespaced_secret(namespace, body)
        return value
    except ApiException as e:
        if e.status != 409:
            raise
    stored = read_secret_key(core_v1, namespace, name, key)
    if stored is None:
        raise DeploymentSecretError(
            f"Secret {name} in namespace {namespace} vanished during create"
        )
    return stored


def _replace_secret_value(
    core_v1: Any,
    cfg: LakebenchConfig,
    namespace: str,
    name: str,
    key: str,
    value: str,
    component: str,
) -> None:
    body = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": namespace, "labels": _labels(cfg, component)},
        "type": "Opaque",
        "stringData": {key: value},
    }
    core_v1.replace_namespaced_secret(name, namespace, body)


def _pvc_exists(core_v1: Any, namespace: str, name: str) -> bool:
    from kubernetes.client.rest import ApiException

    try:
        pvc = core_v1.read_namespaced_persistent_volume_claim(name, namespace)
    except ApiException as e:
        if e.status == 404:
            return False
        raise
    # A claim being deleted holds no data the next Postgres will see.
    return not getattr(getattr(pvc, "metadata", None), "deletion_timestamp", None)


def hive_db_password(core_v1: Any, namespace: str) -> str:
    """The Hive metastore DB password this deployment must use (rules above).

    Read-only: the secrets step writes it through ``secrets.yaml.j2``. A
    generated value is stable for one deploy because the step writes it
    before Postgres starts, and every later deploy reads it back.
    """
    stored = read_secret_key(core_v1, namespace, HIVE_DB_SECRET, HIVE_DB_KEY)
    if stored:
        return stored
    if _pvc_exists(core_v1, namespace, POSTGRES_PVC):
        logger.info(
            "Secret %s absent but PVC %s exists: keeping the v1.6 metastore password",
            HIVE_DB_SECRET,
            POSTGRES_PVC,
        )
        return LEGACY_HIVE_DB_PASSWORD
    return _generate(24)


def ensure_polaris_db_password(
    core_v1: Any, cfg: LakebenchConfig, namespace: str, role_exists: bool
) -> str:
    """The Polaris DB password, from its Secret or written to it now."""
    stored = read_secret_key(core_v1, namespace, POLARIS_DB_SECRET, POLARIS_DB_KEY)
    if stored:
        return stored
    value = LEGACY_POLARIS_DB_PASSWORD if role_exists else _generate(24)
    return create_secret(
        core_v1, cfg, namespace, POLARIS_DB_SECRET, POLARIS_DB_KEY, value, "polaris"
    )


def ensure_polaris_client_secret(
    core_v1: Any, cfg: LakebenchConfig, namespace: str, fresh: bool
) -> str:
    """Deploy: the Polaris client secret, stored in ``lakebench-polaris-client``.

    ``fresh`` is True when Polaris has never been bootstrapped in this
    namespace (no ``polaris`` role); only then may a secret be generated.
    """
    configured = cfg.architecture.catalog.polaris.client_secret
    stored = read_secret_key(core_v1, namespace, POLARIS_CLIENT_SECRET, POLARIS_CLIENT_KEY)
    if configured:
        if stored is None:
            return create_secret(
                core_v1,
                cfg,
                namespace,
                POLARIS_CLIENT_SECRET,
                POLARIS_CLIENT_KEY,
                configured,
                "polaris",
            )
        if stored != configured:
            if not fresh:
                # The realm accepts only the secret it was bootstrapped with,
                # and Polaris stores it hashed: replacing the Secret would
                # lose the only copy and lock every client out.
                raise DeploymentSecretError(
                    "architecture.catalog.polaris.client_secret differs from the secret Polaris "
                    f"in namespace {namespace} was bootstrapped with (Secret "
                    f"{POLARIS_CLIENT_SECRET}); remove the config value to keep it, or destroy "
                    "and redeploy to change it"
                )
            _replace_secret_value(
                core_v1,
                cfg,
                namespace,
                POLARIS_CLIENT_SECRET,
                POLARIS_CLIENT_KEY,
                configured,
                "polaris",
            )
        return configured
    if stored:
        return stored
    if not fresh:
        raise DeploymentSecretError(
            f"Polaris in namespace {namespace} was bootstrapped with a client secret that this "
            f"config does not set and no Secret {POLARIS_CLIENT_SECRET} records; set "
            "architecture.catalog.polaris.client_secret to that value, or destroy and redeploy"
        )
    return create_secret(
        core_v1, cfg, namespace, POLARIS_CLIENT_SECRET, POLARIS_CLIENT_KEY, _generate(32), "polaris"
    )


def polaris_client_secret(cfg: LakebenchConfig, core_v1: Any | None = None) -> str:
    """Run, benchmark and destroy: the client secret deploy stored (or the
    config value). Never generates one."""
    from lakebench.config.schema import PolarisClientSecretMissing

    configured = cfg.architecture.catalog.polaris.client_secret
    if configured:
        return configured
    if core_v1 is None:
        from kubernetes import client as k8s_client

        core_v1 = k8s_client.CoreV1Api()
    namespace = cfg.get_namespace()
    stored = read_secret_key(core_v1, namespace, POLARIS_CLIENT_SECRET, POLARIS_CLIENT_KEY)
    if stored:
        return stored
    raise PolarisClientSecretMissing(
        f"no Polaris client secret: the config sets none and Secret {POLARIS_CLIENT_SECRET} is "
        f"not in namespace {namespace}; run lakebench deploy first, or, for a Polaris deployed "
        "by 1.6, set architecture.catalog.polaris.client_secret to the value it was deployed with"
    )


def polaris_role_exists(exec_psql: Callable[[str, str], str]) -> bool:
    """Whether the ``polaris`` Postgres role exists. ``exec_psql(db, sql)``
    returns psql's output; anything unparseable raises, never a guess."""
    out = exec_psql("hive", "SELECT 'lbrole:' || count(*) FROM pg_roles WHERE rolname = 'polaris';")
    for token in out.split():
        if token.startswith("lbrole:"):
            n = token.split(":", 1)[1]
            if n.isdigit():
                return int(n) > 0
    raise DeploymentSecretError(
        "could not tell whether the polaris role exists in lakebench-postgres-0 "
        f"(psql said: {out.strip()[:120]!r}); deploy stopped rather than guess its password"
    )


def scram_sha256_verifier(
    password: str, *, iterations: int = 4096, salt: bytes | None = None
) -> str:
    """A PostgreSQL SCRAM-SHA-256 verifier for ``password`` (RFC 5802 and
    7677, the format ``pg_authid.rolpassword`` stores). ``ALTER ROLE ...
    PASSWORD`` accepts it pre-hashed, so the plaintext never crosses the exec
    request, the API server audit log or the Postgres log."""
    if salt is None:
        salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()

    def b64(b: bytes) -> str:
        return base64.b64encode(b).decode("ascii")

    return f"SCRAM-SHA-256${iterations}:{b64(salt)}${b64(stored_key)}:{b64(server_key)}"


def sync_role_password(exec_psql: Callable[[str, str], str], role: str, password: str) -> None:
    """Make Postgres role ``role`` accept ``password``: the Secret is the
    authority, so a wrong v1.6 guess or a lost Secret cannot lock the
    metastore out. Idempotent; sends only a SCRAM verifier. Raises without
    echoing psql's output, which may quote the statement."""
    if not role.isidentifier():
        raise DeploymentSecretError(f"refusing to alter unexpected role name {role!r}")
    out = exec_psql("hive", f"ALTER ROLE {role} WITH PASSWORD '{scram_sha256_verifier(password)}';")
    if not psql_command_ok(out, "ALTER ROLE"):
        raise DeploymentSecretError(
            f"could not set the password of Postgres role {role} from its Secret; "
            "check lakebench-postgres-0, then re-run deploy"
        )


def postgres_psql(core_v1: Any, namespace: str) -> Callable[[str, str], str]:
    """``exec_psql(database, sql)`` against lakebench-postgres-0 over the
    local socket (trust auth, as the image's initdb configures it), with
    unaligned tuples-only output."""

    def exec_psql(database: str, sql: str) -> str:
        from kubernetes.stream import stream

        return str(
            stream(
                core_v1.connect_get_namespaced_pod_exec,
                "lakebench-postgres-0",
                namespace,
                command=["psql", "-U", "hive", "-d", database, "-tA", "-c", sql],
                stderr=True,
                stdout=True,
                stdin=False,
                tty=False,
            )
        )

    return exec_psql


def psql_command_ok(out: str, tag: str) -> bool:
    """psql printed the command tag ``tag`` on its own line and no error. An
    error message can quote the statement, so a substring test would pass."""
    lines = [ln.strip() for ln in out.splitlines()]
    if any(ln.startswith(("ERROR", "FATAL", "psql:")) for ln in lines):
        return False
    return tag in lines
