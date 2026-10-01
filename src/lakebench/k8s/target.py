"""The one cluster a lakebench process talks to (SAF-7).

The kubernetes client keeps a process-wide default configuration, and every
bare ``CoreV1Api()``/``AppsV1Api()`` copies it. That default is set only by
the kubeconfig load calls, so pinning the loads pins every client. This
module holds the only calls to ``load_kube_config`` and
``load_incluster_config`` in ``src/lakebench/`` (outside the driver scripts);
``tests/test_context_pinning_lint.py`` fails on any other.

* :meth:`ClusterTarget.resolve` turns a config (or a context string) into a
  named target. A config with no ``platform.kubernetes.context`` resolves the
  kubeconfig's current-context name once, so the process keeps one named
  context even if someone runs ``kubectl config use-context`` mid-run. When a
  target is already active, an empty context resolves to it.
* :meth:`ClusterTarget.activate` loads that context into the default
  configuration. A second activation with a different context in the same
  process raises :class:`ContextConflictError`. Activating the same target
  again reloads the same context (a re-read of the kubeconfig picks up a
  token refreshed by ``oc login``, as the per-client load always did), and
  is refused if that context now points at a different API server.
* :func:`cli_args` gives the ``--context``/``--kube-context`` flag for a
  ``kubectl``, ``oc`` or ``helm`` argv: the active target's, or the explicit
  context when none is active. An explicit context that differs from the
  active one raises :class:`ContextConflictError`. :func:`pin_command`
  activates the target at the start of a command whose cluster calls are
  all subprocesses.

In-cluster credentials are used only when no kubeconfig exists and the
process runs in a pod.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import threading
from dataclasses import dataclass, replace
from typing import Any

import yaml
from kubernetes import client as _k8s_client
from kubernetes import config as _k8s_config
from kubernetes.config import ConfigException


class ContextConflictError(RuntimeError):
    """A process tried to talk to a second cluster context."""


@dataclass(frozen=True)
class ClusterTarget:
    """One cluster context: a kubeconfig context name, or in-cluster."""

    context: str | None
    api_server: str = ""
    in_cluster: bool = False
    # sha256[:12] of the CA bytes read for this context when it was
    # activated: the ownership fingerprint of the pinned cluster
    # (``deploy/ownership.py``), frozen at the load so a kubeconfig rewritten
    # later cannot change what a deploy stamps or a destroy compares.
    ca_fingerprint: str | None = None
    # False when the cluster entry could not be read at activation; the
    # ownership fingerprint then falls back to reading the kubeconfig.
    ca_fp_known: bool = False

    @property
    def label(self) -> str:
        """Human-readable name for output and refusals."""
        name = "in-cluster service account" if self.in_cluster else str(self.context)
        return f"{name} ({self.api_server})" if self.api_server else name

    def _key(self) -> tuple[str | None, bool]:
        return (self.context, self.in_cluster)

    # -- resolution ---------------------------------------------------------

    @classmethod
    def resolve(cls, cfg: Any = None, *, context: str | None = None) -> ClusterTarget:
        """Return the target for ``cfg`` (or an explicit ``context``).

        ``context`` wins over ``cfg``; an empty string means "not set". With
        nothing set, the active target is reused when there is one, and the
        kubeconfig's current context is resolved by name otherwise. Raises
        :class:`ConfigException` when no kubeconfig and no in-cluster
        credentials exist, or when the named context is not in the
        kubeconfig.
        """
        explicit = context or config_context(cfg) or None
        with _LOCK:
            active = _ACTIVE
        if active is not None and (explicit is None or explicit == active.context):
            return active
        return _from_kubeconfig(explicit)

    @classmethod
    def current(cls) -> ClusterTarget:
        """The active target, or the kubeconfig's current context activated.

        For commands that run without a config (``admin status``,
        ``status --namespace``, ``recommend``).
        """
        with _LOCK:
            active = _ACTIVE
        if active is not None:
            return active
        return cls.resolve().activate()

    # -- activation ---------------------------------------------------------

    def activate(self) -> ClusterTarget:
        """Load this target into the process-default client configuration.

        Returns the active target with ``api_server`` filled in. The load
        goes into a scratch configuration first and becomes the default only
        when it targets the same API server as the active target: a
        kubeconfig rewritten mid-run so the same context name points at
        another cluster (every OpenShift installer kubeconfig names its
        context ``admin``) is refused, not followed.
        """
        global _ACTIVE
        _refuse_other(self)
        # Load outside the lock: an exec credential plugin can take a while,
        # and concurrent benchmark streams read the active target meanwhile.
        scratch = _k8s_client.Configuration()
        try:
            if self.in_cluster:
                _k8s_config.load_incluster_config(client_configuration=scratch)
            else:
                _k8s_config.load_kube_config(context=self.context, client_configuration=scratch)
        except ConfigException:
            raise
        except Exception as e:  # noqa: BLE001 -- yaml, permission, exec plugin
            raise ConfigException(f"cannot load cluster context {self.label}: {e}") from e
        host = str(scratch.host or "")
        fingerprint: str | None = None
        fp_known = True
        if self.in_cluster:
            fingerprint = _incluster_ca_fingerprint()
        else:
            block = _kubeconfig_cluster_block(str(self.context))
            if block is None:
                # Read mid-rewrite (``oc login`` truncates, then writes): once more.
                block = _kubeconfig_cluster_block(str(self.context))
            if block is None:
                fp_known = False
            else:
                server = str(block.get("server") or "").rstrip("/")
                if server and server != host:
                    raise ContextConflictError(
                        f"context {self.context!r} changed while it was being loaded "
                        f"({host} then {server}); the kubeconfig changed under this command"
                    )
                fingerprint = _ca_fingerprint(block)
        with _LOCK:
            _refuse_other(self, locked=True)
            if _ACTIVE is not None and _ACTIVE.api_server and host != _ACTIVE.api_server:
                raise ContextConflictError(
                    "one cluster context per process: "
                    f"{_ACTIVE.label} is active, and context {self.context!r} now "
                    f"points at {host}; the kubeconfig changed under this command"
                )
            if not fp_known and _ACTIVE is not None:
                # The cluster entry could not be read: keep what was pinned.
                # With nothing pinned, ownership falls back to its own read.
                fingerprint, fp_known = _ACTIVE.ca_fingerprint, _ACTIVE.ca_fp_known
            if (
                _ACTIVE is not None
                and _ACTIVE.ca_fp_known
                and fp_known
                and fingerprint != _ACTIVE.ca_fingerprint
            ):
                raise ContextConflictError(
                    "one cluster context per process: "
                    f"{_ACTIVE.label} is active, and context {self.context!r} now "
                    f"names a different cluster CA ({fingerprint} instead of "
                    f"{_ACTIVE.ca_fingerprint}); the kubeconfig changed under this command"
                )
            _k8s_client.Configuration.set_default(scratch)
            _ACTIVE = replace(
                self, api_server=host, ca_fingerprint=fingerprint, ca_fp_known=fp_known
            )
            return _ACTIVE

    def cli_args(self, tool: str) -> list[str]:
        """The context flag for ``tool`` (``kubectl``, ``oc`` or ``helm``)."""
        if self.in_cluster or not self.context:
            return []
        return context_flag(tool, self.context)


_LOCK = threading.Lock()
_ACTIVE: ClusterTarget | None = None


def _refuse_other(target: ClusterTarget, *, locked: bool = False) -> None:
    """Raise when a different target is already active."""
    active = _ACTIVE if locked else active_target()
    if active is not None and active._key() != target._key():
        raise ContextConflictError(
            f"one cluster context per process: {active.label} is active, refusing {target.label}"
        )


def config_context(cfg: Any) -> str | None:
    """``cfg.platform.kubernetes.context``, or None; accepts a str or None."""
    if cfg is None:
        return None
    if isinstance(cfg, str):
        return cfg or None
    ctx: Any = cfg
    for attr in ("platform", "kubernetes", "context"):
        ctx = getattr(ctx, attr, None)
        if ctx is None:
            return None
    return ctx if isinstance(ctx, str) and ctx else None


def active_target() -> ClusterTarget | None:
    """The target activated in this process, or None."""
    with _LOCK:
        return _ACTIVE


def context_flag(tool: str, context: str) -> list[str]:
    """``--kube-context X`` for helm, ``--context X`` for kubectl and oc."""
    if tool == "helm":
        return ["--kube-context", context]
    return ["--context", context]


def cli_args(tool: str, explicit: Any = None) -> list[str]:
    """The context flag for a ``tool`` argv.

    ``explicit`` is a config, a context string or None. When a target is
    active it supplies the flag, and an explicit context that differs from
    it raises :class:`ContextConflictError`. With no active target an
    explicit context is used as given; without one the kubeconfig's current
    context is resolved and pinned here (:func:`pin_command`), so a tool
    call made before the first API client still names the context the rest
    of the process uses. The argv carries no flag only when no kubeconfig
    and no in-cluster credentials exist.
    """
    name = config_context(explicit)
    active = active_target()
    if active is not None:
        if name and name != active.context:
            raise ContextConflictError(
                f"one cluster context per process: {active.label} is active, "
                f"refusing {tool} --context {name}"
            )
        if not active.in_cluster and active.context and active.api_server:
            # The tool re-reads the kubeconfig on every call; refuse when the
            # pinned name now points at another server (the API clients
            # would stay on the pinned one).
            block = _kubeconfig_cluster_block(active.context)
            server = str((block or {}).get("server") or "").rstrip("/") or None
            if server is not None and server != active.api_server:
                raise ContextConflictError(
                    f"one cluster context per process: {active.label} is active, and "
                    f"context {active.context!r} now points at {server}; the kubeconfig "
                    "changed under this command"
                )
            if block is not None and active.ca_fp_known:
                ca = _ca_fingerprint(block)
                if ca != active.ca_fingerprint:
                    raise ContextConflictError(
                        f"one cluster context per process: {active.label} is active, and "
                        f"context {active.context!r} now names a different cluster CA; "
                        "the kubeconfig changed under this command"
                    )
        return active.cli_args(tool)
    if name:
        return context_flag(tool, name)
    pinned = pin_command(None)
    return pinned.cli_args(tool) if pinned is not None else []


def pin_command(cfg: Any = None) -> ClusterTarget | None:
    """Pin this process to ``cfg``'s target at the start of a command.

    For commands whose first cluster call is a subprocess. Returns None
    (and pins nothing) only when neither a kubeconfig file nor in-cluster
    credentials exist; the command's first ``kubectl`` then fails on its
    own. Any other failure to resolve or load the context raises
    :class:`ConfigException`, so the command never runs unpinned.
    """
    if not _kubeconfig_exists() and not _in_cluster_available():
        return None
    return ClusterTarget.resolve(cfg).activate()


def _in_cluster_available() -> bool:
    return bool(os.environ.get("KUBERNETES_SERVICE_HOST"))


def _kubeconfig_exists() -> bool:
    """True when any file named by the kubeconfig location exists.

    The location is ``$KUBECONFIG`` (read by the library at import) or
    ``~/.kube/config``, and may list several files.
    """
    return any(os.path.exists(p) for p in _kubeconfig_paths())


def _unwrap(node: Any) -> Any:
    return node.value if hasattr(node, "value") else node


def _merged_kubeconfig() -> dict[str, Any]:
    """The merged kubeconfig file(s) as a dict, read without validation.

    Unlike ``list_kube_config_contexts()`` this does not check the merged
    current-context (the library lets the last file win and refuses a
    dangling one), and it runs no credential plugin. Raises
    :class:`ConfigException` when nothing can be read.
    """
    try:
        merged = _k8s_config.kube_config.KubeConfigMerger(
            _k8s_config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION
        ).config
    except Exception as e:  # noqa: BLE001 -- unreadable or malformed file
        raise ConfigException(f"cannot read kubeconfig: {e}") from e
    raw = _unwrap(merged)
    if not isinstance(raw, dict):
        raise ConfigException("Invalid kube-config file. No configuration found.")
    return raw


def _kubeconfig_cluster_block(context: str) -> dict[str, Any] | None:
    """The kubeconfig's cluster entry for ``context`` now, or None.

    Read from the merged file(s) without running any credential plugin.
    None when the file or the context cannot be read.
    """
    try:
        raw = _merged_kubeconfig()
    except ConfigException:
        return None
    cluster_name = None
    for entry in raw.get("contexts") or []:
        e = _unwrap(entry)
        if isinstance(e, dict) and e.get("name") == context:
            cluster_name = (_unwrap(e.get("context")) or {}).get("cluster")
            break
    if not cluster_name:
        return None
    for entry in raw.get("clusters") or []:
        e = _unwrap(entry)
        if isinstance(e, dict) and e.get("name") == cluster_name:
            block = _unwrap(e.get("cluster"))
            return dict(block) if isinstance(block, dict) else {}
    return None


def load_ca_bytes(cluster_block: dict[str, Any]) -> bytes:
    """Return the CA certificate bytes for a kubeconfig cluster block.

    kubeconfig stores the CA as either:
    - ``certificate-authority-data``: base64-encoded PEM (inline),
    - ``certificate-authority``: path to a PEM file on disk.

    Some producers write hex instead of base64 into
    ``certificate-authority-data`` when they emit the file. We treat
    both encodings as valid CA material and decode to raw bytes so all
    paths (workstation, in-cluster, inline, file) hash the same thing.
    """
    inline = cluster_block.get("certificate-authority-data", "")
    if isinstance(inline, bytes):
        return inline
    if isinstance(inline, str) and inline:
        # Try base64 first (the standard); fall back to hex.
        try:
            return base64.b64decode(inline, validate=True)
        except (binascii.Error, ValueError):
            try:
                return bytes.fromhex(inline)
            except ValueError:
                # Not decodable -- treat as the raw string bytes.
                return inline.encode()

    ca_path = cluster_block.get("certificate-authority")
    if isinstance(ca_path, str) and ca_path:
        try:
            with open(ca_path, "rb") as f:
                return f.read()
        except OSError:
            return b""

    return b""


def _ca_fingerprint(cluster_block: dict[str, Any]) -> str | None:
    """sha256[:12] of a cluster block's CA bytes, or None without a CA."""
    ca = load_ca_bytes(cluster_block)
    return hashlib.sha256(ca).hexdigest()[:12] if ca else None


INCLUSTER_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _incluster_ca_fingerprint() -> str | None:
    """sha256[:12] of the mounted service-account CA, or None outside a pod."""
    try:
        with open(INCLUSTER_CA_PATH, "rb") as f:
            ca = f.read()
    except OSError:
        return None
    return hashlib.sha256(ca).hexdigest()[:12]


def _kubeconfig_paths() -> list[str]:
    location = str(_k8s_config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION)
    return [os.path.expanduser(p) for p in location.split(os.pathsep) if p.strip()]


def _first_current_context() -> str | None:
    """``current-context`` of the first kubeconfig file that sets one.

    kubectl's merge rule for a multi-file ``$KUBECONFIG``. None when no
    readable file sets it.
    """
    for path in _kubeconfig_paths():
        try:
            with open(path) as f:
                doc = yaml.safe_load(f)
        except (OSError, yaml.YAMLError):
            continue
        if isinstance(doc, dict):
            value = doc.get("current-context")
            if isinstance(value, str) and value:
                return value
    return None


def _from_kubeconfig(explicit: str | None) -> ClusterTarget:
    if explicit is None and not _kubeconfig_exists() and _in_cluster_available():
        return ClusterTarget(context=None, in_cluster=True)
    raw = _merged_kubeconfig()
    names = [
        e.get("name")
        for e in (_unwrap(c) for c in raw.get("contexts") or [])
        if isinstance(e, dict)
    ]
    if explicit is not None:
        if explicit not in names:
            raise ConfigException(
                f"context {explicit!r} (platform.kubernetes.context) is not in the kubeconfig"
            )
        return ClusterTarget(context=explicit)
    # kubectl takes current-context from the first file in $KUBECONFIG that
    # sets it; the Python client's merger lets the last one win. Follow
    # kubectl, so an empty platform.kubernetes.context means what
    # `kubectl config current-context` prints.
    name = _first_current_context() or _unwrap(raw.get("current-context")) or None
    if name and name not in names:
        raise ConfigException(f"the kubeconfig's current context {name!r} is not defined in it")
    if not name:
        raise ConfigException(
            "the kubeconfig has no current context; set platform.kubernetes.context"
        )
    return ClusterTarget(context=str(name))


def _reset_for_tests() -> None:
    """Forget the active target (unit tests only)."""
    global _ACTIVE
    with _LOCK:
        _ACTIVE = None


__all__ = [
    "ClusterTarget",
    "ContextConflictError",
    "active_target",
    "cli_args",
    "config_context",
    "context_flag",
    "pin_command",
]
