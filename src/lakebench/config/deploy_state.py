"""Deployment state kept beside a config, and name resolution for nameless configs.

A config with no ``name:`` used to get a time-based name written to
``<config dir>/.lakebench/state.json`` on every load, including read-only
ones, and every nameless config in a directory resolved to that same name.
From v1.7 a nameless config cannot change data (SAF-2): the commands that
change data refuse it, and the read and teardown commands load it under the
name :func:`resolve_name` resolves.

Name resolution only reads. The deploy state below (SAF-2 b, f) is written
only by ``deploy`` and ``relocate_state``.
"""

from __future__ import annotations

import getpass
import hashlib
import json
import re
import socket
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

#: The v1.6 state file, relative to the config's directory. Only ever read.
LEGACY_STATE = Path(".lakebench") / "state.json"

NameSource = Literal["config", "override", "legacy-state", "suggested"]


@dataclass(frozen=True)
class NameResolution:
    """Which name a config resolves to, and where that name came from.

    ``source`` is ``config`` (the file sets ``name:``), ``override`` (the
    caller passed one), ``legacy-state`` (read from the v1.6
    ``.lakebench/state.json``) or ``suggested`` (none of these).
    ``legacy_name`` is the name in the v1.6 state file, when there is one,
    whatever the source.
    """

    name: str
    source: NameSource
    legacy_state_path: Path
    legacy_name: str | None = None

    @property
    def nameless(self) -> bool:
        """True when the config file itself sets no name."""
        return self.source != "config"


def legacy_state_path(config_path: str | Path) -> Path:
    """Where v1.6 kept the auto-generated name for configs in this directory."""
    return Path(config_path).absolute().parent / LEGACY_STATE


def read_legacy_name(config_path: str | Path) -> str | None:
    """The name a v1.6 load recorded for nameless configs in this directory.

    Returned verbatim. A missing, unreadable or malformed file gives None;
    the file is never repaired or rewritten.
    """
    path = legacy_state_path(config_path)
    try:
        with open(path) as f:
            state = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(state, dict):
        return None
    name = state.get("name")
    if isinstance(name, str) and name:
        return name
    return None


def _user_token() -> str:
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 -- no USER/LOGNAME and no passwd entry
        user = ""
    token = re.sub(r"[^a-z0-9]", "", user.lower())[:8]
    return token or "user"


def suggested_name(config_path: str | Path) -> str:
    """A name to offer for a nameless config: ``lb-<user>-<6 hex>``.

    The hex digits are the start of the SHA-256 of this host's name and the
    config's resolved path, so the same file on the same host gets the same
    suggestion every time. The host is in the hash because every lane and
    container here runs as root: with the path alone, ``/root/lakebench.yaml``
    would get one suggestion on every host. At most 18 characters, inside
    the shortest derived-name limit (23, the Hive metastore volume).
    """
    key = f"{socket.gethostname()}:{Path(config_path).resolve()}"
    digest = hashlib.sha256(key.encode()).hexdigest()
    return f"lb-{_user_token()}-{digest[:6]}"


def resolve_name(
    config_path: str | Path,
    raw: dict[str, Any],
    name_override: str | None = None,
) -> NameResolution:
    """Resolve the deployment name for a config without writing anything.

    Order: the file's ``name:``, then ``name_override``, then the v1.6 state
    file, then :func:`suggested_name`. A ``name_override`` that disagrees with
    the file's own ``name:`` is an error, because it would point the command
    at a deployment the file does not describe.
    """
    legacy_path = legacy_state_path(config_path)
    legacy = read_legacy_name(config_path)
    configured = raw.get("name")
    if configured:
        name = str(configured)
        if name_override and name_override != name:
            raise ValueError(
                f"--name {name_override!r} does not match the config's name {name!r}; "
                "--name is only for configs that set no name"
            )
        return NameResolution(name, "config", legacy_path, legacy)
    if name_override:
        return NameResolution(name_override, "override", legacy_path, legacy)
    if legacy:
        return NameResolution(legacy, "legacy-state", legacy_path, legacy)
    return NameResolution(suggested_name(config_path), "suggested", legacy_path, None)


# ---------------------------------------------------------------------------
# Per-deployment state: the nonces this directory's deploys stamped (SAF-2 b)
# ---------------------------------------------------------------------------
#
# Unlike everything above, the functions below write: ``deploy`` records the
# nonce it is about to stamp on the namespace in
# ``<config dir>/.lakebench/<name>.json`` before it stamps it, and
# ``relocate_state`` moves that record with a config. Read-only commands use
# only ``read_state`` and ``current_incarnation``.

#: Schema tag of the state file, also written on the namespace (annotation
#: ``lakebench.deployment/state-schema``) by a deploy that recorded its nonce.
STATE_SCHEMA = "lb-state/1"

#: How many nonces a state keeps. Each crashed deploy can leave one entry the
#: namespace never received; the entry the namespace carries is never evicted.
STATE_NONCES_KEPT = 5

NonceStatus = Literal["pending", "confirmed"]


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _host() -> str:
    return socket.gethostname()


@dataclass
class NonceEntry:
    """One deploy nonce this directory recorded, newest first in the state."""

    nonce: str
    status: NonceStatus
    at: str

    def to_json(self) -> dict[str, Any]:
        return {"nonce": self.nonce, "status": self.status, "at": self.at}


@dataclass
class DeployState:
    """The contents of ``<config dir>/.lakebench/<name>.json``."""

    name: str
    config_path: str
    config_dir: str
    host: str
    namespace: str
    namespace_uid: str | None = None
    api_server: str = ""
    nonces: list[NonceEntry] = field(default_factory=list)
    moved_from: dict[str, Any] | None = None
    moved_to: str | None = None
    recorded_at: str = ""
    schema: str = STATE_SCHEMA

    @property
    def deploying(self) -> bool:
        """The newest deploy has not confirmed its nonce on the namespace."""
        return bool(self.nonces) and self.nonces[0].status == "pending"

    def kept_nonces(self) -> list[str]:
        return [e.nonce for e in self.nonces]

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "name": self.name,
            "config_path": self.config_path,
            "config_dir": self.config_dir,
            "host": self.host,
            "namespace": self.namespace,
            "namespace_uid": self.namespace_uid,
            "api_server": self.api_server,
            "nonces": [e.to_json() for e in self.nonces],
            "moved_from": self.moved_from,
            "moved_to": self.moved_to,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> DeployState:
        if not isinstance(data, dict) or data.get("schema") != STATE_SCHEMA:
            raise ValueError(f"not a {STATE_SCHEMA} state")
        nonces = []
        for e in data.get("nonces") or []:
            status = e.get("status")
            if not isinstance(e.get("nonce"), str) or status not in ("pending", "confirmed"):
                raise ValueError(f"malformed nonce entry {e!r}")
            nonces.append(NonceEntry(e["nonce"], status, str(e.get("at", ""))))
        return cls(
            name=str(data["name"]),
            config_path=str(data["config_path"]),
            config_dir=str(data["config_dir"]),
            host=str(data["host"]),
            namespace=str(data["namespace"]),
            namespace_uid=data.get("namespace_uid"),
            api_server=str(data.get("api_server") or ""),
            nonces=nonces,
            moved_from=data.get("moved_from"),
            moved_to=data.get("moved_to"),
            recorded_at=str(data.get("recorded_at", "")),
        )


class StateError(Exception):
    """A state file exists but cannot be read or written."""


def state_dir(config_path: str | Path) -> Path:
    return Path(config_path).absolute().parent / ".lakebench"


def state_path(config_path: str | Path, name: str) -> Path:
    """``<config dir>/.lakebench/<name>.json``."""
    return state_dir(config_path) / f"{name}.json"


def _config_name(config_path: str | Path) -> str:
    import yaml

    try:
        raw = yaml.safe_load(Path(config_path).read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        raise StateError(f"cannot read {config_path}: {e}") from e
    if not isinstance(raw, dict):
        raw = {}
    return resolve_name(config_path, raw).name


def read_state_file(path: Path) -> DeployState | None:
    """The state at ``path``; None when there is no file. Raises StateError
    when the file exists but is unreadable or malformed."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except OSError as e:
        raise StateError(f"cannot read {path}: {e}") from e
    try:
        return DeployState.from_json(json.loads(text))
    except (ValueError, KeyError, TypeError, AttributeError) as e:
        raise StateError(f"{path} is not a readable {STATE_SCHEMA} state: {e}") from e


def read_state(config_path: str | Path, name: str | None = None) -> DeployState | None:
    """The deploy state recorded beside ``config_path``, or None.

    ``name`` defaults to the name the config resolves to (its ``name:``).
    Never creates a file.
    """
    if name is None:
        name = _config_name(config_path)
    return read_state_file(state_path(config_path, name))


def write_state(path: Path, state: DeployState) -> None:
    """Write ``state`` atomically (temporary file plus ``os.replace``)."""
    import os
    import tempfile

    state.recorded_at = _now()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(state.to_json(), f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


@contextmanager
def state_lock(config_path: str | Path, name: str) -> Iterator[None]:
    """Hold ``<config dir>/.lakebench/<name>.lock`` (``fcntl.flock``).

    Host-local, like the state file: two deploys from one directory cannot
    interleave their read-modify-write of the state.
    """
    import fcntl

    d = state_dir(config_path)
    d.mkdir(parents=True, exist_ok=True)
    with open(d / f"{name}.lock", "a") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True)
class NamespaceIdentity:
    """What one ``read_namespace`` says about a deployment's namespace."""

    uid: str
    nonce: str
    deployment_name: str
    state_schema: str
    created_buckets: frozenset[str]

    @property
    def incarnation(self) -> str:
        return f"{self.uid}#{self.nonce}"


def read_namespace_identity(core_v1: Any, namespace: str) -> NamespaceIdentity | None:
    """UID and lakebench annotations from one ``read_namespace`` call.

    None when the namespace does not exist; any other API error raises.
    """
    from kubernetes.client.rest import ApiException

    from lakebench.deploy.ownership import (
        ANNOTATION_CREATED_BUCKETS,
        ANNOTATION_DEPLOY_NONCE,
        ANNOTATION_DEPLOYMENT_NAME,
        ANNOTATION_STATE_SCHEMA,
    )

    try:
        ns = core_v1.read_namespace(namespace)
    except ApiException as e:
        if e.status == 404:
            return None
        raise
    anns = (ns.metadata.annotations or {}) if ns.metadata else {}
    created = anns.get(ANNOTATION_CREATED_BUCKETS) or ""
    return NamespaceIdentity(
        uid=str(ns.metadata.uid or ""),
        nonce=anns.get(ANNOTATION_DEPLOY_NONCE) or "",
        deployment_name=anns.get(ANNOTATION_DEPLOYMENT_NAME) or "",
        state_schema=anns.get(ANNOTATION_STATE_SCHEMA) or "",
        created_buckets=frozenset(b for b in created.split(",") if b),
    )


def new_state(config_path: str | Path, name: str, namespace: str) -> DeployState:
    p = Path(config_path).absolute()
    return DeployState(
        name=name,
        config_path=str(p),
        config_dir=str(p.parent),
        host=_host(),
        namespace=namespace,
    )


def reconcile(state: DeployState, ident: NamespaceIdentity | None) -> NonceEntry | None:
    """Step 2: mark the entry the namespace carries confirmed; return it.

    That entry is the *carried* entry, which truncation never drops.
    ``pending`` entries the namespace does not carry stay as they are.
    """
    if ident is None or not ident.nonce:
        return None
    for entry in state.nonces:
        if entry.nonce == ident.nonce:
            entry.status = "confirmed"
            state.namespace_uid = ident.uid or state.namespace_uid
            return entry
    return None


def record_pending(state: DeployState, nonce: str, carried: NonceEntry | None) -> None:
    """Step 4: prepend ``nonce`` as pending and keep at most five entries,
    dropping the oldest entry that is not the carried one."""
    state.nonces.insert(0, NonceEntry(nonce, "pending", _now()))
    while len(state.nonces) > STATE_NONCES_KEPT:
        for i in range(len(state.nonces) - 1, -1, -1):
            if state.nonces[i] is not carried:
                del state.nonces[i]
                break


def confirm(state: DeployState, nonce: str, ident: NamespaceIdentity | None) -> bool:
    """Step 7: mark ``nonce`` confirmed when the namespace carries it."""
    if ident is None or ident.nonce != nonce:
        return False
    for entry in state.nonces:
        if entry.nonce == nonce:
            entry.status = "confirmed"
            state.namespace_uid = ident.uid
            return True
    return False


def current_incarnation(state: DeployState, core_v1: Any) -> str | None:
    """``uid#nonce`` of the namespace when it carries one of ``state``'s nonces.

    Reads the namespace (one ``read_namespace``). None when the namespace is
    absent or carries a nonce this state never recorded; raises when the
    read fails.
    """
    ident = read_namespace_identity(core_v1, state.namespace)
    if ident is None or not ident.nonce or ident.nonce not in state.kept_nonces():
        return None
    return ident.incarnation


# ---------------------------------------------------------------------------
# Moving a deployment's config and state (SAF-2 f)
# ---------------------------------------------------------------------------


class RelocateRefused(Exception):
    """``relocate_state`` refused the move; nothing was written."""


def relocate_state(config_path: str | Path, new_dir: str | Path) -> Path:
    """Move a deployment's config and state to ``new_dir``.

    1. Refuse if the source state has ``moved_to`` set or was written on
       another host.
    2. Copy the config byte for byte, and the v1.6 ``state.json`` when there
       is one.
    3. Write ``<new_dir>/.lakebench/<name>.json`` with ``config_dir`` and
       ``config_path`` rewritten, the nonces unchanged and ``moved_from``.
    4. Rewrite the source state with ``moved_to``, so the old directory is
       refused from then on.

    Returns the new config path.
    """
    import shutil

    src = Path(config_path).absolute()
    dst_dir = Path(new_dir).absolute()
    if not src.is_file():
        raise RelocateRefused(f"{src} is not a file")
    if dst_dir == src.parent:
        raise RelocateRefused(f"{dst_dir} is the config's own directory")
    name = _config_name(src)
    src_state_path = state_path(src, name)
    with state_lock(src, name):
        state = read_state_file(src_state_path)
        if state is not None:
            if state.moved_to:
                raise RelocateRefused(
                    f"{src_state_path} already moved to {state.moved_to}; run from there"
                )
            if state.host != _host():
                raise RelocateRefused(
                    f"{src_state_path} was written on host {state.host}, not {_host()}; "
                    "a move across hosts is a new deployment directory"
                )
        dst = dst_dir / src.name
        dst_state_path = state_path(dst, name)
        if dst.exists() or dst_state_path.exists():
            raise RelocateRefused(f"{dst} or {dst_state_path} already exists")
        dst_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        legacy = legacy_state_path(src)
        if legacy.is_file():
            legacy_dst = legacy_state_path(dst)
            legacy_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(legacy, legacy_dst)
        if state is not None:
            moved = DeployState.from_json(state.to_json())
            moved.config_path = str(dst)
            moved.config_dir = str(dst_dir)
            moved.moved_from = {"config_dir": state.config_dir, "host": state.host, "at": _now()}
            moved.moved_to = None
            write_state(dst_state_path, moved)
            state.moved_to = str(dst_dir)
            write_state(src_state_path, state)
    return dst


def main(argv: list[str] | None = None) -> int:
    """``python -m lakebench.config.deploy_state relocate CONFIG NEWDIR``."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog="python -m lakebench.config.deploy_state")
    sub = parser.add_subparsers(dest="cmd", required=True)
    rel = sub.add_parser("relocate", help="move a config and its deploy state")
    rel.add_argument("config")
    rel.add_argument("new_dir")
    args = parser.parse_args(argv)
    try:
        dst = relocate_state(args.config, args.new_dir)
    except (RelocateRefused, StateError) as e:
        print(f"relocate refused: {e}", file=sys.stderr)
        return 3
    print(dst)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


# ---------------------------------------------------------------------------
# Nameless teardown and read (SAF-2 c)
# ---------------------------------------------------------------------------

#: Top-level keys that make a YAML mapping a lakebench config (check 1).
CONFIG_KEYS = ("platform", "architecture", "workload", "recipe", "endpoint", "scale")


def nameless_configs_in(directory: str | Path) -> list[Path]:
    """The nameless lakebench configs in ``directory`` (not recursive).

    Each ``*.yaml``/``*.yml`` file is parsed as raw text (no env
    substitution); parse errors are ignored. A mapping counts when it has a
    config key and no ``name``.
    """
    import yaml

    d = Path(directory)
    found: list[Path] = []
    for p in sorted([*d.glob("*.yaml"), *d.glob("*.yml")]):
        try:
            raw = yaml.safe_load(p.read_text())
        except Exception:  # noqa: BLE001 -- unreadable or not YAML: not a config
            continue
        if isinstance(raw, dict) and not raw.get("name") and any(k in raw for k in CONFIG_KEYS):
            found.append(p.absolute())
    return found


def _init_from(config_path: Path) -> str:
    return f"lakebench init --from {config_path} -o NEW.yaml writes it with an explicit name"


def check_nameless_target(
    cfg: Any,
    get_core_v1: Callable[[], Any],
    *,
    config_path: str | Path,
    allow_absent: bool = False,
    bucket_owned: Any = None,
) -> str | None:
    """Refuse a nameless config that cannot prove the deployment is its own.

    Called by the teardown and read commands before their first cluster
    step. A config with a ``name:`` passes untouched (returns None), and so
    no cluster client is built for it here. For a nameless one, in order
    (``get_core_v1`` is called only after check 1):

    1. it must be the only nameless config in its directory, or ``--name``
       was given;
    2. when ``<config dir>/.lakebench/<name>.json`` exists: it has not moved,
       it was written for this directory on this host, and the namespace
       carries one of its kept nonces;
    3. otherwise (a v1.6 directory): ``--name`` was given and equals the
       v1.6 name when there is one; the namespace does not carry the v1.7
       ``state-schema`` annotation, names this deployment, and its
       created-buckets record (or, through ``bucket_owned(bucket)``, the
       bucket's own tag) names every one of the config's buckets.

    Returns the namespace incarnation (``uid#nonce``) the checks verified,
    for the caller to pass to destroy so a redeploy after the check is not
    destroyed. A missing namespace returns None when ``allow_absent`` (read
    commands) and is refused otherwise. Raises ``SafetyRefusal`` (exit 3) on
    a failed check and ``PrerequisiteError`` (exit 4) when the namespace
    cannot be read.
    """
    from lakebench.exit_codes import PrerequisiteError, SafetyRefusal

    resolution: NameResolution | None = getattr(cfg, "_name_resolution", None)
    if resolution is None or not resolution.nameless:
        return None
    cpath = Path(config_path).absolute()
    name = resolution.name
    namespace = cfg.get_namespace()
    nxt = _init_from(cpath)

    # 1. The only nameless config in the directory, or --name.
    if resolution.source != "override":
        others = [p for p in nameless_configs_in(cpath.parent) if p != cpath]
        if others:
            raise SafetyRefusal(
                f"{cpath.name} has no name, and neither do "
                + ", ".join(p.name for p in others)
                + f" in {cpath.parent}",
                why=f"every nameless config in this directory resolves to '{name}'",
                next=f"pass --name NAME, or {nxt}",
                path="nameless.ambiguous",
            )

    try:
        ident = read_namespace_identity(get_core_v1(), namespace)
    except Exception as e:  # noqa: BLE001
        raise PrerequisiteError(
            f"cannot read namespace {namespace}: {e}",
            why="a nameless config is checked against its namespace before any step",
            path="nameless.namespace_unreadable",
        ) from e
    if ident is None:
        if allow_absent:
            return None
        raise SafetyRefusal(
            f"namespace {namespace} does not exist, so {cpath.name} (no name) cannot "
            "prove which deployment it means",
            next=nxt,
            path="nameless.namespace_missing",
        )

    # 2. A v1.7 state for this name.
    spath = state_path(cpath, name)
    try:
        state = read_state_file(spath)
    except StateError as e:
        raise SafetyRefusal(str(e), next=nxt, path="nameless.nonce_mismatch") from e
    if state is not None:
        if state.moved_to:
            raise SafetyRefusal(
                f"this deployment's state moved to {state.moved_to}; run from there",
                where=str(spath),
                path="nameless.moved",
            )
        if Path(state.config_dir) != cpath.parent or state.host != _host():
            raise SafetyRefusal(
                f"state written for {state.config_dir} on {state.host}; this looks like "
                "a copied directory",
                where=str(spath),
                next=nxt,
                path="nameless.copied_dir",
            )
        if not ident.nonce or ident.nonce not in state.kept_nonces():
            raise SafetyRefusal(
                f"namespace {namespace} carries nonce {ident.nonce or '(none)'}; this "
                "directory recorded " + (", ".join(state.kept_nonces()) or "none"),
                why="another deploy has replaced the deployment this directory made",
                where=str(spath),
                next=nxt,
                path="nameless.nonce_mismatch",
            )
        return ident.incarnation

    # 3. A v1.6 directory: --name plus the namespace's stamps.
    if resolution.source != "override":
        raise SafetyRefusal(
            f"{cpath.name} has no name and no v1.7 state; a v1.6 directory needs --name",
            why=(
                f"'{name}' was read from {resolution.legacy_state_path}"
                if resolution.source == "legacy-state"
                else f"'{name}' is only a suggestion"
            ),
            next=f"pass --name NAME (the namespace's lakebench.deployment/name), or {nxt}",
            path="nameless.name_required",
        )
    if resolution.legacy_name and resolution.legacy_name != name:
        raise SafetyRefusal(
            f"--name {name} does not match {resolution.legacy_name}, the name in "
            f"{resolution.legacy_state_path}",
            next=nxt,
            path="nameless.stamp_mismatch",
        )
    if ident.state_schema:
        raise SafetyRefusal(
            f"{namespace} was deployed by v1.7 from another directory",
            why="the namespace carries lakebench.deployment/state-schema, so its nonce "
            "is recorded in some other directory's state",
            next=f"run from that directory, or {nxt}",
            path="nameless.v17_state_elsewhere",
        )
    if ident.deployment_name != name:
        raise SafetyRefusal(
            f"namespace {namespace} belongs to deployment "
            f"'{ident.deployment_name or '(unstamped)'}', not '{name}'",
            next=nxt,
            path="nameless.stamp_mismatch",
        )
    buckets = cfg.platform.storage.s3.buckets
    unproven = [
        b
        for b in (buckets.bronze, buckets.silver, buckets.gold)
        if b not in ident.created_buckets and not (bucket_owned and bucket_owned(b))
    ]
    if unproven:
        raise SafetyRefusal(
            f"namespace {namespace}'s stamps do not name bucket(s) {', '.join(unproven)}",
            why="neither the created-buckets record nor the bucket's ownership tag "
            f"says '{name}' made them",
            next=nxt,
            path="nameless.stamp_mismatch",
        )
    return ident.incarnation
