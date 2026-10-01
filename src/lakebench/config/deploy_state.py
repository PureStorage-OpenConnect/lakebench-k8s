"""Deployment state kept beside a config, and name resolution for nameless configs.

A config with no ``name:`` used to get a time-based name written to
``<config dir>/.lakebench/state.json`` on every load, including read-only
ones, and every nameless config in a directory resolved to that same name.
From v1.7 a nameless config cannot change data: the commands that
change data refuse it, and the read and teardown commands load it under the
name :func:`resolve_name` resolves.

Everything here only reads. Nothing in this module creates a file or a
directory (a later change adds the state writer here).
"""

from __future__ import annotations

import getpass
import hashlib
import json
import os
import re
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

#: The v1.6 state file, relative to the config's directory. Only ever read.
LEGACY_STATE = Path(".lakebench") / "state.json"

NameSource = Literal["config", "override", "legacy-state", "suggested"]

#: The markers of a lakebench config. The
#: scan also counts the flat v2 keys (see :func:`_config_marker_keys`),
#: because a v1.6 config can be built from flat keys alone.
CONFIG_MARKER_KEYS = frozenset(
    {"platform", "architecture", "workload", "recipe", "endpoint", "scale"}
)

#: Files larger than this are not read by the scan: no config is this big,
#: and a large YAML dump beside a config must not slow every command.
SCAN_MAX_BYTES = 1 << 20


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


def _config_marker_keys() -> frozenset[str]:
    from .loader import _FLAT_FIELD_MAP

    return frozenset(CONFIG_MARKER_KEYS | set(_FLAT_FIELD_MAP))


def _sets_a_name(raw: dict[str, Any]) -> bool:
    """Whether a raw config mapping sets a name of its own.

    A name that is a ``${VAR}`` reference does not count: whether it
    resolved when v1.6 deployed cannot be known now (design check 1 reads
    the raw text with no env substitution).
    """
    name = raw.get("name")
    if not name:
        return False
    return not (isinstance(name, str) and "${" in name)


def other_nameless_configs(config_path: str | Path) -> list[Path] | None:
    """The other nameless lakebench configs in this config's directory.

    v1.6 gave every nameless config in a directory the one name in
    ``.lakebench/state.json``, so when there is more than one, that name
    cannot be tied to any one of them. Each
    ``*.yaml`` and ``*.yml`` file beside the config is parsed with
    ``yaml.safe_load`` on its raw text, and counted when it is a mapping
    with a top-level config key and no name of its own. A file that cannot
    be read or parsed, or is over :data:`SCAN_MAX_BYTES`, is skipped, and the
    config itself is left out however it is linked. Only this directory is
    read, not its subdirectories, and only these two suffixes, as in the
    design. Returns None when the directory cannot be listed.

    ``load_config`` uses the result only to word its refusals: it refuses
    every TEARDOWN and READ load of a v1.6 name whatever the scan finds, so
    a file the scan misses cannot let one config act on another's
    deployment.
    """
    path = Path(config_path).absolute()
    try:
        entries = sorted(path.parent.iterdir())
    except OSError:
        return None
    markers = _config_marker_keys()
    others: list[Path] = []
    for entry in entries:
        if entry.suffix not in (".yaml", ".yml"):
            continue
        try:
            # samefile also catches a symlink or hard link to the config.
            if not entry.is_file() or os.path.samefile(entry, path):
                continue
            if entry.stat().st_size > SCAN_MAX_BYTES:
                continue
            raw = yaml.safe_load(entry.read_text())
        except Exception:  # noqa: BLE001 -- any unreadable file is not a config
            # yaml.safe_load raises plain ValueError on a date such as
            # 2026-02-30, not only YAMLError.
            continue
        if isinstance(raw, dict) and markers & raw.keys() and not _sets_a_name(raw):
            others.append(entry)
    return others


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
