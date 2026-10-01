"""Deployment state kept beside a config, and name resolution for nameless configs.

A config with no ``name:`` used to get a time-based name written to
``<config dir>/.lakebench/state.json`` on every load, including read-only
ones, and every nameless config in a directory resolved to that same name.
From v1.7 a nameless config cannot change data (SAF-2): the commands that
change data refuse it, and the read and teardown commands load it under the
name :func:`resolve_name` resolves.

Everything here only reads. Nothing in this module creates a file or a
directory.
"""

from __future__ import annotations

import getpass
import hashlib
import json
import re
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml

#: The v1.6 state file, relative to the config's directory. Only ever read.
LEGACY_STATE = Path(".lakebench") / "state.json"

NameSource = Literal["config", "override", "legacy-state", "suggested"]

#: A YAML mapping with any of these top-level keys is taken for a lakebench
#: config when counting the nameless configs in a directory.
CONFIG_MARKER_KEYS = frozenset(
    {"platform", "architecture", "workload", "recipe", "endpoint", "scale"}
)


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


def other_nameless_configs(config_path: str | Path) -> list[Path] | None:
    """The other nameless lakebench configs in this config's directory.

    v1.6 gave every nameless config in a directory the one name in
    ``.lakebench/state.json``, so when there is more than one, that name
    cannot be tied to any one of them (SAF-2 part c, check 1). Each
    ``*.yaml`` and ``*.yml`` file beside the config is parsed with
    ``yaml.safe_load`` on its raw text (no env substitution); a file that
    does not parse is skipped, and the config itself is left out, however
    it is linked. Only this directory is read, not its subdirectories.
    Returns None when the directory cannot be listed, so a caller can fail
    closed.
    """
    path = Path(config_path).absolute()
    try:
        own = path.resolve()
        entries = sorted(path.parent.iterdir())
    except OSError:
        return None
    others: list[Path] = []
    for entry in entries:
        if entry.suffix not in (".yaml", ".yml"):
            continue
        try:
            if not entry.is_file() or entry.resolve() == own:
                continue
            raw = yaml.safe_load(entry.read_text())
        except (OSError, UnicodeDecodeError, yaml.YAMLError):
            continue
        if isinstance(raw, dict) and not raw.get("name") and CONFIG_MARKER_KEYS & raw.keys():
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
