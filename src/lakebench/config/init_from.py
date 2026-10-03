"""``lakebench init --from OLD``: rewrite an older config as a 1.7 config.

The conversion works on OLD's raw YAML, never on a loaded model, so no
default is written out and no ``${VAR}`` is expanded:

1. OLD is read with ``yaml.safe_load`` rules and no substitution. A plain
   (unquoted) scalar holding a reference is kept plain (:class:`PlainRef`)
   and a quoted one quoted, because the loader types a plain scalar after
   substituting it and passes a quoted one through as text.
2. The deprecated spellings move to where 1.7 reads them: flat top-level
   keys (``loader._FLAT_FIELD_MAP``), ``architecture.workload`` to
   ``workload``, ``architecture.processing`` to ``architecture.pipeline``,
   ``pipeline.sustained`` to ``pipeline.continuous``, ``mode: sustained`` to
   ``continuous``. A recipe that contradicts the components written becomes
   the recipe 1.6 deployed (the written components won in 1.6).
3. Every ``_removed_keys`` and ``_dead_fields`` entry present is dropped,
   with its fix text; so is each refused-key row whose ``init_from`` is
   ``drop`` (``config/refused_keys.py``), and each ``spark.conf`` key
   Lakebench owns that carries its 1.6 default (1.6 overwrote it too).
4. The name is OLD's, else the name 1.6 recorded in the directory's
   ``.lakebench/state.json``, else a new one. The bucket names are written
   out, so a later rename cannot move them.
5. A plaintext value under a credential key (``access_key``,
   ``secret_key``, ``client_secret``, a ``spark.conf`` secret) becomes a
   ``${VAR}`` reference; the value itself is never printed or written.

:func:`verify` then loads OLD and the new text in-process for a read-only
command, every referenced variable set to a placeholder, and compares the
two models and their planned experiment identities. ``init`` writes the new
file only when they agree, apart from the secrets that became references.
"""

from __future__ import annotations

import json
import logging
import os
import re
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

#: Where ``init --from`` puts a plaintext Polaris client secret.
POLARIS_SECRET_VAR = "LAKEBENCH_POLARIS_CLIENT_SECRET"

# A key whose value is a credential: the whole key, or its last part, names
# one. 'credentials.provider' (a class name) and 'token-refresh' do not match.
_CREDENTIAL_KEY = re.compile(
    r"(?i)(?:^|[._-])(secret|password|passwd|token|credentials?|access[._-]?key|"
    r"secret[._-]?(?:access[._-]?)?key|private[._-]?key|api[._-]?key)$"
)
_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


class InitFromError(Exception):
    """``init --from`` refused; nothing was written.

    ``refused`` is True for a safety refusal (exit 3) and False for a usage
    or config error (exit 2).
    """

    def __init__(self, message: str, *, refused: bool = False):
        super().__init__(message)
        self.refused = refused


# -- reading and writing YAML without losing how a reference was quoted -------


class PlainRef(str):
    """A plain (unquoted) scalar holding a ``${VAR}`` reference."""

    __slots__ = ()


class _RawLoader(yaml.SafeLoader):
    """``SafeLoader`` that marks plain, untagged scalars holding ``${``."""

    def compose_scalar_node(self, anchor: Any) -> Any:
        event = self.peek_event()
        node = super().compose_scalar_node(anchor)
        if event.style is None and event.tag in (None, "!") and "${" in event.value:
            setattr(node, "lb_plain_ref", True)  # noqa: B010 -- yaml nodes have no slots
        return node


def _construct_str(loader: yaml.SafeLoader, node: Any) -> str:
    value = loader.construct_scalar(node)
    assert isinstance(value, str)
    return PlainRef(value) if getattr(node, "lb_plain_ref", False) else value


_RawLoader.add_constructor("tag:yaml.org,2002:str", _construct_str)


def read_raw(text: str) -> Any:
    """*text* parsed as ``yaml.safe_load`` does, with plain references kept."""
    loader = _RawLoader(text)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


class _Dumper(yaml.SafeDumper):
    pass


def _represent_str(dumper: yaml.SafeDumper, value: str) -> Any:
    style = None
    if "${" in value and not isinstance(value, PlainRef):
        # Quoted in OLD (or written by this module): it must stay text.
        style = '"'
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_Dumper.add_representer(str, _represent_str)
_Dumper.add_representer(PlainRef, _represent_str)


def dump(data: dict[str, Any]) -> str:
    """Block-style YAML of *data*, keys in order, references quoted as read."""
    return yaml.dump(
        data,
        Dumper=_Dumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=1000,
    )


# -- dict helpers -------------------------------------------------------------


def _get(data: Any, path: tuple[str, ...]) -> Any:
    node = data
    for part in path:
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _has(data: Any, path: tuple[str, ...]) -> bool:
    node = data
    for part in path:
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _pop(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Pop *path*; a mapping left empty by the pop is removed too."""
    parents = [data]
    for part in path[:-1]:
        parents.append(parents[-1][part])
    value = parents[-1].pop(path[-1])
    for depth in range(len(path) - 1, 0, -1):
        if parents[depth]:
            break
        parents[depth - 1].pop(path[depth - 1])
    return value


def _rename(node: dict[str, Any], old: str, new: str) -> dict[str, Any]:
    """*node* with key *old* renamed to *new*, in the same place."""
    return {(new if k == old else k): v for k, v in node.items()}


def _dotted(path: tuple[str, ...]) -> str:
    return ".".join(path)


# -- the conversion -----------------------------------------------------------


@dataclass(frozen=True)
class Change:
    """One line of the report: ``kind`` is moved, dropped, derived or secret."""

    kind: str
    path: str
    text: str


@dataclass
class Conversion:
    """What ``convert`` made of OLD."""

    #: The new config, as a dict in the shape it is written.
    data: dict[str, Any]
    #: The deployment name it carries (as written, possibly a ``${VAR}``).
    name: str
    #: Where the name came from: config, override, legacy-state or new.
    name_source: str
    changes: list[Change] = field(default_factory=list)
    #: Written paths whose plaintext value became a reference; the only
    #: places the verified models may differ.
    secret_paths: list[tuple[str, ...]] = field(default_factory=list)


@contextmanager
def _quiet() -> Iterator[Any]:
    """Collect load notes and silence their warnings and log lines."""
    from ._load_context import collecting_notes

    schema_log = logging.getLogger("lakebench.config.schema")
    level = schema_log.level
    schema_log.setLevel(logging.CRITICAL)
    try:
        with collecting_notes() as notes, warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield notes
    finally:
        schema_log.setLevel(level)


def _block_models() -> dict[type, dict[str, type]]:
    """For each config model, its sub-blocks: written key -> model."""
    import typing

    from pydantic import BaseModel

    from .schema import ArchitectureConfig, LakebenchConfig, ProcessingConfig

    def models_in(annotation: Any) -> list[type]:
        args = typing.get_args(annotation) or (annotation,)
        return [a for a in args if isinstance(a, type) and issubclass(a, BaseModel)]

    out: dict[type, dict[str, type]] = {}

    def walk(model: type) -> None:
        if model in out:
            return
        blocks: dict[str, type] = {}
        for name, f in model.model_fields.items():  # type: ignore[attr-defined]
            if typing.get_origin(f.annotation) in (dict, list):
                continue
            found = models_in(f.annotation)
            if found:
                blocks[f.alias or name] = found[0]
        if model is LakebenchConfig:
            # Written at the top level, stored on architecture.
            workload = ArchitectureConfig.model_fields["workload"].annotation
            assert isinstance(workload, type)
            blocks["workload"] = workload
        if model is ProcessingConfig:
            blocks["continuous"] = blocks["sustained"]
        out[model] = blocks
        for sub in blocks.values():
            walk(sub)

    walk(LakebenchConfig)
    return out


def _drop_removed(data: dict[str, Any], changes: list[Change]) -> None:
    """Drop every removed key and dead field present, with its fix text."""
    from .schema import LakebenchConfig

    blocks = _block_models()

    def walk(node: dict[str, Any], model: type, path: tuple[str, ...]) -> None:
        removed: dict[str, str] = getattr(model, "_removed_keys", {}) or {}
        dead: dict[str, str] = getattr(model, "_dead_fields", {}) or {}
        for key in list(node):
            if key in removed:
                node.pop(key)
                changes.append(Change("dropped", _dotted((*path, key)), removed[key]))
            elif key in dead:
                node.pop(key)
                text = dead[key] or "nothing in lakebench read it."
                changes.append(Change("dropped", _dotted((*path, key)), text))
        for key, sub in blocks[model].items():
            child = node.get(key)
            if isinstance(child, dict) and child:
                walk(child, sub, (*path, key))
                if not child:  # emptied by the drops above
                    node.pop(key)

    walk(data, LakebenchConfig, ())


def _move_locations(data: dict[str, Any], changes: list[Change]) -> dict[str, Any]:
    """Move the deprecated spellings to the places 1.7 reads."""
    from .loader import _FLAT_FIELD_MAP, ConfigError, _apply_flat_fields
    from .schema import resolve_workload_location

    arch = data.get("architecture")
    if isinstance(arch, dict) and "processing" in arch and "pipeline" not in arch:
        data["architecture"] = arch = _rename(arch, "processing", "pipeline")
        changes.append(Change("moved", "architecture.processing", "-> architecture.pipeline"))

    flat = [k for k in _FLAT_FIELD_MAP if k in data]
    with _quiet() as notes:
        try:
            data = _apply_flat_fields(data)
        except ConfigError as e:
            raise InitFromError(str(e)) from None
    for key in flat:
        changes.append(Change("moved", key, "-> " + _dotted(_FLAT_FIELD_MAP[key])))
    for text in notes.texts():
        if text.startswith("both flat"):
            changes.append(Change("dropped", "", text))

    arch = data.get("architecture")
    if isinstance(arch, dict) and "workload" in arch:
        had_top = "workload" in data
        with _quiet():
            try:
                merged = resolve_workload_location(data)
            except ValueError as e:
                raise InitFromError(str(e)) from None
        block = merged["architecture"].pop("workload")
        if not merged["architecture"]:
            merged.pop("architecture")
        # Top level, after name and recipe, as init writes it.
        head = {k: merged.pop(k) for k in ("name", "recipe") if k in merged}
        data = {**head, "workload": block, **merged}
        text = "-> workload" + (" (merged with the top-level block)" if had_top else "")
        changes.append(Change("moved", "architecture.workload", text))

    pipeline = _get(data, ("architecture", "pipeline"))
    if isinstance(pipeline, dict):
        if "sustained" in pipeline and "continuous" not in pipeline:
            pipeline = _rename(pipeline, "sustained", "continuous")
            data["architecture"]["pipeline"] = pipeline
            changes.append(
                Change(
                    "moved",
                    "architecture.pipeline.sustained",
                    "-> architecture.pipeline.continuous",
                )
            )
        mode = pipeline.get("mode")
        if (
            isinstance(mode, str)
            and not isinstance(mode, PlainRef)
            and mode.strip().lower() == "sustained"
        ):
            pipeline["mode"] = "continuous"
            changes.append(Change("moved", "architecture.pipeline.mode", "sustained -> continuous"))
    return data


def _resolve_recipe_conflict(data: dict[str, Any], changes: list[Change]) -> None:
    """A recipe the written components contradict becomes the recipe they make.

    1.6 let the written components win, so a deployment made from the file
    runs them; a read-only 1.7 load resolves the file the same way.
    """
    from .recipes import RECIPE_OWNED_KEYS, RECIPES, recipe_conflicts, written_recipe

    recipe = data.get("recipe")
    if not isinstance(recipe, str) or recipe not in RECIPES:
        return
    for dotted in RECIPE_OWNED_KEYS:
        value = _get(data, tuple(dotted.split(".")))
        if isinstance(value, str) and "${" in value:
            return  # resolved only at load; leave it for the user
    if not recipe_conflicts(data, recipe):
        return
    written = written_recipe(data, recipe)
    if written is None or written == recipe:
        return
    data["recipe"] = written
    changes.append(
        Change(
            "derived",
            "recipe",
            f"{recipe} -> {written}: the components written contradict {recipe}, and 1.6 "
            f"deployed the written ones ({written})",
        )
    )


def _drop_refused(data: dict[str, Any], changes: list[Change]) -> None:
    """Drop the refused-key rows ``init --from`` translates, and spark.conf
    keys Lakebench owns that carry their 1.6 default."""
    from lakebench.modules.pipeline_engines.spark.conf_keys import (
        V16_DEFAULT_SPARK_CONF,
        is_owned_spark_key,
    )

    from .refused_keys import REFUSED_KEYS

    for row in REFUSED_KEYS:
        path = tuple(row.key.split("."))
        if row.init_from == "keep" or not _has(data, path):
            continue
        if row.init_from == "drop-default":
            default = _field_default(path)
            if default is _NO_DEFAULT or _get(data, path) != default:
                continue
            _pop(data, path)
            changes.append(
                Change(
                    "dropped",
                    row.key,
                    f"was {_show(default)}, the default it keeps without the key ({row.fix} "
                    "for another value)",
                )
            )
            continue
        value = _pop(data, path)
        changes.append(Change("dropped", row.key, f"was {_show(value)}: {row.fix}"))

    conf = _get(data, ("spark", "conf"))
    if not isinstance(conf, dict):
        return
    catalog = _get(data, ("architecture", "query_engine", "trino", "catalog_name"))
    catalog = catalog if isinstance(catalog, str) and catalog else "lakehouse"
    for key in list(conf):
        value = conf[key]
        if (
            isinstance(key, str)
            and is_owned_spark_key(key, catalog)
            and V16_DEFAULT_SPARK_CONF.get(key) == str(value)
        ):
            _pop(data, ("spark", "conf", key))
            changes.append(
                Change(
                    "dropped",
                    f"spark.conf.{key}",
                    "carried its 1.6 default, which Lakebench overwrote then too",
                )
            )


_NO_DEFAULT = object()


def _field_default(path: tuple[str, ...]) -> Any:
    """The schema default of the field at a written *path*, or ``_NO_DEFAULT``."""
    from pydantic_core import PydanticUndefined

    from .schema import LakebenchConfig

    blocks = _block_models()
    model: type = LakebenchConfig
    for part in path[:-1]:
        sub = blocks[model].get(part)
        if sub is None:
            return _NO_DEFAULT
        model = sub
    fields = model.model_fields  # type: ignore[attr-defined]
    if path[-1] not in fields:
        return _NO_DEFAULT
    default = fields[path[-1]].get_default(call_default_factory=True)
    return _NO_DEFAULT if default is PydanticUndefined else default


def _show(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return json.dumps(value) if isinstance(value, str) else str(value)


def _set_name(
    data: dict[str, Any],
    old_path: Path,
    name_override: str | None,
    fresh_name: str,
    changes: list[Change],
) -> tuple[str, str]:
    """The name NEW carries and where it came from; written into *data*."""
    from .deploy_state import legacy_state_path, other_nameless_configs, read_legacy_name

    own = data.get("name")
    if own not in (None, ""):
        if name_override and str(own) != name_override:
            raise InitFromError(
                f"--name {name_override!r} does not match the name {str(own)!r} in "
                f"{old_path.name}; --name is only for a config that sets none"
            )
        return str(own), "config"
    legacy = read_legacy_name(old_path)
    if name_override:
        name, source = name_override, "override"
        text = f"'{name}', from --name"
    elif legacy:
        others = other_nameless_configs(old_path)
        if others is None or others:
            listed = (
                "its directory could not be listed"
                if others is None
                else "its directory also holds nameless " + ", ".join(p.name for p in others)
            )
            raise InitFromError(
                f"{old_path.name} has no name, and {listed}; 1.6 gave every nameless config "
                f"there the name '{legacy}', so which one deployed it cannot be told from the "
                f"files. Pass --name {legacy} if this file deployed it, or --name with a new "
                "name",
                refused=True,
            )
        name, source = legacy, "legacy-state"
        text = f"'{name}', read from {legacy_state_path(old_path)} (1.6 recorded it there)"
    else:
        name, source = fresh_name, "new"
        text = f"'{name}': no deployment was recorded for this config"
    data["name"] = name
    changes.append(Change("derived", "name", text))
    return name, source


def _set_buckets(data: dict[str, Any], name: Any, changes: list[Change]) -> None:
    """Write the bucket names a load derives from the name (``<name>-<layer>``)."""
    if not isinstance(name, str) or not name:
        return  # a name that is not text fails the load; nothing to derive
    s3 = data.setdefault("platform", {}).setdefault("storage", {}).setdefault("s3", {})
    if not isinstance(s3, dict):
        return
    buckets = s3.setdefault("buckets", {})
    if not isinstance(buckets, dict):
        return
    for layer in ("bronze", "silver", "gold"):
        if layer in buckets:
            continue
        value = f"{name}-{layer}"
        buckets[layer] = PlainRef(value) if isinstance(name, PlainRef) else value
        changes.append(
            Change(
                "derived",
                f"platform.storage.s3.buckets.{layer}",
                f"{value} (written out, so a rename cannot move it)",
            )
        )


def _secret_var(path: tuple[str, ...], s3_vars: tuple[str, str]) -> str:
    if path == ("platform", "storage", "s3", "access_key"):
        return s3_vars[0]
    if path == ("platform", "storage", "s3", "secret_key"):
        return s3_vars[1]
    if path == ("architecture", "catalog", "polaris", "client_secret"):
        return POLARIS_SECRET_VAR
    tail = path[-1] if path[:2] == ("spark", "conf") else _dotted(path)
    return "LAKEBENCH_" + re.sub(r"[^A-Za-z0-9]+", "_", tail).strip("_").upper()


def _move_secrets(
    data: dict[str, Any], s3_vars: tuple[str, str], changes: list[Change]
) -> list[tuple[str, ...]]:
    """Replace each plaintext credential with a ``${VAR}`` reference.

    A value that is all references stays; a reference with a default loses
    the default (it is a plaintext secret too). The value is never put in a
    change line.
    """
    moved: list[tuple[str, ...]] = []
    used: dict[str, tuple[str, ...]] = {}

    def leaves(node: dict[str, Any], path: tuple[str, ...]) -> Iterator[tuple[str, ...]]:
        for key, value in node.items():
            if isinstance(value, dict):
                yield from leaves(value, (*path, str(key)))
            elif isinstance(key, str) and _CREDENTIAL_KEY.search(key):
                yield (*path, key)

    for path in list(leaves(data, ())):
        parent = _get(data, path[:-1])
        value = parent[path[-1]]
        if value is None or isinstance(value, bool) or value == "":
            continue
        if isinstance(value, str) and "${" in value:
            if not _REF.sub("", value).strip():
                if any(m.group(2) for m in _REF.finditer(value)):
                    bare = _REF.sub(lambda m: "${" + m.group(1) + "}", value)
                    parent[path[-1]] = PlainRef(bare) if isinstance(value, PlainRef) else bare
                    moved.append(path)
                    changes.append(
                        Change(
                            "secret",
                            _dotted(path),
                            "dropped the plaintext default from its ${VAR} reference",
                        )
                    )
                continue
        var = _secret_var(path, s3_vars)
        base, n = var, 2
        while var in used and used[var] != path:
            var, n = f"{base}_{n}", n + 1
        used[var] = path
        parent[path[-1]] = f"${{{var}}}"
        moved.append(path)
        changes.append(
            Change("secret", _dotted(path), f"moved a plaintext secret to ${{{var}}}; export it")
        )
    return moved


def convert(
    old_path: Path,
    *,
    s3_vars: tuple[str, str],
    name_override: str | None,
    fresh_name: str,
) -> tuple[Conversion, str]:
    """Convert the config at *old_path*; returns the conversion and OLD's text.

    *s3_vars* names the S3 key variables (``--credentials-env``);
    *fresh_name* is used when OLD and its directory name no deployment.
    Raises :class:`InitFromError`; nothing is written.
    """
    try:
        text = old_path.read_text()
    except (OSError, UnicodeDecodeError) as e:
        raise InitFromError(f"cannot read {old_path}: {e}") from None
    try:
        raw = read_raw(text)
    except yaml.YAMLError as e:
        raise InitFromError(f"{old_path} is not YAML: {e}") from None
    if not isinstance(raw, dict) or not raw:
        raise InitFromError(f"{old_path} is not a config: its top level is not a mapping of keys")

    changes: list[Change] = []
    data = _move_locations(dict(raw), changes)
    _resolve_recipe_conflict(data, changes)
    _drop_removed(data, changes)
    _drop_refused(data, changes)
    name, source = _set_name(data, old_path, name_override, fresh_name, changes)
    _set_buckets(data, data["name"], changes)
    secrets = _move_secrets(data, s3_vars, changes)
    # name and recipe first, as init writes them.
    data = {
        **{k: data[k] for k in ("name", "recipe") if k in data},
        **{k: v for k, v in data.items() if k not in ("name", "recipe")},
    }
    conv = Conversion(
        data=data, name=name, name_source=source, changes=changes, secret_paths=secrets
    )
    return conv, text


# -- verification -------------------------------------------------------------


@contextmanager
def _placeholder_env(*texts: str) -> Iterator[None]:
    """Every variable *texts* reference set to a placeholder, or unset where
    every reference has a default; the environment is restored after."""
    refs: dict[str, bool] = {}
    for text in texts:
        for m in _REF.finditer(text):
            refs[m.group(1)] = refs.get(m.group(1), False) or m.group(2) is None
    saved = {var: os.environ.get(var) for var in refs}
    try:
        for i, (var, needs_value) in enumerate(sorted(refs.items())):
            os.environ.pop(var, None)
            if needs_value:
                os.environ[var] = f"lbplaceholder{i}"
        yield
    finally:
        for var, value in saved.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value


def _problems(e: Exception) -> list[str]:
    from pydantic import ValidationError

    if not isinstance(e, ValidationError):
        return [(str(e).splitlines() or [type(e).__name__])[-1]]
    out = []
    for err in e.errors(include_input=False):
        parts = [str(x) for x in err["loc"]]
        if parts[:2] == ["architecture", "workload"]:
            parts = parts[1:]
        loc = ".".join(parts)
        msg = str(err["msg"]).removeprefix("Value error, ")
        out.append(f"{loc}: {msg}" if loc else msg)
    return out


def load_text(text: str, purpose: Any, *, name: str | None = None) -> tuple[Any, list[str]]:
    """*text* loaded as ``load_config`` would, without a file: flat keys
    promoted, ``${VAR}`` substituted from the environment, and *name* used
    when the text sets none (as ``--name`` is). Returns the model, or None
    and the problems."""
    from ._load_context import LoadPurpose
    from .loader import _apply_flat_fields, _EnvLoader
    from .schema import LakebenchConfig

    loader = _EnvLoader(text)
    try:
        data = loader.get_single_data()
    except yaml.YAMLError as e:
        return None, [f"not YAML: {e}"]
    finally:
        loader.dispose()
    if loader.malformed or loader.unresolved:
        return None, ["unresolved or malformed ${VAR}"]
    if not isinstance(data, dict):
        return None, ["the top level is not a mapping"]
    context = {"purpose": purpose, "allow_long_names": purpose != LoadPurpose.RUN}
    try:
        with _quiet():
            data = _apply_flat_fields(data)
            if name and not data.get("name"):
                data["name"] = name
            cfg = LakebenchConfig.model_validate(data, context=context)
    except Exception as e:  # noqa: BLE001 -- every failure is reported as problems
        return None, _problems(e)
    return cfg, []


def _model_path(path: tuple[str, ...]) -> tuple[str, ...]:
    """A written path as the model stores it."""
    if path[:1] == ("workload",):
        path = ("architecture", *path)
    if path[:3] == ("architecture", "pipeline", "continuous"):
        path = ("architecture", "pipeline", "sustained", *path[3:])
    return path


def _flatten(node: Any, path: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    if isinstance(node, dict) and node:
        out: dict[tuple[str, ...], Any] = {}
        for key, value in node.items():
            out.update(_flatten(value, (*path, str(key))))
        return out
    return {path: node}


def verify(old_text: str, new_text: str, conv: Conversion) -> list[str]:
    """Why the new text does not load to what OLD loads to, or ``[]``.

    Both are loaded for a read-only command with every referenced variable
    at a placeholder. Their models must be equal, apart from the secrets
    that became references, and so must their planned experiment
    identities. A problem names a dotted path, never a value.
    """
    from lakebench.metrics.experiment import planned_experiment

    from ._load_context import LoadPurpose

    with _placeholder_env(old_text, new_text):
        old_cfg, old_problems = load_text(old_text, LoadPurpose.READ, name=conv.name)
        if old_cfg is None:
            raise InitFromError(
                "the old config does not load (fix it, then convert): " + "; ".join(old_problems)
            )
        new_cfg, new_problems = load_text(new_text, LoadPurpose.READ)
        if new_cfg is None:
            return ["the converted config does not load: " + "; ".join(new_problems)]
        skip = {_model_path(p) for p in conv.secret_paths}
        old = _flatten(old_cfg.model_dump(mode="json"))
        new = _flatten(new_cfg.model_dump(mode="json"))
        differ = sorted(
            _dotted(p)
            for p in set(old) | set(new)
            if p not in skip and (p not in old or p not in new or old[p] != new[p])
        )
        old_id = json.dumps(planned_experiment(old_cfg), sort_keys=True, default=str)
        new_id = json.dumps(planned_experiment(new_cfg), sort_keys=True, default=str)
    if old_id != new_id:
        differ.append("the planned experiment identity")
    return differ


def remaining_refusals(new_text: str) -> list[str]:
    """What ``run`` (and so ``deploy``) still refuses in the new text."""
    from ._load_context import LoadPurpose

    with _placeholder_env(new_text):
        cfg, problems = load_text(new_text, LoadPurpose.RUN)
    return [] if cfg is not None else problems
