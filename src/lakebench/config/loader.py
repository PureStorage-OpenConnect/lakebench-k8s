"""Configuration loader for Lakebench."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from ._load_context import (
    CHANGES_DATA,
    SKIPS_NAME_LENGTH,
    TARGETS_DEPLOYMENT,
    LoadNotes,
    LoadPurpose,
    collecting_notes,
    emit_note,
)
from .deploy_state import NameResolution, other_nameless_configs, resolve_name, suggested_name
from .schema import LakebenchConfig

# -- Env var substitution ----------------------------------------------------

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")
# What a YAML plain scalar trims: spaces, tabs and the YAML line breaks.
_YAML_SPACE = " \t\r\n\x85\u2028\u2029"
_CUT_DEFAULT = re.compile(r"\$\{[A-Za-z_][A-Za-z0-9_]*:-[^}]*$")


def _substitute_env_vars(text: str, unresolved: list[str] | None = None) -> str:
    """Replace ``${VAR}`` and ``${VAR:-default}`` in one string.

    Unresolved variables without defaults raise ``ConfigError``, or are
    appended to *unresolved* when a list is given (``load_yaml`` names them
    all at once).
    """
    missing: list[str] = [] if unresolved is None else unresolved

    def _replace(m: re.Match) -> str:
        var_name = m.group(1)
        default = m.group(2)
        value = os.environ.get(var_name)
        if value is not None:
            return value
        if default is not None:
            return str(default)
        missing.append(var_name)
        return str(m.group(0))

    result = _ENV_PATTERN.sub(_replace, text)
    if unresolved is None and missing:
        raise ConfigError(
            f"Unresolved environment variables: {', '.join(missing)}. "
            f"Set them or provide defaults with ${{VAR:-default}} syntax."
        )
    return result


# -- Flat field mapping (v2 config) ------------------------------------------

_FLAT_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "endpoint": ("platform", "storage", "s3", "endpoint"),
    "access_key": ("platform", "storage", "s3", "access_key"),
    "secret_key": ("platform", "storage", "s3", "secret_key"),
    "scale": ("workload", "datagen", "scale"),
    "namespace": ("platform", "kubernetes", "namespace"),
    "mode": ("architecture", "pipeline", "mode"),
    "cycles": ("architecture", "pipeline", "cycles"),
    "spark_image": ("images", "spark"),
}


def _apply_flat_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Promote flat top-level fields to their nested locations.

    Each promoted key adds a deprecation note naming the nested key to write
    instead. If both flat and nested are present, flat wins, as in v1.6, and
    the note says so.
    """
    for flat_key, nested_path in _FLAT_FIELD_MAP.items():
        if flat_key not in data:
            continue
        value = data.pop(flat_key)
        emit_note(f"flat '{flat_key}' is deprecated; write {'.'.join(nested_path)}")

        # A config still using the deprecated 'architecture.workload' block
        # (and no top-level one) gets flat 'scale' there, so the two blocks
        # are not both set with only the flat value in one of them.
        arch = data.get("architecture")
        if (
            nested_path[0] == "workload"
            and "workload" not in data
            and isinstance(arch, dict)
            and "workload" in arch
        ):
            nested_path = ("architecture", *nested_path)

        # A config still using the deprecated 'architecture.processing' key
        # gets flat pipeline fields there, rather than a new 'pipeline'
        # block that would collide with it. An empty 'processing:' is None.
        arch = data.get("architecture")
        if (
            nested_path[:2] == ("architecture", "pipeline")
            and isinstance(arch, dict)
            and "processing" in arch
            and (arch["processing"] is None or isinstance(arch["processing"], dict))
            and "pipeline" not in arch
        ):
            nested_path = ("architecture", "processing", *nested_path[2:])

        # Walk the nested path, creating intermediate dicts as needed. An
        # empty YAML mapping ('architecture:' with nothing under it) is None.
        target = data
        for key in nested_path[:-1]:
            nxt = target.get(key)
            if nxt is None:
                nxt = target[key] = {}
            if not isinstance(nxt, dict):
                raise ConfigError(
                    f"flat field '{flat_key}' belongs under '{'.'.join(nested_path[:-1])}', "
                    f"but '{key}' is not a mapping"
                )
            target = nxt

        final_key = nested_path[-1]
        if final_key in target:
            emit_note(
                f"both flat '{flat_key}' and nested '{'.'.join(nested_path)}' are set; "
                "the flat value is used"
            )
        target[final_key] = value

    return data


class ConfigError(Exception):
    """Base exception for configuration errors."""

    pass


class ConfigFileNotFoundError(ConfigError):
    """Raised when configuration file is not found."""

    pass


class ConfigParseError(ConfigError):
    """Raised when configuration file cannot be parsed."""

    pass


class ConfigValidationError(ConfigError):
    """Raised when configuration validation fails."""

    def __init__(self, message: str, errors: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.errors = errors or []


class ConfigProtectedCorpusError(ConfigValidationError):
    """The config names a seed or corpus role the AML protocol refuses
    (``datagen_seed.ProtectedCorpusError``): a spent seed, a held-out seed
    outside its registered run, or a role that does not match its seed. The
    message never names a seed. Exit 2, path ``run.protected_corpus``."""


class ConfigNameRequired(ConfigValidationError):
    """A nameless config was loaded by a command that may not use it.

    The commands that change data refuse every nameless config. The teardown
    commands also refuse one that has only a suggested name (v1.7 never
    deploys a nameless config, so a deployment under that name was made by
    some other config file). The teardown commands and the read commands
    that look at a deployment refuse one whose name comes from the v1.6
    ``.lakebench/state.json``: v1.6 gave every nameless config in the
    directory that name, so nothing ties it to this one. ``destroy``,
    ``stop``, ``status`` and ``logs`` take ``--name``, which loads the config
    under that name and leaves the proof to the namespace's own stamps
    (``config.deploy_state.check_nameless_target``). ``siblings`` lists the
    other nameless configs found there, for the message (None when the
    directory could not be listed). ``linked`` is a nameless config reached
    through a symbolic link whose two directories record different v1.6
    names (``NameResolution.resolved_legacy``); every command that may look
    at a deployment refuses it.
    """

    def __init__(
        self,
        resolution: NameResolution,
        *,
        teardown: bool = False,
        siblings: list[Path] | None = None,
        suggestion: str | None = None,
        linked: bool = False,
    ):
        self.resolution = resolution
        self.siblings = siblings
        name = resolution.name
        shared = siblings is None or bool(siblings)
        if siblings is None:
            others = "this directory could not be listed"
        else:
            others = "this directory also holds nameless " + ", ".join(p.name for p in siblings)
        if linked and resolution.resolved_legacy is not None:
            other_path, other = resolution.resolved_legacy
            here = f"'{resolution.legacy_name}'" if resolution.legacy_name else "no name"
            msg = (
                "config has no name and is reached through a symbolic link: v1.6 "
                f"recorded {here} in {resolution.legacy_state_path} and '{other}' in "
                f"{other_path}, beside the file the link points to. The two directories "
                "share this one file, so which deployment it names cannot be told. Fix: "
                "pass --name to destroy, stop, status or logs, which then check the "
                "namespace's stamps (when the link's directory records a name, only that "
                "name is accepted through this path; reach the other deployment through "
                "the file's own path); to keep using the config, replace the link with a copy of "
                "the file and add each deployment's name to its own copy."
            )
        elif teardown and resolution.source == "legacy-state":
            also = f", and {others}" if shared else ""
            msg = (
                f"config has no name; '{name}' (read from {resolution.legacy_state_path}) "
                f"is the name v1.6 gave every nameless config in this directory{also}, "
                "so nothing ties that deployment to this config. Fix: add "
                f"'name: {name}' to the config that deployed it and run this command "
                f"with that config, or pass --name {name} to destroy, stop, status or "
                "logs, which then check the namespace's stamps."
            )
        elif teardown:
            msg = (
                "config has no name and this directory has no readable v1.6 "
                f"{resolution.legacy_state_path}, so no deployment can be its own; "
                f"'{name}' is only a suggestion. Fix: add the deployment's name to the "
                "config (the namespace's lakebench.deployment/name annotation holds it), "
                "or pass it with --name to destroy, stop, status or logs."
            )
        elif resolution.source == "legacy-state" and shared:
            msg = (
                f"config has no name, so it cannot change data, and {others}; v1.6 "
                f"gave every nameless config here the name '{name}' (read from "
                f"{resolution.legacy_state_path}), so which one made that deployment "
                "cannot be told from the files. Fix: give this config a new unique "
                f"name, for example 'name: {suggestion or 'my-unique-name'}'; add "
                f"'name: {name}' only to the one config that deployed '{name}'."
            )
        elif resolution.source == "legacy-state":
            msg = (
                "config has no name, so it cannot change data; without a name it "
                f"resolves to '{name}' (read from {resolution.legacy_state_path}), the "
                "name v1.6 gave the nameless configs in this directory. Fix: if this "
                f"config made deployment '{name}', add 'name: {name}' to it; otherwise "
                "add a new unique name."
            )
        else:
            msg = (
                "config has no name, so it cannot change data. Fix: add a unique "
                f"name to the config, for example 'name: {name}'."
            )
        super().__init__(
            f"Configuration validation failed:\n  - name: {msg}",
            errors=[{"loc": ("name",), "msg": msg, "type": "name_required"}],
        )


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file and return as dictionary.

    Performs ``${VAR}`` / ``${VAR:-default}`` env-var substitution scalar by
    scalar while the file is composed (``_EnvLoader``), never on the raw
    text: a plain scalar resolves as v1.6 did, a quoted one arrives verbatim.

    Args:
        path: Path to YAML file

    Returns:
        Dictionary containing parsed YAML

    Raises:
        ConfigFileNotFoundError: If file doesn't exist
        ConfigParseError: If YAML parsing fails
        ConfigError: If env vars are unresolved
    """
    if not path.exists():
        raise ConfigFileNotFoundError(f"Configuration file not found: {path}")

    with open(path) as f:
        raw = f.read()
    loader = _EnvLoader(raw)
    try:
        content = loader.get_single_data()
    except yaml.YAMLError as e:
        raise ConfigParseError(f"Failed to parse YAML: {e}")  # noqa: B904
    finally:
        loader.dispose()
    if loader.malformed:
        raise ConfigError(
            "Unclosed ${VAR:-default} (a default cut short by a ' #' comment?) at "
            + ", ".join(loader.malformed)
        )
    if loader.unresolved:
        names = ", ".join(dict.fromkeys(loader.unresolved))
        raise ConfigError(
            f"Unresolved environment variables: {names}. "
            f"Set them or provide defaults with ${{VAR:-default}} syntax."
        )
    if not content:
        return {}
    if not isinstance(content, dict):
        raise ConfigParseError(
            f"{path} is not a config: its top level is a YAML {type(content).__name__}, "
            "not a mapping of keys such as `name:` and `architecture:`"
        )
    return content


class _EnvLoader(yaml.SafeLoader):
    """SafeLoader that substitutes ``${VAR}`` in each scalar as it is composed.

    A plain scalar keeps v1.6's typing: v1.6 substituted the raw text and
    then parsed it, so the substituted text is trimmed and, when untagged,
    typed with YAML 1.1's implicit resolvers (``0042`` is octal 34, an empty
    value or ``~`` is null, ``true`` is a bool); an explicit tag such as
    ``!!str`` is kept. The value itself is never parsed as YAML, so an env
    value holding `` #``, quotes, ``[..]`` or ``a: b`` stays that text
    instead of being cut or turned into structure. A quoted or block scalar
    arrives verbatim as a string (``init`` writes the credential references
    quoted). Keys are substituted as v1.6 did; comments are not.
    """

    def __init__(self, stream: str) -> None:
        super().__init__(stream)
        self.unresolved: list[str] = []
        self.malformed: list[str] = []

    def compose_scalar_node(self, anchor: Any) -> Any:
        # PyYAML's Composer.compose_scalar_node, with the substitution added
        # between reading the event and resolving its tag.
        event = self.get_event()
        tag, value = event.tag, event.value
        if "${" in value:
            if _CUT_DEFAULT.search(_ENV_PATTERN.sub("", value)):
                self.malformed.append(f"line {event.start_mark.line + 1}")
            else:
                value = _substitute_env_vars(value, self.unresolved)
                if event.style is None:
                    # Plain: v1.6 parsed the substituted text, which trimmed
                    # YAML whitespace (not every Unicode space).
                    value = value.strip(_YAML_SPACE)
        if tag is None or tag == "!":
            # Untagged plain text is typed from the substituted value, as in
            # v1.6; a quoted or block scalar resolves to str either way.
            tag = self.resolve(yaml.ScalarNode, value, event.implicit)
        node = yaml.ScalarNode(tag, value, event.start_mark, event.end_mark, style=event.style)
        if anchor is not None:
            self.anchors[anchor] = node
        return node


def load_config(
    path: str | Path,
    *,
    purpose: LoadPurpose | None = None,
    name_override: str | None = None,
    allow_long_names: bool = False,
    print_notes: bool = True,
) -> LakebenchConfig:
    """Load and validate Lakebench configuration from file.

    Processing order:
    1. Read YAML with ``${VAR}`` env-var substitution
    2. Promote flat top-level fields (v2 config) to nested locations
    3. Resolve the name (``deploy_state.resolve_name``; nothing is written)
    4. Validate with Pydantic, with the purpose in the validation context

    The loader never writes to disk.

    Args:
        path: Path to configuration YAML file
        purpose: What the calling command will do with the config (see
            ``LoadPurpose``). MUTATE and RUN refuse a config with no name and
            a config that carries a removed key; the others drop removed
            keys with a note. A nameless config loads under its resolved
            name, except that TEARDOWN refuses one whose name is only a
            suggestion, and TEARDOWN and READ refuse one whose name comes
            from the v1.6 state file. Every purpose but INSPECT refuses a
            nameless config reached through a symbolic link whose target's
            directory records another v1.6 name. Defaults to MUTATE, or to TEARDOWN
            when only ``allow_long_names`` is given.
        name_override: The name for a config that sets none (``--name``).
            It must equal the config's own name when the config has one.
        allow_long_names: Skip the derived-name length check.
            TEARDOWN, READ and INSPECT always skip it. Given alone it means TEARDOWN,
            as in v1.6; with an explicit purpose it only skips the length
            check, which ``clean`` uses so a deployment
            whose namespace is too long to finish deploying can still be
            cleaned while keeping the MUTATE refusals.
        print_notes: Print the notes block on stderr (the default). A caller
            that reports the notes itself (``load_notes``) passes False.

    Returns:
        Validated LakebenchConfig object. ``load_notes(cfg)`` and
        ``name_resolution(cfg)`` read what the load collected.

    Raises:
        ConfigFileNotFoundError: If file doesn't exist
        ConfigParseError: If YAML parsing fails
        ConfigNameRequired: A nameless config the purpose may not use (see
            ``purpose``)
        ConfigValidationError: If validation fails
    """
    if purpose is None:
        purpose = LoadPurpose.TEARDOWN if allow_long_names else LoadPurpose.MUTATE
    purpose = LoadPurpose(purpose)
    skip_name_length = allow_long_names or purpose in SKIPS_NAME_LENGTH

    path = Path(path)
    with collecting_notes() as notes:
        cfg, resolution = _load_and_validate(path, purpose, name_override, skip_name_length)
    cfg._load_notes = notes
    cfg._name_resolution = resolution
    if print_notes:
        _print_load_notes(path, notes)
    _print_load_advisories(cfg)
    return cfg


def _load_and_validate(
    path: Path,
    purpose: LoadPurpose,
    name_override: str | None,
    skip_name_length: bool,
) -> tuple[LakebenchConfig, NameResolution]:
    data = load_yaml(path)
    data = _apply_flat_fields(data)

    try:
        resolution = resolve_name(path, data, name_override)
    except ValueError as e:
        raise ConfigValidationError(
            f"Configuration validation failed:\n  - name: {e}",
            errors=[{"loc": ("name",), "msg": str(e), "type": "name_override"}],
        ) from None
    if resolution.nameless:
        if resolution.resolved_legacy is not None and resolution.source != "override":
            # Reached through a symbolic link, and the directory of the file
            # it points to records another v1.6 name than the link's own
            # directory, the one 1.6 read. A command that looks at no
            # deployment loads it under 1.6's name; every other refuses, or
            # it could act on, or report, the other directory's deployment.
            if purpose != LoadPurpose.INSPECT:
                raise ConfigNameRequired(resolution, linked=True)
            other_path, other = resolution.resolved_legacy
            what = (
                "the name v1.6 used through this path"
                if resolution.source == "legacy-state"
                else "a suggestion, as v1.6 recorded no name beside the link"
            )
            emit_note(
                f"no name: loaded as '{resolution.name}', {what}; {other_path} "
                f"records '{other}' for the file the link points to",
                category=None,
            )
        siblings = other_nameless_configs(path) if resolution.source == "legacy-state" else []
        if purpose in CHANGES_DATA:
            raise ConfigNameRequired(resolution, siblings=siblings, suggestion=suggested_name(path))
        if purpose == LoadPurpose.TEARDOWN and resolution.source == "suggested":
            raise ConfigNameRequired(resolution, teardown=True)
        if purpose in TARGETS_DEPLOYMENT and resolution.source == "legacy-state":
            # A v1.6 directory with no recorded nonce is refused
            # without --name. v1.6 gave every nameless config here this one
            # name, so destroy, stop, admin or status from any of them would
            # act on, or report, whichever deployment it names. Naming the
            # config that deployed it, or a --name that the namespace's
            # stamps then confirm, is the way through.
            raise ConfigNameRequired(resolution, teardown=True, siblings=siblings)
        data["name"] = resolution.name

    # The model stores the workload block at architecture.workload; report
    # errors at the location the user wrote it.
    _arch = data.get("architecture")
    _top_level_workload = "workload" in data and not (
        isinstance(_arch, dict) and "workload" in _arch
    )

    # Read before validation: the recipe expansion fills the dict in place.
    default_recipe_note = _default_recipe_note(data)

    context = {"purpose": purpose, "allow_long_names": skip_name_length}
    try:
        cfg = LakebenchConfig.model_validate(data, context=context)
    except ValidationError as e:
        from lakebench.config.datagen_seed import ProtectedCorpusError

        protected = [
            err
            for err in e.errors(include_input=False)
            if isinstance((err.get("ctx") or {}).get("error"), ProtectedCorpusError)
        ]
        # Messages are rewritten against the model's own locations, before
        # the locations are re-rooted to where the user wrote each key.
        # include_input=False: the input of a model-level error is the whole
        # block, which can carry a datagen seed or a key, and callers print
        # these dicts.
        errors = [_explain_error(dict(err)) for err in e.errors(include_input=False)]
        if _top_level_workload:
            errors = [
                {**err, "loc": tuple(err["loc"][1:])}
                if tuple(err["loc"][:2]) == ("architecture", "workload")
                else err
                for err in errors
            ]
        # The continuous block is stored on the 'sustained' field; name it
        # the way the user wrote it.
        _pipe = _arch.get("pipeline") if isinstance(_arch, dict) else None
        if not (isinstance(_pipe, dict) and "sustained" in _pipe):
            errors = [
                {**err, "loc": ("architecture", "pipeline", "continuous", *err["loc"][3:])}
                if tuple(err["loc"][:3]) == ("architecture", "pipeline", "sustained")
                else err
                for err in errors
            ]
        error_messages = []
        for err in errors:
            loc = ".".join(str(x) for x in err["loc"])
            msg = err["msg"]
            # A model-level error (unknown recipe, recipe conflict) has no
            # location; its message names the keys itself.
            error_messages.append(f"  - {loc}: {msg}" if loc else f"  - {msg}")

        # from None: the chained ValidationError still holds the input.
        cls = ConfigProtectedCorpusError if protected else ConfigValidationError
        raise cls(
            "Configuration validation failed:\n" + "\n".join(error_messages),
            errors=errors,
        ) from None
    if default_recipe_note:
        emit_note(default_recipe_note)
    return cfg, resolution


#: What a config with no recipe, or ``recipe: default``, resolves to when it
#: sets no component.
DEFAULT_RECIPE_RESOLUTION = "hive-iceberg-spark-trino"


def _default_recipe_note(data: dict[str, Any]) -> str | None:
    """The deprecation note for a config with no recipe, or None.

    A config with no ``recipe``, or ``recipe: default``, resolves to
    ``hive-iceberg-spark-trino`` in v1.7 when it sets no component. One that
    sets components resolves to them (a v1.6 ``init`` wrote
    ``catalog.type`` with no recipe), so the note names the recipe they
    resolve to rather than claiming Hive.
    """
    from .recipes import RECIPE_OWNED_KEYS, _raw_value, recipe_components
    from .support import recipe_for

    recipe = data.get("recipe")
    if recipe not in (None, "", "default"):
        return None
    resolved = recipe_components(DEFAULT_RECIPE_RESOLUTION)
    written = False
    for dotted in RECIPE_OWNED_KEYS:
        value = _raw_value(data, dotted)
        if isinstance(value, (str, int, float)) and str(value):
            resolved[dotted] = str(getattr(value, "value", value))
            written = True
    parts = [resolved[k].lower() for k in RECIPE_OWNED_KEYS]
    name = recipe_for(*parts)
    if name is None:
        # No recipe has these components; the schema's combination check
        # names the problem, so only the components are reported.
        name = "-".join([*parts[:3], "thrift" if parts[3] == "spark-thrift" else parts[3]])
    said = "recipe 'default'" if recipe == "default" else "no recipe"
    if not written:
        return f"{said}: resolves to {name}; write `recipe: {name}` explicitly (required in v1.8)"
    return (
        f"{said}: the components written resolve to {name}; write `recipe: {name}` "
        "explicitly (required in v1.8)"
    )


def _explain_error(err: dict[str, Any]) -> dict[str, Any]:
    """Name the nearest valid key or recipe in an error."""
    from ._hints import unknown_key_hint

    if err.get("type") == "extra_forbidden":
        loc = tuple(err["loc"])
        # The same place in the spellings a user writes, for the tie-break.
        user_loc = loc
        if loc[:2] == ("architecture", "workload"):
            user_loc = ("workload", *loc[2:])
        elif loc[:3] == ("architecture", "pipeline", "sustained"):
            user_loc = ("architecture", "pipeline", "continuous", *loc[3:])
        hint = unknown_key_hint(loc, user_loc)
        err["msg"] = f"unknown key; {hint}" if hint else "unknown key"
    elif err.get("type") in ("unknown_recipe", "recipe_conflict"):
        err["loc"] = ("recipe",)
    return err


def load_notes(cfg: LakebenchConfig) -> LoadNotes:
    """The notes the ``load_config`` call that built *cfg* collected."""
    notes = cfg._load_notes
    return notes if notes is not None else LoadNotes()


def name_resolution(cfg: LakebenchConfig) -> NameResolution | None:
    """How *cfg*'s name was resolved; None for a config not built by ``load_config``."""
    return cfg._name_resolution


_printed_notes: set[tuple[str, str]] = set()


def _print_load_notes(path: Path, notes: LoadNotes) -> None:
    """Print a load's notes once per process and config as one block on stderr."""
    key = str(path.absolute())
    fresh = [t for t in notes.texts() if (key, t) not in _printed_notes]
    if not fresh:
        return
    _printed_notes.update((key, t) for t in fresh)
    from rich.console import Console
    from rich.markup import escape

    console = Console(stderr=True)
    console.print(f"[yellow]Upgrade notes[/yellow] for {escape(str(path))}:", soft_wrap=True)
    for text in fresh:
        console.print(f"  - {escape(text)}", highlight=False, soft_wrap=True)


# Iceberg snapshot-expiry floor while continuous streams are live. Mirrors
# LIVE_EXPIRE_MIN_RETENTION_SECONDS in
# modules/table_formats/iceberg/maintenance.py; a test holds them in step.
# Delta is not checked: continuous Delta has no effective table maintenance
# in v1.6 (VACUUM keeps the 7 d default while streams are live, OPTIMIZE is
# skipped), and the report says so.
_ICEBERG_LIVE_EXPIRE_FLOOR_SECONDS = 3600
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
# Engines that run continuous Iceberg maintenance (DuckDB and none skip it).
_MAINTENANCE_ENGINES = ("trino", "spark-thrift")
_printed_advisories: set[str] = set()


def retention_floor_advisory(cfg: LakebenchConfig) -> str | None:
    """Warning text when the continuous retention_threshold is below the live floor.

    A continuous run floors Iceberg snapshot expiry at 1 h while streams are
    live, so a lower retention_threshold is silently raised. The maintenance
    journal records the effective value, but nothing told the user. Only
    Iceberg recipes whose query engine runs maintenance (Trino, Spark
    Thrift) are checked, and only a threshold the config sets. Ignores the
    pipeline mode; callers decide.
    """
    if cfg.architecture.table_format.type.value != "iceberg":
        return None
    if cfg.architecture.query_engine.type.value not in _MAINTENANCE_ENGINES:
        return None
    sustained = cfg.architecture.pipeline.sustained
    if "retention_threshold" not in sustained.model_fields_set:
        # The 30m default stays (it is in every config's fingerprint), and
        # the floor raises live expiry to 1h on its own. The run records the
        # applied value (continuous.retention), so the default needs no
        # warning.
        return None
    threshold = sustained.retention_threshold
    m = re.fullmatch(r"(\d+)([smhd])", threshold)
    if m is None:
        return None
    if int(m.group(1)) * _UNIT_SECONDS[m.group(2)] >= _ICEBERG_LIVE_EXPIRE_FLOOR_SECONDS:
        return None
    return (
        f"architecture.pipeline.continuous.retention_threshold is {threshold}, below the "
        "1h floor for Iceberg snapshot expiry while continuous streams are live; "
        "continuous maintenance expires at 1h. Set 1h or more to make the config say "
        "what runs."
    )


def load_advisories(cfg: LakebenchConfig) -> list[str]:
    """Settings that are valid but will not do what they say (continuous configs).

    A batch config run with ``run --continuous`` gets the same retention
    warning from the continuous loop instead: saved configs carry every
    field, so an explicit threshold does not mean the user chose it.
    """
    if cfg.architecture.pipeline.mode.value != "continuous":
        return []
    msg = retention_floor_advisory(cfg)
    return [msg] if msg else []


def _print_load_advisories(cfg: LakebenchConfig) -> None:
    advisories = load_advisories(cfg)
    if not advisories:
        return
    from rich.console import Console

    console = Console(stderr=True)
    for msg in advisories:
        # Once per process: some commands load the config several times.
        if msg in _printed_advisories:
            continue
        _printed_advisories.add(msg)
        console.print(f"[yellow]WARN[/yellow] {msg}")


def save_config(config: LakebenchConfig, path: str | Path) -> None:
    """Save configuration to YAML file.

    Args:
        config: LakebenchConfig object
        path: Path to save YAML file
    """
    path = Path(path)
    data = config.model_dump(mode="json", exclude_defaults=False)
    # benchmark.streams is the throughput stream count for `lakebench
    # benchmark`; `lakebench run` refuses an explicit value above 1, so the
    # default is written only when the config set it.
    bench = (data.get("architecture") or {}).get("benchmark") or {}
    if "streams" not in config.architecture.benchmark.model_fields_set:
        bench.pop("streams", None)
    # Write the canonical locations, so the saved file reloads without
    # deprecation warnings: 'workload' is a top-level key and the continuous
    # settings block is 'pipeline.continuous' (the model stores them at
    # architecture.workload and pipeline.sustained).
    arch = data.get("architecture") or {}
    if "workload" in arch:
        data["workload"] = arch.pop("workload")
    pipeline = arch.get("pipeline") or {}
    if "sustained" in pipeline:
        pipeline["continuous"] = pipeline.pop("sustained")
    # Every component is written, so name the recipe they make (a
    # config with no recipe, or 'default', loads with a deprecation note).
    if data.get("recipe") in (None, "", "default"):
        from .support import recipe_for

        parts = [
            str((arch.get("catalog") or {}).get("type")),
            str((arch.get("table_format") or {}).get("type")),
            str(arch.get("pipeline_engine") or "spark"),
            str((arch.get("query_engine") or {}).get("type")),
        ]
        recipe = recipe_for(*parts)
        if recipe:
            data["recipe"] = recipe

    with open(path, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False, indent=2)


def generate_default_config(
    name: str,
    s3_endpoint: str = "",
    s3_access_key: str = "",
    s3_secret_key: str = "",
    namespace: str = "",
) -> LakebenchConfig:
    """Generate a default configuration with common values pre-filled.

    A programmatic helper; `lakebench init` writes its file from
    `cli/_init.first_day_config` instead.

    Args:
        name: Deployment name (required)
        s3_endpoint: S3 endpoint URL
        s3_access_key: S3 access key
        s3_secret_key: S3 secret key
        namespace: Kubernetes namespace (defaults to name)

    Returns:
        LakebenchConfig with defaults
    """
    config_dict: dict[str, Any] = {"name": name}

    # Platform configuration
    platform: dict[str, Any] = {}

    if namespace:
        platform["kubernetes"] = {"namespace": namespace}

    if s3_endpoint or s3_access_key or s3_secret_key:
        s3_config: dict[str, Any] = {}
        if s3_endpoint:
            s3_config["endpoint"] = s3_endpoint
        if s3_access_key:
            s3_config["access_key"] = s3_access_key
        if s3_secret_key:
            s3_config["secret_key"] = s3_secret_key
        platform["storage"] = {"s3": s3_config}

    if platform:
        config_dict["platform"] = platform

    return LakebenchConfig.model_validate(config_dict)
