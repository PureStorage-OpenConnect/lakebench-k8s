"""``lakebench init``: write a first-day config.

The file is built from a small dict and written by a short emitter that
keeps comments, so it holds only what a first run needs: a unique name, the
recipe written once, the workload and scale, the S3 endpoint and the two
credentials as ``${VAR}`` references. The components the recipe sets are
written commented. No plaintext secret is ever written, and no Polaris
client secret is written: deploy generates one per deployment, and this
module must not ship ahead of that deploy change. Before anything is written the text is validated in-process, so
``init`` never leaves a file that does not load.

The guided wizard (``init_wizard.py``) is removed. ``--interactive``, ``-i``
and ``--advanced`` print one line saying so and write the default config;
``--access-key`` and ``--secret-key`` are refused, without echoing the
values.
"""

from __future__ import annotations

import re
import secrets
import warnings
from pathlib import Path
from typing import Annotated, Any, NoReturn

import typer
import yaml

from lakebench.cli._helpers import (
    DEPRECATED_SHORT_F_HELP,
    console,
    err_console,
    print_error,
    print_info,
    print_success,
    warn_deprecated_short_f,
)

#: The recipe ``init`` writes when ``--recipe`` is not given.
DEFAULT_RECIPE = "polaris-iceberg-spark-trino"
DEFAULT_WORKLOAD = "customer360"
DEFAULT_SCALE = 1.0
#: ``--credentials-env`` default: the config references
#: ``${LAKEBENCH_S3_ACCESS_KEY}`` and ``${LAKEBENCH_S3_SECRET_KEY}``.
DEFAULT_CREDENTIALS_ENV = "LAKEBENCH_S3"
#: Non-comment lines in the default output (design 02 section 2.5).
LINE_BUDGET = 12
#: Exit code for a refused flag or a config that would not load.
_EXIT_REFUSED = 2

_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# A YAML plain scalar that reads back as this same string: no quoting needed.
_PLAIN = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*|\$\{[A-Za-z_][A-Za-z0-9_]*\}")

# Bronze per scale unit (docs/data-generation.md): customer360 about 10 GB,
# financial about 8.4 GB of pacs.008.
_SCALE_COMMENT = {
    "customer360": "1 is about 10 GB of bronze",
    "financial": "1 is about 8.4 GB of bronze",
}

_WIZARD_REMOVED = "the init wizard is removed; init writes a default config (see init --help)"


def default_name(user: str | None = None, token: str | None = None) -> str:
    """``lb-<user>-<4 hex>``, unique per call and inside the Hive limit.

    The user part is lowercased, reduced to ``[a-z0-9-]`` and cut to 10
    characters, so the name is at most 18 characters and fits the
    23-character namespace a Hive recipe accepts (``schema.py``
    ``_DERIVED_NAMES``).
    """
    if user is None:
        import getpass

        try:
            user = getpass.getuser()
        except Exception:  # noqa: BLE001 -- no passwd entry, no LOGNAME
            user = ""
    slug = re.sub(r"[^a-z0-9]+", "-", user.lower()).strip("-")[:10].strip("-") or "user"
    return f"lb-{slug}-{token or secrets.token_hex(2)}"


def credential_vars(prefix: str) -> tuple[str, str]:
    """The two environment variable names a ``--credentials-env`` prefix sets."""
    prefix = prefix.rstrip("_")
    return f"{prefix}_ACCESS_KEY", f"{prefix}_SECRET_KEY"


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    # Plain only when YAML reads it back as this same string: '0x1f',
    # '2024-01-01', 'yes' or 'a: b' would come back as something else.
    # A ${VAR} reference is always quoted: the loader passes a quoted
    # scalar through verbatim, so the secret it names is never retyped.
    if _PLAIN.fullmatch(text) and "${" not in text:
        try:
            if yaml.safe_load(text) == text:
                return text
        except yaml.YAMLError:
            pass
    # A JSON string is a valid YAML double-quoted scalar.
    import json

    return json.dumps(text)


def emit(tree: dict[str, Any], comments: dict[str, str] | None = None) -> list[str]:
    """Block-style YAML for a nested dict of scalars, with end-of-line comments.

    *comments* maps a dotted key path to the comment written after it.
    """
    comments = comments or {}
    lines: list[str] = []

    def walk(node: dict[str, Any], depth: int, prefix: tuple[str, ...]) -> None:
        for key, value in node.items():
            path = ".".join((*prefix, key))
            pad = "  " * depth
            if isinstance(value, dict):
                lines.append(f"{pad}{key}:")
                walk(value, depth + 1, (*prefix, key))
                continue
            line = f"{pad}{key}: {_scalar(value)}"
            if path in comments:
                line = f"{line}  # {comments[path]}"
            lines.append(line)

    walk(tree, 0, ())
    return lines


def first_day_dict(
    *,
    name: str,
    recipe: str,
    workload: str,
    scale: float,
    endpoint: str,
    credentials_env: str,
    namespace: str = "",
) -> dict[str, Any]:
    """The values ``init`` writes, as the nested config dict."""
    access_var, secret_var = credential_vars(credentials_env)
    platform: dict[str, Any] = {}
    if namespace:
        platform["kubernetes"] = {"namespace": namespace}
    platform["storage"] = {
        "s3": {
            "endpoint": endpoint,
            "access_key": f"${{{access_var}}}",
            "secret_key": f"${{{secret_var}}}",
        }
    }
    return {
        "name": name,
        "recipe": recipe,
        "workload": {"schema": workload, "datagen": {"scale": scale}},
        "platform": platform,
    }


def first_day_config(
    *,
    name: str,
    recipe: str = DEFAULT_RECIPE,
    workload: str = DEFAULT_WORKLOAD,
    scale: float = DEFAULT_SCALE,
    endpoint: str = "",
    credentials_env: str = DEFAULT_CREDENTIALS_ENV,
    namespace: str = "",
) -> str:
    """The text of the config ``init`` writes."""
    from lakebench.config.recipes import recipe_components

    tree = first_day_dict(
        name=name,
        recipe=recipe,
        workload=workload,
        scale=scale,
        endpoint=endpoint,
        credentials_env=credentials_env,
        namespace=namespace,
    )
    comments = {"workload.datagen.scale": _SCALE_COMMENT.get(workload, "")}
    comments = {k: v for k, v in comments.items() if v}
    if not endpoint:
        comments["platform.storage.s3.endpoint"] = "set this, e.g. http://your-s3:80"
    lines = [
        "# Written by `lakebench init`. Every key is in docs/configuration.md;",
        "# `lakebench config recipes` lists the recipes.",
        *emit(tree, comments),
    ]
    components = recipe_components(recipe)
    lines += [
        "# Set by the recipe. Written out, each must agree with it:",
        "# architecture:",
        f"#   catalog: {{type: {components['architecture.catalog.type']}}}",
        f"#   table_format: {{type: {components['architecture.table_format.type']}}}",
        f"#   pipeline_engine: {components['architecture.pipeline_engine']}",
        f"#   query_engine: {{type: {components['architecture.query_engine.type']}}}",
    ]
    return "\n".join(lines) + "\n"


def config_problem(text: str, credentials_env: str) -> str | None:
    """Why *text* would not load under a command that changes data, or None.

    The two credential references are given placeholder values, as the
    user's environment will; nothing else is substituted.
    """
    from pydantic import ValidationError

    from lakebench.config._load_context import LoadPurpose, collecting_notes
    from lakebench.config.schema import LakebenchConfig

    access_var, secret_var = credential_vars(credentials_env)
    text = text.replace(f"${{{access_var}}}", "placeholder").replace(
        f"${{{secret_var}}}", "placeholder"
    )
    data = yaml.safe_load(text)
    try:
        with collecting_notes(), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            LakebenchConfig.model_validate(
                data, context={"purpose": LoadPurpose.MUTATE, "allow_long_names": False}
            )
    except ValidationError as e:
        problems = []
        for err in e.errors():
            parts = [str(x) for x in err["loc"]]
            if parts[:2] == ["architecture", "workload"]:
                parts = parts[1:]  # the file writes it at the top level
            loc = ".".join(parts)
            msg = str(err["msg"]).removeprefix("Value error, ")
            problems.append(f"{loc}: {msg}" if loc else msg)
        return "; ".join(problems)
    return None


def _refuse(message: str) -> NoReturn:
    from rich.markup import escape

    err_console.print(f"[red]ERROR[/red] {escape(message)}", highlight=False, soft_wrap=True)
    raise typer.Exit(_EXIT_REFUSED)


def _say(text: str) -> None:
    err_console.print(text, markup=False, highlight=False, soft_wrap=True)


def init(
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output file path for configuration"),
    ] = Path("lakebench.yaml"),
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            "-n",
            help="Deployment name (default: lb-<user>-<4 hex>, unique per init)",
        ),
    ] = None,
    scale: Annotated[
        float | None,
        typer.Option(
            "--scale",
            "-s",
            help=(
                "Scale factor: 1 is about 10 GB of bronze for customer360, 8.4 GB "
                "for financial (default 1; 0.1 with --local)"
            ),
        ),
    ] = None,
    endpoint: Annotated[
        str,
        typer.Option(
            "--endpoint",
            help="S3 endpoint URL (e.g. http://your-s3:80 or https://your-s3:443)",
        ),
    ] = "",
    credentials_env: Annotated[
        str,
        typer.Option(
            "--credentials-env",
            metavar="PREFIX",
            help=(
                "Environment variable prefix for the S3 credentials: the config "
                "references ${PREFIX_ACCESS_KEY} and ${PREFIX_SECRET_KEY}"
            ),
        ),
    ] = DEFAULT_CREDENTIALS_ENV,
    namespace: Annotated[
        str,
        typer.Option(
            "--namespace",
            help="Kubernetes namespace (default: same as deployment name)",
        ),
    ] = "",
    recipe: Annotated[
        str,
        typer.Option(
            "--recipe",
            "-r",
            help=f"Architecture recipe (default {DEFAULT_RECIPE}; see 'config recipes')",
        ),
    ] = "",
    workload: Annotated[
        str,
        typer.Option(
            "--workload",
            "-w",
            help="Workload schema (customer360 | financial). Default is customer360.",
        ),
    ] = "",
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Overwrite an existing file"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Old spelling of --overwrite"),
    ] = False,
    force_short_f: Annotated[
        bool,
        typer.Option("-f", hidden=True, help=DEPRECATED_SHORT_F_HELP),
    ] = False,
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            help="Generate a config for local mode (podman/docker, no Kubernetes)",
        ),
    ] = False,
    access_key: Annotated[
        str | None,
        typer.Option("--access-key", hidden=True, help="Refused: use --credentials-env"),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option("--secret-key", hidden=True, help="Refused: use --credentials-env"),
    ] = None,
    interactive: Annotated[
        bool | None,
        typer.Option(
            "--interactive/--no-interactive",
            "-i",
            hidden=True,
            help="Removed: init no longer has a wizard",
        ),
    ] = None,
    advanced: Annotated[
        bool,
        typer.Option("--advanced", hidden=True, help="Removed: init no longer has a wizard"),
    ] = False,
) -> None:
    """Write a starter configuration file.

    The file names the deployment (lb-<user>-<4 hex> unless --name is
    given), the recipe (polaris-iceberg-spark-trino unless --recipe is
    given), the workload, the scale (1), the S3 endpoint and the two S3
    credentials as ${VAR} references: export LAKEBENCH_S3_ACCESS_KEY and
    LAKEBENCH_S3_SECRET_KEY, or pick the names with --credentials-env.
    The choices are printed on stderr.

    Use 'lakebench config recommend' for cluster sizing guidance.
    """
    access_var, secret_var = credential_vars(credentials_env)
    # Refusals first, before anything is written. A credential value is
    # never echoed: the message names only the variable to export.
    if not _ENV_NAME.fullmatch(credentials_env.rstrip("_") or "-"):
        _refuse(
            f"--credentials-env must be an environment variable prefix such as "
            f"{DEFAULT_CREDENTIALS_ENV}, not {credentials_env!r}"
        )
    if access_key is not None:
        _refuse(f"--access-key is no longer accepted: export {access_var}; init writes a reference")
    if secret_key is not None:
        _refuse(f"--secret-key is no longer accepted: export {secret_var}; init writes a reference")
    if interactive or advanced:
        _say(_WIZARD_REMOVED)

    if force_short_f:
        warn_deprecated_short_f("--overwrite")
        force = True
    overwrite = overwrite or force
    if output.exists() and not overwrite:
        print_error(f"File already exists: {output}")
        print_info("Use --overwrite to replace it")
        raise typer.Exit(1)
    replaced = _replaced_config(output) if overwrite else None
    kept_name = None if name else _name_of(replaced)
    if overwrite and output.is_file() and not name and not kept_name:
        from lakebench.config.deploy_state import read_legacy_name

        legacy = read_legacy_name(output)
        if legacy:
            # v1.6 gave every nameless config in this directory that name, so
            # it may be this file's deployment or a sibling's.
            _refuse(
                f"{output} has no name, and v1.6 recorded '{legacy}' for nameless "
                f"configs here: pass --name {legacy} if this file deployed it, or "
                "--name with a new name"
            )

    from lakebench.config.support import recipe_names, workloads

    if workload and workload not in workloads():
        _refuse(f"unknown workload {workload!r}; valid: {', '.join(workloads())}")

    if local:
        # 1 is the cluster default; a laptop gets 0.1 unless a scale was given.
        local_text = _local_config_text(
            output,
            name or kept_name or "local-lakehouse",
            0.1 if scale is None else scale,
            workload_schema=workload,
        )
        problem = config_problem(local_text, credentials_env)
        if problem:
            _refuse(f"nothing written: this config would not load: {problem}")
        if kept_name:
            _refuse_if_target_moves(output, replaced, local_text)
        _write_local_config(output, local_text, 0.1 if scale is None else scale)
        return

    recipe = recipe or DEFAULT_RECIPE
    if recipe == "default":
        # The alias is deprecated: write what it resolves to.
        from lakebench.config.loader import DEFAULT_RECIPE_RESOLUTION

        recipe = DEFAULT_RECIPE_RESOLUTION
    if recipe not in recipe_names():
        from lakebench.config._hints import nearest

        near = nearest(recipe, recipe_names())
        hint = f"did you mean '{near}'?" if near else "valid: " + ", ".join(recipe_names())
        _refuse(f"unknown recipe {recipe!r}; {hint}")
    workload = workload or DEFAULT_WORKLOAD
    # Overwriting a config keeps its name, so a deployment made from it is
    # still the one this file names (a new random name would orphan it).
    name = name or kept_name or default_name()
    scale = DEFAULT_SCALE if scale is None else scale

    text = first_day_config(
        name=name,
        recipe=recipe,
        workload=workload,
        scale=scale,
        endpoint=endpoint,
        credentials_env=credentials_env,
        namespace=namespace,
    )
    problem = config_problem(text, credentials_env)
    if problem:
        _refuse(f"nothing written: this config would not load: {problem}")
    if kept_name:
        _refuse_if_target_moves(output, replaced, text)
    output.write_text(text)

    _say(f"wrote {output}")
    _say(f"  name:        {name}" + (" (kept from the file it replaced)" if kept_name else ""))
    _say(f"  recipe:      {recipe}")
    _say(f"  workload:    {workload}")
    _say(f"  scale:       {_scalar(scale)}")
    _say(f"  credentials: ${{{access_var}}} and ${{{secret_var}}}")
    if not endpoint:
        _say("  set platform.storage.s3.endpoint")
    _say(f"next: export {access_var} and {secret_var}, then lakebench validate {output}")


# ---------------------------------------------------------------------------
# --local
# ---------------------------------------------------------------------------

_LOCAL_CONFIG_TEMPLATE = """\
# Lakebench local mode -- runs on one host with podman or docker.
#
# No Kubernetes, no object store to provision, no credentials to fill in:
# `deploy --local` starts a Garage container and mints its own keys.
#
#   lakebench deploy {output} --local
#   lakebench run {output} --local --generate --yes
#
# The first `run --local` needs `--generate` to write bronze into the
# local Garage bucket; without it the pipeline runs against an empty
# bronze. On a later run against the same workdir the bronze corpus is
# reused, so `--generate` is only needed again when the workdir was
# cleared or the scale changed.
#
# Local mode is Iceberg-only. DuckDB cannot read Delta on a non-AWS S3
# endpoint, so a Delta config here would fail at query time.
# See: lakebench config recipes --local

name: {name}
recipe: hive-iceberg-spark-duckdb

# Workload lives at the top level (D12 workload move). The deprecated
# architecture.workload block still loads but emits a warning.
workload:{schema_line}
  datagen:
    # 1 unit is roughly 10 GB of bronze, so {scale} is about {approx_gb:.1f} GB.
    # Local mode is sized for 1 and below; larger scales still run but a
    # single JVM shuffling that much on one host takes a long time.
    scale: {scale}

platform:
  storage:
    s3:
      # Filled in by `deploy --local`. Garage runs on localhost:3900 and
      # generates its own access key on first deploy.
      endpoint: "http://localhost:3900"
      region: us-east-1
      path_style: true
      buckets:
        bronze: {prefix}-bronze
        silver: {prefix}-silver
        gold: {prefix}-gold
"""


def _replaced_config(path: Path) -> dict[str, Any] | None:
    """The raw dict of the config about to be overwritten, if it parses."""
    if not path.is_file():
        return None
    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _name_of(data: dict[str, Any] | None) -> str | None:
    value = (data or {}).get("name")
    return value if isinstance(value, str) and value else None


def _sub(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _deployment_target(data: dict[str, Any]) -> dict[str, str]:
    """What destroy and status act on for a raw config: namespace, buckets
    and the recipe its components resolve to (v1.6 precedence)."""
    from lakebench.config.loader import DEFAULT_RECIPE_RESOLUTION
    from lakebench.config.recipes import RECIPES, written_recipe

    name = str(data.get("name") or "")
    platform = _sub(data, "platform")
    k8s = _sub(platform, "kubernetes")
    buckets = _sub(_sub(_sub(platform, "storage"), "s3"), "buckets")
    recipe = data.get("recipe")
    if recipe in (None, "", "default") or recipe not in RECIPES:
        recipe = DEFAULT_RECIPE_RESOLUTION
    target = {
        "namespace": str(k8s.get("namespace") or name),
        "recipe": written_recipe(data, str(recipe)) or str(recipe),
    }
    for layer in ("bronze", "silver", "gold"):
        target[f"buckets.{layer}"] = str(buckets.get(layer) or f"{name}-{layer}")
    return target


def _refuse_if_target_moves(output: Path, replaced: dict[str, Any] | None, text: str) -> None:
    """Refuse an overwrite that keeps the name but moves what it deploys.

    The kept name says "the same deployment"; if the replaced file put it
    in another namespace, other buckets or another architecture, a later
    destroy from the new file would aim at the wrong one.
    """
    if not replaced:
        return
    before = _deployment_target(replaced)
    after = _deployment_target(yaml.safe_load(text))
    moved = [f"{k} '{before[k]}' -> '{after[k]}'" for k in before if before[k] != after[k]]
    if moved:
        _refuse(
            f"{output} keeps its name but the new file would move its deployment: "
            + "; ".join(moved)
            + ". Pass --name to write a config for a new deployment, or edit the file"
        )


def _local_config_text(output: Path, name: str, scale: float, workload_schema: str = "") -> str:
    """The text of a ready-to-run local mode config."""
    prefix = "".join(c if c.isalnum() or c == "-" else "-" for c in name).strip("-").lower()
    # When the user picked a workload, emit `schema:` under `workload:`; the
    # bare `workload:` block otherwise falls back to the customer360 default.
    schema_line = f"\n  schema: {workload_schema}" if workload_schema else ""
    return _LOCAL_CONFIG_TEMPLATE.format(
        output=output,
        name=_scalar(name),
        scale=scale,
        approx_gb=scale * 10.0,
        prefix=prefix or "lakebench",
        schema_line=schema_line,
    )


def _write_local_config(output: Path, content: str, scale: float) -> None:
    """Write a local mode config and print the next steps."""
    output.write_text(content)

    print_success(f"Created local configuration: {output}")
    print_info(f"Scale: {scale} (~{scale * 10.0:.1f} GB bronze)")
    console.print()
    console.print("  Next:")
    console.print(f"    [bold]lakebench deploy {output} --local[/bold]")
    console.print(f"    [bold]lakebench run {output} --local --generate --yes[/bold]")
    console.print("  (--generate populates bronze on the first local run.)")
