"""``lakebench plan CONFIG...``: what a config needs, before anything is deployed.

Read-only. For each config it prints the resolved components and recipe with
their support state, the sizing plan from the one sizing source
(``config.sizing``), the cluster prerequisites from the one registry
(``deploy.prereqs``), the Polaris client secret's source, and the hosts the
deploy-time dependency resolve and image pulls contact. With several
configs it then names the experiment-identity and execution-condition
differences between each one and the first.

With ``--offline``, ``--cores/--memory`` or ``--json`` it makes no cluster
call: prerequisites print "not checked (offline)" and the exit is 0 unless a
config does not load (2), has a value the sizing cannot read (4), or does
not fit the given ``--cores/--memory`` (4). Online, any prerequisite that
fails, a scratch StorageClass, Spark Operator or Stackable that cannot be
checked, or a failed capacity check ends with exit 4 (a missing shared
component prints the ``admin install --component`` command a cluster admin
runs); an unreachable cluster is exit 4 too.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

import typer

from lakebench.cli._helpers import console, err_console, esc, print_error
from lakebench.exit_codes import ExitCode

#: Prerequisites whose failure ends ``plan`` with exit 4: nothing can deploy
#: until a cluster admin installs them.
FATAL_PREREQS = ("scratch-storage-class", "spark-operator", "stackable")

#: Secret per-deployment Polaris credentials are kept in.
POLARIS_CLIENT_SECRET = "lakebench-polaris-client"
_POLARIS_DEPLOYMENT = "lakebench-polaris"


# -- egress -------------------------------------------------------------------


def _host(url: str) -> str:
    from urllib.parse import urlparse

    return urlparse(url).hostname or url


def _registry(image: str) -> str:
    """The registry host of an image reference (``docker.io`` when unqualified)."""
    first = image.split("/", 1)[0]
    if "/" in image and ("." in first or ":" in first or first == "localhost"):
        return first.split(":", 1)[0]  # the host, without a port
    return "docker.io"


def egress_hosts(cfg: Any) -> list[tuple[str, str]]:
    """``[(host, why)]`` this deployment contacts outside the cluster.

    The dependency hosts are ``deps.request.egress_hosts``, the one list
    (the ``egress-hosts`` prerequisite reads it too): the dependency server
    (lb-deps) resolves every jar, wheel and DuckDB extension from them at
    deploy, and nothing resolves at run time. The rest is what the deploy
    pulls besides: the observability Helm chart when enabled, and the image
    registries of every image this config deploys.
    """
    from lakebench.deps import request as deps_request

    arch = cfg.architecture
    catalog = arch.catalog.type.value
    engine = arch.query_engine.type.value
    maven = {deps_request._host(r) for r in deps_request.repositories(cfg)}
    wheels = {deps_request._host(deps_request.pypi_index(cfg)), deps_request.PYPI_FILES_HOST}
    extensions = deps_request._host(deps_request.duckdb_extension_repository(cfg))
    out: list[tuple[str, str]] = []
    for host in deps_request.egress_hosts(cfg):
        # One mirror can serve several kinds.
        kinds = [
            kind
            for kind, hit in (
                ("Spark jars", host in maven),
                ("Python wheels", host in wheels),
                ("DuckDB extensions", host == extensions),
            )
            if hit
        ]
        what = " and ".join(kinds) or "dependencies"
        out.append((host, f"lb-deps resolves the {what} from it at deploy"))
    if cfg.observability.enabled:
        out.append(
            ("prometheus-community.github.io", "kube-prometheus-stack Helm chart, at deploy")
        )
        out += [
            ("quay.io", "kube-prometheus-stack images (Prometheus, operator)"),
            ("registry.k8s.io", "kube-prometheus-stack images (kube-state-metrics)"),
        ]
    images = cfg.images
    used = {"spark": images.spark, "datagen": images.datagen, "postgres": images.postgres}
    # Helper images the templates name directly (init containers, bootstrap).
    helpers = {"busybox": "busybox:1.36"}
    if catalog == "polaris":
        used["polaris"] = images.polaris
        used["polaris_admin_tool"] = images.polaris_admin_tool
        helpers["curl"] = "curlimages/curl"
    if catalog == "unity":
        used["unity"] = images.unity
    if catalog == "hive":
        used["hive (Stackable)"] = "oci.stackable.tech/sdp/hive"
    if engine == "trino":
        used["trino"] = images.trino
    if engine == "duckdb":
        used["duckdb"] = images.duckdb
    if cfg.observability.enabled:
        used["jmx_exporter"] = images.jmx_exporter
    used.update(helpers)
    by_registry: dict[str, list[str]] = {}
    for role, image in used.items():
        by_registry.setdefault(_registry(str(image)), []).append(role)
    for registry, roles in by_registry.items():
        out.append((registry, f"image registry ({', '.join(roles)})"))
    seen: set[str] = set()
    unique = []
    for host, why in out:
        if host not in seen:
            seen.add(host)
            unique.append((host, why))
    return unique


# -- Polaris client secret ----------------------------------------------------


def _raw_polaris_secret(path: Path) -> Any:
    import yaml

    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except Exception:  # noqa: BLE001 -- the loaded config already parsed
        return None
    arch = raw.get("architecture") if isinstance(raw, dict) else None
    cat = (arch or {}).get("catalog") if isinstance(arch, dict) else None
    pol = (cat or {}).get("polaris") if isinstance(cat, dict) else None
    return pol.get("client_secret") if isinstance(pol, dict) else None


def _deploy_generates_polaris_secret() -> bool:
    """Whether this tree's deploy generates the Polaris client secret.

    Before per-deployment secrets, ``schema.require_polaris_client_secret``
    makes deploy refuse a Polaris config with no ``client_secret``; the
    per-deployment secrets change removes it.
    """
    from lakebench.config import schema

    return not hasattr(schema, "require_polaris_client_secret")


def polaris_secret_line(cfg: Any, path: Path, k8s: Any = None) -> str | None:
    """Where the Polaris client secret comes from; None for other catalogs.

    Informational: it never changes the exit code and never prints a value
    (a ``${VAR:-default}`` reference is named by its variable only). Offline
    it cannot tell a new Polaris from one already bootstrapped, so it says
    what holds for a new one. Online it reads the deployment's Secret and
    whether the namespace already runs Polaris.
    """
    from lakebench.config.loader import _ENV_PATTERN

    if cfg.architecture.catalog.type.value != "polaris":
        return None
    raw = _raw_polaris_secret(path)
    if isinstance(raw, str) and (m := _ENV_PATTERN.search(raw)):
        partly = "" if raw.strip() == m.group(0) else " (with literal text in the file around it)"
        return f"client secret: from ${{{m.group(1)}}}{partly}"
    if raw:
        return "client secret: set in the config file (plaintext)"
    if not _deploy_generates_polaris_secret():
        return (
            "client secret: not set; deploy refuses a Polaris config without "
            "architecture.catalog.polaris.client_secret"
        )
    if k8s is None:
        return (
            "client secret: generated at deploy for a new Polaris "
            "(a Polaris this namespace already runs keeps its own)"
        )
    ns = cfg.get_namespace()
    try:
        if k8s.secret_exists(POLARIS_CLIENT_SECRET, ns):
            return f"client secret: from Secret {POLARIS_CLIENT_SECRET} in {ns}"
        running = k8s.namespace_exists(ns) and _polaris_deployed(k8s, ns)
    except Exception as e:  # noqa: BLE001
        return f"client secret: not checked ({type(e).__name__})"
    if running:
        return (
            f"client secret: {ns} already runs Polaris with no Secret "
            f"{POLARIS_CLIENT_SECRET}; deploy refuses until "
            "architecture.catalog.polaris.client_secret is set"
        )
    return "client secret: generated at deploy for a new Polaris"


def _polaris_deployed(k8s: Any, ns: str) -> bool:
    from kubernetes.client.rest import ApiException

    try:
        k8s._apps_v1.read_namespaced_deployment(_POLARIS_DEPLOYMENT, ns)
    except ApiException as e:
        if e.status == 404:
            return False
        raise
    return True


# -- one config ---------------------------------------------------------------


def _manual_capacity(cores: int, memory_gb: int) -> Any:
    from lakebench.k8s.client import ClusterCapacity

    return ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=memory_gb * 1024**3,
        node_count=0,
        largest_node_cpu_millicores=cores * 1000,
        largest_node_memory_bytes=memory_gb * 1024**3,
    )


def plan_one(
    cfg: Any,
    path: Path,
    *,
    offline: bool,
    capacity: Any = None,
) -> tuple[dict[str, Any], int]:
    """The plan of one loaded config, as a dict, and its exit code."""
    from lakebench.config.sizing import breakdown_text, check_capacity, plan_requirements
    from lakebench.config.support import recipe_for, support_state_for_config

    arch = cfg.architecture
    parts = (
        arch.catalog.type.value,
        arch.table_format.type.value,
        arch.pipeline_engine.value,
        arch.query_engine.type.value,
    )
    support = support_state_for_config(cfg)
    k8s = None
    code = int(ExitCode.OK)
    out: dict[str, Any] = {
        "config": str(path),
        "name": cfg.name,
        "namespace": cfg.get_namespace(),
        "workload": arch.workload.schema_type.value,
        "mode": "continuous" if arch.pipeline.mode.value != "batch" else "batch",
        "scale": float(arch.workload.datagen.get_effective_scale()),
        "recipe": recipe_for(*parts) or "-".join(parts),
        "components": dict(
            zip(("catalog", "table_format", "pipeline_engine", "query_engine"), parts, strict=True)
        ),
        "support": {"state": support.get("state"), "basis": support.get("basis")},
    }

    sizing_capacity = capacity
    if not offline:
        from lakebench.k8s import K8sConnectionError, get_k8s_client

        k8s = get_k8s_client(context=cfg.platform.kubernetes.context, namespace=cfg.get_namespace())
        reachable, why = k8s.test_connectivity(timeout=(5, 10))
        if not reachable:
            raise K8sConnectionError(why)
        # As run sizes it: against the allocatable total it reads. A node
        # quantity it cannot read leaves the sizing on the reference table;
        # the capacity row below fails on it.
        from lakebench.quantity import QuantityError

        try:
            sizing_capacity = k8s.get_cluster_capacity()
        except QuantityError:
            sizing_capacity = None
    try:
        plan = plan_requirements(cfg, capacity=sizing_capacity)
    except Exception as e:  # noqa: BLE001 -- refused as deploy and run refuse it
        from lakebench.cli._prerequisites import _unreadable_config

        bad = _unreadable_config(e)
        out["sizing"] = {"unreadable": bad.message, "next": bad.hint}
        code = int(ExitCode.PREREQUISITE)
        plan = None
    if plan is not None:
        out["sizing"] = {
            "floor": {"cpu_cores": plan.floor.cpu_cores, "memory_gb": plan.floor.memory_gb},
            "full": {"cpu_cores": plan.full.cpu_cores, "memory_gb": plan.full.memory_gb},
            "driven_by": plan.floor_driver,
            "breakdown": breakdown_text(plan),
            "scratch_gi": plan.scratch_gb,
            "scratch": (
                f"{plan.scratch_gb:,} Gi of StorageClass {plan.scratch_storage_class}"
                if plan.scratch_enabled
                else "not requested (scratch disabled)"
            ),
            "against": (
                "this cluster"
                if (not offline and sizing_capacity is not None)
                else (
                    "the given --cores/--memory"
                    if capacity is not None
                    else (
                        "no cluster"
                        if offline
                        else "the reference table (cluster capacity not readable)"
                    )
                )
            ),
            "basis": list(plan.basis),
        }
    if plan is not None and capacity is not None:
        # Node shape unknown: aggregate only, no one-pod-on-one-node check.
        verdict = check_capacity(cfg, capacity, check_pod=False)
        out["capacity"] = {
            "status": verdict.status,
            "shortfalls": list(verdict.shortfalls),
            "note": "aggregate only: node shape unknown, so the largest pod is not checked",
        }
        if verdict.status == "refused":
            code = int(ExitCode.PREREQUISITE)

    if offline:
        out["prerequisites"] = "not checked (offline)"
    else:
        from lakebench.cli._prerequisites import _check_cluster_capacity
        from lakebench.deploy.prereqs import PrereqStatus, run_prereqs

        rows = []
        for outcome in run_prereqs(cfg):
            p, r = outcome.prereq, outcome.result
            row: dict[str, Any] = {
                "id": p.id,
                "title": p.title,
                "status": r.status.value,
                "message": r.message,
            }
            fatal = p.id in FATAL_PREREQS and r.status is PrereqStatus.UNKNOWN
            if r.status is PrereqStatus.FAIL or fatal:
                # Nothing deploys until these are in place, and one that
                # could not be checked is not known to be (fails closed).
                code = int(ExitCode.PREREQUISITE)
                row["next"] = (
                    f"(cluster admin) lakebench admin install --component {p.component}"
                    if p.component
                    else p.fix
                )
            rows.append(row)
        cap = _check_cluster_capacity(cfg, sizing_capacity=sizing_capacity)
        detail = [ln.strip() for ln in (cap.hint or "").splitlines() if ln.strip()]
        cap_row: dict[str, Any] = {
            "id": "cluster-capacity",
            "title": "Free cluster capacity",
            "status": "ok" if cap.passed else "fail",
            "message": cap.message,
        }
        if not cap.passed:
            code = int(ExitCode.PREREQUISITE)
            cap_row["detail"] = [ln for ln in detail if not ln.startswith("Next:")]
            nexts = [ln.removeprefix("Next:").strip() for ln in detail if ln.startswith("Next:")]
            if nexts:
                cap_row["next"] = "\n".join(nexts)
        rows.append(cap_row)
        out["prerequisites"] = rows

    secret = polaris_secret_line(cfg, path, k8s)
    if secret is not None:
        out["polaris"] = secret
    out["egress"] = [{"host": h, "why": w} for h, w in egress_hosts(cfg)]
    return out, code


def _print_plan(p: dict[str, Any]) -> None:
    def say(text: str) -> None:
        console.print(text, soft_wrap=True)

    say(f"[bold]{esc(p['config'])}[/bold]: {esc(p['name'])} (namespace {esc(p['namespace'])})")
    c = p["components"]
    say(
        f"  recipe {esc(p['recipe'])}: catalog {esc(c['catalog'])}, format "
        f"{esc(c['table_format'])}, pipeline {esc(c['pipeline_engine'])}, query "
        f"{esc(c['query_engine'])}"
    )
    say(
        f"  workload {esc(p['workload'])}, {esc(p['mode'])}, scale {p['scale']:g}; support "
        f"{esc(p['support']['state'])} ({esc(p['support']['basis'])})"
    )
    s = p["sizing"]
    if "unreadable" in s:
        say(f"  sizing: {esc(s['unreadable'])}")
        say(f"    Next: {esc(s['next'])}")
    else:
        say(
            f"  needs {s['floor']['cpu_cores']} cores / {s['floor']['memory_gb']} GB at once, "
            f"driven by {esc(s['driven_by'])} (sized against {esc(s['against'])})"
        )
        say(f"    {esc(s['breakdown'])}")
        say(f"  scratch: {esc(s['scratch'])}")
    if "capacity" in p:
        cap = p["capacity"]
        say(f"  capacity (--cores/--memory): {esc(cap['status'])} ({esc(cap['note'])})")
        for line in cap["shortfalls"]:
            say(f"    {esc(line)}")
    pre = p["prerequisites"]
    if isinstance(pre, str):
        say(f"  prerequisites: {esc(pre)}")
    else:
        say("  prerequisites:")
        for row in pre:
            say(f"    {esc(row['status'])}: {esc(row['title'])}: {esc(row['message'])}")
            for line in row.get("detail", []):
                say(f"      {esc(line)}")
            if "next" in row:
                for line in str(row["next"]).splitlines():
                    say(f"      Next: {esc(line.strip())}")
    if "polaris" in p:
        say(f"  Polaris {esc(p['polaris'])}")
    say("  egress (hosts outside the cluster this deployment contacts):")
    for e in p["egress"]:
        say(f"    {esc(e['host'])}: {esc(e['why'])}")


def _differences(configs: list[Any]) -> list[dict[str, Any]]:
    from lakebench.metrics.experiment import (
        condition_differences,
        identity_differences,
        planned_experiment,
    )

    blocks = [planned_experiment(c) for c in configs]
    out = []
    for cfg, block in zip(configs[1:], blocks[1:], strict=True):
        out.append(
            {
                "a": configs[0].name,
                "b": cfg.name,
                "identity": identity_differences(blocks[0], block),
                "conditions": condition_differences(blocks[0], block),
            }
        )
    return out


def _deploy_refusal(path: Path, name: str | None) -> str | None:
    """Why ``deploy`` would refuse the config at load, or None. Runs the
    load deploy runs, without printing its notes."""
    import warnings

    from lakebench.config._load_context import LoadPurpose, collecting_notes
    from lakebench.config.loader import ConfigError, ConfigNameRequired, _load_and_validate

    try:
        with collecting_notes(), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _load_and_validate(path, LoadPurpose.MUTATE, name, False)
    except ConfigNameRequired:
        # A nameless config plans under --name or its resolved name (the
        # READ rules); writing name: into the file is deploy's own refusal.
        # Validate the rest as deploy would, under that name.
        return _nameless_refusal(path, name)
    except ConfigError as e:
        lines = [ln.strip(" -") for ln in str(e).splitlines() if ln.strip()]
        return "; ".join(ln for ln in lines if ln != "Configuration validation failed:")
    return None


def _nameless_refusal(path: Path, name: str | None) -> str | None:
    import warnings

    from pydantic import ValidationError

    from lakebench.config._load_context import LoadPurpose, collecting_notes
    from lakebench.config.loader import _apply_flat_fields, load_yaml
    from lakebench.config.schema import LakebenchConfig

    if not name:
        return None
    try:
        with collecting_notes(), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = _apply_flat_fields(load_yaml(path))
            data["name"] = name
            LakebenchConfig.model_validate(
                data, context={"purpose": LoadPurpose.MUTATE, "allow_long_names": False}
            )
    except ValidationError as e:
        return "; ".join(str(err["msg"]).removeprefix("Value error, ") for err in e.errors())
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {e}"
    return None


def plan(
    config_files: Annotated[
        list[Path],
        typer.Argument(help="One or more configuration files", exists=True, dir_okay=False),
    ],
    offline: Annotated[
        bool,
        typer.Option("--offline", help="Make no cluster call: size without a cluster"),
    ] = False,
    cores: Annotated[
        int | None,
        typer.Option("--cores", min=1, help="Cluster CPU cores to size against (implies offline)"),
    ] = None,
    memory: Annotated[
        int | None,
        typer.Option("--memory", min=1, help="Cluster memory in GB to size against (with --cores)"),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option("--name", help="The deployment name for a config that sets none"),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print the plan as JSON (always offline)"),
    ] = False,
) -> None:
    """Show what each config needs: components, sizing, prerequisites, egress.

    Read-only. Online it checks the cluster prerequisites and free capacity
    and exits 4 when a scratch StorageClass, the Spark Operator or Stackable
    is missing, naming the admin install command. --offline, --cores/--memory
    and --json make no cluster call.
    """
    from lakebench.config import ConfigError, LoadPurpose, load_config
    from lakebench.deploy.prereqs import ClusterUnreachable
    from lakebench.k8s import K8sConnectionError

    if (cores is None) != (memory is None):
        print_error("--cores and --memory go together")
        raise typer.Exit(ExitCode.USAGE)
    capacity = _manual_capacity(cores, memory) if cores is not None and memory else None
    offline = offline or as_json or capacity is not None

    configs, plans, code = [], [], int(ExitCode.OK)
    for path in config_files:
        try:
            cfg = load_config(path, purpose=LoadPurpose.READ, name_override=name, print_notes=False)
        except ConfigError as e:
            print_error(f"{path}: {e}")
            raise typer.Exit(ExitCode.USAGE) from None
        # Under the resolved name, so a nameless config gets deploy's full
        # validation too.
        refusal = _deploy_refusal(path, cfg.name)
        if refusal:
            # READ skips what only deploy checks (derived name lengths,
            # removed keys); plan must not call a config deploy refuses fine.
            print_error(f"{path}: deploy refuses this config: {refusal}")
            raise typer.Exit(ExitCode.USAGE)
        from lakebench.config.loader import _print_load_notes, load_notes

        _print_load_notes(path, load_notes(cfg))
        try:
            one, one_code = plan_one(cfg, path, offline=offline, capacity=capacity)
        except (ClusterUnreachable, K8sConnectionError) as e:
            print_error(f"cluster unreachable: {e}")
            err_console.print("Next: use --offline to size without a cluster", markup=False)
            raise typer.Exit(ExitCode.PREREQUISITE) from None
        configs.append(cfg)
        plans.append(one)
        code = max(code, one_code)

    diffs = _differences(configs) if len(configs) > 1 else []
    if as_json:
        print(json.dumps({"plans": plans, "differences": diffs}, indent=2, default=str))
    else:
        for i, p in enumerate(plans):
            if i:
                console.print()
            _print_plan(p)
        for d in diffs:
            console.print()
            console.print(f"[bold]{esc(d['a'])} vs {esc(d['b'])}[/bold]")
            if not d["identity"] and not d["conditions"]:
                console.print("  same experiment identity and execution conditions")
            for line in d["identity"]:
                console.print(f"  identity: {esc(line)}")
            for line in d["conditions"]:
                console.print(f"  conditions: {esc(line)}")
    if code:
        raise typer.Exit(code)
