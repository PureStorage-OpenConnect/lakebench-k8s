"""Shared fixtures for Lakebench test suite."""

from __future__ import annotations

import os
import tempfile

# Hermetic kube config. Code under test loads kube config before making
# (mocked) API calls; on a developer machine that silently used the real
# ~/.kube/config while CI has none, so tests passed locally and failed in CI.
# The kubernetes client reads KUBECONFIG when it is imported, so this must run
# at conftest import time, before anything imports kubernetes. Nothing
# listens on the fake server.
_FAKE_KUBECONFIG = os.path.join(tempfile.mkdtemp(prefix="lb-test-kube-"), "config")
with open(_FAKE_KUBECONFIG, "w") as _f:
    _f.write(
        "apiVersion: v1\n"
        "kind: Config\n"
        "clusters:\n"
        "- name: test\n"
        "  cluster: {server: 'https://127.0.0.1:1', insecure-skip-tls-verify: true}\n"
        "users:\n"
        "- name: test\n"
        "  user: {token: test}\n"
        "contexts:\n"
        "- name: test\n"
        "  context: {cluster: test, user: test, namespace: default}\n"
        "- name: stale-ctx\n"
        "  context: {cluster: test, user: test, namespace: default}\n"
        "current-context: test\n"
    )
os.environ["KUBECONFIG"] = _FAKE_KUBECONFIG

# Unit tests must never run real cluster CLIs. Before this guard, destroy
# tests ran `helm get values` (and could reach `helm upgrade`) against the
# developer's live cluster. These stubs shadow helm/kubectl/oc on PATH and
# fail loudly; tests that exercise those calls mock subprocess.run.
_CLI_GUARD_DIR = tempfile.mkdtemp(prefix="lb-test-cli-guard-")
for _tool in ("helm", "kubectl", "oc"):
    _path = os.path.join(_CLI_GUARD_DIR, _tool)
    with open(_path, "w") as _f:
        _f.write(
            "#!/bin/sh\n"
            f'echo "{_tool} is blocked in unit tests (mock subprocess.run)" >&2\n'
            "exit 97\n"
        )
    os.chmod(_path, 0o755)
os.environ["PATH"] = _CLI_GUARD_DIR + os.pathsep + os.environ.get("PATH", "")

# Rich forces coloured output under GitHub Actions; ANSI codes then split
# words in captured CLI help (e.g. "--force-legacy") and assertions that pass
# locally fail in CI. Plain text everywhere.
os.environ["NO_COLOR"] = "1"
os.environ.pop("FORCE_COLOR", None)
# Typer forces a terminal when GITHUB_ACTIONS is set (read at import time);
# this is its documented off switch.
os.environ["_TYPER_FORCE_DISABLE_TERMINAL"] = "1"

import functools  # noqa: E402
import importlib  # noqa: E402
import importlib.abc  # noqa: E402
import importlib.util  # noqa: E402
import sys  # noqa: E402
from collections.abc import Callable, Iterator  # noqa: E402
from pathlib import Path  # noqa: E402
from types import ModuleType  # noqa: E402
from typing import Any  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

import pytest  # noqa: E402

from lakebench.config import LakebenchConfig  # noqa: E402

# SAF-4 / DEP-3 oracle (SD-9): `recording_k8s` is available to every test.
from tests.fixtures.recording_k8s import recording_k8s  # noqa: E402, F401

# ---------------------------------------------------------------------------
# Spark script loader (QA-2). The Spark scripts import each other by plain
# name (``from common import ...``), as they do in the driver pod, where the
# scripts ConfigMap is one flat directory. Tests used to put the scripts
# directory on sys.path and import them, so every test in the process shared
# one ``common``: a monkeypatch or a ``sys.modules.pop("common")`` in one test
# changed what a later test saw. ``load_script`` gives each test (or each test
# module, with ``load_script_module``) its own private copy of the scripts it
# loads, ``common`` included, and puts sys.modules back afterwards.
# Two guards keep it that way: tests/test_script_loader_static.py fails on
# the usual hand-made loads (sys.path edits, sys.modules pops), and the
# pytest_runtest_teardown hook fails any test that leaves the scripts
# directory on sys.path or a script module in sys.modules outside a namespace,
# whatever code did it.
# ---------------------------------------------------------------------------

_SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"
SPARK_SCRIPTS_DIR = _SRC / "spark" / "scripts"


@functools.cache
def _shipped_script_modules() -> tuple[tuple[str, Path], ...]:
    from lakebench.modules.pipeline_engines.spark.scripts_maps import SCRIPT_MAPS

    names = {p.stem: p for p in SPARK_SCRIPTS_DIR.glob("*.py")}
    for sources in SCRIPT_MAPS.values():
        for src in sources:
            if src.key.endswith(".py"):
                names[src.key[: -len(".py")]] = _SRC / src.path
    return tuple(sorted(names.items()))


def shipped_script_modules() -> dict[str, Path]:
    """Every plain module name a Spark script can import in the driver pod,
    with its file: the .py files the scripts ConfigMaps ship flat under one
    directory (scripts_maps.SCRIPT_MAPS, which also ships reference_score,
    fidelity_gate and datagen_seed from outside spark/scripts), plus any
    script in spark/scripts the maps miss (tests/test_script_loader.py checks
    there is none)."""
    return dict(_shipped_script_modules())


class _ScriptFinder(importlib.abc.MetaPathFinder):
    """Resolves the shipped plain names to their files while a namespace is
    active, without putting the scripts directory on sys.path."""

    def __init__(self, files: dict[str, Path]) -> None:
        self._files = files

    def find_spec(self, fullname, path=None, target=None):  # noqa: ANN001
        f = self._files.get(fullname)
        if f is None or path is not None:
            return None
        return importlib.util.spec_from_file_location(fullname, f)


class ScriptNamespace:
    """One private set of script modules.

    ``activate`` saves and removes every shipped name from sys.modules and
    installs a finder, so the first import of ``common`` (by the test, or by
    a script at top level or inside a function) executes a fresh copy, and
    every later import in the same namespace gets that copy. ``deactivate``
    removes the finder and every shipped name, then restores what was in
    sys.modules before.
    """

    active: list[ScriptNamespace] = []

    def __init__(self, scope: str, owner: str) -> None:
        self.scope = scope
        self.owner = owner
        self._files = shipped_script_modules()
        self._finder = _ScriptFinder(self._files)
        self._saved: dict[str, ModuleType] = {}

    def activate(self) -> None:
        for name in self._files:
            mod = sys.modules.pop(name, None)
            if mod is not None:
                self._saved[name] = mod
        sys.meta_path.insert(0, self._finder)
        ScriptNamespace.active.append(self)

    def deactivate(self) -> None:
        if self in ScriptNamespace.active:
            ScriptNamespace.active.remove(self)
        if self._finder in sys.meta_path:
            sys.meta_path.remove(self._finder)
        for name in self._files:
            sys.modules.pop(name, None)
        sys.modules.update(self._saved)
        self._saved = {}

    def load(self, name: str, *, extra: tuple[str, ...] = ()) -> Any:
        """Import *name* (and each of *extra*) in this namespace. Returns the
        module, or a tuple ``(module, *extra_modules)`` when *extra* is given.
        A module a script imports is the same object the test gets back:
        ``load("silver_stream_financial", extra=("common",))`` returns the
        ``common`` that silver_stream_financial uses."""
        if ScriptNamespace.active[-1:] != [self]:
            raise RuntimeError(f"the script namespace of {self.owner} is not the active one")
        for n in (name, *extra):
            if n not in self._files:
                raise ValueError(f"{n!r} is not a Spark script module ({SPARK_SCRIPTS_DIR})")
        mod = importlib.import_module(name)
        if not extra:
            return mod
        return (mod, *(importlib.import_module(n) for n in extra))


def _script_namespace(scope: str, request: pytest.FixtureRequest) -> Iterator[Callable[..., Any]]:
    owner = request.node.nodeid
    outer = [ns for ns in ScriptNamespace.active if ns.scope == "module"]
    if scope == "function" and outer:
        raise RuntimeError(
            f"{owner}: load_script and load_script_module in one test module; use one "
            f"(the module namespace of {outer[-1].owner} is active)"
        )
    ns = ScriptNamespace(scope, owner)
    ns.activate()
    try:
        yield ns.load
    finally:
        ns.deactivate()


def exec_repo_script(path: Path, name: str) -> ModuleType:
    """Exec a repository tool script (``scripts/*.py``) as module *name*.
    Some of these put ``src`` or the Spark scripts directory on sys.path at
    import, for their command-line use; the path is restored afterwards so
    the scripts directory does not stay importable for later tests."""
    saved = list(sys.path)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = saved
    return mod


def _script_leaks() -> list[str]:
    """Script state left behind: the scripts directory on sys.path (never
    needed, the finder resolves the names), or a script module in
    sys.modules while no namespace is active."""
    found = []
    scripts = str(SPARK_SCRIPTS_DIR)
    on_path = [p for p in sys.path if p and os.path.realpath(p) == os.path.realpath(scripts)]
    if on_path:
        found.append(f"{scripts} is on sys.path")
    if not ScriptNamespace.active:
        found += [f"{n!r} is in sys.modules" for n in shipped_script_modules() if n in sys.modules]
    return found


def _clear_script_leaks() -> None:
    scripts = os.path.realpath(str(SPARK_SCRIPTS_DIR))
    sys.path[:] = [p for p in sys.path if not p or os.path.realpath(p) != scripts]
    if not ScriptNamespace.active:
        for n in shipped_script_modules():
            sys.modules.pop(n, None)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> Any:
    """Fail a test that leaves the Spark scripts importable for later tests:
    the scripts directory on sys.path, or a script module in sys.modules
    outside a load_script namespace. It runs after the fixtures this test's
    teardown finalizes (its function fixtures with monkeypatch undo, and on
    the last test of a module or session the wider-scoped ones too), so a
    leak made by the test is reported as that test's teardown error and not
    the next test's. A leak made in a module or session fixture's own
    teardown lands on the last test of that scope. If a fixture teardown
    raises, the leak is cleared but only that error is reported. The state
    is cleared either way, so the next test starts without it."""
    try:
        result = yield
    except BaseException:
        _clear_script_leaks()
        raise
    leaks = _script_leaks()
    if leaks:
        _clear_script_leaks()
        pytest.fail(
            "Spark script state leaked (use load_script): " + "; ".join(leaks), pytrace=False
        )
    return result


@pytest.fixture
def load_script(request: pytest.FixtureRequest) -> Iterator[Callable[..., Any]]:
    """Load a Spark script with a private ``common`` for this test:
    ``mod = load_script("silver_build_financial")``. While the test runs, a
    plain ``import common`` (or of any other script), in the test or inside a
    script, resolves to the same private copy."""
    yield from _script_namespace("function", request)


@pytest.fixture(scope="module")
def load_script_module(request: pytest.FixtureRequest) -> Iterator[Callable[..., Any]]:
    """``load_script`` with one private namespace for the whole test module,
    for modules whose tests share a loaded script or a Spark session that
    uses one."""
    yield from _script_namespace("module", request)


@pytest.fixture(autouse=True)
def _offline_deps_set(request, monkeypatch):
    """Spark job manifests need the deployment's verified dependency set,
    which `run`, `continuous` and `financial` load from the cluster before
    any submit (deps.runtime.load_handle). A unit test that builds a manifest
    gets the offline placeholder set instead. A test marked ``real_deps``
    keeps the production default (no set) and proves the guard; the static
    and CLI tests in test_job_deps.py prove every production path loads one.
    """
    if request.node.get_closest_marker("real_deps"):
        return
    from lakebench.deps import runtime
    from lakebench.deps.manifest import placeholder_handle
    from lakebench.modules.pipeline_engines.spark import job

    # The CLI's run-start check reads the cluster; CLI tests run offline.
    monkeypatch.setattr(runtime, "load_handle", lambda cfg, k8s: placeholder_handle(cfg))
    monkeypatch.setattr(
        runtime, "check_pods", lambda cfg, handle, since: {"pods_checked": 0, "pod_mismatches": []}
    )
    monkeypatch.setattr(runtime, "attach_refusal", lambda cfg, run: None)

    real = job.SparkJobManager.__init__

    def init(self, *a, **k):
        real(self, *a, **k)
        if self.deps is None:
            try:
                self.deps = placeholder_handle(self.config)
            except Exception:  # noqa: BLE001 -- a config the request cannot serve
                pass

    monkeypatch.setattr(job.SparkJobManager, "__init__", init)


@pytest.fixture(autouse=True)
def _journal_in_tmp(tmp_path, monkeypatch):
    """CLI commands journal to ./lakebench-output/journal by default, which
    left session files in the repository after every test run. Point the
    shared journal at a per-test directory instead."""
    import lakebench.cli._helpers as helpers
    from lakebench.journal import Journal

    monkeypatch.setattr(helpers, "_journal", Journal(tmp_path / "lakebench-journal"))


@pytest.fixture(autouse=True)
def _fake_destroy_namespace_clock(monkeypatch):
    """Destroy waits (bounded) for the namespace to be NotFound (LB-157).

    Unit tests drive destroy with mocked clients, so run that wait on a fake
    clock: sleeping advances time instantly instead of blocking the suite.
    """
    import lakebench.deploy.destroy as destroy_mod

    now = [0.0]

    def _sleep(seconds: float) -> None:
        now[0] += max(float(seconds), 0.001)

    monkeypatch.setattr(destroy_mod, "_monotonic", lambda: now[0])
    monkeypatch.setattr(destroy_mod, "_sleep", _sleep)


def make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing.

    This is the canonical config factory for tests. Prefer this over
    hand-building dicts so that new required fields are handled in one place.

    LB-090: when the resulting config selects Polaris and no explicit
    ``client_secret`` was supplied, fill in a test-only value so the
    deploy-time ``require_polaris_client_secret`` gate does not fire
    inside unrelated tests. Real production configs must set the secret
    themselves; the loader's ${VAR} substitution is the recommended
    channel.
    """
    from lakebench.config.schema import CatalogType

    base: dict = {
        "name": "test-fixture",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                }
            }
        },
    }
    base.update(overrides)
    cfg = LakebenchConfig(**base)
    if (
        cfg.architecture.catalog.type == CatalogType.POLARIS
        and not cfg.architecture.catalog.polaris.client_secret
    ):
        cfg.architecture.catalog.polaris.client_secret = "test-only-secret"
    return cfg


@pytest.fixture
def default_config() -> LakebenchConfig:
    """A default LakebenchConfig for tests that don't care about specifics."""
    return make_config()


@pytest.fixture
def duckdb_config() -> LakebenchConfig:
    """Config with DuckDB engine selected."""
    return make_config(recipe="hive-iceberg-spark-duckdb")


@pytest.fixture
def trino_config() -> LakebenchConfig:
    """Config with Trino engine selected."""
    return make_config(recipe="hive-iceberg-spark-trino")


@pytest.fixture
def mock_subprocess():
    """Mock subprocess.run for tests that exercise kubectl/helm calls."""
    with patch("subprocess.run") as m:
        m.return_value = MagicMock(
            returncode=0,
            stdout="",
            stderr="",
        )
        yield m


@pytest.fixture
def mock_k8s_client():
    """Pre-configured mock K8sClient for unit tests."""
    client = MagicMock()
    client.namespace = "test-ns"
    client.namespace_exists.return_value = True
    client.apply_manifest.return_value = True
    client.test_connectivity.return_value = (True, "Connected")
    return client


def stub_experiment(
    query_names=(),
    *,
    mode: str = "batch",
    failed=(),
    **identity_over,
) -> dict:
    """A minimal metrics.json ``experiment`` block (metrics/experiment.py) for
    fixtures that hand-build run records: one fixed experiment identity and
    a usable result fingerprint per query in *query_names* (None for the
    names in *failed*, as the runner records a failed query). Keyword
    overrides replace identity fields (seed=..., scale=...)."""
    from lakebench.benchmark.fingerprint import fingerprint_rows
    from lakebench.metrics.experiment import EXPERIMENT_SCHEMA_V1
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID

    results: dict = {
        "query_set_id": None,
        "fingerprints": {
            n: (None if n in failed else fingerprint_rows([(n, 1)], adapted_sql=n))
            for n in query_names
        },
    }
    # A continuous run carries the fingerprints of its end-of-run result
    # check (metrics/experiment.py _continuous_results), like a batch run.
    return {
        "schema": EXPERIMENT_SCHEMA_V1,
        "workload": {"name": "customer360", "version": "c360-1", "parameters_id": "p"},
        "corpus": {
            "id": "corpus",
            "generator_image": "img",
            "seed": identity_over.get("seed", 42),
            "scale": identity_over.get("scale", 10),
            "datagen": {"digest": None},
        },
        "architecture": {"query_access_path": identity_over.get("access_path", "catalog")},
        "mode": mode,
        "maintenance_policy_id": MAINTENANCE_POLICY_ID,
        "effective_maintenance": {
            "id": identity_over.get("maintenance", MAINTENANCE_POLICY_ID + ":expire=on")
        },
        "results": results,
    }


@pytest.fixture(autouse=True)
def _continuous_short_window_check_off(request, monkeypatch):
    """Tests that drive _run_sustained with short windows exercise other
    paths; the short-window refusal (cli/_sustained.short_window_problem)
    is tested in tests/test_continuous_window.py, where it stays on."""
    if request.module.__name__.endswith("test_continuous_window"):
        return
    try:
        from lakebench.cli import _sustained
    except Exception:  # noqa: BLE001
        return
    monkeypatch.setattr(_sustained, "short_window_problem", lambda cfg, run_duration: None)
