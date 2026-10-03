#!/usr/bin/env python3.11
"""Release harness: runs the release matrix from a detached release worktree.

Usage::

    python3.11 scripts/release/harness.py plan   --matrix scripts/release/matrix-1.7.yaml --freeze SHA
    python3.11 scripts/release/harness.py run    --matrix ... --freeze SHA --context CTX \\
        --out /root/lakebench-release/1.7.0 --deployments-ledger PATH \\
        [--rows M01,M02] [--slots 3] [--rehearsal]
    python3.11 scripts/release/harness.py resume --out DIR --context CTX --deployments-ledger PATH

It ships outside the wheel (``scripts/`` is not packaged). It runs lakebench
only as ``env PYTHONPATH=<worktree>/src python3.11 -m lakebench ...`` and
imports the same tree in-process for sizing, state and verdict helpers.

Refusals, checked before any cluster call (each prints and exits 2): HEAD on
a branch (the release worktree is detached); HEAD not the ``--freeze``
commit (``--rehearsal`` waives only this); a tracked change, or an
untracked or ignored file under ``src/`` or ``scripts/`` other than
``__pycache__``; lakebench imported from outside ``<worktree>/src``, in a
child process or in this one; ``--context`` not in the kubeconfig; no
``--deployments-ledger`` for ``run``/``resume``; ``--out`` inside the
worktree or under ``/tmp``.

Per row: write the config (``lakebench init`` plus the row's keys, the
context pinned), refuse a config whose Spark minor and table format version
differ from the release matrix's, admit (``cluster.admit``), write the
ledger row, ``deploy --yes --require-new``, read the incarnation through the
release tree's ``read_state`` and ``current_incarnation`` (the namespace
must carry the state's newest, confirmed nonce), ``run --generate --yes``
(the default per-job timeout, at least 3,600 s), ``report``, scrub the
record into ``<out>/uat/runs/``, then ``destroy --yes --expect-incarnation
<uid>#<nonce>``. The row's verdict is ``release_record.record_problems``
on its record; an exit code is never enough on its own. Destroy is never
passed ``--force`` and is never re-invoked: exit 6 is polled read-only, any
refusal stops admission. A deploy that fails is never retried: the row's
namespace is destroyed by incarnation only when it carries the row's
confirmed nonce, otherwise it is left and reported.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import yaml

HERE = Path(__file__).resolve().parent
TREE = HERE.parents[1]


def _sibling(name: str) -> Any:
    """Load ``scripts/release/<name>.py`` as ``lb_release_<name>`` (generic
    names such as ``cluster`` must not collide with other modules)."""
    key = f"lb_release_{name}"
    if key in sys.modules:
        return sys.modules[key]
    spec = importlib.util.spec_from_file_location(key, HERE / f"{name}.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {HERE / f'{name}.py'}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[key] = mod
    spec.loader.exec_module(mod)
    return mod


_cluster = _sibling("cluster")
_ledger = _sibling("ledger")
ActiveRow = _cluster.ActiveRow
Candidate = _cluster.Candidate
ClusterReader = _cluster.ClusterReader
Peak = _cluster.Peak
Snapshot = _cluster.Snapshot
Unknown = _cluster.Unknown
admit = _cluster.admit
TERMINAL = _ledger.TERMINAL
LedgerError = _ledger.LedgerError
LedgerRow = _ledger.LedgerRow
MarkdownLedger = _ledger.MarkdownLedger
RowLog = _ledger.RowLog
utc_now = _ledger.utc_now

WORKLOADS = ("customer360", "financial")
MODES = ("batch", "continuous")
#: Seeds by workload: C360 42; AML 43, the development seed (42 is spent).
SEEDS = {"customer360": 42, "financial": 43}
CREDENTIAL_VARS = ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY")
PLACEHOLDER_ENDPOINT = "http://10.0.1.50:80"
DESTROY_POLL_S = 30
DESTROY_POLL_LIMIT_S = 20 * 60
ADMIT_POLL_S = 60
BLOCKING_REPORT_S = 10 * 60


class Refused(Exception):
    """A harness refusal: printed, exit 2."""


# -- matrix ------------------------------------------------------------------


@dataclass(frozen=True)
class Row:
    id: str
    workload: str
    mode: str
    recipe: str
    scale: float
    seed: int
    alone: bool = False
    ml_loop: bool = False
    extra_steps: tuple[str, ...] = ()

    @property
    def key(self) -> tuple[str, str, str, float]:
        return (self.workload, self.mode, self.recipe, float(self.scale))

    @property
    def aml_continuous(self) -> bool:
        return self.workload == "financial" and self.mode == "continuous"


_ROW_KEYS = {"id", "workload", "mode", "recipe", "scale", "seed", "alone", "ml_loop", "extra_steps"}


def load_matrix(path: Path, known_steps: Sequence[str] = ()) -> tuple[str, list[Row]]:
    """(version, rows) of a matrix file; Refused on any malformed row."""
    try:
        data = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as e:
        raise Refused(f"cannot read matrix {path}: {e}") from e
    if not isinstance(data, dict) or not isinstance(data.get("rows"), list):
        raise Refused(f"{path}: expected a mapping with a rows list")
    version = str(data.get("version") or "")
    if not version:
        raise Refused(f"{path}: no version")
    rows: list[Row] = []
    seen: set[str] = set()
    for i, raw in enumerate(data["rows"]):
        where = f"{path} row {i + 1}"
        if not isinstance(raw, dict):
            raise Refused(f"{where}: not a mapping")
        unknown = set(raw) - _ROW_KEYS
        if unknown:
            raise Refused(f"{where}: unknown keys {sorted(unknown)}")
        try:
            row = Row(
                id=str(raw["id"]),
                workload=str(raw["workload"]),
                mode=str(raw["mode"]),
                recipe=str(raw["recipe"]),
                scale=float(raw["scale"]),
                seed=int(raw.get("seed", SEEDS.get(str(raw["workload"]), 0))),
                alone=bool(raw.get("alone", False)),
                ml_loop=bool(raw.get("ml_loop", False)),
                extra_steps=tuple(str(s) for s in raw.get("extra_steps") or ()),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise Refused(f"{where}: {e}") from e
        if not row.id.replace("-", "").isalnum():
            raise Refused(f"{where}: id {row.id!r} must be letters, digits and dashes")
        if row.id in seen:
            raise Refused(f"{where}: duplicate id {row.id}")
        seen.add(row.id)
        if row.workload not in WORKLOADS:
            raise Refused(f"{where}: workload {row.workload!r} not in {WORKLOADS}")
        if row.mode not in MODES:
            raise Refused(f"{where}: mode {row.mode!r} not in {MODES}")
        if row.seed != SEEDS[row.workload]:
            raise Refused(
                f"{where}: {row.workload} rows use seed {SEEDS[row.workload]}, not {row.seed}"
            )
        if row.ml_loop:
            raise Refused(f"{where}: ml_loop rows are not supported by this matrix")
        bad = [s for s in row.extra_steps if s not in known_steps]
        if bad:
            raise Refused(f"{where}: unknown extra steps {bad}")
        rows.append(row)
    return version, rows


def matrix_problems(rows: Sequence[Row]) -> list[str]:
    """Rows that are not release-matrix rows (``release_record.RELEASE_MATRIX``)."""
    from lakebench.metrics.release_record import RELEASE_MATRIX

    want = {(w, m, r, float(s)) for w, m, r, s in RELEASE_MATRIX}
    return [f"{r.id}: {r.key} is not a release-matrix row" for r in rows if r.key not in want]


def missing_matrix_rows(rows: Sequence[Row]) -> list[tuple[str, str, str, float]]:
    from lakebench.metrics.release_record import RELEASE_MATRIX

    have = {r.key for r in rows}
    return [k for k in ((w, m, r, float(s)) for w, m, r, s in RELEASE_MATRIX) if k not in have]


# -- refusals ----------------------------------------------------------------


def _git(tree: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(
        ["git", "-C", str(tree), *args], capture_output=True, text=True, env=env, check=False
    )


def child_env(tree: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
    """The environment of every lakebench child: this tree's ``src`` first."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("PYTHONPATH", "PYTHONHOME", "LB_EXIT_PATH_FILE", "PYTHONSTARTUP")
    }
    env["PYTHONPATH"] = str(tree / "src")
    env.update(extra or {})
    return env


def imported_from(tree: Path, cwd: Path) -> str:
    """``lakebench.__file__`` as a child process with the harness env sees it."""
    out = subprocess.run(
        [sys.executable, "-c", "import lakebench,sys;print(lakebench.__file__)"],
        capture_output=True,
        text=True,
        env=child_env(tree),
        cwd=str(cwd),
        check=False,
    )
    return out.stdout.strip() if out.returncode == 0 else f"(import failed: {out.stderr.strip()})"


def _under(path: str | Path, root: Path) -> bool:
    try:
        Path(path).resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def refuse_reasons(
    tree: Path,
    *,
    freeze: str,
    rehearsal: bool,
    out: Path | None,
    live: bool,
    ledger: Path | None,
    context: str | None,
    contexts: Callable[[], list[str]] | None = None,
    in_process_file: str | None = None,
) -> list[str]:
    """Every reason the harness must not start; empty when it may.

    *in_process_file* is the ``lakebench.__file__`` this process imported
    (default: read it); the child check always spawns a real interpreter.
    """
    reasons: list[str] = []
    if _git(tree, "symbolic-ref", "-q", "HEAD").returncode == 0:
        reasons.append("HEAD is on a branch; the release worktree must be detached at --freeze")
    head = _git(tree, "rev-parse", "HEAD").stdout.strip()
    want = _git(tree, "rev-parse", "--verify", "-q", f"{freeze}^{{commit}}").stdout.strip()
    if not rehearsal and (not want or head != want):
        reasons.append(f"HEAD {head[:12]} is not the freeze commit {freeze[:12]}")
    status = _git(tree, "status", "--porcelain", "--ignored", "--untracked-files=all")
    if status.returncode != 0:
        reasons.append(f"git status failed: {status.stderr.strip()}")
    for line in status.stdout.splitlines():
        code, path = line[:2], line[3:]
        if code not in ("??", "!!"):
            reasons.append(f"tracked change: {line.strip()}")
        elif path.startswith(("src/", "scripts/")) and "__pycache__/" not in path:
            what = "untracked" if code == "??" else "ignored"
            reasons.append(f"{what} file under src/ or scripts/: {path}")
    if in_process_file is None:
        import lakebench

        in_process_file = str(lakebench.__file__)
    here = Path(in_process_file)
    if not _under(here, tree / "src" / "lakebench"):
        reasons.append(f"this process imports lakebench from {here}, not {tree / 'src'}")
    child = imported_from(tree, out if out and out.is_dir() else Path(tempfile.gettempdir()))
    if not _under(child, tree / "src" / "lakebench"):
        reasons.append(f"a lakebench child imports from {child}, not {tree / 'src'}")
    if out is not None:
        if _under(out, tree):
            reasons.append(f"--out {out} is inside the release worktree")
        if _under(out, Path("/tmp")):
            reasons.append(f"--out {out} is under /tmp")
    if live:
        if ledger is None:
            reasons.append("--deployments-ledger is required for a live run")
        elif not ledger.is_file():
            reasons.append(f"--deployments-ledger {ledger} does not exist")
        if not context:
            reasons.append("--context is required for a live run")
        elif contexts is not None:
            try:
                names = contexts()
            except Exception as e:  # noqa: BLE001 -- unreadable kubeconfig refuses
                names = []
                reasons.append(f"cannot read the kubeconfig contexts: {e}")
            if names and context not in names:
                reasons.append(f"--context {context} is not in the kubeconfig")
        missing = [v for v in CREDENTIAL_VARS if not os.environ.get(v)]
        if missing:
            reasons.append(f"unset credential variables: {', '.join(missing)}")
        if not os.environ.get("LB_S3_ENDPOINT"):
            reasons.append("LB_S3_ENDPOINT is unset (the S3 endpoint the row configs use)")
    return reasons


def kube_contexts() -> list[str]:
    from kubernetes import config as kconfig

    contexts, _active = kconfig.list_kube_config_contexts()
    return [c["name"] for c in contexts or []]


# -- children ----------------------------------------------------------------


@dataclass
class ChildResult:
    code: int
    paths: list[str]
    log: Path

    def text(self) -> str:
        try:
            return self.log.read_text(errors="replace")
        except OSError:
            return ""


class Runner(Protocol):
    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        log: Path,
        interruptible: bool = False,
        on_spawn: Callable[[int, str], None] | None = None,
    ) -> ChildResult: ...


def _start_time(pid: int) -> str:
    """Field 22 of /proc/<pid>/stat, or "" when unreadable."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return ""
    return stat.rsplit(")", 1)[1].split()[19]


def child_alive(pid: Any, start: Any) -> bool:
    """Whether the recorded child is still running (same pid and start time)."""
    if not isinstance(pid, int) or not start:
        return False
    return _start_time(pid) == str(start)


class ProcessRunner:
    """Runs ``python -m lakebench ...`` from the release tree."""

    def __init__(self, tree: Path) -> None:
        self.tree = tree
        self._lock = threading.Lock()
        self._children: dict[int, bool] = {}
        self._interrupted = False

    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        log: Path,
        interruptible: bool = False,
        on_spawn: Callable[[int, str], None] | None = None,
    ) -> ChildResult:
        log.parent.mkdir(parents=True, exist_ok=True)
        fd, path_file = tempfile.mkstemp(prefix="exit-path-", dir=str(log.parent))
        os.close(fd)
        env = child_env(self.tree, {"LB_EXIT_PATH_FILE": path_file})
        with open(log, "ab") as out:
            out.write(f"$ lakebench {' '.join(args)}\n".encode())
            out.flush()
            proc = subprocess.Popen(  # noqa: S603 -- fixed argv, our own interpreter
                [sys.executable, "-m", "lakebench", *args],
                cwd=str(cwd),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            with self._lock:
                self._children[proc.pid] = interruptible
                if interruptible and self._interrupted:
                    _signal_group(proc.pid)
            if on_spawn is not None:
                on_spawn(proc.pid, _start_time(proc.pid))
            code = proc.wait()
            with self._lock:
                self._children.pop(proc.pid, None)
        paths = _read_paths(Path(path_file))
        with contextlib.suppress(OSError):
            os.unlink(path_file)
        return ChildResult(code, paths, log)

    def interrupt(self) -> None:
        """Forward one SIGINT to each interruptible child; the others finish."""
        with self._lock:
            if self._interrupted:
                return
            self._interrupted = True
            for pid, interruptible in self._children.items():
                if interruptible:
                    _signal_group(pid)


def _signal_group(pid: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGINT)


def _read_paths(path: Path) -> list[str]:
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return []
    out: list[str] = []
    for line in lines:
        for p in line.split()[1:]:
            if p != "-" and p not in out:
                out.append(p)
    return out


# -- release-tree helpers ----------------------------------------------------


@contextlib.contextmanager
def placeholder_credentials() -> Iterator[None]:
    """Unset credential variables get a placeholder while a config is loaded
    in-process (loading needs them; nothing here uses them)."""
    added = [v for v in CREDENTIAL_VARS if not os.environ.get(v)]
    for v in added:
        os.environ[v] = "placeholder"
    try:
        yield
    finally:
        for v in added:
            os.environ.pop(v, None)


def load_row_config(path: Path) -> Any:
    from lakebench.config import load_config
    from lakebench.config._load_context import LoadPurpose

    with placeholder_credentials():
        return load_config(path, purpose=LoadPurpose.INSPECT, print_notes=False)


def config_peak(cfg: Any) -> Peak:
    """The deployment's plan peak (``config.sizing.plan_requirements``):
    Spark, the co-resident engines and catalog, and datagen."""
    from lakebench.config.sizing import plan_requirements

    plan = plan_requirements(cfg)
    return Peak(float(plan.full.cpu_cores), float(plan.full.memory_gb))


def versions_problem(row: Row, cfg: Any) -> str | None:
    from lakebench.config.support import config_versions, matrix_versions_problem

    spark, version = config_versions(cfg)
    if not spark or not version:
        return f"{row.id}: the config names no Spark minor or table format version"
    return matrix_versions_problem(row.workload, row.mode, row.recipe, spark, version)


def confirmed_incarnation(config: Path, core_v1: Any) -> tuple[str | None, str]:
    """(``uid#nonce``, "") when the namespace carries the state's newest,
    confirmed nonce; (None, why) otherwise. Reads the state only through the
    release tree's ``read_state`` and ``current_incarnation``."""
    from lakebench.config.deploy_state import StateError, current_incarnation, read_state

    try:
        state = read_state(config)
    except StateError as e:
        return None, f"deploy state unreadable: {e}"
    if state is None or not state.nonces:
        return None, "no deploy state with a nonce beside the config"
    head = state.nonces[0]
    if head.status == "pending":
        return None, "the newest nonce is still pending (the deploy did not finish)"
    try:
        inc = current_incarnation(state, core_v1)
    except Exception as e:  # noqa: BLE001 -- a failed read is not a match
        return None, f"namespace read failed: {e}"
    if inc is None:
        return None, "the namespace is absent or carries no nonce this state recorded"
    if inc.rsplit("#", 1)[1] != head.nonce:
        return None, "the namespace carries an older recorded nonce, not the newest"
    return inc, ""


def deploy_state_present(config: Path) -> bool:
    from lakebench.config.deploy_state import StateError, read_state

    try:
        return read_state(config) is not None
    except StateError:
        return True  # unreadable: assume something was written


def _scrub_module() -> Any:
    path = TREE / "tests" / "fixtures" / "scrub.py"
    spec = importlib.util.spec_from_file_location("lb_release_scrub", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the scrubber at {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def record_verdict(record: dict[str, Any], freeze: str, version: str) -> list[str]:
    """``release_record.record_problems`` against this tree's expected results."""
    from lakebench.metrics.release_record import load_expected, record_problems

    expected_path = TREE / "uat" / f"expected-results-{version}.json"
    expected = load_expected(expected_path) if expected_path.is_file() else None
    return record_problems(record, freeze, expected, root=TREE)


# -- parallel-safety scenarios -------------------------------------------------


@dataclass(frozen=True)
class ScenarioSpec:
    """One S-P script and what the harness checks around it.

    ``kept``: configs whose namespace must still be present, with a
    confirmed incarnation, when the script ends. ``cleanup_refusals``: the
    exit-3 paths the harness's own destroy of a leftover may meet by design
    (a legacy or foreign bucket refused and left); the namespace must still
    be gone. ``legacy_bucket``: the harness creates an untagged bronze bucket
    for A with one object and deletes it afterwards. ``shared_bucket``: A
    and B name one bronze bucket, which A creates.
    """

    id: str
    script: str
    configs: tuple[str, ...]
    kept: tuple[str, ...] = ()
    cleanup_refusals: tuple[tuple[str, frozenset[str]], ...] = ()
    legacy_bucket: bool = False
    shared_bucket: bool = False

    def allowed(self, config: str) -> frozenset[str]:
        return dict(self.cleanup_refusals).get(config, frozenset())


SCENARIOS: dict[str, ScenarioSpec] = {
    s.id: s
    for s in (
        ScenarioSpec("S-P1", "s-p1-destroy-running.sh", ("A", "B"), kept=("B",)),
        ScenarioSpec("S-P2", "s-p2-concurrent-deploy.sh", ("A", "B"), kept=("A", "B")),
        ScenarioSpec("S-P3", "s-p3-destroy-during-deploy.sh", ("A", "B")),
        ScenarioSpec("S-P4", "s-p4-double-destroy.sh", ("A",)),
        ScenarioSpec(
            "S-P5",
            "s-p5-legacy-bucket-destroy.sh",
            ("A",),
            cleanup_refusals=(("A", frozenset({"deploy.identity_foreign"})),),
            legacy_bucket=True,
        ),
        ScenarioSpec(
            "S-P6",
            "s-p6-same-bucket-two-configs.sh",
            ("A", "B"),
            kept=("A",),
            cleanup_refusals=(("B", frozenset({"deploy.identity_foreign"})),),
            shared_bucket=True,
        ),
    )
}
#: Every scenario deployment is this row's composition (C360 batch s1).
SCENARIO_BASE = Row("base", "customer360", "batch", "hive-iceberg-spark-trino", 1.0, 42)
SHIM = """#!{python}
# The release harness's lakebench for scenario scripts: the release tree only.
import os
import runpy
import sys

sys.path.insert(0, {src!r})
os.environ["PYTHONPATH"] = {src!r}
if os.environ.get("LB_SHIM_WHICH"):
    import lakebench

    print(lakebench.__file__)
    sys.exit(0)
sys.argv[0] = "lakebench"
runpy.run_module("lakebench", run_name="__main__", alter_sys=True)
"""


def write_shim(tree: Path, bin_dir: Path) -> Path:
    bin_dir.mkdir(parents=True, exist_ok=True)
    shim = bin_dir / "lakebench"
    shim.write_text(SHIM.format(python=sys.executable, src=str(tree / "src")))
    shim.chmod(0o755)
    return shim


def shim_imports_from(bin_dir: Path, env: dict[str, str]) -> str:
    """``lakebench.__file__`` as the scenario scripts' ``lakebench`` sees it."""
    probe = dict(env, LB_SHIM_WHICH="1")
    found = shutil.which("lakebench", path=probe.get("PATH"))
    if found is None or Path(found).resolve() != (bin_dir / "lakebench").resolve():
        return f"(PATH resolves lakebench to {found}, not the shim)"
    out = subprocess.run([found], capture_output=True, text=True, env=probe, check=False)
    return out.stdout.strip() if out.returncode == 0 else f"(shim failed: {out.stderr.strip()})"


def exit_code_env() -> dict[str, str]:
    """``LB_EXIT_<NAME>=<code>`` for every exit code of the release tree."""
    from lakebench.exit_codes import ExitCode

    return {f"LB_EXIT_{c.name}": str(int(c)) for c in ExitCode}


def default_s3(config: Path) -> Any:
    """The release tree's S3 client for *config* (credentials from the env)."""
    from lakebench.config import load_config
    from lakebench.config._load_context import LoadPurpose
    from lakebench.s3 import S3Client

    cfg = load_config(config, purpose=LoadPurpose.INSPECT, print_notes=False)
    s3 = cfg.platform.storage.s3
    return S3Client(
        endpoint=s3.endpoint,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        path_style=s3.path_style,
        ca_cert=s3.ca_cert,
        verify_ssl=s3.verify_ssl,
    )


def run_script(argv: Sequence[str], *, env: dict[str, str], cwd: Path, log: Path) -> int:
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "ab") as out:
        proc = subprocess.Popen(  # noqa: S603 -- our own scenario script
            list(argv),
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            return proc.wait()
        except KeyboardInterrupt:
            _signal_group(proc.pid)
            return proc.wait()


@dataclass
class ScenarioRun:
    spec: ScenarioSpec
    plans: dict[str, RowPlan]
    buckets: dict[str, list[str]]
    legacy: str | None = None
    legacy_keys: list[str] = field(default_factory=list)
    shared: str | None = None


class ScenarioMixin:
    """The scenario half of the harness (``Harness.scenario``)."""

    def scenario_rows(self: Any, spec: ScenarioSpec) -> dict[str, RowPlan]:
        sdir = self.out / "scenarios" / spec.id
        tag = secrets.token_hex(3)
        plans: dict[str, RowPlan] = {}
        for c in spec.configs:
            row = Row(
                f"{spec.id}-{c}",
                SCENARIO_BASE.workload,
                SCENARIO_BASE.mode,
                SCENARIO_BASE.recipe,
                SCENARIO_BASE.scale,
                SCENARIO_BASE.seed,
            )
            name = f"rel17-{spec.id.lower()}-{c.lower()}-{tag}"
            plans[c] = self.plan_row(row, sdir / c, name)
        return plans

    def _set_buckets(self: Any, plan: RowPlan, bronze: str | None = None) -> list[str]:
        """Write explicit bucket names; returns the buckets the config owns."""
        data = yaml.safe_load(plan.config.read_text())
        name = data["name"]
        buckets = {k: f"{name}-{k}" for k in ("bronze", "silver", "gold")}
        if bronze:
            buckets["bronze"] = bronze
        s3 = data.setdefault("platform", {}).setdefault("storage", {}).setdefault("s3", {})
        s3["buckets"] = buckets
        plan.config.write_text(yaml.safe_dump(data, sort_keys=False))
        return [v for k, v in buckets.items() if not (bronze and k == "bronze")]

    def scenario(self: Any, spec: ScenarioSpec) -> int:
        existing = self.rowlog.latest()
        clash = [r for r in existing if r.startswith(f"{spec.id}-")]
        if clash:
            raise Refused(f"{spec.id} already ran in this --out ({', '.join(clash)})")
        plans = self.scenario_rows(spec)
        run = ScenarioRun(spec, plans, {})
        a = plans["A"]
        if spec.legacy_bucket:
            run.legacy = f"{a.namespace}-legacy"
        if spec.shared_bucket:
            run.shared = f"rel17-{spec.id.lower()}-shared-{secrets.token_hex(3)}"
        for c, plan in plans.items():
            bronze = run.legacy if c == "A" and run.legacy else run.shared
            run.buckets[c] = self._set_buckets(plan, bronze)
            if run.shared and c == "A":
                run.buckets[c].append(run.shared)
            self.log(
                plan.row.id,
                "planned",
                namespace=plan.namespace,
                config=str(plan.config),
                peak=[plan.peak.cores, plan.peak.gib],
                scenario=spec.id,
                owned_buckets=run.buckets[c],
            )
        self._admit_group(spec, plans)
        for plan in plans.values():
            self.ledger_add(plan)
            self.log(plan.row.id, "ledgered")
        problems: list[str] = []
        try:
            if run.legacy:
                problems += self._make_legacy_bucket(run)
            if not problems:
                problems += self._run_scenario_script(run)
        finally:
            problems += self._scenario_cleanup(run)
        verdict = "PASS" if not problems else "FAIL"
        self.log(
            f"{spec.id}-A",
            self.rowlog.latest()[f"{spec.id}-A"]["status"],
            scenario_verdict=verdict,
            scenario_problems=problems,
        )
        self.write_extra_results()
        for p in problems:
            self.say(f"{spec.id}: {p}")
        self.say(f"{spec.id}: {verdict}")
        return 0 if verdict == "PASS" else 1

    def _admit_group(self: Any, spec: ScenarioSpec, plans: dict[str, RowPlan]) -> None:
        peak = Peak(0.0, 0.0)
        for p in plans.values():
            peak = peak + p.peak
        group = RowPlan(
            Row(spec.id, "customer360", "batch", SCENARIO_BASE.recipe, 1.0, 42),
            plans["A"].namespace,
            plans["A"].config,
            peak,
        )
        last = -BLOCKING_REPORT_S
        while True:
            decision = self.decide(
                group, peak, size=len(plans), namespaces=tuple(p.namespace for p in plans.values())
            )
            if not isinstance(decision, Unknown) and decision.admit:
                return
            if self.stopping:
                raise Refused(f"{spec.id}: stopped before admission")
            reasons = [decision.reason] if isinstance(decision, Unknown) else decision.reasons
            if self.monotonic() - last >= BLOCKING_REPORT_S:
                self.say(f"{spec.id} waits: {'; '.join(reasons)}")
                last = self.monotonic()
            self.sleep(ADMIT_POLL_S)

    def _make_legacy_bucket(self: Any, run: ScenarioRun) -> list[str]:
        s3 = self.s3_factory(run.plans["A"].config)
        assert run.legacy is not None
        if s3.bucket_exists(run.legacy):
            return [f"legacy bucket {run.legacy} already exists; not touched"]
        if not s3.create_bucket(run.legacy):
            return [f"could not create the legacy bucket {run.legacy}"]
        key = "harness-legacy/object.txt"
        s3.raw_client.put_object(Bucket=run.legacy, Key=key, Body=b"legacy\n")
        run.legacy_keys = [key]
        self.log(f"{run.spec.id}-A", "ledgered", legacy_bucket=run.legacy, legacy_keys=[key])
        return []

    def _run_scenario_script(self: Any, run: ScenarioRun) -> list[str]:
        spec = run.spec
        sdir = self.out / "scenarios" / spec.id
        bin_dir = sdir / "bin"
        write_shim(self.tree, bin_dir)
        env = child_env(self.tree)
        env.update(exit_code_env())
        env["PATH"] = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
        env["LB_KUBE_CONTEXT"] = self.context
        env["LB_UAT_LOG_DIR"] = str(sdir / "logs")
        for c, plan in run.plans.items():
            env[f"LB_CONFIG_{c}"] = str(plan.config)
        if run.legacy:
            env["LB_LEGACY_BUCKET"] = run.legacy
        if run.shared:
            env["LB_SHARED_BUCKET"] = run.shared
        seen = shim_imports_from(bin_dir, env)
        if not _under(seen, self.tree / "src" / "lakebench"):
            return [f"the scenario shim imports lakebench from {seen}, not the release tree"]
        script = self.tree / "scripts" / "release" / "scenarios" / spec.script
        log = sdir / "script.log"
        runner = self.script_runner or run_script
        rc = runner(["bash", str(script)], env=env, cwd=sdir, log=log)
        problems = []
        text = log.read_text(errors="replace") if log.exists() else ""
        if rc != 0:
            problems.append(f"script exited {rc}")
        elif f"PASS: {spec.id}" not in text:
            problems.append("script exited 0 without its PASS line")
        self.log(f"{spec.id}-A", "ledgered", script_rc=rc)
        return problems

    def _scenario_incarnation(self: Any, config: Path) -> tuple[str | None, str]:
        """``uid#nonce`` when the namespace carries any confirmed nonce the
        config's state kept (a script may deploy the same config twice)."""
        from lakebench.config.deploy_state import StateError, current_incarnation, read_state

        try:
            state = read_state(config)
        except StateError as e:
            return None, f"deploy state unreadable: {e}"
        if state is None or not state.nonces:
            return None, "no deploy state with a nonce"
        try:
            inc = current_incarnation(state, self.cluster.core_v1)
        except Exception as e:  # noqa: BLE001
            return None, f"namespace read failed: {e}"
        if inc is None:
            return None, "the namespace is absent or carries no nonce this state recorded"
        nonce = inc.rsplit("#", 1)[1]
        if any(e.nonce == nonce and e.status == "pending" for e in state.nonces):
            return None, "the namespace's nonce is still pending"
        return inc, ""

    def _scenario_cleanup(self: Any, run: ScenarioRun, check_kept: bool = True) -> list[str]:
        """Check what the script left, then destroy every leftover by
        incarnation (B before A); never by name, never forced."""
        spec = run.spec
        problems: list[str] = []
        order = [c for c in reversed(spec.configs) if c in run.plans]
        for c in order:
            plan = run.plans[c]
            try:
                present = self.cluster.namespace_exists(plan.namespace)
            except Exception as e:  # noqa: BLE001
                problems.append(f"{c}: namespace unreadable ({e}); not destroyed")
                self.log(plan.row.id, "failed", detail=f"namespace unreadable: {e}")
                continue
            inc, why = self._scenario_incarnation(plan.config) if present else (None, "absent")
            if check_kept and c in spec.kept and inc is None:
                problems.append(
                    f"{c}: expected present with its incarnation after the script ({why})"
                )
            if not present:
                problems += self._close_if_buckets_gone(run, c, "gone after the script")
                continue
            if inc is None:
                problems.append(f"{c}: present without a confirmed incarnation ({why}); left")
                self.log(plan.row.id, "left", detail=f"present after the script: {why}")
                continue
            self.log(plan.row.id, "deployed", incarnation=inc)
            res = self.runner(
                ["destroy", str(plan.config), "--yes", "--expect-incarnation", inc],
                cwd=plan.config.parent,
                log=self.log_path(plan.row, "destroy"),
                on_spawn=self._spawn_logger(plan, "destroying"),
            )
            from lakebench.exit_codes import ExitCode

            allowed = spec.allowed(c)
            refused_by_design = (
                res.code == ExitCode.REFUSED and res.paths and set(res.paths) <= allowed
            )
            if res.code == ExitCode.OK or refused_by_design:
                try:
                    gone = not self.cluster.namespace_exists(plan.namespace)
                except Exception:  # noqa: BLE001
                    gone = False
                if gone:
                    problems += self._close_if_buckets_gone(
                        run,
                        c,
                        f"destroyed by the harness (exit {res.code} {res.paths or ''})".strip(),
                    )
                    continue
            if res.code == ExitCode.INCOMPLETE:
                self.poll_gone(plan, "scenario cleanup destroy exited 6")
                if self.rowlog.latest()[plan.row.id]["status"] != "destroyed":
                    problems.append(f"{c}: namespace still terminating after cleanup")
                continue
            self.classify_destroy(plan, res)
            problems.append(f"{c}: cleanup destroy exited {res.code} ({', '.join(res.paths)})")
        if run.legacy and "A" in run.plans:
            problems += self._remove_legacy_bucket(run)
        return problems

    def _close_if_buckets_gone(self: Any, run: ScenarioRun, c: str, why: str) -> list[str]:
        plan = run.plans[c]
        try:
            s3 = self.s3_factory(plan.config)
            left = [b for b in run.buckets[c] if s3.bucket_exists(b)]
        except Exception as e:  # noqa: BLE001
            self.log(plan.row.id, "left", detail=f"{why}; buckets unreadable ({e})")
            return [f"{c}: buckets unreadable after the namespace went ({e})"]
        if left:
            self.log(plan.row.id, "left", detail=f"{why}; buckets remain: {', '.join(left)}")
            return [f"{c}: buckets remain after the namespace went: {', '.join(left)}"]
        self.log(plan.row.id, "destroyed", detail=why)
        self.ledger_close(plan)
        return []

    def _remove_legacy_bucket(self: Any, run: ScenarioRun) -> list[str]:
        """The legacy bucket is the harness's own: check it was left as it
        was made, then empty and delete it."""
        assert run.legacy is not None
        problems: list[str] = []
        s3 = self.s3_factory(run.plans["A"].config)
        if not s3.bucket_exists(run.legacy):
            return [f"legacy bucket {run.legacy} is gone: something deleted it"]
        listing = sorted(
            o["Key"]
            for page in s3.raw_client.get_paginator("list_objects_v2").paginate(Bucket=run.legacy)
            for o in page.get("Contents", [])
        )
        if listing != sorted(run.legacy_keys):
            problems.append(f"legacy bucket {run.legacy} changed: {listing}")
        s3.empty_bucket(run.legacy, keep_prefixes=())
        if not s3.delete_bucket(run.legacy):
            problems.append(f"could not delete the harness's legacy bucket {run.legacy}")
        self.log(
            f"{run.spec.id}-A",
            self.rowlog.latest()[f"{run.spec.id}-A"]["status"],
            legacy_bucket_removed=not problems,
        )
        return problems

    def write_extra_results(self: Any) -> Path:
        """``results-extra.md``: scenario (and upgrade) runs, which are not
        release-matrix evidence and stay out of results.md."""
        states = self.rowlog.latest()
        lines = [
            f"# Parallel-safety and upgrade runs {self.version}",
            "",
            f"Freeze commit: {self.freeze}",
            "",
            "| run | deployments | verdict | problems |",
            "|---|---|---|---|",
        ]
        for rid, s in states.items():
            if "scenario_verdict" not in s:
                continue
            spec_id = s.get("scenario") or rid.rsplit("-", 1)[0]
            ns = [
                st.get("namespace", "?") for r, st in states.items() if r.startswith(f"{spec_id}-")
            ]
            problems = "; ".join(s.get("scenario_problems") or []) or "-"
            lines.append(f"| {spec_id} | {', '.join(ns)} | {s['scenario_verdict']} | {problems} |")
        path = self.out / "results-extra.md"
        path.write_text("\n".join(lines) + "\n")
        return path


# -- the harness -------------------------------------------------------------


@dataclass
class RowPlan:
    row: Row
    namespace: str
    config: Path
    peak: Peak


@dataclass
class Harness(ScenarioMixin):
    tree: Path
    out: Path
    freeze: str
    version: str
    rehearsal: bool
    context: str
    runner: Runner
    cluster: Any
    rowlog: RowLog
    ledger: MarkdownLedger
    slots: int = 3
    s3_endpoint: str = PLACEHOLDER_ENDPOINT
    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic
    say: Callable[[str], None] = print
    rows: dict[str, Row] = field(default_factory=dict)
    script_runner: Callable[..., int] | None = None
    s3_factory: Callable[[Path], Any] | None = None
    _log_lock: threading.Lock = field(default_factory=threading.Lock)
    _stop: threading.Event = field(default_factory=threading.Event)
    _active: dict[str, RowPlan] = field(default_factory=dict)
    _ledger_peaks: dict[str, Peak | None] = field(default_factory=dict)

    # -- bookkeeping --

    def log(self, row: str, status: str, **fields: Any) -> dict[str, Any]:
        with self._log_lock:
            return self.rowlog.append(
                row, status, freeze_sha=self.freeze, rehearsal=self.rehearsal, **fields
            )

    def stop_admission(self, why: str) -> None:
        if not self._stop.is_set():
            self.say(f"admission stopped: {why}")
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def row_dir(self, row: Row) -> Path:
        return self.out / "configs" / row.id

    def log_path(self, row: Row, step: str) -> Path:
        return self.out / "logs" / row.id / f"{step}.log"

    # -- config --

    def write_config(self, row: Row, row_dir: Path, name: str | None = None) -> Path:
        """``lakebench init`` for the row, then its keys; returns the path."""
        row_dir.mkdir(parents=True, exist_ok=True)
        cfg = row_dir / f"{row.id}.yaml"
        if cfg.exists():
            raise Refused(f"{cfg} exists; this --out already holds row {row.id} (use resume)")
        name = name or f"rel17-{row.id.lower()}-{secrets.token_hex(3)}"
        res = self.runner(
            [
                "init",
                "-r",
                row.recipe,
                "-w",
                row.workload,
                "-n",
                name,
                "-s",
                f"{row.scale:g}",
                "--endpoint",
                self.s3_endpoint,
                "-o",
                str(cfg),
            ],
            cwd=row_dir,
            log=self.log_path(row, "init"),
        )
        if res.code != 0 or not cfg.is_file():
            raise Refused(f"{row.id}: lakebench init exited {res.code} (see {res.log})")
        data = yaml.safe_load(cfg.read_text()) or {}
        data.setdefault("platform", {}).setdefault("kubernetes", {})["context"] = self.context
        data.setdefault("workload", {}).setdefault("datagen", {})["seed"] = row.seed
        if row.mode != "batch":
            data.setdefault("architecture", {}).setdefault("pipeline", {})["mode"] = row.mode
        cfg.write_text(yaml.safe_dump(data, sort_keys=False))
        return cfg

    def plan_row(self, row: Row, row_dir: Path, name: str | None = None) -> RowPlan:
        cfg_path = self.write_config(row, row_dir, name)
        cfg = load_row_config(cfg_path)
        problem = versions_problem(row, cfg)
        if problem:
            raise Refused(f"{row.id}: {problem}")
        return RowPlan(row, cfg.get_namespace(), cfg_path, config_peak(cfg))

    # -- admission --

    def _ledger_peak(self, row: LedgerRow) -> Peak | None:
        if row.namespace not in self._ledger_peaks:
            peak: Peak | None = None
            path = Path(row.config)
            if path.is_file():
                try:
                    peak = config_peak(load_row_config(path))
                except Exception:  # noqa: BLE001 -- unloadable: the fallback peak counts
                    peak = None
            self._ledger_peaks[row.namespace] = peak
        return self._ledger_peaks[row.namespace]

    def decide(
        self,
        plan: RowPlan,
        fallback: Peak,
        *,
        size: int = 1,
        namespaces: tuple[str, ...] = (),
    ) -> Any:
        try:
            live_rows = self.ledger.live_rows()
        except (OSError, LedgerError) as e:
            return Unknown(f"ledger unreadable: {e}")
        try:
            managed = self.cluster.managed_namespaces()
        except Exception as e:  # noqa: BLE001 -- fail closed
            return Unknown(f"namespace list failed: {e}")
        snapshot: Snapshot | Unknown = self.cluster.snapshot()
        own = [
            ActiveRow(p.namespace, p.peak, p.row.alone, p.row.aml_continuous)
            for p in self._active.values()
        ]
        return admit(
            Candidate(
                plan.row.id,
                plan.namespace,
                plan.peak,
                plan.row.alone,
                plan.row.aml_continuous,
                size=size,
                namespaces=namespaces,
            ),
            snapshot,
            managed=managed,
            ledger_live={r.namespace for r in live_rows},
            own_active=own,
            ledger_peaks={r.namespace: self._ledger_peak(r) for r in live_rows},
            fallback_peak=fallback,
            slots=self.slots,
        )

    # -- the row lifecycle --

    def ledger_add(self, plan: RowPlan) -> None:
        self.ledger.add(
            LedgerRow(
                plan.namespace,
                str(plan.config),
                f"release-harness {self.freeze[:8]} row {plan.row.id}",
                f"{plan.row.scale:g}",
                utc_now(),
            )
        )

    def ledger_close(self, plan: RowPlan) -> None:
        try:
            self.ledger.close(plan.namespace)
        except (OSError, LedgerError) as e:
            self.say(f"{plan.row.id}: could not close the ledger row for {plan.namespace}: {e}")

    def _spawn_logger(
        self, plan: RowPlan, status: str, **fields: Any
    ) -> Callable[[int, str], None]:
        def on_spawn(pid: int, start: str) -> None:
            self.log(plan.row.id, status, child_pid=pid, child_start=start, **fields)

        return on_spawn

    def deploy(self, plan: RowPlan) -> None:
        """Ledger row, deploy, incarnation; then the run and the destroy."""
        row = plan.row
        try:
            self.ledger_add(plan)
        except (OSError, LedgerError) as e:
            self.log(row.id, "not-deployed", detail=f"ledger row not written: {e}")
            self.stop_admission(f"{row.id}: ledger write failed")
            return
        self.log(row.id, "ledgered", namespace=plan.namespace, config=str(plan.config))
        res = self.runner(
            ["deploy", str(plan.config), "--yes", "--require-new"],
            cwd=plan.config.parent,
            log=self.log_path(row, "deploy"),
            on_spawn=self._spawn_logger(plan, "deploying"),
        )
        if res.code != 0:
            self.resolve_failed_deploy(plan, f"deploy exited {res.code} {res.paths or ''}".strip())
            return
        inc, why = confirmed_incarnation(plan.config, self.cluster.core_v1)
        if inc is None:
            self.log(row.id, "failed", detail=f"deployed, but no confirmed incarnation: {why}")
            self.say(f"{row.id}: {plan.namespace} left for a human: {why}")
            return
        self.log(row.id, "deployed", incarnation=inc)
        if self.stopping:
            return
        self.run_and_destroy(plan, inc)

    def resolve_failed_deploy(self, plan: RowPlan, why: str) -> None:
        """A failed or interrupted deploy is never retried."""
        row = plan.row
        try:
            exists = self.cluster.namespace_exists(plan.namespace)
        except Exception as e:  # noqa: BLE001
            self.log(row.id, "failed", detail=f"{why}; namespace unreadable ({e}); not destroyed")
            self.stop_admission(f"{row.id}: namespace unreadable")
            return
        if not exists and not deploy_state_present(plan.config):
            self.log(row.id, "not-deployed", detail=f"{why}; nothing was created")
            self.ledger_close(plan)
            return
        inc, reason = confirmed_incarnation(plan.config, self.cluster.core_v1)
        if inc is None:
            self.log(row.id, "left", detail=f"{why}; not destroyed by the harness: {reason}")
            self.say(f"{row.id}: {plan.namespace} left for a human: {reason}")
            return
        self.log(row.id, "deployed", incarnation=inc, detail=why, verdict="FAIL")
        self.destroy(plan, inc)

    def run_dirs(self, plan: RowPlan) -> set[str]:
        runs = plan.config.parent / "lakebench-output" / "runs"
        return {p.name for p in runs.iterdir() if p.is_dir()} if runs.is_dir() else set()

    def run_and_destroy(self, plan: RowPlan, inc: str) -> None:
        row = plan.row
        before = sorted(self.run_dirs(plan))
        res = self.runner(
            ["run", str(plan.config), "--generate", "--yes"],
            cwd=plan.config.parent,
            log=self.log_path(row, "run"),
            interruptible=True,
            on_spawn=self._spawn_logger(plan, "running", runs_before=before),
        )
        self.finish_run(plan, inc, set(before), res.code)

    def finish_run(self, plan: RowPlan, inc: str, before: set[str], run_code: int | None) -> None:
        row = plan.row
        run_ids = sorted(self.run_dirs(plan) - before)
        problems: list[str] = []
        if run_code is None:
            problems.append("the harness stopped during the run; the run was not re-run")
        elif run_code != 0:
            problems.append(f"run exited {run_code}")
        if len(run_ids) != 1:
            problems.append(f"expected one run record, found {len(run_ids)}")
        for rid in run_ids:
            problems += self.collect(plan, rid)
        rep = self.runner(
            ["report", str(plan.config)], cwd=plan.config.parent, log=self.log_path(row, "report")
        )
        if rep.code != 0:
            problems.append(f"report exited {rep.code}")
        verdict = "PASS" if not problems else "FAIL"
        self.log(row.id, "recorded", run_ids=run_ids, verdict=verdict, problems=problems)
        if self.stopping:
            return
        self.destroy(plan, inc)

    def collect(self, plan: RowPlan, run_id: str) -> list[str]:
        """Scrub the record into ``<out>/uat/runs/``; its release problems."""
        src = plan.config.parent / "lakebench-output" / "runs" / run_id / "metrics.json"
        try:
            record = json.loads(src.read_text())
        except (OSError, ValueError) as e:
            return [f"{run_id}: record unreadable: {e}"]
        problems = [f"{run_id}: {p}" for p in record_verdict(record, self.freeze, self.version)]
        try:
            scrub = _scrub_module()
            clean, refusals = scrub.scrub_record(record)
        except Exception as e:  # noqa: BLE001 -- a scrub failure keeps the record out
            return [*problems, f"{run_id}: scrub failed: {e}"]
        if refusals:
            return [*problems, *(f"{run_id}: scrub refused: {r}" for r in refusals)]
        dest_root = self.out / ("rehearsal" if self.rehearsal else "uat") / "runs" / run_id
        dest_root.mkdir(parents=True, exist_ok=True)
        (dest_root / "metrics.json").write_text(scrub.dump(clean))
        return problems

    def destroy(self, plan: RowPlan, inc: str) -> None:
        """Destroy by incarnation; never ``--force``, never re-invoked."""
        row = plan.row
        try:
            exists = self.cluster.namespace_exists(plan.namespace)
        except Exception as e:  # noqa: BLE001
            self.log(row.id, "failed", detail=f"namespace unreadable before destroy: {e}")
            self.stop_admission(f"{row.id}: namespace unreadable")
            return
        if not exists:
            self.log(
                row.id,
                "left",
                detail="namespace gone; buckets may remain; not destroyed by the harness",
            )
            return
        res = self.runner(
            ["destroy", str(plan.config), "--yes", "--expect-incarnation", inc],
            cwd=plan.config.parent,
            log=self.log_path(row, "destroy"),
            on_spawn=self._spawn_logger(plan, "destroying"),
        )
        self.classify_destroy(plan, res)

    def classify_destroy(self, plan: RowPlan, res: ChildResult) -> None:
        from lakebench.exit_codes import ExitCode

        row = plan.row
        not_completed = "Destroy NOT completed" in res.text()
        if res.code == ExitCode.OK and not not_completed:
            try:
                gone = not self.cluster.namespace_exists(plan.namespace)
            except Exception:  # noqa: BLE001
                gone = False
            if gone:
                self.log(row.id, "destroyed", destroy_paths=res.paths)
                self.ledger_close(plan)
                return
            self.log(row.id, "failed", detail="destroy exited 0 but the namespace is present")
            self.stop_admission(f"{row.id}: destroy exited 0 with the namespace present")
            return
        if res.code == ExitCode.INCOMPLETE:
            self.poll_gone(plan, "destroy exited 6 (incomplete, still terminating)")
            return
        if res.code == ExitCode.REFUSED and "destroy.incarnation_mismatch" in res.paths:
            try:
                gone = not self.cluster.namespace_exists(plan.namespace)
            except Exception:  # noqa: BLE001
                gone = False
            if gone:
                self.log(row.id, "left", detail="namespace gone before destroy; buckets may remain")
                return
            self.log(
                row.id,
                "destroy-refused",
                detail="the namespace is not the incarnation this row recorded",
                destroy_paths=res.paths,
            )
            self.stop_admission(f"{row.id}: destroy refused on an incarnation mismatch")
            return
        detail = f"destroy exited {res.code}"
        if res.paths:
            detail += f" ({', '.join(res.paths)})"
        if not_completed:
            detail += "; Destroy NOT completed"
        self.log(row.id, "failed", detail=detail, destroy_paths=res.paths)
        self.stop_admission(f"{row.id}: {detail}")

    def poll_gone(self, plan: RowPlan, why: str) -> None:
        """Read-only polls until the namespace is gone; never re-invokes destroy."""
        row = plan.row
        self.log(row.id, "destroying", detail=why)
        deadline = self.monotonic() + DESTROY_POLL_LIMIT_S
        while True:
            try:
                if not self.cluster.namespace_exists(plan.namespace):
                    self.log(row.id, "destroyed", detail=f"{why}; namespace gone on a later read")
                    self.ledger_close(plan)
                    return
            except Exception:  # noqa: BLE001 -- keep polling until the deadline
                pass
            if self.monotonic() >= deadline:
                break
            self.sleep(DESTROY_POLL_S)
        self.log(row.id, "failed", detail=f"{why}; namespace still present after 20 min")
        self.stop_admission(f"{row.id}: namespace still terminating after 20 min")

    # -- scheduling --

    def schedule(self, pending: list[RowPlan], continuing: list[Callable[[], None]]) -> None:
        """Admit *pending* rows as capacity allows; run *continuing* steps
        (resumed rows already deployed) at once. Returns when every thread
        has finished or admission stopped with nothing in flight."""
        threads: dict[str, threading.Thread] = {}
        fallback = Peak(0.0, 0.0)
        for p in pending:
            fallback = fallback.max(p.peak)
        for i, fn in enumerate(continuing):
            t = threading.Thread(target=fn, name=f"resume-{i}", daemon=False)
            t.start()
            threads[f"resume-{i}"] = t
        last_report = -BLOCKING_REPORT_S
        queue = list(pending)
        while True:
            for key in [k for k, t in threads.items() if not t.is_alive()]:
                threads.pop(key).join()
                self._active.pop(key, None)
            if self.stopping:
                queue.clear()
            if not queue and not threads:
                return
            admitted_any = False
            for plan in list(queue):
                if plan.row.alone and plan is not queue[0]:
                    continue
                decision = self.decide(plan, fallback)
                if isinstance(decision, Unknown):
                    reasons, blocking = [decision.reason], []
                    ok = False
                else:
                    reasons, blocking, ok = decision.reasons, decision.blocking, decision.admit
                if ok:
                    queue.remove(plan)
                    self._active[plan.row.id] = plan
                    t = threading.Thread(
                        target=self._guarded, args=(plan,), name=plan.row.id, daemon=False
                    )
                    t.start()
                    threads[plan.row.id] = t
                    admitted_any = True
                    break
                if self.monotonic() - last_report >= BLOCKING_REPORT_S:
                    self.say(
                        f"{plan.row.id} waits: {'; '.join(reasons)}"
                        + (f" (blocking: {', '.join(blocking)})" if blocking else "")
                    )
                    last_report = self.monotonic()
                if queue and plan is queue[0] and plan.row.alone:
                    break
            if not admitted_any:
                self.sleep(ADMIT_POLL_S if queue else 5)

    def _guarded(self, plan: RowPlan) -> None:
        try:
            self.deploy(plan)
        except Exception as e:  # noqa: BLE001 -- one row's crash stops admission, not the harness
            self.log(plan.row.id, "failed", detail=f"harness error: {type(e).__name__}: {e}")
            self.stop_admission(f"{plan.row.id}: harness error {e}")

    # -- entry points --

    def run(self, rows: Sequence[Row]) -> int:
        existing = self.rowlog.latest()
        clash = [r.id for r in rows if r.id in existing]
        if clash:
            raise Refused(f"rows already in {self.rowlog.path}: {', '.join(clash)} (use resume)")
        plans = []
        for row in rows:
            plan = self.plan_row(row, self.row_dir(row))
            self.log(
                row.id,
                "planned",
                namespace=plan.namespace,
                config=str(plan.config),
                peak=[plan.peak.cores, plan.peak.gib],
                alone=row.alone,
                aml_continuous=row.aml_continuous,
            )
            plans.append(plan)
        self.schedule(plans, [])
        return self.finish()

    def finish(self) -> int:
        path = self.write_results()
        states = self.rowlog.latest()
        open_rows = [r for r, s in states.items() if s["status"] not in TERMINAL]
        self.say(f"results: {path}")
        if open_rows:
            self.say(
                f"rows not finished: {', '.join(open_rows)}; resume with: python3.11 "
                f"{HERE / 'harness.py'} resume --out {self.out} --context {self.context} "
                f"--deployments-ledger {self.ledger.path}"
            )
        bad = [
            r
            for r, s in states.items()
            if r in self.rows and (s.get("verdict") != "PASS" or s["status"] != "destroyed")
        ]
        return 0 if not bad and not open_rows else 1

    def write_results(self) -> Path:
        states = self.rowlog.latest()
        name = "results-rehearsal.md" if self.rehearsal else "results.md"
        lines = [
            f"# UAT results {self.version}",
            "",
            f"Freeze commit: {self.freeze}",
            "",
        ]
        if self.rehearsal:
            lines += ["Rehearsal: these runs are not release evidence.", ""]
        lines += [
            "| row | workload | mode | recipe | Spark / format | scale | run id | verdict | group check |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        from lakebench.metrics.release_record import RELEASE_MATRIX_VERSIONS

        for rid, s in states.items():
            row = self.rows.get(rid)
            if row is None:
                continue
            spark, fmt = RELEASE_MATRIX_VERSIONS.get(
                (row.workload, row.mode, row.recipe), ("?", "?")
            )
            run_ids = ", ".join(r.removeprefix("run-") for r in s.get("run_ids") or []) or "-"
            verdict = s.get("verdict") or "-"
            if s["status"] != "destroyed" and verdict == "PASS":
                verdict = f"PASS ({s['status']})"
            lines.append(
                f"| {rid} | {row.workload} | {row.mode} | {row.recipe} | {spark} / {fmt} | "
                f"{row.scale:g} | {run_ids} | {verdict} | - |"
            )
        path = self.out / name
        path.write_text("\n".join(lines) + "\n")
        return path


def _rebuild_plan(row: Row, state: dict[str, Any]) -> RowPlan:
    peak = state.get("peak") or [0.0, 0.0]
    return RowPlan(row, str(state["namespace"]), Path(state["config"]), Peak(*map(float, peak)))


def resume(h: Harness, rows: dict[str, Row]) -> int:
    """Continue every row of ``rows.jsonl`` that is not terminal; never
    re-deploy, never re-run, never re-invoke a destroy."""
    states = h.rowlog.latest()
    alive = [
        r
        for r, s in states.items()
        if s["status"] not in TERMINAL and child_alive(s.get("child_pid"), s.get("child_start"))
    ]
    if alive:
        raise Refused(
            f"rows with a live child process: {', '.join(alive)}; wait for them or stop them first"
        )
    pending: list[RowPlan] = []
    continuing: list[Callable[[], None]] = []
    for rid, s in states.items():
        status = s["status"]
        if status in TERMINAL:
            continue
        if s.get("scenario") in SCENARIOS:
            continue  # resumed below, one cleanup per scenario
        row = rows.get(rid)
        if row is None:
            raise Refused(f"row {rid} of {h.rowlog.path} is not in the matrix")
        plan = _rebuild_plan(row, s)
        inc = s.get("incarnation")
        if status == "planned":
            pending.append(plan)
        elif status in ("ledgered", "deploying"):
            h._active[rid] = plan
            continuing.append(
                _bind(
                    h,
                    rid,
                    lambda p=plan: h.resolve_failed_deploy(p, "harness stopped during deploy"),
                )
            )
        elif status == "deployed" and inc:
            h._active[rid] = plan
            continuing.append(_bind(h, rid, lambda p=plan, i=inc: h.run_and_destroy(p, i)))
        elif status == "running" and inc:
            before = set(s.get("runs_before") or [])
            h._active[rid] = plan
            continuing.append(
                _bind(h, rid, lambda p=plan, i=inc, b=before: h.finish_run(p, i, b, None))
            )
        elif status == "recorded" and inc:
            h._active[rid] = plan
            continuing.append(_bind(h, rid, lambda p=plan, i=inc: h.destroy(p, i)))
        elif status == "destroying":
            h._active[rid] = plan
            continuing.append(
                _bind(h, rid, lambda p=plan: h.poll_gone(p, "harness stopped during destroy"))
            )
        else:
            h.log(rid, "failed", detail=f"cannot resume from {status} without an incarnation")
    for spec_id in dict.fromkeys(
        st["scenario"]
        for st in states.values()
        if st.get("scenario") in SCENARIOS and st["status"] not in TERMINAL
    ):
        continuing.append(_bind(h, spec_id, lambda sid=spec_id: _resume_scenario(h, sid, states)))
    h.schedule(pending, continuing)
    return h.finish()


def _resume_scenario(h: Harness, spec_id: str, states: dict[str, dict[str, Any]]) -> None:
    """Clean up a scenario the harness stopped in: destroy each leftover
    by incarnation, remove the harness's legacy bucket; never re-run it."""
    spec = SCENARIOS[spec_id]
    run = ScenarioRun(spec, {}, {})
    for c in spec.configs:
        st = states.get(f"{spec_id}-{c}")
        if st is None:
            continue
        if c == "A" and st.get("legacy_bucket") and not st.get("legacy_bucket_removed"):
            run.legacy = st["legacy_bucket"]
            run.legacy_keys = list(st.get("legacy_keys") or [])
        if st["status"] in TERMINAL and not (c == "A" and run.legacy):
            continue
        row = Row(f"{spec_id}-{c}", *astuple_base())
        run.plans[c] = _rebuild_plan(row, st)
        run.buckets[c] = list(st.get("owned_buckets") or [])
    problems = h._scenario_cleanup(run, check_kept=False)
    h.log(
        f"{spec_id}-A",
        h.rowlog.latest()[f"{spec_id}-A"]["status"],
        scenario_verdict="FAIL",
        scenario_problems=["the harness stopped during the scenario", *problems],
    )
    h.write_extra_results()


def astuple_base() -> tuple[str, str, str, float, int]:
    b = SCENARIO_BASE
    return (b.workload, b.mode, b.recipe, b.scale, b.seed)


def _bind(h: Harness, rid: str, fn: Callable[[], None]) -> Callable[[], None]:
    def go() -> None:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            h.log(rid, "failed", detail=f"harness error on resume: {type(e).__name__}: {e}")
            h.stop_admission(f"{rid}: harness error {e}")
        finally:
            h._active.pop(rid, None)

    return go


# -- CLI ---------------------------------------------------------------------


def _core_v1(context: str) -> Any:
    from kubernetes import client as kclient
    from kubernetes import config as kconfig

    api = kconfig.new_client_from_config(context=context)
    return kclient.CoreV1Api(api)


def _select(rows: list[Row], spec: str | None) -> list[Row]:
    if not spec:
        return rows
    want = [s.strip() for s in spec.split(",") if s.strip()]
    by_id = {r.id: r for r in rows}
    missing = [w for w in want if w not in by_id]
    if missing:
        raise Refused(f"--rows names unknown rows: {', '.join(missing)}")
    return [by_id[w] for w in want]


def _ensure_tree_imports() -> None:
    src = str(TREE / "src")
    if sys.path[0] != src:
        sys.path.insert(0, src)
    for name in [m for m in sys.modules if m == "lakebench" or m.startswith("lakebench.")]:
        del sys.modules[name]


KNOWN_STEPS: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="harness.py", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    plan = sub.add_parser("plan", help="print each row's config check and peak; no cluster")
    plan.add_argument("--matrix", type=Path, required=True)
    plan.add_argument("--freeze", required=True)
    plan.add_argument("--rows")
    for name in ("run", "resume", "scenario"):
        sp = sub.add_parser(name)
        if name == "scenario":
            sp.add_argument("scenario_id", choices=sorted(SCENARIOS))
            sp.add_argument("--freeze", required=True)
            sp.add_argument("--rehearsal", action="store_true")
            sp.add_argument("--matrix", type=Path, default=HERE / "matrix-1.7.yaml")
        elif name == "run":
            sp.add_argument("--matrix", type=Path, required=True)
            sp.add_argument("--freeze", required=True)
            sp.add_argument("--rows")
            sp.add_argument("--slots", type=int, default=3)
            sp.add_argument("--rehearsal", action="store_true")
        else:
            sp.add_argument("--matrix", type=Path, default=HERE / "matrix-1.7.yaml")
        sp.add_argument("--context", required=True)
        sp.add_argument("--out", type=Path, required=True)
        sp.add_argument("--deployments-ledger", type=Path, required=True)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _ensure_tree_imports()
    try:
        return _main(args)
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return 2


def _main(args: argparse.Namespace) -> int:
    version, rows = load_matrix(args.matrix, KNOWN_STEPS)
    problems = matrix_problems(rows)
    if problems:
        raise Refused("; ".join(problems))
    if args.cmd == "plan":
        reasons = refuse_reasons(
            TREE,
            freeze=args.freeze,
            rehearsal=True,
            out=None,
            live=False,
            ledger=None,
            context=None,
        )
        for r in reasons:
            print(f"note: {r}")
        return _plan(rows, args, version)
    out: Path = args.out.expanduser().resolve()
    rowlog = RowLog(out)
    if args.cmd == "resume":
        prior = rowlog.latest()
        if not prior:
            raise Refused(f"no rows in {rowlog.path}")
        freeze = str(next(iter(prior.values())).get("freeze_sha"))
        rehearsal = bool(next(iter(prior.values())).get("rehearsal"))
        slots = 3
    else:
        freeze, rehearsal = args.freeze, args.rehearsal
        slots = getattr(args, "slots", 3)
    reasons = refuse_reasons(
        TREE,
        freeze=freeze,
        rehearsal=rehearsal,
        out=out,
        live=True,
        ledger=args.deployments_ledger,
        context=args.context,
        contexts=kube_contexts,
    )
    if reasons:
        raise Refused("\n  ".join(["the harness does not start:", *reasons]))
    try:
        rowlog.acquire()
    except LedgerError as e:
        raise Refused(str(e)) from e
    runner = ProcessRunner(TREE)
    cluster = ClusterReader(_core_v1(args.context))
    h = Harness(
        tree=TREE,
        out=out,
        freeze=freeze,
        version=version,
        rehearsal=rehearsal,
        context=args.context,
        runner=runner,
        cluster=cluster,
        rowlog=rowlog,
        ledger=MarkdownLedger(args.deployments_ledger, out / "ledger-backups"),
        slots=slots,
        s3_endpoint=os.environ["LB_S3_ENDPOINT"],
        s3_factory=default_s3,
    )
    h.rows = {r.id: r for r in rows}

    def on_sigint(_signum: int, _frame: Any) -> None:
        h.stop_admission("interrupted (SIGINT); running lakebench runs get one SIGINT")
        runner.interrupt()

    signal.signal(signal.SIGINT, on_sigint)
    try:
        if args.cmd == "resume":
            return resume(h, {r.id: r for r in rows})
        if args.cmd == "scenario":
            return h.scenario(SCENARIOS[args.scenario_id])
        return h.run(_select(rows, args.rows))
    finally:
        rowlog.release()


def _plan(rows: list[Row], args: argparse.Namespace, version: str) -> int:
    selected = _select(rows, args.rows)
    missing = missing_matrix_rows(rows)
    print(f"matrix {args.matrix} version {version}: {len(rows)} rows")
    if missing:
        print(f"note: release-matrix rows not in this file: {missing}")
    work = Path(tempfile.mkdtemp(prefix="harness-plan-"))
    status = 0
    try:

        def run_init(
            argv: Sequence[str],
            *,
            cwd: Path,
            log: Path,
            interruptible: bool = False,
            on_spawn: Callable[[int, str], None] | None = None,
        ) -> ChildResult:
            log.parent.mkdir(parents=True, exist_ok=True)
            with open(log, "wb") as fh:
                code = subprocess.run(
                    [sys.executable, "-m", "lakebench", *argv],
                    cwd=str(cwd),
                    env=child_env(TREE),
                    stdout=fh,
                    stderr=subprocess.STDOUT,
                    check=False,
                ).returncode
            return ChildResult(code, [], log)

        h = Harness(
            tree=TREE,
            out=work,
            freeze=args.freeze,
            version=version,
            rehearsal=True,
            context="plan",
            runner=run_init,
            cluster=None,
            rowlog=RowLog(work),
            ledger=MarkdownLedger(work / "none.md", work / "b"),
        )
        print("| row | workload | mode | recipe | scale | peak cores / GiB | config check |")
        print("|---|---|---|---|---|---|---|")
        for row in selected:
            try:
                plan = h.plan_row(row, work / row.id)
                check, peak = "ok", f"{plan.peak.cores:g} / {plan.peak.gib:g}"
            except Refused as e:
                check, peak, status = str(e), "-", 1
            print(
                f"| {row.id} | {row.workload} | {row.mode} | {row.recipe} | {row.scale:g} | "
                f"{peak} | {check} |"
            )
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return status


if __name__ == "__main__":
    sys.exit(main())
