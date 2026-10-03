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


# -- the harness -------------------------------------------------------------


@dataclass
class RowPlan:
    row: Row
    namespace: str
    config: Path
    peak: Peak


@dataclass
class Harness:
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

    def decide(self, plan: RowPlan, fallback: Peak) -> Any:
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
                plan.row.id, plan.namespace, plan.peak, plan.row.alone, plan.row.aml_continuous
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
            r for r, s in states.items() if s.get("verdict") != "PASS" or s["status"] != "destroyed"
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
    h.schedule(pending, continuing)
    return h.finish()


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
    for name in ("run", "resume"):
        sp = sub.add_parser(name)
        if name == "run":
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
        freeze, rehearsal, slots = args.freeze, args.rehearsal, args.slots
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
    )
    h.rows = {r.id: r for r in rows}

    def on_sigint(_signum: int, _frame: Any) -> None:
        h.stop_admission("interrupted (SIGINT); running lakebench runs get one SIGINT")
        runner.interrupt()

    signal.signal(signal.SIGINT, on_sigint)
    try:
        if args.cmd == "resume":
            return resume(h, {r.id: r for r in rows})
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
