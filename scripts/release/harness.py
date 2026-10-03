#!/usr/bin/env python3.11
"""Release harness: runs the release matrix from a detached release worktree.

Usage::

    python3.11 scripts/release/harness.py plan     --matrix scripts/release/matrix-1.7.yaml --freeze SHA
    python3.11 scripts/release/harness.py run      --matrix ... --freeze SHA --context CTX \\
        --out /root/lakebench-release/1.7.0 --deployments-ledger PATH [--ledger-lock PATH] \\
        [--rows M01,M02] [--slots 3] [--rehearsal]
    python3.11 scripts/release/harness.py scenario S-P1 --freeze SHA --context CTX --out DIR \\
        --deployments-ledger PATH [--ledger-lock PATH]
    python3.11 scripts/release/harness.py resume   --out DIR --context CTX --deployments-ledger PATH

It ships outside the wheel (``scripts/`` is not packaged). It runs lakebench
only as ``env PYTHONPATH=<worktree>/src python3.11 -m lakebench ...`` and
imports the same tree in-process for sizing, state and verdict helpers.

Refusals, checked before any cluster call (each prints and exits 2): HEAD on
a branch (the release worktree is detached); ``--freeze`` not a commit, or
HEAD not that commit (``--rehearsal`` waives only the second); a tracked
change, or an untracked or ignored file under ``src/`` or ``scripts/``
other than ``__pycache__``; lakebench imported from outside
``<worktree>/src``, in a child process or in this one; ``--context`` not in
the kubeconfig (or a kubeconfig with no contexts); no ledger file or
credential variable for a live command; ``--out`` inside the worktree or
under ``/tmp``.

Per row: write the config (``lakebench init`` plus the row's keys, the
context pinned), refuse a config whose Spark minor and table format version
differ from the release matrix's, admit (``cluster.admit``) and write the
ledger row under one ledger lock, ``deploy --yes --require-new``, read the
incarnation through the release tree's ``read_state`` and
``current_incarnation`` (the namespace must carry the state's newest,
confirmed nonce), ``run --generate --yes`` (the default per-job timeout,
at least 3,600 s), ``report``, scrub the record into ``<out>/uat/runs/``,
then ``destroy --yes --expect-incarnation <uid>#<nonce>``. The row's
verdict is ``release_record.record_problems`` on its scrubbed record; an
exit code is never enough on its own. Destroy is never passed ``--force``
and never re-invoked: exit 6 is polled read-only, any refusal stops
admission. A ledger row is closed only when the namespace and the row's
buckets are gone. A deploy that fails is never retried: the row's namespace
is destroyed by incarnation only when it carries the row's confirmed nonce,
otherwise it is left and reported.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import re
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


#: The harness's sibling modules, by name and tracked path.
SIBLINGS = {"cluster": "scripts/release/cluster.py", "ledger": "scripts/release/ledger.py"}


def _sibling(name: str) -> Any:
    """Load a sibling module as ``lb_release_<name>`` (generic names such as
    ``cluster`` must not collide with other modules)."""
    key = f"lb_release_{name}"
    if key in sys.modules:
        return sys.modules[key]
    path = TREE / SIBLINGS[name]
    spec = importlib.util.spec_from_file_location(key, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
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
#: Seeds by workload: C360 42; AML 43, the development (calibration) seed.
SEEDS = {"customer360": 42, "financial": 43}
CREDENTIAL_VARS = ("LAKEBENCH_S3_ACCESS_KEY", "LAKEBENCH_S3_SECRET_KEY")
PLACEHOLDER_ENDPOINT = "http://10.0.1.50:80"
DESTROY_POLL_S = 30
DESTROY_POLL_LIMIT_S = 20 * 60
ADMIT_POLL_S = 60
BLOCKING_REPORT_S = 10 * 60
#: Wall-clock limits per lakebench verb; on expiry the child gets SIGINT,
#: then SIGTERM, and the step counts as failed (exit 124).
TIMEOUTS_S = {
    "init": 300,
    "deploy": 2 * 3600,
    "run": 12 * 3600,
    "report": 1800,
    "destroy": 2 * 3600,
    "query": 600,
    "logs": 600,
}
SCRIPT_TIMEOUT_S = 6 * 3600
#: The ledger session cell of an alone row written by any harness process.
ALONE_SESSION = re.compile(r"release-harness \S+ row \S+ alone")
#: After a scenario script exits, how long its background children may run on.
SCRIPT_REAP_S = 30 * 60
TIMED_OUT = 124
#: The status a row is in while the given lakebench verb runs.
STEP_STATUS = {"deploy": "deploying", "run": "running", "destroy": "destroying"}


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
        if "continuous-after-batch" in row.extra_steps and (
            row.workload != "customer360" or row.mode != "batch"
        ):
            raise Refused(f"{where}: continuous-after-batch runs on a Customer 360 batch row only")
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


def aml_seed_problem(seed: int) -> str | None:
    """An AML row must run the calibration seed and never a protected one
    (the message never names a seed value)."""
    from lakebench.config.datagen_seed import calibration_seed, protected_seeds

    if seed in protected_seeds():
        return "the AML seed is a protected (held-out) seed"
    if seed != calibration_seed():
        return "the AML seed is not the pre-registered calibration seed"
    return None


# -- refusals ----------------------------------------------------------------


def _git(tree: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(
        ["git", "-C", str(tree), *args], capture_output=True, text=True, env=env, check=False
    )


def resolve_commit(tree: Path, ref: str) -> str | None:
    """The 40-hex sha *ref* names, or None when it is not a commit."""
    out = _git(tree, "rev-parse", "--verify", "-q", f"{ref}^{{commit}}").stdout.strip()
    return out if len(out) == 40 else None


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
    want = resolve_commit(tree, freeze)
    if want is None:
        reasons.append(f"--freeze {freeze!r} is not a commit")
    elif not rehearsal and head != want:
        reasons.append(f"HEAD {head[:12]} is not the freeze commit {want[:12]}")
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
            if not names:
                reasons.append("the kubeconfig lists no contexts")
            elif context not in names:
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
    #: Where this invocation's output starts in *log* (logs are appended).
    offset: int = 0

    def text(self) -> str:
        try:
            with open(self.log, "rb") as fh:
                fh.seek(self.offset)
                return fh.read().decode(errors="replace")
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
        stdout: Path | None = None,
    ) -> ChildResult: ...


def _start_time(pid: int) -> str:
    """Field 22 of /proc/<pid>/stat, or "" when unreadable."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return ""
    return stat.rsplit(")", 1)[1].split()[19]


def processes_naming(config: str) -> list[int]:
    """Pids of running processes whose command line names *config* (a
    lakebench child whose pid was never recorded)."""
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            argv = (proc / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if config.encode() in argv:
            found.append(int(proc.name))
    return found


def child_alive(pid: Any, start: Any) -> bool:
    """Whether the recorded child is still running (same pid and start time)."""
    if not isinstance(pid, int) or not start:
        return False
    return _start_time(pid) == str(start)


def _signal_group(pid: int, sig: int = signal.SIGINT) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, sig)


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


#: How a child that must stop is stopped. A lakebench command holding the
#: cluster lease defers the first catchable signals until its shared change
#: is done (up to the lease's 750 s hold) and releases the lease on the third
#: (the SIGINT counts as the first), so SIGINT and three SIGTERMs, over 35
#: minutes, come before any SIGKILL.
STOP_SEQUENCE: tuple[tuple[int, float], ...] = (
    (signal.SIGINT, 600),
    (signal.SIGTERM, 900),
    (signal.SIGTERM, 300),
    (signal.SIGTERM, 300),
    (signal.SIGKILL, 60),
)


def _wait_or_stop(
    proc: subprocess.Popen[bytes],
    limit: float,
    sequence: tuple[tuple[int, float], ...] | None = None,
) -> int:
    """Wait up to *limit* seconds, then stop the group by STOP_SEQUENCE; a
    stopped child gives TIMED_OUT."""
    try:
        return proc.wait(timeout=limit)
    except subprocess.TimeoutExpired:
        pass
    for sig, grace in sequence or STOP_SEQUENCE:
        _signal_group(proc.pid, sig)
        try:
            proc.wait(timeout=grace)
            return TIMED_OUT
        except subprocess.TimeoutExpired:
            continue
    return TIMED_OUT


class ProcessRunner:
    """Runs ``python -m lakebench ...`` (and scenario scripts) from the
    release tree, each child in its own session. With *python* (another
    interpreter, such as a v1.6 venv's) the child runs that interpreter's
    installed lakebench, with no ``PYTHONPATH`` at all."""

    def __init__(
        self,
        tree: Path,
        timeouts: dict[str, int] | None = None,
        python: str | None = None,
    ) -> None:
        self.tree = tree
        self.timeouts = dict(TIMEOUTS_S, **(timeouts or {}))
        self.python = python
        self._lock = threading.Lock()
        self._children: dict[int, bool] = {}
        self._level = 0

    def env(self, extra: dict[str, str]) -> dict[str, str]:
        if self.python is None:
            return child_env(self.tree, extra)
        env = child_env(self.tree, extra)
        env.pop("PYTHONPATH", None)
        return env

    def _register(self, pid: int, interruptible: bool) -> None:
        with self._lock:
            self._children[pid] = interruptible
            # A child started after a Ctrl-C is signalled only if it is a run:
            # a cleanup destroy started after the second Ctrl-C must finish.
            if interruptible and self._level >= 1:
                _signal_group(pid)

    def _forget(self, pid: int) -> None:
        with self._lock:
            self._children.pop(pid, None)

    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        log: Path,
        interruptible: bool = False,
        on_spawn: Callable[[int, str], None] | None = None,
        stdout: Path | None = None,
    ) -> ChildResult:
        """*stdout*: write the child's standard output there (machine output
        such as ``query --format json``) and only its stderr to *log*."""
        log.parent.mkdir(parents=True, exist_ok=True)
        fd, path_file = tempfile.mkstemp(prefix="exit-path-", dir=str(log.parent))
        os.close(fd)
        env = self.env({"LB_EXIT_PATH_FILE": path_file})
        with contextlib.ExitStack() as stack:
            out = stack.enter_context(open(log, "ab"))
            out.write(f"$ lakebench {' '.join(args)}\n".encode())
            out.flush()
            offset = out.tell()
            sink = stack.enter_context(open(stdout, "wb")) if stdout is not None else out
            proc = subprocess.Popen(  # noqa: S603 -- fixed argv, our own interpreter
                [self.python or sys.executable, "-m", "lakebench", *args],
                cwd=str(cwd),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=sink,
                stderr=out if stdout is not None else subprocess.STDOUT,
                start_new_session=True,
            )
            self._register(proc.pid, interruptible)
            try:
                if on_spawn is not None:
                    try:
                        on_spawn(proc.pid, _start_time(proc.pid))
                    except Exception as e:  # noqa: BLE001 -- never orphan the child
                        out.write(f"[harness] could not record the child: {e}\n".encode())
                code = _wait_or_stop(proc, self.timeouts.get(args[0], 12 * 3600))
            finally:
                self._forget(proc.pid)
        paths = _read_paths(Path(path_file))
        with contextlib.suppress(OSError):
            os.unlink(path_file)
        return ChildResult(code, paths, log, offset)

    def script(
        self,
        argv: Sequence[str],
        *,
        env: dict[str, str],
        cwd: Path,
        log: Path,
        on_spawn: Callable[[int, str], None] | None = None,
    ) -> int:
        """Run a scenario script; then wait for its whole process group
        (background deploys or runs it left) before returning."""
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
            # A scenario script runs to its end (SCRIPT_TIMEOUT_S bounds it):
            # its deploys and destroys must finish, so only a second Ctrl-C
            # reaches it.
            self._register(proc.pid, False)
            try:
                if on_spawn is not None:
                    with contextlib.suppress(Exception):
                        on_spawn(proc.pid, _start_time(proc.pid))
                code = _wait_or_stop(proc, SCRIPT_TIMEOUT_S)
                reap_group(proc.pid, out)
            finally:
                self._forget(proc.pid)
        return code

    def interrupt(self, level: int = 1) -> None:
        """Level 1: one SIGINT to each running ``lakebench run`` (deploys,
        destroys and scenario scripts finish). Level 2: one SIGINT to every
        child running at that moment."""
        with self._lock:
            if level <= self._level:
                return
            for pid, interruptible in self._children.items():
                if level >= 2 and (interruptible and self._level >= 1):
                    continue  # already signalled at level 1
                if level >= 2 or interruptible:
                    _signal_group(pid)
            self._level = level


def reap_group(
    pgid: int,
    out: Any,
    limit: float | None = None,
    poll: float = 5,
    sequence: tuple[tuple[int, float], ...] | None = None,
) -> None:
    """Wait for every process of *pgid* to exit; those still running after
    *limit* seconds (default ``SCRIPT_REAP_S``) are stopped by STOP_SEQUENCE."""
    deadline = time.monotonic() + (SCRIPT_REAP_S if limit is None else limit)
    while _group_alive(pgid) and time.monotonic() < deadline:
        time.sleep(poll)
    for sig, grace in sequence or STOP_SEQUENCE:
        if not _group_alive(pgid):
            return
        out.write(f"[harness] stopping leftover processes of the script ({sig.name})\n".encode())
        _signal_group(pgid, sig)
        end = time.monotonic() + grace
        while _group_alive(pgid) and time.monotonic() < end:
            time.sleep(poll)


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


def config_peak(cfg: Any, run_mode: str | None = None) -> Peak:
    """The deployment's plan peak (``config.sizing.plan_requirements``):
    Spark, the co-resident engines and catalog, and datagen; *run_mode*
    sizes another mode on the same config (an extra step)."""
    from lakebench.config.sizing import plan_requirements

    plan = plan_requirements(cfg, run_mode=run_mode)
    return Peak(float(plan.full.cpu_cores), float(plan.full.memory_gb))


def worst_case_peak(scale: float) -> Peak:
    """The largest default-recipe peak of any workload and mode at *scale*:
    what a ledger deployment whose config cannot be read is counted at."""
    from lakebench.config.sizing import default_sizing_config, plan_requirements

    peak = Peak(0.0, 0.0)
    for workload in WORKLOADS:
        for mode in MODES:
            plan = plan_requirements(default_sizing_config(workload, mode, scale))
            peak = peak.max(Peak(float(plan.full.cpu_cores), float(plan.full.memory_gb)))
    return peak


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


def config_buckets(config: Path) -> list[str]:
    """The bronze, silver and gold bucket names the config resolves to."""
    cfg = load_row_config(config)
    b = cfg.platform.storage.s3.buckets
    return list(dict.fromkeys([b.bronze, b.silver, b.gold]))


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


def ledger_config_path(cell: str) -> Path | None:
    """A ledger row's config: the file, or the one yaml in the directory named."""
    p = Path(cell)
    if p.is_file():
        return p
    if p.is_dir():
        yamls = sorted(p.glob("*.yaml")) + sorted(p.glob("*.yml"))
        if len(yamls) == 1:
            return yamls[0]
    return None


# -- parallel-safety scenarios -------------------------------------------------


@dataclass(frozen=True)
class ScenarioSpec:
    """One S-P script and what the harness checks around it.

    ``kept``: configs whose namespace must still be present, with a
    confirmed incarnation, when the script ends. ``cleanup_refusals``: the
    exit-3 paths the harness's own destroy of a leftover may meet by design
    (a legacy or foreign bucket refused and left); the namespace must still
    be gone. ``legacy_bucket``: the harness creates an untagged bronze bucket
    for A, named outside A's prefix, with one object, and deletes it
    afterwards. ``shared_bucket``: B names A's bronze bucket.
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


@dataclass
class ScenarioRun:
    spec: ScenarioSpec
    plans: dict[str, RowPlan]
    buckets: dict[str, list[str]]
    legacy: str | None = None
    legacy_created: bool = False
    legacy_keys: list[str] = field(default_factory=list)
    shared: str | None = None
    #: The config whose S3 settings reach the legacy bucket (A's).
    s3_config: Path | None = None


@dataclass
class RowPlan:
    row: Row
    namespace: str
    config: Path
    peak: Peak


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
            # Outside A's name prefix, so no backend can read it as A's.
            run.legacy = f"rel17-{spec.id.lower()}-legacy-{secrets.token_hex(3)}"
        if spec.shared_bucket:
            run.shared = f"{a.namespace}-bronze"
        for c, plan in plans.items():
            if c == "A":
                bronze = run.legacy
            else:
                bronze = run.shared
            run.buckets[c] = self._set_buckets(plan, bronze)
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
        self.admit_together(spec.id, list(plans.values()))

    def admit_together(self: Any, label: str, plans: list[RowPlan]) -> None:
        """Wait until every deployment of *plans* fits at once, then write
        their ledger rows under the same lock as the decision."""
        peak = Peak(0.0, 0.0)
        for p in plans:
            peak = peak + p.peak
        group = RowPlan(
            Row(label, "customer360", "batch", SCENARIO_BASE.recipe, 1.0, 42),
            plans[0].namespace,
            plans[0].config,
            peak,
        )
        namespaces = tuple(p.namespace for p in plans)
        last = -BLOCKING_REPORT_S
        while True:
            if self.admitting_stopped:
                raise Refused(f"{label}: admission stopped before it was admitted")
            with self.ledger.transaction():
                obs = self.observe()
                decision = self.decide(group, obs, peak, size=len(plans), namespaces=namespaces)
                if not isinstance(decision, Unknown) and decision.admit:
                    for plan in plans:
                        self.ledger_add_logged(plan)
                    return
            reasons = [decision.reason] if isinstance(decision, Unknown) else decision.reasons
            if self.monotonic() - last >= BLOCKING_REPORT_S:
                self.say(f"{label} waits: {'; '.join(reasons)}")
                last = self.monotonic()
            self.sleep(ADMIT_POLL_S)

    def _make_legacy_bucket(self: Any, run: ScenarioRun) -> list[str]:
        s3 = self.s3_factory(run.plans["A"].config)
        assert run.legacy is not None
        if s3.bucket_exists(run.legacy):
            return [f"legacy bucket {run.legacy} already exists; not touched"]
        if not s3.create_bucket(run.legacy):
            return [f"could not create the legacy bucket {run.legacy}; not touched"]
        run.legacy_created = True
        self.log(f"{run.spec.id}-A", "ledgered", legacy_bucket=run.legacy)
        key = "harness-legacy/object.txt"
        s3.raw_client.put_object(Bucket=run.legacy, Key=key, Body=b"legacy\n")
        run.legacy_keys = [key]
        self.log(f"{run.spec.id}-A", "ledgered", legacy_keys=[key])
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
        offset = log.stat().st_size if log.exists() else 0
        rid = f"{spec.id}-A"

        def on_spawn(pid: int, start: str) -> None:
            self.log(rid, "ledgered", script_pid=pid, script_start=start)

        rc = self.script_runner(
            ["bash", str(script)], env=env, cwd=sdir, log=log, on_spawn=on_spawn
        )
        self.log(rid, "ledgered", script_rc=rc, script_pid=None)
        problems = []
        text = log.read_bytes()[offset:].decode(errors="replace") if log.exists() else ""
        if rc != 0:
            problems.append(f"script exited {rc}")
        elif f"PASS: {spec.id}" not in text:
            problems.append("script exited 0 without its PASS line")
        return problems

    def _scenario_incarnation(self: Any, config: Path) -> tuple[str | None, str]:
        """``uid#nonce`` when the namespace carries a confirmed nonce the
        config's own state kept (a script may deploy the same config twice)."""
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

    def _scenario_cleanup(
        self: Any, run: ScenarioRun, check_kept: bool = True, polls: frozenset[str] = frozenset()
    ) -> list[str]:
        """Check what the script left, then destroy every leftover by
        incarnation (B before A); never by name, never forced. Configs in
        *polls* had a destroy running when the harness stopped: they are
        polled, never destroyed again."""
        from lakebench.exit_codes import ExitCode

        spec = run.spec
        problems: list[str] = []
        for c in [c for c in reversed(spec.configs) if c in run.plans]:
            plan = run.plans[c]
            if c in polls:
                self.poll_gone(plan, "harness stopped during a scenario destroy", run.buckets[c])
                if self.rowlog.latest()[plan.row.id]["status"] != "destroyed":
                    problems.append(f"{c}: not destroyed after the stopped destroy")
                continue
            try:
                present = self.cluster.namespace_exists(plan.namespace)
            except Exception as e:  # noqa: BLE001
                problems.append(f"{c}: namespace unreadable ({e}); not destroyed")
                self.stop_admission(f"{plan.row.id}: namespace unreadable")
                continue
            inc, why = self._scenario_incarnation(plan.config) if present else (None, "absent")
            if check_kept and c in spec.kept and inc is None:
                problems.append(
                    f"{c}: expected present with its incarnation after the script ({why})"
                )
            if not present:
                problems += self.finalize_gone(plan, "gone after the script", run.buckets[c])
                continue
            if inc is None:
                problems.append(f"{c}: present without a confirmed incarnation ({why}); left")
                self.log(plan.row.id, "left", detail=f"present after the script: {why}")
                self.stop_admission(f"{plan.row.id}: left for a human")
                continue
            self.log(plan.row.id, "recorded", incarnation=inc)
            res = self.spawn_destroy(plan, inc)
            allowed = spec.allowed(c)
            by_design = res.code == ExitCode.REFUSED and res.paths and set(res.paths) <= allowed
            if res.code == ExitCode.OK or by_design:
                try:
                    gone = not self.cluster.namespace_exists(plan.namespace)
                except Exception:  # noqa: BLE001
                    gone = False
                if gone:
                    problems += self.finalize_gone(
                        plan,
                        f"destroyed by the harness (exit {res.code} {res.paths or ''})".strip(),
                        run.buckets[c],
                    )
                    continue
            if res.code == ExitCode.INCOMPLETE:
                self.poll_gone(plan, "scenario cleanup destroy exited 6", run.buckets[c])
                if self.rowlog.latest()[plan.row.id]["status"] != "destroyed":
                    problems.append(f"{c}: not destroyed after the cleanup destroy")
                continue
            self.classify_destroy(plan, res, run.buckets[c])
            problems.append(f"{c}: cleanup destroy exited {res.code} ({', '.join(res.paths)})")
        if run.legacy and run.legacy_created:
            problems += self._remove_legacy_bucket(run)
        return problems

    def _remove_legacy_bucket(self: Any, run: ScenarioRun) -> list[str]:
        """The legacy bucket is the harness's own: when it is exactly as the
        harness made it, empty and delete it; otherwise leave it for a person."""
        assert run.legacy is not None
        s3 = self.s3_factory(run.s3_config or run.plans["A"].config)
        if not s3.bucket_exists(run.legacy):
            return [f"legacy bucket {run.legacy} is gone: something deleted it"]
        listing = sorted(
            o["Key"]
            for page in s3.raw_client.get_paginator("list_objects_v2").paginate(Bucket=run.legacy)
            for o in page.get("Contents", [])
        )
        rid = f"{run.spec.id}-A"
        if listing != sorted(run.legacy_keys):
            self.log(rid, self.rowlog.latest()[rid]["status"], legacy_bucket_removed=False)
            return [
                f"legacy bucket {run.legacy} changed ({listing}); left in place for a person, "
                "with the ledger row of A"
            ]
        s3.empty_bucket(run.legacy, keep_prefixes=())
        ok = bool(s3.delete_bucket(run.legacy))
        self.log(rid, self.rowlog.latest()[rid]["status"], legacy_bucket_removed=ok)
        return [] if ok else [f"could not delete the harness's legacy bucket {run.legacy}"]

    def write_extra_results(self: Any) -> Path:
        """``results-extra.md``: scenario (and upgrade) runs, which are not
        release-matrix evidence and stay out of results.md."""
        with self._log_lock:
            return self._write_extra_results()

    def _write_extra_results(self: Any) -> Path:
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
            done = s.get("extra") or {}
            row = self.rows.get(rid)
            for step in row.extra_steps if row is not None else ():
                if step not in done:
                    lines.append(
                        f"| {rid} {step} | {s.get('namespace', '?')} | MISSING | "
                        "the step did not finish |"
                    )
            for step, res in done.items():
                problems = "; ".join(res.get("problems") or []) or "-"
                ids = ", ".join(r.removeprefix("run-") for r in res.get("run_ids") or []) or "-"
                lines.append(
                    f"| {rid} {step} (runs {ids}) | {s.get('namespace', '?')} | "
                    f"{res.get('verdict')} | {problems} |"
                )
            if "upgrade_verdict" in s:
                problems = "; ".join(s.get("upgrade_problems") or []) or "-"
                lines.append(
                    f"| upgrade (1.6 to {self.version}) | {s.get('namespace', '?')} | "
                    f"{s['upgrade_verdict']} | {problems} |"
                )
                continue
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


# -- upgrade from 1.6 ----------------------------------------------------------

#: The upgrade row: one Customer 360 batch scale 1 deployment on the default
#: Hive recipe, deployed and run by 1.6, then run and destroyed by this tree.
UPGRADE_ROW = Row("UPGRADE", "customer360", "batch", "hive-iceberg-spark-trino", 1.0, 42)
V16_SPEC = "lakebench-k8s==1.6.0"
#: The 1.6 batch jobs whose output rows must all be above zero.
V16_STAGES = ("lakebench-bronze-verify", "lakebench-silver-build", "lakebench-gold-finalize")


def v16_python_problem(python: str) -> str | None:
    """Why *python* is not an isolated lakebench 1.6 interpreter, or None."""
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME")}
    out = subprocess.run(
        [python, "-c", "import lakebench;print(lakebench.__version__);print(lakebench.__file__)"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if out.returncode != 0:
        return f"{python} cannot import lakebench: {out.stderr.strip()[-200:]}"
    version, _, where = out.stdout.strip().partition("\n")
    if not version.startswith("1.6."):
        return f"{python} has lakebench {version}, not 1.6"
    venv = Path(python).absolute().parent.parent
    if not _under(where, venv):
        return f"{python} imports lakebench from {where}, outside its own environment {venv}"
    return None


def make_v16_venv(out: Path, spec: str = V16_SPEC) -> str:
    """``<out>/v16-venv`` with *spec* installed (no editable install, no
    access to this tree); returns its interpreter."""
    venv = out / "v16-venv"
    python = venv / "bin" / "python"
    if not python.exists():
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME")}
        subprocess.run([str(python), "-m", "pip", "install", "-q", spec], check=True, env=env)
    return str(python)


def parse_count(text: str) -> int:
    """The single count a ``query --sql 'SELECT count(*) ...'`` printed.

    1.6 ``--format json`` prints ``{"rows": [{"0": "<n>"}], "count": 1}``
    and then a ``N rows in Xs`` line on stdout; this tree's ``--json`` prints
    the ``lb-cli/1`` envelope with ``data.rows`` as lists of strings.
    """
    text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)  # colour from FORCE_COLOR and the like
    start = text.find("{")
    if start < 0:
        raise ValueError("no JSON in the query output")
    doc, _end = json.JSONDecoder().raw_decode(text[start:])
    rows = (doc.get("data") or {}).get("rows") if "data" in doc else doc.get("rows")
    if not rows:
        raise ValueError("the count query returned no row")
    first = rows[0]
    if isinstance(first, dict):
        cell: Any = next(iter(first.values()))
    elif isinstance(first, (list, tuple)):
        cell = first[0]
    else:
        cell = first
    return int(str(cell).strip().strip('"'))


class UpgradeMixin:
    """The upgrade routine (``Harness.upgrade``): deploy and run with 1.6,
    then deploy, run and destroy the ``init --from`` config with this tree."""

    def datagen_listing(self: Any, config: Path) -> dict[str, list[Any]]:
        """{key: [size, etag]} under the config's bronze datagen prefix."""
        from lakebench.deploy.datagen import bronze_datagen_prefix

        cfg = load_row_config(config)
        bucket = cfg.platform.storage.s3.buckets.bronze
        prefix = bronze_datagen_prefix(cfg).strip("/")
        s3 = self.s3_factory(config)
        listing: dict[str, list[Any]] = {}
        pages = s3.raw_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{prefix}/" if prefix else ""
        )
        for page in pages:
            for o in page.get("Contents", []):
                listing[o["Key"]] = [o.get("Size"), o.get("ETag")]
        return listing

    def table_counts(
        self: Any, runner: Runner, query_config: Path, tables_from: Path, tag: str, v16: bool
    ) -> dict[str, int] | None:
        """{table: rows} of the config's silver and gold tables; None when a
        count cannot be read."""
        cfg = load_row_config(tables_from)
        catalog = cfg.architecture.query_engine.trino.catalog_name
        counts: dict[str, int] = {}
        for table in (cfg.architecture.tables.silver, cfg.architecture.tables.gold):
            out = self.out / "logs" / UPGRADE_ROW.id / f"{tag}-count-{table}.out"
            fmt = ["--format", "json"] if v16 else ["--json"]
            res = runner(
                ["query", str(query_config), "--sql", f"SELECT count(*) FROM {catalog}.{table}"]
                + fmt,
                cwd=query_config.parent,
                log=self.log_path(UPGRADE_ROW, f"{tag}-query"),
                stdout=out,
            )
            if res.code != 0:
                return None
            try:
                counts[table] = parse_count(out.read_text())
            except (OSError, ValueError, AttributeError) as e:
                self.say(f"upgrade: could not read the {tag} count of {table}: {e}")
                return None
        return counts

    def upgrade(self: Any, bystander: Path | None = None) -> int:
        row = UPGRADE_ROW
        rid = row.id
        if rid in self.rowlog.latest():
            raise Refused(f"the upgrade already ran in {self.out} (use resume to clean up)")
        if self.v16_runner is None:
            raise Refused("the upgrade needs a lakebench 1.6 interpreter")
        name = f"rel17-up-{secrets.token_hex(3)}"
        v16dir = self.out / "upgrade" / "v16"
        v17dir = self.out / "upgrade" / "v17"
        v16dir.mkdir(parents=True, exist_ok=True)
        v17dir.mkdir(parents=True, exist_ok=True)
        old = v16dir / f"{name}.yaml"
        new = v17dir / f"{name}.yaml"
        self._v16_init(old, name)
        # NEW is written before anything is deployed, so the ledger row names
        # a config that exists and destroys this deployment from the start.
        res = self.runner(
            ["init", "--from", str(old), "-o", str(new)],
            cwd=new.parent,
            log=self.log_path(row, "v17-init-from"),
        )
        if res.code != 0 or not new.is_file():
            raise Refused(f"init --from exited {res.code} (see {res.log})")
        new_cfg = load_row_config(new)
        expected = [f"{name}-{layer}" for layer in ("bronze", "silver", "gold")]
        if new_cfg.name != name or new_cfg.get_namespace() != name:
            raise Refused(f"init --from changed the name to {new_cfg.name}")
        if config_buckets(new) != expected:
            raise Refused(f"init --from changed the buckets to {config_buckets(new)}")
        if self.cluster.namespace_exists(name):
            raise Refused(f"namespace {name} exists before the 1.6 deploy; not this row's")
        # 1.6 sizing is not readable from this tree: admit at the largest
        # default peak at this scale.
        peak = worst_case_peak(row.scale)
        plan = RowPlan(row, name, new, peak)
        self.log(
            rid,
            "planned",
            namespace=name,
            config=str(new),
            v16_config=str(old),
            peak=[peak.cores, peak.gib],
            upgrade=True,
            owned_buckets=expected,
        )
        self.admit_together(rid, [plan])
        problems: list[str] = []
        try:
            problems += self._upgrade_steps(plan, old, new, expected, bystander)
        except Exception as e:  # noqa: BLE001 -- recorded; resume cleans up
            problems.append(f"harness error: {type(e).__name__}: {e}")
            self.log(rid, self.status_of(rid), harness_error=f"{type(e).__name__}: {e}")
            self.stop_admission(f"{rid}: harness error {e}")
        st = self.rowlog.latest()[rid]
        if st["status"] != "destroyed":
            problems.append(f"the deployment ended {st['status']}, not destroyed")
        verdict = "PASS" if not problems else "FAIL"
        self.log(rid, st["status"], upgrade_verdict=verdict, upgrade_problems=problems)
        self.write_extra_results()
        for p in problems:
            self.say(f"upgrade: {p}")
        self.say(f"upgrade: {verdict}")
        return 0 if verdict == "PASS" else 1

    def _v16_init(self: Any, old: Path, name: str) -> None:
        row = UPGRADE_ROW
        res = self.v16_runner(
            [
                "init",
                "-n",
                name,
                "-r",
                row.recipe,
                "-w",
                row.workload,
                "-s",
                f"{row.scale:g}",
                "--endpoint",
                self.s3_endpoint,
                # written literally; the 1.6 loader expands ${VAR} at load
                "--access-key",
                "${LAKEBENCH_S3_ACCESS_KEY}",
                "--secret-key",
                "${LAKEBENCH_S3_SECRET_KEY}",
                "--no-interactive",
                "-o",
                str(old),
            ],
            cwd=old.parent,
            log=self.log_path(row, "v16-init"),
        )
        if res.code != 0 or not old.is_file():
            raise Refused(f"lakebench 1.6 init exited {res.code} (see {res.log})")
        data = yaml.safe_load(old.read_text()) or {}
        data.setdefault("platform", {}).setdefault("kubernetes", {})["context"] = self.context
        old.write_text(yaml.safe_dump(data, sort_keys=False))

    def _upgrade_steps(
        self: Any,
        plan: RowPlan,
        old: Path,
        new: Path,
        owned: list[str],
        bystander: Path | None,
    ) -> list[str]:
        rid, name = plan.row.id, plan.namespace
        problems: list[str] = []
        v16 = self.v16_runner
        if self.cluster.namespace_exists(name):
            # checked again after admission: 1.6 has no --require-new
            self.log(rid, "not-deployed", detail="namespace appeared before the 1.6 deploy")
            self.ledger_close(plan)
            return [f"namespace {name} appeared before the 1.6 deploy; not this row's"]
        res = self._spawn(
            plan, "deploy", [str(old), "--yes"], runner=v16, cwd=old.parent, log_name="v16-deploy"
        )
        ident = self.cluster.namespace_identity(name)
        ours = ident is not None and ident.deployment_name == name and bool(ident.nonce)
        if ours:
            # The namespace did not exist before this deploy: it is this row's.
            self.log(
                rid, "deployed", incarnation=ident.incarnation, v16_incarnation=ident.incarnation
            )
        if res.code != 0 or not ours:
            problems.append(f"1.6 deploy exited {res.code}" + ("" if ours else " with no identity"))
            return problems + self._upgrade_cleanup(plan, new, owned)
        v16_inc = ident.incarnation
        if self.interrupted:
            return problems
        before = sorted(self.run_dirs_in(old.parent))
        res = self._spawn(
            plan,
            "run",
            [str(old), "--generate", "--yes"],
            runner=v16,
            cwd=old.parent,
            interruptible=True,
            log_name="v16-run",
            runs_before=before,
            phase="1.6",
        )
        v16_runs = sorted(self.run_dirs_in(old.parent) - set(before))
        problems += self._v16_record_problems(old.parent, v16_runs, res.code)
        self.log(rid, "running", v16_run_ids=v16_runs)
        if self.interrupted:
            return problems
        for r in v16_runs:  # the 1.6 count queries append their metrics to this record
            src = old.parent / "lakebench-output" / "runs" / r / "metrics.json"
            dest = self.out / "extra" / "v16-runs" / r
            dest.mkdir(parents=True, exist_ok=True)
            if src.is_file():
                shutil.copyfile(src, dest / "metrics.json")
        listing_before = self.datagen_listing(new)
        if not listing_before:
            problems.append("the 1.6 run left no generated bronze objects to compare")
        counts_v16 = self.table_counts(v16, old, new, "v16", v16=True)
        if counts_v16 is None or not all(counts_v16.values()):
            problems.append(f"the 1.6 silver and gold tables are unreadable or empty: {counts_v16}")
        watch = self._bystander_before(bystander) if bystander else None
        ident = self.cluster.namespace_identity(name)
        if ident is None or ident.incarnation != v16_inc:
            problems.append("the namespace is no longer the incarnation 1.6 deployed")
            self.log(rid, "left", detail="namespace changed before the 1.7 deploy")
            self.stop_admission(f"{rid}: left for a human")
            return problems
        if self.interrupted:
            return problems
        res = self._spawn(plan, "deploy", [str(new), "--yes"], log_name="v17-deploy", phase="1.7")
        inc, why = confirmed_incarnation(new, self.cluster.core_v1)
        if res.code != 0 or inc is None:
            problems.append(f"1.7 deploy over the 1.6 deployment exited {res.code} ({why})")
            return problems + self._upgrade_cleanup(plan, new, owned)
        self.log(rid, "deployed", incarnation=inc)
        # Intact through the 1.7 deploy: the 1.7 run rewrites silver and
        # gold, so the 1.6 tables are compared before it.
        counts_after_deploy = self.table_counts(self.runner, new, new, "v17-deploy", v16=False)
        if counts_after_deploy != counts_v16:
            problems.append(
                f"the 1.6 tables changed through the 1.7 deploy: {counts_v16} -> "
                f"{counts_after_deploy}"
            )
        if self.interrupted:
            return problems
        before = sorted(self.run_dirs(plan))
        res = self._spawn(
            plan,
            "run",
            [str(new), "--yes"],
            interruptible=True,
            log_name="v17-run",
            runs_before=before,
            phase="1.7",
        )
        v17_runs = sorted(self.run_dirs(plan) - set(before))
        if res.code != 0:
            problems.append(f"1.7 run exited {res.code}")
        if len(v17_runs) != 1:
            problems.append(f"expected one 1.7 run record, found {len(v17_runs)}")
        for r in v17_runs:
            problems += self.collect_upgrade(plan, r)
        listing_after = self.datagen_listing(new)
        if listing_after != listing_before:
            gone = sorted(set(listing_before) - set(listing_after))
            changed = sorted(
                k
                for k in listing_before
                if k in listing_after and listing_after[k] != listing_before[k]
            )
            added = sorted(set(listing_after) - set(listing_before))
            problems.append(
                f"the 1.6 bronze changed under 1.7: {len(gone)} gone, {len(changed)} changed, "
                f"{len(added)} added"
            )
        self.log(
            rid,
            "recorded",
            v17_run_ids=v17_runs,
            run_ids=v17_runs,
            counts_v16=counts_v16,
            counts_after_v17_deploy=counts_after_deploy,
            bronze_objects=len(listing_before),
            verdict="PASS" if not problems else "FAIL",
            problems=list(problems),
        )
        if self.interrupted:
            return problems
        if bystander is not None and watch is not None and "error" not in watch:
            try:  # the objects are compared across the destroy only
                watch["listing"] = self.datagen_listing(bystander)
            except Exception as e:  # noqa: BLE001
                watch = {"error": f"{type(e).__name__}: {e}"}
        self.destroy(plan, inc, owned)
        if bystander is not None:
            problems += self._bystander_after(bystander, watch)
        return problems

    def collect_upgrade(self: Any, plan: RowPlan, run_id: str) -> list[str]:
        """Scrub the 1.7 record into ``<out>/extra/runs/`` and judge it by
        its verdict, rows per layer and stages, and the freeze commit. The
        release-image and corpus-lineage checks cannot apply: the corpus was
        generated by 1.6, which writes no corpus markers."""
        from lakebench.metrics.release_record import _exp, _layer_rows_problem, _stage_problems
        from lakebench.metrics.verdict import passed

        src = plan.config.parent / "lakebench-output" / "runs" / run_id / "metrics.json"
        try:
            record = json.loads(src.read_text())
            scrub = _scrub_module()
            clean, _rewritten = scrub.scrub_record(record)
        except Exception as e:  # noqa: BLE001 -- a refused scrub keeps the record out
            return [f"{run_id}: record unreadable or scrub refused: {e}"]
        problems: list[str] = []
        try:
            exp = _exp(clean)
            if exp is None:
                problems.append("no experiment block")
            if not passed(dict(clean)):
                problems.append("the 1.7 run did not pass")
            layer = _layer_rows_problem(clean)
            if layer:
                problems.append(layer)
            if exp is not None:
                problems += _stage_problems(clean, exp)
        except Exception as e:  # noqa: BLE001
            problems.append(f"the 1.7 record could not be judged: {type(e).__name__}: {e}")
        sha = (clean.get("provenance") or {}).get("git_sha")
        if sha != (self.judge_sha or self.freeze):
            problems.append(f"the 1.7 run is not from {(self.judge_sha or self.freeze)[:12]}")
        dest = self.out / "extra" / "runs" / run_id
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "metrics.json").write_text(scrub.dump(clean))
        return [f"{run_id}: {p}" for p in problems]

    def run_dirs_in(self: Any, directory: Path) -> set[str]:
        runs = directory / "lakebench-output" / "runs"
        return {p.name for p in runs.iterdir() if p.is_dir()} if runs.is_dir() else set()

    def _v16_record_problems(self: Any, directory: Path, runs: list[str], code: int) -> list[str]:
        """A 1.6 record cannot pass this tree's release checks (another
        commit, no current experiment block); a 1.6 run passes on its own
        stored verdict with output rows above zero in every batch stage."""
        problems = []
        if code != 0:
            problems.append(f"1.6 run exited {code}")
        if len(runs) != 1:
            return [*problems, f"expected one 1.6 run record, found {len(runs)}"]
        path = directory / "lakebench-output" / "runs" / runs[0] / "metrics.json"
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as e:
            return [*problems, f"1.6 record unreadable: {e}"]
        status = (record.get("verdict") or {}).get("status")
        if status != "PASSED":
            problems.append(f"the 1.6 run's verdict is {status}, not PASSED")
        rows = {j.get("job_name"): j.get("output_rows") for j in record.get("jobs") or []}
        empty = [s for s in V16_STAGES if not isinstance(rows.get(s), int) or rows[s] <= 0]
        if empty:
            problems.append(f"1.6 stages with no output rows: {', '.join(empty)}")
        return problems

    def _upgrade_incarnation(self: Any, plan: RowPlan, new: Path) -> str | None:
        """An incarnation this row made: this tree's confirmed nonce; else
        the namespace's own token when its UID is the one 1.6 created for
        this row and its nonce is 1.6's or one NEW's state recorded (a
        pending nonce of a 1.7 deploy that did not finish)."""
        inc = confirmed_incarnation(new, self.cluster.core_v1)[0]
        if inc is not None:
            return inc
        st = self.rowlog.latest().get(plan.row.id, {})
        v16_inc = st.get("v16_incarnation")
        ident = self.cluster.namespace_identity(plan.namespace)
        if ident is None or not v16_inc or ident.uid != v16_inc.split("#", 1)[0]:
            return None
        if ident.incarnation == v16_inc:
            return v16_inc
        from lakebench.config.deploy_state import StateError, read_state

        try:
            state = read_state(new)
        except StateError:
            return None
        if state is not None and ident.nonce in state.kept_nonces():
            return ident.incarnation
        return None

    def _upgrade_cleanup(self: Any, plan: RowPlan, new: Path, owned: list[str] | None) -> list[str]:
        """After a failed step: destroy by an incarnation this row made, or
        close the ledger row when nothing was created; otherwise leave it."""
        rid = plan.row.id
        try:
            inc = self._upgrade_incarnation(plan, new)
            gone = self.cluster.namespace_identity(plan.namespace) is None
        except Exception as e:  # noqa: BLE001
            self.log(rid, self.status_of(rid), detail=f"cleanup: namespace unreadable ({e})")
            self.stop_admission(f"{rid}: namespace unreadable")
            return [f"cleanup: namespace unreadable ({e})"]
        if inc is None:
            if gone and self.buckets_left(plan, owned) == []:
                if self.ledger_close(plan, required=False):
                    self.log(rid, "not-deployed", detail="nothing was created")
                return []
            self.log(rid, "left", detail="no incarnation this row made; not destroyed")
            self.stop_admission(f"{rid}: left for a human")
            return ["cleanup: no incarnation to destroy by; left for a person"]
        self.log(rid, "recorded", incarnation=inc, verdict="FAIL")
        if self.interrupted:
            return []
        self.destroy(plan, inc, owned)
        return []

    def _bystander_before(self: Any, config: Path) -> dict[str, Any]:
        try:
            cfg = load_row_config(config)
            ident = self.cluster.namespace_identity(cfg.get_namespace())
            return {
                "namespace": cfg.get_namespace(),
                "incarnation": ident.incarnation if ident is not None else None,
                "listing": self.datagen_listing(config),
                "buckets": config_buckets(config),
            }
        except Exception as e:  # noqa: BLE001 -- recorded; the destroy still runs
            return {"error": f"{type(e).__name__}: {e}"}

    def _bystander_after(self: Any, config: Path, before: dict[str, Any] | None) -> list[str]:
        """The bystander is the same incarnation, its buckets exist, and
        none of its generated objects went or changed. (Its silver and gold
        change while it runs; its own harness judges its record.)"""
        if before is None or "error" in before:
            return [f"bystander: not read before the 1.7 deploy ({(before or {}).get('error')})"]
        problems = []
        try:
            if before["incarnation"] is None:
                problems.append(f"bystander {before['namespace']} was not deployed")
            ident = self.cluster.namespace_identity(before["namespace"])
            if ident is None or ident.incarnation != before["incarnation"]:
                problems.append(f"bystander {before['namespace']} changed incarnation or is gone")
            s3 = self.s3_factory(config)
            missing = [b for b in before["buckets"] if not s3.bucket_exists(b)]
            if missing:
                problems.append(f"bystander buckets gone: {', '.join(missing)}")
            after = self.datagen_listing(config)
            lost = [k for k, v in before["listing"].items() if after.get(k) != v]
            if lost:
                problems.append(f"bystander lost or changed {len(lost)} generated objects")
        except Exception as e:  # noqa: BLE001
            problems.append(f"bystander could not be read after the destroy: {e}")
        self.log(UPGRADE_ROW.id, self.status_of(UPGRADE_ROW.id), bystander_checked=not problems)
        return problems


# -- extra steps -------------------------------------------------------------

#: The continuous run's own line for a refused reset (three causes share it:
#: ownership not proved, data present without --force-reset, a datagen Job
#: still running), and the line it prints once the reset job succeeded.
RESET_REFUSED = "Refusing to reset continuous state"
RESET_DONE = "Continuous tables reset in"
#: The reset job's per-table lines (spark/scripts/common.py reset_stream_tables).
RESET_LINE = re.compile(r"Continuous reset: (.*)")
RESET_LOCATION = re.compile(r"(?:deleted \d+ data entries under|deleted|kept) (\S+)")


def _judge_extra(record: dict[str, Any], sha: str) -> list[str]:
    """A non-matrix record passes on its verdict, rows per layer and the
    commit it ran."""
    from lakebench.metrics.release_record import _layer_rows_problem
    from lakebench.metrics.verdict import passed

    problems = []
    if not passed(dict(record)):
        problems.append("the run did not pass")
    layer = _layer_rows_problem(record)
    if layer:
        problems.append(layer)
    ran = (record.get("provenance") or {}).get("git_sha")
    if ran != sha:
        problems.append(f"the run is not from {sha[:12]} (ran {str(ran)[:12]})")
    return problems


def reset_problems(log_text: str, owned_buckets: Sequence[str]) -> list[str]:
    """What the reset job's own lines say against "data removed only from
    this deployment's buckets": at least one table dropped with PURGE, no
    table kept as foreign, every location deleted inside an owned bucket."""
    lines = [m.group(1) for m in RESET_LINE.finditer(log_text)]
    if not lines:
        return ["the reset job's log has no 'Continuous reset:' line"]
    problems = []
    if not any(line.startswith("DROP PURGE ") for line in lines):
        problems.append("no table was dropped with PURGE")
    for line in lines:
        m = RESET_LOCATION.match(line)
        if not m:
            continue
        location = m.group(1)
        bucket = location.split("://", 1)[-1].split("/", 1)[0]
        if line.startswith("kept "):
            problems.append(f"the reset kept {location} as not this deployment's")
        elif bucket not in owned_buckets:
            problems.append(f"the reset deleted {location}, outside this deployment's buckets")
    return problems


class ExtraStepsMixin:
    """Steps a matrix row runs on its own deployment after its run and
    before its destroy (``extra_steps`` in the matrix). Their records go to
    ``<out>/extra/runs/`` and their results to ``results-extra.md``; they do
    not change the row's own verdict."""

    def run_extra_steps(self: Any, plan: RowPlan, verdict: str, inc: str) -> None:
        row = plan.row
        for step in row.extra_steps:
            if self.interrupted:
                return
            if verdict != "PASS":
                result = {"verdict": "SKIPPED", "problems": ["the row's own run did not pass"]}
            else:
                try:
                    result = EXTRA_STEPS[step](self, plan, inc)
                except Exception as e:  # noqa: BLE001 -- recorded; the destroy still runs
                    result = {"verdict": "FAIL", "problems": [f"{type(e).__name__}: {e}"]}
            extras = dict(self.rowlog.latest()[row.id].get("extra") or {})
            extras[step] = result
            self.log(row.id, "recorded", extra=extras, extra_running=None)
            self.write_extra_results()

    def step_continuous_after_batch(self: Any, plan: RowPlan, inc: str) -> dict[str, Any]:
        """A continuous run on the deployment the batch row just used. It
        must reset the batch's tables (``--force-reset``, which still needs
        the ownership proof), the reset job's own lines must show data
        removed only inside this deployment's buckets, and the record must
        pass. ``--skip-deploy``: the step never deploys."""
        now, why = confirmed_incarnation(plan.config, self.cluster.core_v1)
        if now != inc:
            return {
                "verdict": "SKIPPED",
                "problems": [f"the deployment is no longer this row's incarnation ({why or now})"],
            }
        before = sorted(self.run_dirs(plan))
        res = self._spawn(
            plan,
            "run",
            [str(plan.config), "--continuous", "--force-reset", "--skip-deploy", "--yes"],
            interruptible=True,
            log_name="extra-continuous-after-batch",
            status="recorded",
            extra_running="continuous-after-batch",
        )
        runs = sorted(self.run_dirs(plan) - set(before))
        text = res.text()
        problems = []
        if res.code != 0:
            problems.append(f"the continuous run exited {res.code} {res.paths or ''}".strip())
        refused = [ln.strip() for ln in text.splitlines() if RESET_REFUSED in ln]
        if refused:
            problems.append(f"the continuous run refused to reset: {refused[0][:300]}")
        elif RESET_DONE not in text:
            problems.append("the continuous run did not reset the batch's tables")
        else:
            logs = self.out / "logs" / plan.row.id / "extra-reset-driver.log"
            got = self.runner(
                ["logs", str(plan.config), "bronze-verify", "--lines", "5000"],
                cwd=plan.config.parent,
                log=self.log_path(plan.row, "extra-logs"),
                stdout=logs,
            )
            if got.code != 0 or not logs.is_file():
                problems.append(f"the reset job's log could not be read (logs exited {got.code})")
            else:
                owned = config_buckets(plan.config)
                problems += reset_problems(logs.read_text(errors="replace"), owned)
        if len(runs) != 1:
            problems.append(f"expected one continuous record, found {len(runs)}")
        for rid in runs:
            src = plan.config.parent / "lakebench-output" / "runs" / rid / "metrics.json"
            try:
                record = json.loads(src.read_text())
                scrub = _scrub_module()
                clean, _rewritten = scrub.scrub_record(record)
            except Exception as e:  # noqa: BLE001
                problems.append(f"{rid}: record unreadable or scrub refused: {e}")
                continue
            problems += [f"{rid}: {p}" for p in _judge_extra(clean, self.judge_sha or self.freeze)]
            dest = self.out / "extra" / "runs" / rid
            dest.mkdir(parents=True, exist_ok=True)
            (dest / "metrics.json").write_text(scrub.dump(clean))
        return {
            "verdict": "PASS" if not problems else "FAIL",
            "problems": problems,
            "run_ids": runs,
        }


#: Step name -> method. A step is listed in a matrix row's extra_steps.
EXTRA_STEPS: dict[str, Callable[..., dict[str, Any]]] = {
    "continuous-after-batch": ExtraStepsMixin.step_continuous_after_batch,
}


# -- the harness -------------------------------------------------------------


@dataclass(frozen=True)
class Observation:
    live_rows: list[Any]
    managed: set[str]
    snapshot: Any


@dataclass
class Harness(ScenarioMixin, UpgradeMixin, ExtraStepsMixin):
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
    #: Runs the v1.6 interpreter's installed lakebench (the upgrade routine).
    v16_runner: Runner | None = None
    s3_factory: Callable[[Path], Any] | None = None
    #: The commit records are judged against: --freeze, or HEAD in a rehearsal.
    judge_sha: str = ""
    _log_lock: threading.Lock = field(default_factory=threading.Lock)
    _admission: threading.Event = field(default_factory=threading.Event)
    _interrupt: threading.Event = field(default_factory=threading.Event)
    _active_lock: threading.Lock = field(default_factory=threading.Lock)
    _active: dict[str, RowPlan] = field(default_factory=dict)
    _ledger_peaks: dict[str, Peak | None] = field(default_factory=dict)
    _worst: dict[float, Peak] = field(default_factory=dict)

    # -- bookkeeping --

    def log(self, row: str, status: str, **fields: Any) -> dict[str, Any]:
        with self._log_lock:
            return self.rowlog.append(
                row, status, freeze_sha=self.freeze, rehearsal=self.rehearsal, **fields
            )

    def status_of(self, row: str) -> str:
        return str(self.rowlog.latest().get(row, {}).get("status", "planned"))

    def stop_admission(self, why: str) -> None:
        """No new rows are admitted; rows in flight finish and destroy."""
        if not self._admission.is_set():
            self.say(f"admission stopped: {why}")
        self._admission.set()

    def interrupt(self) -> None:
        """SIGINT: stop admission, and rows in flight stop before their
        next step (no new run, no destroy); resume continues them."""
        self.stop_admission("interrupted (SIGINT)")
        self._interrupt.set()

    @property
    def admitting_stopped(self) -> bool:
        return self._admission.is_set()

    @property
    def interrupted(self) -> bool:
        return self._interrupt.is_set()

    def row_dir(self, row: Row) -> Path:
        return self.out / "configs" / row.id

    def log_path(self, row: Row, step: str) -> Path:
        return self.out / "logs" / row.id / f"{step}.log"

    def _set_active(self, rid: str, plan: RowPlan | None) -> None:
        with self._active_lock:
            if plan is None:
                self._active.pop(rid, None)
            else:
                self._active[rid] = plan

    def _active_rows(self) -> list[RowPlan]:
        with self._active_lock:
            return list(self._active.values())

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
        if row.workload == "financial":
            problem = aml_seed_problem(row.seed)
            if problem:
                raise Refused(f"{row.id}: {problem}")
        cfg_path = self.write_config(row, row_dir, name)
        cfg = load_row_config(cfg_path)
        problem = versions_problem(row, cfg)
        if problem:
            raise Refused(f"{row.id}: {problem}")
        peak = config_peak(cfg)
        if "continuous-after-batch" in row.extra_steps:
            peak = peak.max(config_peak(cfg, run_mode="continuous"))
        return RowPlan(row, cfg.get_namespace(), cfg_path, peak)

    # -- admission --

    def _worst_case(self, scale: float) -> Peak:
        if scale not in self._worst:
            self._worst[scale] = worst_case_peak(scale)
        return self._worst[scale]

    def _ledger_peak(self, row: Any) -> Peak:
        """A ledger deployment's plan peak; the worst default-recipe peak at
        its scale when its config cannot be read."""
        if row.namespace not in self._ledger_peaks:
            peak: Peak | None = None
            path = ledger_config_path(row.config)
            if path is not None:
                try:
                    peak = config_peak(load_row_config(path))
                except Exception:  # noqa: BLE001 -- unloadable: the worst case counts
                    peak = None
            self._ledger_peaks[row.namespace] = peak
        found = self._ledger_peaks[row.namespace]
        if found is not None:
            return found
        # An unreadable scale raises: admission fails closed.
        return self._worst_case(float(row.scale))

    def observe(self) -> Observation | Any:
        """One read of the ledger, the lakebench namespaces and the cluster's
        load; Unknown when any of them cannot be read (fail closed)."""
        try:
            live_rows = self.ledger.live_rows()
        except Exception as e:  # noqa: BLE001
            return Unknown(f"ledger unreadable: {e}")
        try:
            managed = set(self.cluster.managed_namespaces())
        except Exception as e:  # noqa: BLE001
            return Unknown(f"namespace list failed: {e}")
        try:
            snapshot = self.cluster.snapshot()
        except Exception as e:  # noqa: BLE001
            return Unknown(f"capacity read failed: {e}")
        return Observation(live_rows, managed, snapshot)

    def decide(
        self,
        plan: RowPlan,
        obs: Any,
        fallback: Peak,
        *,
        size: int = 1,
        namespaces: tuple[str, ...] = (),
    ) -> Any:
        if isinstance(obs, Unknown):
            return obs
        try:
            own = [
                ActiveRow(p.namespace, p.peak, p.row.alone, p.row.aml_continuous)
                for p in self._active_rows()
            ]
            live = obs.live_rows
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
                obs.snapshot,
                managed=obs.managed,
                ledger_live={r.namespace for r in live},
                own_active=own,
                ledger_peaks={r.namespace: self._ledger_peak(r) for r in live},
                ledger_alone={r.namespace for r in live if ALONE_SESSION.fullmatch(r.session)},
                fallback_peak=fallback,
                slots=self.slots,
            )
        except Exception as e:  # noqa: BLE001 -- fail closed
            return Unknown(f"admission check failed: {type(e).__name__}: {e}")

    # -- the row lifecycle --

    def ledger_add_logged(self, plan: RowPlan) -> None:
        """Ledger row first (with the intent logged before it), then 'ledgered'."""
        self.log(plan.row.id, "planned", ledger_intent=True)
        self.ledger.add(
            LedgerRow(
                plan.namespace,
                str(plan.config),
                f"release-harness {self.freeze[:8]} row {plan.row.id}"
                + (" alone" if plan.row.alone else ""),
                f"{plan.row.scale:g}",
                utc_now(),
            )
        )
        self.log(plan.row.id, "ledgered", namespace=plan.namespace, config=str(plan.config))

    def ledger_close(self, plan: RowPlan, required: bool = True) -> bool:
        """Close the row's ledger row; False (and admission stops) when it
        cannot be closed."""
        try:
            if not self.ledger.has_row(plan.namespace):
                if required:
                    self.say(
                        f"{plan.row.id}: the ledger has no row for {plan.namespace} to close "
                        "(written by hand or lost); nothing to close"
                    )
                return True
            self.ledger.close(plan.namespace)
        except (OSError, LedgerError) as e:
            self.say(f"{plan.row.id}: could not close the ledger row for {plan.namespace}: {e}")
            self.stop_admission(f"{plan.row.id}: ledger close failed")
            return False
        return True

    def _spawn(
        self,
        plan: RowPlan,
        verb: str,
        args: list[str],
        *,
        interruptible: bool = False,
        runner: Runner | None = None,
        cwd: Path | None = None,
        log_name: str | None = None,
        **fields: Any,
    ) -> ChildResult:
        """Log the step's status before the child exists (resume treats it
        as started), then its pid once it does."""
        status = fields.pop("status", None) or STEP_STATUS[verb]
        self.log(plan.row.id, status, child_pid=None, child_start=None, **fields)

        def on_spawn(pid: int, start: str) -> None:
            self.log(plan.row.id, status, child_pid=pid, child_start=start)

        return (runner or self.runner)(
            [verb, *args],
            cwd=cwd or plan.config.parent,
            log=self.log_path(plan.row, log_name or verb),
            interruptible=interruptible,
            on_spawn=on_spawn,
        )

    def deploy(self, plan: RowPlan) -> None:
        """Deploy (the ledger row is written), incarnation, run, destroy."""
        row = plan.row
        res = self._spawn(plan, "deploy", [str(plan.config), "--yes", "--require-new"])
        if res.code != 0:
            self.resolve_failed_deploy(plan, f"deploy exited {res.code} {res.paths or ''}".strip())
            return
        inc, why = confirmed_incarnation(plan.config, self.cluster.core_v1)
        if inc is None:
            self.log(row.id, "left", detail=f"deployed, but no confirmed incarnation: {why}")
            self.say(f"{row.id}: {plan.namespace} left for a human: {why}")
            self.stop_admission(f"{row.id}: left for a human")
            return
        self.log(row.id, "deployed", incarnation=inc)
        if self.interrupted:
            return
        self.run_and_destroy(plan, inc)

    def resolve_failed_deploy(self, plan: RowPlan, why: str) -> None:
        """A failed or interrupted deploy is never retried."""
        row = plan.row
        try:
            exists = self.cluster.namespace_exists(plan.namespace)
        except Exception as e:  # noqa: BLE001
            self.log(row.id, self.status_of(row.id), detail=f"{why}; namespace unreadable ({e})")
            self.stop_admission(f"{row.id}: namespace unreadable")
            return
        if not exists:
            # The namespace comes before the buckets in a deploy; with no
            # namespace and none of the row's buckets, nothing was created.
            left = self.buckets_left(plan, None)
            if left == []:
                if self.ledger_close(plan, required=False):
                    self.log(row.id, "not-deployed", detail=f"{why}; nothing was created")
                else:
                    self.log(row.id, self.status_of(row.id), detail=f"{why}; ledger close pending")
                return
            self.log(row.id, "left", detail=f"{why}; no namespace, buckets: {left}")
            self.stop_admission(f"{row.id}: left for a human")
            return
        inc, reason = confirmed_incarnation(plan.config, self.cluster.core_v1)
        if inc is None:
            self.log(row.id, "left", detail=f"{why}; not destroyed by the harness: {reason}")
            self.say(f"{row.id}: {plan.namespace} left for a human: {reason}")
            self.stop_admission(f"{row.id}: left for a human")
            return
        # 'recorded' sends a resumed row straight to destroy, never to a run.
        self.log(row.id, "recorded", incarnation=inc, detail=why, verdict="FAIL", run_ids=[])
        if self.interrupted:
            return
        self.destroy(plan, inc)

    def run_dirs(self, plan: RowPlan) -> set[str]:
        runs = plan.config.parent / "lakebench-output" / "runs"
        return {p.name for p in runs.iterdir() if p.is_dir()} if runs.is_dir() else set()

    def run_and_destroy(self, plan: RowPlan, inc: str) -> None:
        before = sorted(self.run_dirs(plan))
        res = self._spawn(
            plan,
            "run",
            [str(plan.config), "--generate", "--yes"],
            interruptible=True,
            runs_before=before,
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
        if self.interrupted:
            return
        self.run_extra_steps(plan, verdict, inc)
        if self.interrupted:
            return
        self.destroy(plan, inc)

    def collect(self, plan: RowPlan, run_id: str, kind: str | None = None) -> list[str]:
        """Scrub the record into ``<out>/uat/runs/`` (``<out>/<kind>/runs/``
        for runs that are not matrix evidence) and judge the scrubbed copy
        (what the release gate reads); its release problems."""
        src = plan.config.parent / "lakebench-output" / "runs" / run_id / "metrics.json"
        try:
            record = json.loads(src.read_text())
        except (OSError, ValueError) as e:
            return [f"{run_id}: record unreadable: {e}"]
        try:
            scrub = _scrub_module()
            clean, _rewritten = scrub.scrub_record(record)
        except Exception as e:  # noqa: BLE001 -- a refused scrub keeps the record out
            return [f"{run_id}: scrub refused: {e}"]
        try:
            problems = record_verdict(clean, self.judge_sha or self.freeze, self.version)
        except Exception as e:  # noqa: BLE001 -- a record that cannot be judged fails
            problems = [f"the record could not be judged: {type(e).__name__}: {e}"]
        dest = self.out / (kind or ("rehearsal" if self.rehearsal else "uat")) / "runs" / run_id
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "metrics.json").write_text(scrub.dump(clean))
        return [f"{run_id}: {p}" for p in problems]

    def spawn_destroy(self, plan: RowPlan, inc: str) -> ChildResult:
        return self._spawn(
            plan, "destroy", [str(plan.config), "--yes", "--expect-incarnation", inc]
        )

    def destroy(self, plan: RowPlan, inc: str, owned: list[str] | None = None) -> None:
        """Destroy by incarnation; never ``--force``, never re-invoked.
        *owned*: the buckets the row owns (default: the config's three)."""
        row = plan.row
        try:
            exists = self.cluster.namespace_exists(plan.namespace)
        except Exception as e:  # noqa: BLE001
            self.log(row.id, self.status_of(row.id), detail=f"namespace unreadable: {e}")
            self.stop_admission(f"{row.id}: namespace unreadable before destroy")
            return
        if not exists:
            self.log(
                row.id,
                "left",
                detail="namespace gone before destroy; buckets may remain; not destroyed "
                "by the harness",
            )
            self.stop_admission(f"{row.id}: left for a human")
            return
        self.classify_destroy(plan, self.spawn_destroy(plan, inc), owned)

    def buckets_left(self, plan: RowPlan, owned: list[str] | None) -> list[str] | None:
        """The row's buckets that still exist; None when they cannot be read."""
        if self.s3_factory is None:
            return None
        try:
            names = owned if owned is not None else config_buckets(plan.config)
            s3 = self.s3_factory(plan.config)
            return [b for b in names if s3.bucket_exists(b)]
        except Exception:  # noqa: BLE001
            return None

    def finalize_gone(self, plan: RowPlan, why: str, owned: list[str] | None = None) -> list[str]:
        """The namespace is gone: close the ledger row only when the row's
        buckets are gone too; otherwise leave the row for a person."""
        left = self.buckets_left(plan, owned)
        if left is None:
            self.log(plan.row.id, "left", detail=f"{why}; buckets unreadable")
            self.stop_admission(f"{plan.row.id}: buckets unreadable after destroy")
            return [f"{plan.row.id}: buckets unreadable after the namespace went"]
        if left:
            self.log(plan.row.id, "left", detail=f"{why}; buckets remain: {', '.join(left)}")
            self.stop_admission(f"{plan.row.id}: buckets remain after destroy")
            return [f"{plan.row.id}: buckets remain after the namespace went: {', '.join(left)}"]
        if not self.ledger_close(plan):
            # Not terminal until the ledger row is closed: resume polls the
            # (gone) namespace and closes it then.
            self.log(plan.row.id, "destroying", detail=f"{why}; ledger close pending")
            return [f"{plan.row.id}: destroyed, but its ledger row could not be closed"]
        self.log(plan.row.id, "destroyed", detail=why)
        return []

    def classify_destroy(
        self, plan: RowPlan, res: ChildResult, owned: list[str] | None = None
    ) -> None:
        from lakebench.exit_codes import ExitCode

        row = plan.row
        not_completed = "Destroy NOT completed" in res.text()
        if res.code == ExitCode.OK and not not_completed:
            try:
                gone = not self.cluster.namespace_exists(plan.namespace)
            except Exception:  # noqa: BLE001
                gone = False
            if gone:
                self.finalize_gone(plan, f"destroy exited 0 {res.paths or ''}".strip(), owned)
                return
            self.log(row.id, "failed", detail="destroy exited 0 but the namespace is present")
            self.stop_admission(f"{row.id}: destroy exited 0 with the namespace present")
            return
        if res.code == ExitCode.INCOMPLETE:
            self.poll_gone(plan, "destroy exited 6 (incomplete, still terminating)", owned)
            return
        if res.code == ExitCode.REFUSED and "destroy.incarnation_mismatch" in res.paths:
            try:
                gone = not self.cluster.namespace_exists(plan.namespace)
            except Exception:  # noqa: BLE001
                gone = False
            if gone:
                self.log(row.id, "left", detail="namespace gone before destroy; buckets may remain")
                self.stop_admission(f"{row.id}: left for a human")
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

    def poll_gone(self, plan: RowPlan, why: str, owned: list[str] | None = None) -> None:
        """Read-only polls until the namespace is gone; never re-invokes destroy."""
        row = plan.row
        self.log(row.id, "destroying", detail=why)
        deadline = self.monotonic() + DESTROY_POLL_LIMIT_S
        while True:
            try:
                if not self.cluster.namespace_exists(plan.namespace):
                    self.finalize_gone(plan, f"{why}; namespace gone on a later read", owned)
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
        """Admit *pending* rows as capacity allows (decision and ledger row
        under one ledger lock); run *continuing* steps (resumed rows) at once.
        Returns when every thread has finished; never leaves one running."""
        threads: dict[str, threading.Thread] = {}
        fallback = Peak(0.0, 0.0)
        for p in pending:
            fallback = fallback.max(p.peak)
        try:
            for i, fn in enumerate(continuing):
                t = threading.Thread(target=fn, name=f"resume-{i}", daemon=False)
                t.start()
                threads[f"resume-{i}"] = t
            last_report = -BLOCKING_REPORT_S
            queue = list(pending)
            while True:
                for key in [k for k, t in threads.items() if not t.is_alive()]:
                    threads.pop(key).join()
                if self.admitting_stopped:
                    queue.clear()
                if not queue and not threads:
                    return
                admitted = self._admit_one(queue, threads, fallback, last_report)
                if admitted is None:
                    last_report = self.monotonic()
                elif admitted:
                    continue
                self.sleep(ADMIT_POLL_S if queue else 5)
        finally:
            for t in threads.values():
                t.join()

    def _admit_one(
        self,
        queue: list[RowPlan],
        threads: dict[str, threading.Thread],
        fallback: Peak,
        last_report: float,
    ) -> bool | None:
        """Admit the first row that fits: True; nothing fits: False, or None
        when the waiting reasons were just printed."""
        if not queue:
            return False
        reported = False
        with self.ledger.transaction():
            obs = self.observe()
            for plan in list(queue):
                if plan.row.alone and plan is not queue[0]:
                    continue
                decision = self.decide(plan, obs, fallback)
                if not isinstance(decision, Unknown) and decision.admit:
                    queue.remove(plan)
                    try:
                        self.ledger_add_logged(plan)
                    except Exception as e:  # noqa: BLE001
                        self.log(plan.row.id, "not-deployed", detail=f"ledger row not written: {e}")
                        self.stop_admission(f"{plan.row.id}: ledger write failed")
                        return True
                    self._set_active(plan.row.id, plan)
                    t = threading.Thread(
                        target=self._guarded, args=(plan,), name=plan.row.id, daemon=False
                    )
                    t.start()
                    threads[plan.row.id] = t
                    return True
                if self.monotonic() - last_report >= BLOCKING_REPORT_S and not reported:
                    reasons, blocking = (
                        ([decision.reason], [])
                        if isinstance(decision, Unknown)
                        else (decision.reasons, decision.blocking)
                    )
                    self.say(
                        f"{plan.row.id} waits: {'; '.join(reasons)}"
                        + (f" (blocking: {', '.join(blocking)})" if blocking else "")
                    )
                    reported = True
                if plan.row.alone:
                    break
        return None if reported else False

    def _guarded(self, plan: RowPlan) -> None:
        """A harness error keeps the row's last status (resume continues it)
        and stops admission; it is never written as a terminal status."""
        try:
            self.deploy(plan)
        except Exception as e:  # noqa: BLE001
            rid = plan.row.id
            self.log(rid, self.status_of(rid), harness_error=f"{type(e).__name__}: {e}")
            self.stop_admission(f"{rid}: harness error {e}")
        finally:
            self._set_active(plan.row.id, None)

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
                slots=self.slots,
            )
            plans.append(plan)
        self.schedule(plans, [])
        return self.finish()

    def finish(self) -> int:
        path = self.write_results()
        self.write_extra_results()
        states = self.rowlog.latest()
        open_rows = [r for r, s in states.items() if s["status"] not in TERMINAL]
        self.say(f"results: {path}")
        if open_rows:
            self.say(
                f"rows not finished: {', '.join(open_rows)}; resume with: python3.11 "
                f"{HERE / 'harness.py'} resume --out {self.out} --context {self.context} "
                f"--deployments-ledger {self.ledger.path} --ledger-lock {self.ledger.lock_path}"
            )
        bad = [
            r
            for r, s in states.items()
            if r in self.rows
            and (
                s.get("verdict") != "PASS"
                or s["status"] != "destroyed"
                or any(x.get("verdict") != "PASS" for x in (s.get("extra") or {}).values())
                or set(self.rows[r].extra_steps) - set(s.get("extra") or {})
            )
        ]
        return 0 if not bad and not open_rows else 1

    def write_results(self) -> Path:
        """``results.md``: one table row per matrix row; run ids only on
        rows that passed (the release gate reads every id in the table), the
        others listed below the table."""
        states = self.rowlog.latest()
        if self.rehearsal:
            name = "results-rehearsal.md"
            lines = [
                f"# Rehearsal results {self.version}",
                "",
                f"Rehearsal at {self.judge_sha or self.freeze}; these runs are not release "
                "evidence.",
                "",
            ]
        else:
            name = "results.md"
            lines = [f"# UAT results {self.version}", "", f"Freeze commit: {self.freeze}", ""]
        lines += [
            "| row | workload | mode | recipe | Spark / format | scale | run id | verdict | group check |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        from lakebench.metrics.release_record import RELEASE_MATRIX_VERSIONS

        not_passed: list[str] = []
        for rid, s in states.items():
            row = self.rows.get(rid)
            if row is None:
                continue
            spark, fmt = RELEASE_MATRIX_VERSIONS.get(
                (row.workload, row.mode, row.recipe), ("?", "?")
            )
            ids = [r.removeprefix("run-") for r in s.get("run_ids") or []]
            verdict = s.get("verdict") or "-"
            passed = verdict == "PASS" and s["status"] == "destroyed"
            if verdict == "PASS" and not passed:
                verdict = f"PASS ({s['status']})"
            if not passed and ids:
                not_passed.append(f"{rid}: {', '.join(ids)}")
            cell = ", ".join(ids) if passed and ids else "-"
            lines.append(
                f"| {rid} | {row.workload} | {row.mode} | {row.recipe} | {spark} / {fmt} | "
                f"{row.scale:g} | {cell} | {verdict} | not checked |"
            )
        lines += [
            "",
            "Group check: cross-row result fingerprint equality is checked separately "
            "(verify-groups); this table does not claim it.",
        ]
        if not_passed:
            lines += ["", "Runs of rows that did not pass (not evidence):", ""]
            lines += [f"- {entry}" for entry in not_passed]
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
        if s["status"] not in TERMINAL
        and (
            child_alive(s.get("child_pid"), s.get("child_start"))
            or child_alive(s.get("script_pid"), s.get("script_start"))
            or (s.get("config") and processes_naming(str(s["config"])))
        )
    ]
    if alive:
        raise Refused(
            f"rows with a live child process: {', '.join(alive)}; wait for them or stop them first"
        )
    unknown = [
        rid
        for rid, s in states.items()
        if s["status"] not in TERMINAL
        and s.get("scenario") not in SCENARIOS
        and not s.get("upgrade")
        and rid not in rows
    ]
    if unknown:
        raise Refused(f"rows of {h.rowlog.path} not in the matrix: {', '.join(unknown)}")
    pending: list[RowPlan] = []
    continuing: list[Callable[[], None]] = []
    for rid, s in states.items():
        status = s["status"]
        if status in TERMINAL or s.get("scenario") in SCENARIOS:
            continue
        if s.get("upgrade"):
            continuing.append(_bind(h, rid, lambda st=s: _resume_upgrade(h, st)))
            continue
        plan = _rebuild_plan(rows[rid], s)
        inc = s.get("incarnation")
        if status == "planned" and not s.get("ledger_intent"):
            pending.append(plan)
            continue
        h._set_active(rid, plan)
        if status in ("planned", "ledgered", "deploying"):
            fn = lambda p=plan: h.resolve_failed_deploy(p, "harness stopped during deploy")  # noqa: E731
        elif status == "deployed" and inc:
            fn = lambda p=plan, i=inc: h.run_and_destroy(p, i)  # noqa: E731
        elif status == "running" and inc:
            before = set(s.get("runs_before") or [])
            fn = lambda p=plan, i=inc, b=before: h.finish_run(p, i, b, None)  # noqa: E731
        elif status == "recorded" and inc:
            fn = lambda p=plan, i=inc: h.destroy(p, i)  # noqa: E731
        elif status == "destroying":
            fn = lambda p=plan: h.poll_gone(p, "harness stopped during destroy")  # noqa: E731
        else:
            h._set_active(rid, None)
            h.log(rid, "failed", detail=f"cannot resume from {status} without an incarnation")
            continue
        continuing.append(_bind(h, rid, fn))
    for spec_id in dict.fromkeys(
        st["scenario"]
        for st in states.values()
        if st.get("scenario") in SCENARIOS
        and (
            st["status"] not in TERMINAL
            or (st.get("legacy_bucket") and st.get("legacy_bucket_removed") is None)
        )
    ):
        continuing.append(_bind(h, spec_id, lambda sid=spec_id: _resume_scenario(h, sid, states)))
    h.schedule(pending, continuing)
    return h.finish()


def _resume_upgrade(h: Harness, st: dict[str, Any]) -> None:
    """Clean up an upgrade the harness stopped in: destroy by incarnation
    (a destroy that was running is polled); never re-deploy or re-run."""
    row = UPGRADE_ROW
    plan = _rebuild_plan(row, st)
    owned = list(st.get("owned_buckets") or []) or None
    if st["status"] == "destroying":
        h.poll_gone(plan, "harness stopped during the upgrade's destroy", owned)
        problems: list[str] = []
    else:
        problems = h._upgrade_cleanup(plan, plan.config, owned)
    h.log(
        row.id,
        h.status_of(row.id),
        upgrade_verdict="FAIL",
        upgrade_problems=["the harness stopped during the upgrade", *problems],
    )
    h.write_extra_results()


def _resume_scenario(h: Harness, spec_id: str, states: dict[str, dict[str, Any]]) -> None:
    """Clean up a scenario the harness stopped in: destroy each leftover
    by incarnation (a destroy that was running is polled), remove the
    harness's legacy bucket; never re-run it."""
    spec = SCENARIOS[spec_id]
    run = ScenarioRun(spec, {}, {})
    polls: set[str] = set()
    for c in spec.configs:
        st = states.get(f"{spec_id}-{c}")
        if st is None:
            continue
        if c == "A" and st.get("legacy_bucket") and st.get("legacy_bucket_removed") is None:
            run.legacy = st["legacy_bucket"]
            run.legacy_created = True
            run.legacy_keys = list(st.get("legacy_keys") or [])
            run.s3_config = Path(st["config"])
        if st["status"] in TERMINAL:
            continue
        row = Row(
            f"{spec_id}-{c}",
            SCENARIO_BASE.workload,
            SCENARIO_BASE.mode,
            SCENARIO_BASE.recipe,
            SCENARIO_BASE.scale,
            SCENARIO_BASE.seed,
        )
        run.plans[c] = _rebuild_plan(row, st)
        run.buckets[c] = list(st.get("owned_buckets") or [])
        if st["status"] == "destroying":
            polls.add(c)
    problems = h._scenario_cleanup(run, check_kept=False, polls=frozenset(polls))
    h.log(
        f"{spec_id}-A",
        h.rowlog.latest()[f"{spec_id}-A"]["status"],
        scenario_verdict="FAIL",
        scenario_problems=["the harness stopped during the scenario", *problems],
    )
    h.write_extra_results()


def _bind(h: Harness, rid: str, fn: Callable[[], None]) -> Callable[[], None]:
    def go() -> None:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            h.log(rid, h.status_of(rid), harness_error=f"on resume: {type(e).__name__}: {e}")
            h.stop_admission(f"{rid}: harness error {e}")
        finally:
            h._set_active(rid, None)

    return go


# -- CLI ---------------------------------------------------------------------


def _core_v1(context: str) -> Any:
    """A CoreV1Api pinned to *context* through the release tree's
    ClusterTarget (one context per process, as every lakebench command)."""
    from kubernetes import client as kclient

    from lakebench.k8s.target import ClusterTarget

    ClusterTarget.resolve(context=context).activate()
    return kclient.CoreV1Api()


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


#: Steps a matrix row may run after its own run and before its destroy.
KNOWN_STEPS: tuple[str, ...] = tuple(EXTRA_STEPS)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="harness.py", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    plan = sub.add_parser("plan", help="print each row's config check and peak; no cluster")
    plan.add_argument("--matrix", type=Path, required=True)
    plan.add_argument("--freeze", required=True)
    plan.add_argument("--rows")
    for name in ("run", "resume", "scenario", "upgrade"):
        sp = sub.add_parser(name)
        if name == "upgrade":
            sp.add_argument("--freeze", required=True)
            sp.add_argument("--rehearsal", action="store_true")
            sp.add_argument("--matrix", type=Path, default=HERE / "matrix-1.7.yaml")
            sp.add_argument(
                "--v16-venv",
                type=Path,
                help="an existing venv with lakebench 1.6 installed (default: <out>/v16-venv, "
                f"created with {V16_SPEC})",
            )
            sp.add_argument("--v16-spec", default=V16_SPEC)
            sp.add_argument(
                "--bystander-config",
                type=Path,
                help="the config of a deployment running meanwhile, checked untouched",
            )
        elif name == "scenario":
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
        sp.add_argument(
            "--ledger-lock",
            type=Path,
            help="the lock file the ledger's other writers take (default <ledger>.lock)",
        )
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
    slots = 3
    if args.cmd == "resume":
        prior = rowlog.latest()
        if not prior:
            raise Refused(f"no rows in {rowlog.path}")
        first = next(iter(prior.values()))
        freeze = str(first.get("freeze_sha"))
        rehearsal = bool(first.get("rehearsal"))
        slots = int(first.get("slots") or 3)
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
    full = resolve_commit(TREE, freeze)
    head = resolve_commit(TREE, "HEAD")
    assert full is not None and head is not None  # refuse_reasons checked both
    try:
        rowlog.acquire()
    except LedgerError as e:
        raise Refused(str(e)) from e
    runner = ProcessRunner(TREE)
    cluster = ClusterReader(_core_v1(args.context))
    ledger_path = args.deployments_ledger.resolve()
    h = Harness(
        tree=TREE,
        out=out,
        freeze=full,
        version=version,
        rehearsal=rehearsal,
        context=args.context,
        runner=runner,
        cluster=cluster,
        rowlog=rowlog,
        ledger=MarkdownLedger(ledger_path, out / "ledger-backups", args.ledger_lock),
        slots=slots,
        s3_endpoint=os.environ["LB_S3_ENDPOINT"],
        s3_factory=default_s3,
        script_runner=runner.script,
        judge_sha=head if rehearsal else full,
    )
    h.rows = {r.id: r for r in rows}
    runners = [runner]
    if args.cmd == "upgrade":
        # resume cleans an upgrade up with this tree's CLI only
        h.v16_runner = ProcessRunner(TREE, python=_v16_python(args, out))
        runners.append(h.v16_runner)
    sigints = {"n": 0}
    pending = threading.Event()

    def on_sigint(_signum: int, _frame: Any) -> None:
        # Signal-safe: only flags here; a watcher thread does the rest.
        sigints["n"] += 1
        pending.set()

    def watcher() -> None:
        seen = 0
        while True:
            pending.wait()
            pending.clear()
            n = sigints["n"]
            if n > seen:
                seen = n
                h.interrupt()
                for r in runners:
                    r.interrupt(level=min(n, 2))
                h.say(
                    "SIGINT: admission stopped; runs and scenario scripts got one SIGINT"
                    if n == 1
                    else "second SIGINT: every lakebench child got one SIGINT"
                )

    threading.Thread(target=watcher, name="sigint", daemon=True).start()
    signal.signal(signal.SIGINT, on_sigint)
    try:
        if args.cmd == "resume":
            return resume(h, {r.id: r for r in rows})
        if args.cmd == "scenario":
            return h.scenario(SCENARIOS[args.scenario_id])
        if args.cmd == "upgrade":
            return h.upgrade(args.bystander_config)
        return h.run(_select(rows, args.rows))
    finally:
        rowlog.release()


def _v16_python(args: argparse.Namespace, out: Path) -> str:
    """The lakebench 1.6 interpreter for ``upgrade``: --v16-venv, or
    ``<out>/v16-venv`` created with --v16-spec."""
    venv = getattr(args, "v16_venv", None)
    if venv is not None:
        python = str(Path(venv).absolute() / "bin" / "python")
    else:
        python = make_v16_venv(out, args.v16_spec)
    problem = v16_python_problem(python)
    if problem:
        raise Refused(problem)
    return python


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
