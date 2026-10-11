#!/usr/bin/env python3
"""Release gate: every check a release needs, in one command (GOALS P9.6).

Runs each check, prints one line per check, and exits non-zero if any check
failed. A check that cannot run here (gitleaks not installed) is reported as
SKIP; ``--require-all`` turns every SKIP into a failure, which is how
release.yml runs it.

Usage:
    python scripts/release_gate.py                 # everything
    python scripts/release_gate.py --tag v1.6.0    # also check the tag
    python scripts/release_gate.py --only ruff-check,mypy
    python scripts/release_gate.py --list
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUST_DIR = ROOT / "datagen_rs"

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"

# Placeholders for the ${VAR} references in the shipped examples; values are
# never used, validation only needs them resolved.
_EXAMPLE_ENV = {
    "LAKEBENCH_POLARIS_CLIENT_SECRET": "placeholder",
    "LAKEBENCH_S3_ACCESS_KEY": "placeholder",
    "LAKEBENCH_S3_SECRET_KEY": "placeholder",
}


@dataclass
class Result:
    name: str
    status: str
    detail: str = ""


@dataclass
class Check:
    name: str
    run: Callable[[], Result]
    description: str


# -- helpers -----------------------------------------------------------------


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _tail(text: str, lines: int = 15) -> str:
    kept = [ln.strip() for ln in _ANSI_RE.sub("", text).splitlines() if ln.strip()]
    return "\n".join(kept[-lines:])


def command_check(
    name: str, argv: Sequence[str], cwd: Path = ROOT, env: dict[str, str] | None = None
) -> Result:
    """Run *argv*; PASS on exit 0, FAIL otherwise with the output tail."""
    if shutil.which(argv[0]) is None and not Path(argv[0]).exists():
        return Result(name, FAIL, f"{argv[0]} not found on PATH")
    try:
        proc = subprocess.run(
            list(argv),
            cwd=cwd,
            capture_output=True,
            text=True,
            env={**os.environ, **(env or {})},
        )
    except OSError as exc:
        return Result(name, FAIL, str(exc))
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode == 0:
        return Result(name, PASS, _tail(out, 1))
    return Result(name, FAIL, f"exit {proc.returncode}\n{_tail(out)}")


def _tracked(*patterns: str) -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "--", *patterns],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        return sorted({p for pat in patterns for p in ROOT.glob(pat)})
    return [ROOT / p for p in out]


# -- checks ------------------------------------------------------------------


def _pythonpath_with_src() -> str:
    existing = os.environ.get("PYTHONPATH", "")
    return os.pathsep.join([str(ROOT / "src"), *([existing] if existing else [])])


def check_pytest() -> Result:
    # No bytecode: compiled tests carry their fake key patterns into
    # __pycache__, where the working-tree gitleaks check would find them.
    env = {"PYTHONPATH": _pythonpath_with_src(), "PYTHONDONTWRITEBYTECODE": "1"}
    return command_check(
        "pytest",
        # The unit suite as CI runs it (.github/workflows/ci.yml).
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/",
            "-q",
            "-p",
            "no:cacheprovider",
            "-n",
            "auto",
            "--dist",
            "loadfile",
            "-m",
            "not slow and not e2e and not integration",
            "--ignore=tests/spark",
            "--ignore=tests/test_e2e.py",
            "--ignore=tests/test_integration.py",
        ],
        env=env,
    )


def check_examples() -> Result:
    examples = sorted((ROOT / "examples").glob("*.yaml"))
    if not examples:
        return Result("examples", FAIL, "no examples/*.yaml found")
    saved_path = list(sys.path)
    sys.path.insert(0, str(ROOT / "src"))
    try:
        from lakebench.config import load_config
    finally:
        sys.path[:] = saved_path
    saved = {k: os.environ.get(k) for k in _EXAMPLE_ENV}
    os.environ.update({k: v for k, v in _EXAMPLE_ENV.items() if not os.environ.get(k)})
    failures = []
    try:
        for path in examples:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", DeprecationWarning)
                    load_config(path)
            except Exception as exc:  # noqa: BLE001 -- report every failure
                first = str(exc).strip().splitlines()[:3]
                failures.append(f"{path.name}: {' '.join(first)}")
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    if failures:
        return Result("examples", FAIL, "\n".join(failures))
    return Result("examples", PASS, f"{len(examples)} examples validate")


def _load_script(name: str):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_version_check(tag: str | None) -> Callable[[], Result]:
    def run() -> Result:
        cv = _load_script("check_version")
        problems = cv.check(tag)
        if problems:
            return Result("version", FAIL, "\n".join(problems))
        suffix = f", matches tag {tag}" if tag else ""
        return Result("version", PASS, f"{cv.package_version()}{suffix}")

    return run


def check_changelog() -> Result:
    cv = _load_script("check_version")
    version = cv.package_version()
    text = (ROOT / "CHANGELOG.md").read_text()
    if re.search(rf"^## \[{re.escape(version)}\]", text, re.MULTILINE):
        return Result("changelog", PASS, f"## [{version}] present")
    return Result("changelog", FAIL, f"CHANGELOG.md has no '## [{version}]' section")


def check_prose() -> Result:
    """scripts/prose_guard.py over every tracked file: em dashes, emoji, AI
    attribution, and allowlist entries that no longer match a hit."""
    skipped: list[str] = []
    problems = _load_script("prose_guard").check(skipped=skipped)
    note = f"; {len(skipped)} not scanned: {', '.join(skipped[:10])}" if skipped else ""
    if problems:
        shown = problems[:20] + (
            [f"... and {len(problems) - 20} more"] if len(problems) > 20 else []
        )
        detail = f"{len(problems)} prose problems{note}:\n" + "\n".join(shown)
        return Result("prose", FAIL, detail)
    return Result("prose", PASS, f"tracked files clean{note}")


def check_gitleaks() -> Result:
    exe = os.environ.get("GITLEAKS") or shutil.which("gitleaks")
    if not exe:
        return Result("gitleaks", SKIP, "gitleaks not installed (set GITLEAKS or add to PATH)")
    return command_check(
        "gitleaks",
        [
            exe,
            "dir",
            ".",
            "--config",
            ".gitleaks.toml",
            "--redact",
            "--no-banner",
            "--exit-code",
            "1",
        ],
    )


def _is_shallow(root: Path) -> bool | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return out == "true"


#: The history scanner, shared with the secrets-history CI job.
_HISTORY_SCAN = Path(__file__).resolve().parent / "gitleaks_history.py"


def check_gitleaks_history() -> Result:
    """Every commit reachable from HEAD, merges included, beyond the
    .gitleaksignore baseline."""
    exe = os.environ.get("GITLEAKS") or shutil.which("gitleaks")
    if not exe:
        return Result(
            "gitleaks-history", SKIP, "gitleaks not installed (set GITLEAKS or add to PATH)"
        )
    shallow = _is_shallow(ROOT)
    if shallow is None:
        return Result("gitleaks-history", FAIL, "not a git checkout; the history cannot be scanned")
    if shallow:
        return Result(
            "gitleaks-history",
            FAIL,
            "shallow clone: only part of the history would be scanned (git fetch --unshallow)",
        )
    if not (ROOT / ".gitleaksignore").is_file():
        return Result("gitleaks-history", FAIL, ".gitleaksignore (the history baseline) is missing")
    # The scanner CI's secrets-history job runs: every commit with
    # --remerge-diff (a merge commit's own change), commit and tag messages,
    # no inline allow, and a failure when gitleaks scanned nothing. The
    # baseline is this release worktree's own.
    return command_check(
        "gitleaks-history",
        [
            sys.executable,
            str(_HISTORY_SCAN),
            "--repo",
            str(ROOT),
            "--config",
            str(ROOT / ".gitleaks.toml"),
            "--ignore",
            str(ROOT / ".gitleaksignore"),
            "--gitleaks",
            exe,
        ],
        cwd=ROOT,
    )


def check_package_guard() -> Result:
    """scripts/package_guard.py over a fresh wheel and sdist: names, key
    patterns, gitleaks and the held-out absence check. A SKIP (no gitleaks,
    no hash file) or a PENDING-OA5 hit makes this check SKIP, so
    --require-all fails on it."""
    import tempfile

    guard = _load_script("package_guard")
    with tempfile.TemporaryDirectory(prefix="release-gate-package-") as tmp:
        work = Path(tmp)
        try:
            guard.build(work / "dist")
        except subprocess.CalledProcessError as exc:
            err = (exc.stderr or b"").decode("utf-8", "replace")
            return Result("package-guard", FAIL, f"python -m build failed\n{_tail(err)}")
        findings = guard.guard(work / "dist", work)
    lines = [f.render() for f in findings]
    statuses = {f.status for f in findings}
    if guard.FAIL in statuses:
        bad = [f.render() for f in findings if f.status == guard.FAIL]
        return Result("package-guard", FAIL, f"{len(bad)} failing:\n" + "\n".join(bad + lines))
    if statuses & {guard.SKIP, guard.PENDING}:
        open_ = [f.render() for f in findings if f.status in (guard.SKIP, guard.PENDING)]
        return Result("package-guard", SKIP, f"{len(open_)} not passed:\n" + "\n".join(open_))
    return Result("package-guard", PASS, "; ".join(f"{f.check} {f.detail}" for f in findings))


def check_pre_push_hook() -> Result:
    """The pre-push hook installed in this clone is the tracked one."""
    tracked = ROOT / "scripts" / "hooks" / "pre-push"
    try:
        # The hooks directory git runs: core.hooksPath when set, otherwise the
        # common directory's hooks/ (shared by every worktree).
        hooks = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-path", "hooks"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return Result("pre-push-hook", FAIL, "not a git checkout")
    installed = Path(hooks) / "pre-push"
    if not tracked.is_file():
        return Result("pre-push-hook", FAIL, "scripts/hooks/pre-push is missing")
    if not installed.is_file():
        return Result(
            "pre-push-hook",
            SKIP,
            f"no pre-push hook installed at {installed} (docs/development.md)",
        )
    if installed.read_bytes() != tracked.read_bytes():
        return Result(
            "pre-push-hook",
            FAIL,
            f"{installed} differs from scripts/hooks/pre-push; install the tracked copy",
        )
    return Result("pre-push-hook", PASS, f"{installed} equals scripts/hooks/pre-push")


def build_checks(tag: str | None = None) -> list[Check]:
    py = sys.executable
    return [
        Check(
            "ruff-check",
            lambda: command_check(
                "ruff-check", [py, "-m", "ruff", "check", "src", "tests", "scripts"]
            ),
            "ruff check src tests scripts",
        ),
        Check(
            "ruff-format",
            lambda: command_check(
                "ruff-format",
                [py, "-m", "ruff", "format", "--check", "src", "tests", "scripts"],
            ),
            "ruff format --check src tests scripts",
        ),
        Check(
            "mypy",
            lambda: command_check("mypy", [py, "-m", "mypy", "src/lakebench/"]),
            "mypy src/lakebench/",
        ),
        Check("pytest", check_pytest, "unit test suite"),
        Check(
            "cargo-fmt",
            lambda: command_check("cargo-fmt", ["cargo", "fmt", "--check"], cwd=RUST_DIR),
            "cargo fmt --check (datagen_rs)",
        ),
        Check(
            "cargo-clippy",
            lambda: command_check(
                "cargo-clippy",
                ["cargo", "clippy", "--all-targets", "--locked", "--", "-D", "warnings"],
                cwd=RUST_DIR,
            ),
            "cargo clippy --all-targets -D warnings (datagen_rs)",
        ),
        Check(
            "cargo-test",
            lambda: command_check(
                "cargo-test", ["cargo", "test", "--release", "--locked"], cwd=RUST_DIR
            ),
            "cargo test --release (datagen_rs)",
        ),
        Check("gitleaks", check_gitleaks, "secret scan of the working tree"),
        Check(
            "gitleaks-history",
            check_gitleaks_history,
            "secret scan of the history beyond .gitleaksignore",
        ),
        Check(
            "package-guard",
            check_package_guard,
            "the wheel, sdist and script maps ship no local docs, keys or held-out seeds",
        ),
        Check("pre-push-hook", check_pre_push_hook, "installed pre-push hook is the tracked one"),
        Check("examples", check_examples, "every examples/*.yaml validates"),
        Check("version", make_version_check(tag), "single version source; tag matches"),
        Check("changelog", check_changelog, "CHANGELOG.md has a section for the version"),
        Check("prose", check_prose, "no em dash, emoji or AI attribution in a tracked file"),
    ]


# -- runner and report -------------------------------------------------------


def run_checks(checks: Sequence[Check]) -> list[Result]:
    results = []
    for check in checks:
        try:
            res = check.run()
        except Exception as exc:  # noqa: BLE001 -- a crashing check is a failure
            res = Result(check.name, FAIL, f"check crashed: {type(exc).__name__}: {exc}")
        results.append(res)
    return results


def failures(results: Sequence[Result], require_all: bool = False) -> list[Result]:
    bad = {FAIL, SKIP} if require_all else {FAIL}
    return [r for r in results if r.status in bad]


def format_report(results: Sequence[Result], require_all: bool = False) -> str:
    lines = ["Release gate", ""]
    width = max((len(r.name) for r in results), default=0)
    for r in results:
        first = r.detail.splitlines()[0] if r.detail else ""
        lines.append(f"  {r.status:4}  {r.name:{width}}  {first}".rstrip())
    bad = failures(results, require_all)
    lines.append("")
    if not bad:
        skipped = [r.name for r in results if r.status == SKIP]
        note = f" ({len(skipped)} skipped: {', '.join(skipped)})" if skipped else ""
        lines.append(f"PASSED: {len(results)} checks{note}")
        return "\n".join(lines)
    lines.append(f"FAILED: {len(bad)} of {len(results)} checks")
    for r in bad:
        reason = "skipped, and --require-all is set" if r.status == SKIP else "failed"
        lines.append("")
        lines.append(f"-- {r.name} ({reason})")
        for detail_line in r.detail.splitlines():
            lines.append(f"   {detail_line}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run every release check.")
    parser.add_argument("--tag", help="release tag being cut, e.g. v1.6.0")
    parser.add_argument("--only", help="comma-separated check names to run")
    parser.add_argument("--require-all", action="store_true", help="treat SKIP as failure")
    parser.add_argument("--list", action="store_true", help="list checks and exit")
    args = parser.parse_args(argv)

    checks = build_checks(args.tag)
    if args.list:
        for c in checks:
            print(f"{c.name:15} {c.description}")
        return 0
    if args.only:
        wanted = {n.strip() for n in args.only.split(",") if n.strip()}
        unknown = wanted - {c.name for c in checks}
        if unknown:
            parser.error(f"unknown check(s): {', '.join(sorted(unknown))}")
        checks = [c for c in checks if c.name in wanted]

    results = run_checks(checks)
    print(format_report(results, args.require_all))
    return 1 if failures(results, args.require_all) else 0


if __name__ == "__main__":
    sys.exit(main())
