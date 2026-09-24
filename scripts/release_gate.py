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

EM_DASH = "\u2014"


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
    env = {"PYTHONPATH": _pythonpath_with_src()}
    return command_check(
        "pytest",
        [sys.executable, "-m", "pytest", "tests/", "-q", "-p", "no:cacheprovider"],
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


def find_em_dashes(paths: Sequence[Path]) -> list[str]:
    """Return 'path:line' for every line containing U+2014."""
    hits = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for n, line in enumerate(lines, 1):
            if EM_DASH in line:
                hits.append(f"{path.relative_to(ROOT)}:{n}")
    return hits


# Everything a reader of the repository or the CLI sees. CLI help strings
# live in the Python sources under src/lakebench/cli, so those are scanned
# whole.
EM_DASH_SCOPE = ("*.md", "**/*.md", ".github/**", "examples/**", "src/lakebench/cli/**/*.py")


def check_em_dashes() -> Result:
    paths = [p for p in _tracked(*EM_DASH_SCOPE) if p.is_file()]
    hits = find_em_dashes(paths)
    if hits:
        shown = hits[:20] + ([f"... and {len(hits) - 20} more"] if len(hits) > 20 else [])
        return Result("em-dashes", FAIL, f"{len(hits)} lines with U+2014:\n" + "\n".join(shown))
    return Result("em-dashes", PASS, f"{len(paths)} files clean")


# UAT evidence for a release lives at this path (docs/releasing.md). The file
# must exist and name the version; its content is the maintainers' record of
# which recipe x workload x mode runs passed, with run ids.
UAT_RESULTS = "uat/results-{version}.md"


def check_uat_results() -> Result:
    cv = _load_script("check_version")
    version = cv.package_version()
    path = ROOT / UAT_RESULTS.format(version=version)
    rel = path.relative_to(ROOT)
    if not path.is_file():
        return Result("uat-results", FAIL, f"{rel} not found (see docs/releasing.md)")
    if version not in path.read_text():
        return Result("uat-results", FAIL, f"{rel} does not mention version {version}")
    return Result("uat-results", PASS, str(rel))


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
        Check("examples", check_examples, "every examples/*.yaml validates"),
        Check("version", make_version_check(tag), "single version source; tag matches"),
        Check("changelog", check_changelog, "CHANGELOG.md has a section for the version"),
        Check("em-dashes", check_em_dashes, "no U+2014 in *.md, .github/, examples/, CLI"),
        Check("uat-results", check_uat_results, "uat/results-<version>.md exists"),
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
            print(f"{c.name:14} {c.description}")
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
