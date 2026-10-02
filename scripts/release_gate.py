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
    python scripts/release_gate.py --only perf-baselines --perf-run c360-batch-s10=<run id>
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


# UAT evidence for a release lives at this path (docs/releasing.md). The
# file must start its record with the exact heading below and contain at
# least one markdown table data row; the rows are the maintainers' record of
# which recipe x workload x mode runs passed, with run ids.
UAT_RESULTS = "uat/results-{version}.md"
UAT_HEADING = "# UAT results {version}"


def _table_data_rows(text: str) -> list[str]:
    rows = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("|")]
    # Drop the header row and the |---|---| separator of each table.
    data = []
    for i, row in enumerate(rows):
        is_sep = set(row.replace("|", "").replace(" ", "")) <= set("-:") and "-" in row
        next_is_sep = i + 1 < len(rows) and (
            set(rows[i + 1].replace("|", "").replace(" ", "")) <= set("-:") and "-" in rows[i + 1]
        )
        if not is_sep and not next_is_sep:
            data.append(row)
    return data


# A lakebench run id, as in lakebench-output/runs/run-<id>/ (run_id in
# metrics.json): YYYYMMDD-HHMMSS-<6 hex>.
_RUN_ID = re.compile(r"\b(\d{8}-\d{6}-[0-9a-f]{6})\b")
# Anything shaped like a run id; a token that matches this but not _RUN_ID
# (wrong case, separator or length) is a typo the check must not skip.
# Leading word characters are part of the token, so "120260926-..." is
# malformed rather than read as the id inside it.
_RUN_ID_LIKE = re.compile(r"[0-9A-Za-z_]*\d{8}[-_]\d{6}[-_][0-9A-Za-z]+")
# An explicit path to a metrics.json named in the results file.
_METRICS_PATH = re.compile(r"[\w./-]*metrics\.json")
# Where cited runs are looked up, relative to the repository root: the local
# runs directory and the checked-in evidence directories CI can see.
# $LAKEBENCH_PERF_RUNS_DIR, when set, is searched first.
UAT_RUN_DIRS = ("lakebench-output/runs", "uat/runs", "uat/perf")


def _metrics_run_id(path: Path) -> str | None:
    import json

    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    run_id = data.get("run_id") if isinstance(data, dict) else None
    return str(run_id) if run_id else None


def _unresolved_run_ids(rows: list[str]) -> tuple[list[str], list[str], list[str]]:
    """(cited run ids, ids with no matching metrics.json, malformed ids).

    Only the results table rows are read, so an id mentioned in prose (a
    superseded run) needs no evidence. A named metrics.json path counts only
    inside the repository, where the release workflow can see it.
    """
    text = "\n".join(rows)
    cited = sorted(set(_RUN_ID.findall(text)))
    malformed = sorted({t for t in _RUN_ID_LIKE.findall(text) if not _RUN_ID.fullmatch(t)})
    dirs = [ROOT / d for d in UAT_RUN_DIRS]
    env_dir = os.environ.get(PERF_RUNS_ENV)
    if env_dir:
        env_path = Path(env_dir)
        dirs.insert(0, env_path if env_path.is_absolute() else ROOT / env_path)
    root = ROOT.resolve()
    named: set[str] = set()
    for token in _METRICS_PATH.findall(text):
        path = (ROOT / token).resolve()
        if path.is_relative_to(root) and path.is_file():
            rid = _metrics_run_id(path)
            if rid:
                named.add(rid)
    missing = [
        rid
        for rid in cited
        if rid not in named
        and not any(_metrics_run_id(d / f"run-{rid}" / "metrics.json") == rid for d in dirs)
    ]
    return cited, missing, malformed


def _local_only_run_ids(rows: list[str], cited: list[str]) -> list[str]:
    """Cited ids whose evidence is only in directories the workflow cannot see."""
    text = "\n".join(rows)
    root = ROOT.resolve()
    named = set()
    for token in _METRICS_PATH.findall(text):
        path = (ROOT / token).resolve()
        if path.is_relative_to(root) and path.is_file():
            rid = _metrics_run_id(path)
            if rid:
                named.add(rid)
    checked_in = [ROOT / d for d in UAT_RUN_DIRS if d.startswith("uat/")]
    return [
        rid
        for rid in cited
        if rid not in named
        and not any(_metrics_run_id(d / f"run-{rid}" / "metrics.json") == rid for d in checked_in)
    ]


def check_uat_results() -> Result:
    cv = _load_script("check_version")
    version = cv.package_version()
    path = ROOT / UAT_RESULTS.format(version=version)
    rel = path.relative_to(ROOT)
    if not path.is_file():
        return Result("uat-results", FAIL, f"{rel} not found (see docs/releasing.md)")
    text = path.read_text()
    heading = UAT_HEADING.format(version=version)
    if heading not in [ln.rstrip() for ln in text.splitlines()]:
        return Result("uat-results", FAIL, f"{rel} has no '{heading}' heading line")
    rows = _table_data_rows(text)
    if not rows:
        return Result("uat-results", FAIL, f"{rel} has no results table rows")
    cited, missing, malformed = _unresolved_run_ids(rows)
    if malformed:
        return Result(
            "uat-results",
            FAIL,
            f"{rel}: malformed run id(s) (want YYYYMMDD-HHMMSS-<6 lowercase hex>): "
            + ", ".join(malformed),
        )
    if not cited:
        return Result("uat-results", FAIL, f"{rel} cites no run ids (YYYYMMDD-HHMMSS-xxxxxx)")
    if missing:
        where = ", ".join(f"{d}/run-<id>/metrics.json" for d in UAT_RUN_DIRS)
        return Result(
            "uat-results",
            FAIL,
            f"{rel}: {len(missing)} of {len(cited)} cited run id(s) have no metrics.json "
            f"({where}, or a metrics.json path named in the file): " + ", ".join(missing),
        )
    local_only = _local_only_run_ids(rows, cited)
    note = (
        f"; {len(local_only)} resolved only outside uat/ (lakebench-output/ is not "
        "committed), so the release workflow will fail until their metrics.json is "
        "checked in under uat/runs/"
        if local_only
        else ""
    )
    return Result(
        "uat-results",
        PASS,
        f"{rel}: {len(rows)} result rows, {len(cited)} run ids resolved{note}",
    )


# -- release evidence ------------------------------------------------------------
#
# records, support-record, freeze and expected-results read the cited run
# records through lakebench.metrics.release_record. They SKIP until the
# freeze is declared (uat/freeze-<version>); --require-all at the tag turns
# a SKIP into a FAIL.

FREEZE_FILE = "uat/freeze-{version}"
EXPECTED_RESULTS = "uat/expected-results-{version}.json"
_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SRC = Path(__file__).resolve().parents[1] / "src"

#: Paths a commit after the freeze may change, by prefix or exact path.
#: CHANGELOG.md (the release heading only) and README.md and docs/*.md
#: (generated blocks only) are checked by content in _post_freeze_problems.
POST_FREEZE_ALLOWED_PREFIXES = ("uat/", "docs/benchmarks/examples/")
POST_FREEZE_ALLOWED_PATHS = (
    "src/lakebench/config/validated_combinations.yaml",
    "benchmarks/perf/baselines.yaml",
)


def _lb(module: str):
    """A lakebench module, from this checkout's src."""
    import importlib

    if str(_SRC) not in sys.path:
        sys.path.insert(0, str(_SRC))
    return importlib.import_module(module)


def _git_out(*args: str) -> tuple[int, str]:
    r = subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False, timeout=60
    )
    return r.returncode, r.stdout


def _version() -> str:
    return _load_script("check_version").package_version()


def _freeze() -> tuple[str | None, str | None, str]:
    """(sha, problem, relative path) of the declared freeze; sha None and
    problem None when no freeze file exists yet."""
    rel = FREEZE_FILE.format(version=_version())
    path = ROOT / rel
    if not path.is_file():
        return None, None, rel
    sha = path.read_text().strip()
    if not _SHA40.match(sha):
        return None, f"{rel} does not hold a 40-hex commit sha", rel
    return sha, None, rel


def _no_freeze(name: str, rel: str) -> Result:
    return Result(name, SKIP, f"{rel} not declared yet; release evidence is checked at the freeze")


def _expected() -> tuple[dict | None, str | None, Path]:
    path = ROOT / EXPECTED_RESULTS.format(version=_version())
    if not path.is_file():
        return None, f"{path.relative_to(ROOT)} not found", path
    try:
        return _lb("lakebench.metrics.release_record").load_expected(path), None, path
    except (OSError, ValueError) as e:
        return None, f"{path.relative_to(ROOT)}: {e}", path


def _record_path(run_id: str, named: Sequence[str] = ()) -> Path | None:
    """The metrics.json of *run_id*: a path the results table names, else
    a run directory the uat-results check also searches."""
    root = ROOT.resolve()
    for token in named:
        p = (ROOT / token).resolve()
        if p.is_relative_to(root) and p.is_file() and _metrics_run_id(p) == run_id:
            return p
    dirs = [ROOT / d for d in UAT_RUN_DIRS]
    env_dir = os.environ.get(PERF_RUNS_ENV)
    if env_dir:
        env_path = Path(env_dir)
        dirs.insert(0, env_path if env_path.is_absolute() else ROOT / env_path)
    for d in dirs:
        p = d / f"run-{run_id}" / "metrics.json"
        if _metrics_run_id(p) == run_id:
            return p
    return None


def _changelog_heading_only(freeze: str) -> bool:
    rc, out = _git_out("diff", "-U0", freeze, "HEAD", "--", "CHANGELOG.md")
    if rc != 0:
        return False
    changed = [
        ln[1:] for ln in out.splitlines() if ln[:1] in "+-" and not ln.startswith(("+++", "---"))
    ]
    return all(ln.startswith("## [") for ln in changed)


def _generated_blocks_only(freeze: str, rel: str) -> str | None:
    """None when *rel* changed since *freeze* only inside its generated
    blocks and each block equals what its generator writes now; else why."""
    support = _lb("lakebench.config.support")
    names = support.DOCS_WITH_BLOCKS.get(rel)
    if not names:
        return f"{rel} changed after the freeze and carries no generated block"
    rc, old = _git_out("show", f"{freeze}:{rel}")
    if rc != 0:
        return f"{rel} is not in the freeze tree"
    new = (ROOT / rel).read_text()
    for name in names:
        block_new, block_old = support.block_in(new, name), support.block_in(old, name)
        if block_new is None or block_old is None:
            return f"{rel}: generated block {name} missing"
        if block_new != support.expected_block(name):
            return f"{rel}: generated block {name} is not what its generator writes"
        new = new.replace(block_new, f"<<{name}>>")
        old = old.replace(block_old, f"<<{name}>>")
    if new != old:
        return f"{rel} changed outside its generated blocks after the freeze"
    return None


def _post_freeze_problems(freeze: str) -> list[str]:
    # --no-renames: a moved file lists both paths, so a source file moved
    # under uat/ is seen leaving src/.
    rc, out = _git_out("diff", "--name-only", "--no-renames", freeze, "HEAD")
    if rc != 0:
        return [f"git diff {freeze[:12]} HEAD failed"]
    problems = []
    for path in [p for p in out.splitlines() if p]:
        if path.startswith(POST_FREEZE_ALLOWED_PREFIXES) or path in POST_FREEZE_ALLOWED_PATHS:
            continue
        if path == "CHANGELOG.md":
            if not _changelog_heading_only(freeze):
                problems.append("CHANGELOG.md changed after the freeze beyond the release heading")
            continue
        if path == "README.md" or (path.startswith("docs/") and path.count("/") == 1):
            why = _generated_blocks_only(freeze, path)
            if why:
                problems.append(why)
            continue
        problems.append(f"{path} changed after the freeze")
    return problems


def check_freeze() -> Result:
    sha, problem, rel = _freeze()
    if problem:
        return Result("freeze", FAIL, problem)
    if sha is None:
        return _no_freeze("freeze", rel)
    rc, _ = _git_out("merge-base", "--is-ancestor", sha, "HEAD")
    if rc != 0:
        return Result("freeze", FAIL, f"freeze {sha[:12]} is not an ancestor of HEAD")
    rc, status = _git_out("status", "--porcelain")
    if rc != 0 or status.strip():
        return Result("freeze", FAIL, "the working tree is not clean")
    problems = _post_freeze_problems(sha)
    if problems:
        return Result("freeze", FAIL, "; ".join(problems))
    return Result("freeze", PASS, f"freeze {sha[:12]}; post-freeze changes within the allowlist")


def check_expected_results() -> Result:
    sha, problem, rel = _freeze()
    if problem:
        return Result("expected-results", FAIL, problem)
    if sha is None:
        return _no_freeze("expected-results", rel)
    expected, why, path = _expected()
    if why:
        return Result("expected-results", FAIL, why)
    rc, last = _git_out("log", "-1", "--format=%H", "--", str(path.relative_to(ROOT)))
    last = last.strip()
    if rc != 0 or not last:
        return Result("expected-results", FAIL, f"{path.relative_to(ROOT)} is not committed")
    rc, _ = _git_out("merge-base", "--is-ancestor", last, sha)
    if rc != 0 or last == sha:
        return Result(
            "expected-results",
            FAIL,
            f"{path.relative_to(ROOT)} was last changed in {last[:12]}, not before the freeze "
            "(a fingerprint set newer than the freeze is refused)",
        )
    n = len((expected or {}).get("entries") or []) + len((expected or {}).get("continuous") or [])
    return Result("expected-results", PASS, f"{n} expected entries, committed before the freeze")


def _problems_of(
    run_id: str, freeze: str | None, expected: dict | None, named: Sequence[str] = ()
) -> list[str]:
    import json

    path = _record_path(run_id, named)
    if path is None:
        return ["no metrics.json"]
    rr = _lb("lakebench.metrics.release_record")
    return rr.record_problems(json.loads(path.read_text()), freeze, expected, root=ROOT)


def check_records() -> Result:
    sha, problem, rel = _freeze()
    if problem:
        return Result("records", FAIL, problem)
    if sha is None:
        return _no_freeze("records", rel)
    results_path = ROOT / UAT_RESULTS.format(version=_version())
    if not results_path.is_file():
        return Result("records", FAIL, f"{results_path.relative_to(ROOT)} not found")
    rows = _table_data_rows(results_path.read_text())
    cited, _missing, _malformed = _unresolved_run_ids(rows)
    if not cited:
        return Result("records", FAIL, "the results table cites no run ids")
    expected, _why, _path = _expected()
    named = _METRICS_PATH.findall("\n".join(rows))
    bad = {rid: p for rid in cited if (p := _problems_of(rid, sha, expected, named))}
    if bad:
        return Result(
            "records",
            FAIL,
            f"{len(bad)} of {len(cited)} cited records are not release evidence: "
            + "; ".join(f"{rid}: {', '.join(p)}" for rid, p in sorted(bad.items())),
        )
    return Result("records", PASS, f"{len(cited)} cited records are release evidence")


def make_support_record_check(tag: str | None) -> Callable[[], Result]:
    def run() -> Result:
        if not tag:
            return Result("support-record", SKIP, "checked at a release tag (--tag)")
        support = _lb("lakebench.config.support")
        rr = _lb("lakebench.metrics.release_record")
        record = support.load_validation_record()
        if not record:
            return Result("support-record", FAIL, "validated_combinations.yaml lists nothing")
        sha, problem, _rel = _freeze()
        expected, _why, _path = _expected()
        problems = []
        keys_run: set[tuple] = set()
        for workload, mode, recipe, _scale in rr.RELEASE_MATRIX:
            if (workload, recipe, support.canonical_mode(mode)) not in record:
                problems.append(f"no validated entry for {workload} {recipe} {mode}")
        for (workload, recipe, mode), v in sorted(record.items()):
            if sha and not sha.startswith(str(v.tree)):
                problems.append(
                    f"{workload} {recipe} {mode}: validated on tree {v.tree}, not the freeze "
                    f"{sha[:12]}"
                )
            for run_id in v.runs:
                rid = run_id.removeprefix("run-")
                path = ROOT / "uat" / "runs" / f"run-{rid}" / "metrics.json"
                if not path.is_file():
                    problems.append(f"{rid}: not in uat/runs/")
                    continue
                import json

                data = json.loads(path.read_text())
                for p in rr.record_problems(data, sha, expected, root=ROOT):
                    problems.append(f"{rid}: {p}")
                key = rr.record_key(data)
                if key is None or (key[0], key[2], support.canonical_mode(key[1])) != (
                    workload,
                    recipe,
                    mode,
                ):
                    problems.append(f"{rid}: record is not {workload} {recipe} {mode}")
                elif key:
                    keys_run.add(key)
        for row in rr.RELEASE_MATRIX:
            if (row[0], row[1], row[2], float(row[3])) not in keys_run:
                problems.append(
                    f"no validated run for {row[0]} {row[2]} {row[1]} at scale {row[3]:g}"
                )
        if problem:
            problems.append(problem)
        if problems:
            return Result("support-record", FAIL, "; ".join(problems))
        return Result("support-record", PASS, f"{len(record)} validated entries, every run checked")

    return run


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


# Performance-regression gate (docs/perf-regression-gate.md). Candidate runs
# are searched in the local runs directory (or $LAKEBENCH_PERF_RUNS_DIR) and
# in uat/perf/, where a release checks in the metrics.json of its perf runs
# so CI can gate them. --perf-run NAME=RUN names a run explicitly.
PERF_STORE = ROOT / "benchmarks" / "perf" / "baselines.yaml"
PERF_RUNS_ENV = "LAKEBENCH_PERF_RUNS_DIR"
PERF_UAT_RUNS = "uat/perf"


def make_perf_check(
    perf_runs: dict[str, str] | None = None,
    store_path: Path | None = None,
    runs_dirs: list[Path] | None = None,
) -> Callable[[], Result]:
    """FAIL when a required pinned config has no baseline, no run, or regressed."""

    def run() -> Result:
        saved_path = list(sys.path)
        sys.path.insert(0, str(ROOT / "src"))
        try:
            from lakebench.metrics import perf_gate as pg
        finally:
            sys.path[:] = saved_path
        local = Path(os.environ.get(PERF_RUNS_ENV) or ROOT / "lakebench-output" / "runs")
        rdirs = runs_dirs if runs_dirs is not None else [local, ROOT / PERF_UAT_RUNS]
        try:
            store = pg.load_store(store_path or PERF_STORE)
        except pg.PerfGateError as exc:
            return Result("perf-baselines", FAIL, str(exc))
        passed, lines = pg.release_check(store, rdirs, perf_runs)
        failed = [ln for ln in lines if ln.startswith("FAIL")]
        if passed:
            return Result("perf-baselines", PASS, "\n".join(lines))
        summary = f"{len(failed)} required pinned config(s) failed"
        return Result("perf-baselines", FAIL, "\n".join([summary, *lines]))

    return run


def build_checks(tag: str | None = None, perf_runs: dict[str, str] | None = None) -> list[Check]:
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
        Check("pre-push-hook", check_pre_push_hook, "installed pre-push hook is the tracked one"),
        Check("examples", check_examples, "every examples/*.yaml validates"),
        Check("version", make_version_check(tag), "single version source; tag matches"),
        Check("changelog", check_changelog, "CHANGELOG.md has a section for the version"),
        Check("em-dashes", check_em_dashes, "no U+2014 in *.md, .github/, examples/, CLI"),
        Check("uat-results", check_uat_results, "uat/results-<version>.md exists"),
        Check("records", check_records, "every cited run record is release evidence"),
        Check(
            "support-record",
            make_support_record_check(tag),
            "every release row is validated by runs that are release evidence",
        ),
        Check("freeze", check_freeze, "declared freeze; only allowed changes after it"),
        Check(
            "expected-results",
            check_expected_results,
            "expected-results file committed before the freeze",
        ),
        Check(
            "perf-baselines",
            make_perf_check(perf_runs),
            "required pinned perf configs have a baseline and no regression",
        ),
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
    parser.add_argument(
        "--perf-run",
        action="append",
        metavar="NAME=RUN",
        help="run (id or metrics.json) to gate pinned perf config NAME with; repeatable",
    )
    args = parser.parse_args(argv)

    perf_runs = {}
    for item in args.perf_run or []:
        if "=" not in item:
            parser.error(f"--perf-run expects NAME=RUN, got {item!r}")
        name, run_ref = item.split("=", 1)
        perf_runs[name] = run_ref
    checks = build_checks(args.tag, perf_runs)
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
