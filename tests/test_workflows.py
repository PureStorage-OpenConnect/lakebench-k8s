"""CI and release workflow hygiene (QA-11, QA-3).

Runner labels are pinned so a GitHub move of ``ubuntu-latest`` cannot change
the build mid-release; every action is pinned by SHA and has a recorded
runtime so a retired Node version shows up here and not on a release day;
the coverage leg must be a matrix entry or coverage stops silently; and the
release jobs that write anywhere run only in the upstream repository.
Offline: reads the workflow files and .github/action-runtimes.json only.
"""

from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github" / "workflows"
UPSTREAM = "PureStorage-OpenConnect/lakebench-k8s"

_spec = importlib.util.spec_from_file_location(
    "check_action_runtimes", ROOT / "scripts" / "check_action_runtimes.py"
)
car = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(car)

_FLOATING = re.compile(r"^(ubuntu|macos|windows)-latest\b")


def _load(name: str) -> dict:
    return yaml.safe_load((WF / name).read_text())


def _workflows() -> dict[str, dict]:
    return {p.name: yaml.safe_load(p.read_text()) for p in sorted(WF.glob("*.y*ml"))}


def _on(wf: dict) -> dict:
    # PyYAML reads the bare key `on` as boolean True.
    return wf.get("on", wf.get(True)) or {}


def _strings(node) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [s for v in node.values() for s in _strings(v)]
    if isinstance(node, list):
        return [s for v in node for s in _strings(v)]
    return []


def test_no_floating_runner_labels():
    bad = []
    for name, wf in _workflows().items():
        for job_name, job in (wf.get("jobs") or {}).items():
            # runs-on may be a label, a list, or a matrix expression whose
            # values live under strategy.matrix (including include entries).
            labels = _strings(job.get("runs-on")) + _strings(
                (job.get("strategy") or {}).get("matrix")
            )
            bad += [f"{name}:{job_name}: {lab}" for lab in labels if _FLOATING.match(lab)]
    assert not bad, bad


def test_actions_pinned_by_sha():
    pinned = re.compile(r"^\s*-?\s*uses:\s*[\w.-]+/[\w./-]+@[0-9a-f]{40} # \S")
    bad = []
    for path in sorted(WF.glob("*.y*ml")):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            m = re.match(r"^\s*-?\s*uses:\s*(\S+)", line)
            if not m or m.group(1).startswith(("./", "docker://")):
                continue
            if not pinned.match(line):
                bad.append(f"{path.name}:{n}: {line.strip()}")
    assert not bad, "want `owner/repo@<40-hex sha> # <tag or ref>`: " + repr(bad)


def test_every_action_has_a_runtime_entry():
    uses = car.workflow_uses(WF)
    runtimes = car.load_runtimes()
    missing = sorted(set(uses) - set(runtimes))
    stale = sorted(set(runtimes) - set(uses))
    assert not missing, f"no entry in .github/action-runtimes.json: {missing}"
    assert not stale, f"entries no workflow uses: {stale}"


def test_no_node20_actions():
    runtimes = car.load_runtimes()
    bad = {}
    for ref in car.workflow_uses(WF):
        using = runtimes.get(ref)
        if using is None or using in car.RETIRED_RUNTIMES:
            bad[ref] = using
    assert not bad, f"unknown or retired runtime: {bad}"
    assert not [v for v in runtimes.values() if v in car.RETIRED_RUNTIMES]


def test_runtime_script_offline_check_passes():
    assert car.offline_problems(car.workflow_uses(WF), car.load_runtimes()) == []


def test_runtime_verify_reports_drift():
    runtimes = {"actions/checkout@" + "a" * 40: "node24", "x/y@" + "b" * 40: "composite"}
    served = {"actions/checkout@" + "a" * 40: "node20", "x/y@" + "b" * 40: "composite"}
    problems = car.verify_problems(runtimes, served.__getitem__)
    assert len(problems) == 1 and "node20" in problems[0]


def test_runtime_verify_skips_without_token(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    assert car.main(["--verify"]) == 0
    assert "SKIP" in capsys.readouterr().out
    # In CI a missing token must fail, or the lint step passes having read nothing.
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert car.main(["--verify"]) == 1


def _unit_matrix(ci: dict) -> list[str]:
    return [str(v) for v in ci["jobs"]["test"]["strategy"]["matrix"]["python-version"]]


def _coverage_versions(ci: dict) -> set[str]:
    """Python versions named in the test job's coverage conditions."""
    found: set[str] = set()
    for step in ci["jobs"]["test"]["steps"]:
        text = f"{step.get('if', '')} {step.get('run', '')}"
        if "--cov" in text or "check_coverage.py" in text:
            found |= set(re.findall(r"matrix\.python-version\s*==\s*'([\d.]+)'", text))
    return found


def _coverage_leg_problems(ci: dict) -> list[str]:
    versions = _coverage_versions(ci)
    if not versions:
        return ["no coverage condition names a matrix Python version"]
    matrix = set(_unit_matrix(ci))
    return [
        f"coverage runs on {v}, which is not in the matrix {sorted(matrix)}"
        for v in versions - matrix
    ]


def test_coverage_leg_is_in_matrix():
    assert _coverage_leg_problems(_load("ci.yml")) == []

    # The silent case: the matrix loses 3.11 but the coverage conditions
    # still name it, so coverage and the floor check never run and CI is green.
    fixture = yaml.safe_load(
        """
jobs:
  test:
    strategy:
      matrix:
        python-version: ["3.10", "3.13"]
    steps:
      - name: Run unit tests
        run: >-
          pytest tests/
          ${{ matrix.python-version == '3.11' && '--cov=lakebench' || '' }}
      - name: Coverage floors
        if: matrix.python-version == '3.11'
        run: python scripts/check_coverage.py --suite unit coverage-unit.json
"""
    )
    assert _coverage_leg_problems(fixture)


def test_unit_matrix_is_310_and_313():
    assert _unit_matrix(_load("ci.yml")) == ["3.10", "3.13"]


_GUARD = f"github.repository == '{UPSTREAM}'"


def test_publish_jobs_guarded_by_repository():
    jobs = _load("release.yml")["jobs"]
    for name in ("github-release", "publish"):
        cond = str(jobs[name].get("if", ""))
        assert _GUARD in cond, (name, cond)
        # An `||` would let a fork through on the other branch, and a status
        # function would run the job after a failed build.
        for bad in ("||", "always()", "cancelled()", "failure()"):
            assert bad not in cond, (name, cond)


def test_release_has_no_workflow_dispatch():
    assert "workflow_dispatch" not in _on(_load("release.yml"))
    # The check reads the trigger the way GitHub does.
    assert "workflow_dispatch" in _on(yaml.safe_load("on:\n  workflow_dispatch:\n"))


def test_release_dry_run_only_on_forks():
    jobs = _load("release.yml")["jobs"]
    dry = jobs["release-dry-run"]
    assert str(dry["if"]).strip() == f"github.repository != '{UPSTREAM}'"
    assert set(dry["needs"]) == set(jobs["github-release"]["needs"])
    assert (dry.get("permissions") or {}).get("contents") != "write"
    uses = [str(s.get("uses", "")) for s in dry["steps"]]
    assert not [u for u in uses if u.startswith(("softprops/", "pypa/"))], uses

    # It downloads exactly what the real jobs download, with the same inputs.
    def downloads(job: dict) -> list[dict]:
        return [
            s["with"]
            for s in job["steps"]
            if str(s.get("uses", "")).startswith("actions/download-artifact@")
        ]

    real = downloads(jobs["github-release"]) + downloads(jobs["publish"])
    assert sorted(map(str, downloads(dry))) == sorted(map(str, real))
    runs = " ".join(str(s.get("run", "")) for s in dry["steps"])
    assert "ls -R" in runs and "sha256sum" in runs


# -- QA-3: fail-fast, triggers, concurrency, pip cache, skip reasons ---------


def _matrix_jobs():
    for name, wf in _workflows().items():
        for job_name, job in (wf.get("jobs") or {}).items():
            if "matrix" in (job.get("strategy") or {}):
                yield name, job_name, job


def test_matrix_does_not_fail_fast():
    jobs = list(_matrix_jobs())
    assert jobs, "no matrix job found; the check reads nothing"
    bad = [f"{n}:{j}" for n, j, job in jobs if job["strategy"].get("fail-fast") is not False]
    # fail-fast defaults to true, so a sibling leg is cancelled and its result lost.
    assert not bad, f"matrix jobs without `fail-fast: false`: {bad}"


def test_prs_to_integrate_trigger_ci():
    on = _on(_load("ci.yml"))
    assert "integrate/**" in on["pull_request"]["branches"]
    assert "main" in on["pull_request"]["branches"]
    # Train and look branches are pushed, never opened as PRs, and need CI.
    assert on["push"]["branches"] == ["**"]


_TOKEN = re.compile(r"\s*(?:(\|\||&&|==|!=|[()!,])|'((?:[^']|'')*)'|([A-Za-z_][\w.\-]*))")


def _eval_expr(expr: str, ctx: dict[str, str]):
    """Evaluate the GitHub expression subset the concurrency group uses:
    string literals, context names, == != ! && || and startsWith/format."""
    toks: list[tuple[str, str]] = []
    pos = 0
    expr = expr.strip()
    while pos < len(expr):
        m = _TOKEN.match(expr, pos)
        assert m and m.end() > pos, f"cannot parse {expr[pos:]!r}"
        op, lit, name = m.groups()
        toks.append(
            ("op", op)
            if op
            else ("str", lit.replace("''", "'"))
            if lit is not None
            else ("name", name)
        )
        pos = m.end()
    toks.append(("end", ""))
    i = 0

    def peek(v=None):
        return toks[i][1] == v if v else toks[i]

    def take():
        nonlocal i
        i += 1
        return toks[i - 1]

    def primary():
        kind, val = take()
        if kind == "op" and val == "(":
            v = disj()
            assert take()[1] == ")"
            return v
        if kind == "op" and val == "!":
            return not primary()
        if kind == "str":
            return val
        assert kind == "name", (kind, val)
        if peek("("):
            take()
            args = [disj()]
            while peek(","):
                take()
                args.append(disj())
            assert take()[1] == ")"
            if val == "startsWith":
                return str(args[0]).lower().startswith(str(args[1]).lower())
            if val == "format":
                return str(args[0]).format(*args[1:])
            raise AssertionError(f"function {val} not supported")
        return ctx[val]

    def cmp():
        v = primary()
        while peek("==") or peek("!="):
            op = take()[1]
            r = primary()
            # GitHub compares strings ignoring case.
            a, b = (x.lower() if isinstance(x, str) else x for x in (v, r))
            v = (a == b) if op == "==" else (a != b)
        return v

    def conj():
        v = cmp()
        while peek("&&"):
            take()
            r = cmp()
            v = r if v else v
        return v

    def disj():
        v = conj()
        while peek("||"):
            take()
            r = conj()
            v = v if v else r
        return v

    out = disj()
    assert peek()[0] == "end", toks[i:]
    return out


def _render(template: str, ctx: dict[str, str]) -> str:
    return re.sub(
        r"\$\{\{(.*?)\}\}", lambda m: str(_eval_expr(m.group(1), ctx)), template, flags=re.S
    )


def test_expression_evaluator_follows_github_rules():
    ctx = {"a": "x", "e": ""}
    assert _eval_expr("a == 'x' && 'yes' || 'no'", ctx) == "yes"
    assert _eval_expr("a == 'y' && 'yes' || 'no'", ctx) == "no"
    assert _eval_expr("!(e) && startsWith('refs/heads/Main', 'refs/heads/main')", ctx) is True
    assert _eval_expr("format('-{0}', a)", ctx) == "-x"
    assert _eval_expr("a == 'X'", ctx) is True


def test_concurrency_cancels_only_lane_and_pr_runs():
    conc = _load("ci.yml")["concurrency"]
    assert conc["cancel-in-progress"] is True

    def group(ref: str, run_id: str, workflow: str = "CI") -> str:
        ctx = {"github.workflow": workflow, "github.ref": ref, "github.run_id": run_id}
        return _render(conc["group"], ctx).lower()  # GitHub matches groups ignoring case

    # Two runs of a lane branch or a PR share a group, so the newer cancels the older.
    for ref in ("refs/heads/lane/v17-x", "refs/heads/lane/ci-hygiene", "refs/pull/7/merge"):
        assert group(ref, "1") == group(ref, "2"), ref
    # Any other ref keeps every run: a shared group would cancel a pending one.
    # Train runs are merge evidence; unknown branches default to kept.
    for ref in (
        "refs/heads/integrate/v1.5.0",
        "refs/heads/main",
        "refs/heads/train/1003-am",
        "refs/heads/release/1.7",
        "refs/heads/lanes-old",
        "refs/tags/v1.7.0",
    ):
        assert group(ref, "1") != group(ref, "2"), ref
    # release.yml calls ci.yml on a tag; github.workflow is then the caller's.
    assert group("refs/tags/v1.7.0", "1", "Release") != group("refs/tags/v1.7.0", "2", "Release")
    # Different refs never share a group.
    assert group("refs/heads/lane/a", "1") != group("refs/heads/lane/b", "1")


def _pip_cache_keys() -> dict[str, tuple]:
    """{job: (python, dependency paths)} for each ci.yml job that installs
    `.[dev]`; the paths are None when the job sets no pip cache."""
    out = {}
    for job_name, job in _load("ci.yml")["jobs"].items():
        steps = job.get("steps") or []
        if not any('".[dev]"' in str(s.get("run", "")) for s in steps):
            continue
        for step in steps:
            if str(step.get("uses", "")).startswith("actions/setup-python@"):
                w = step.get("with") or {}
                paths = str(w.get("cache-dependency-path", "")).split() or None
                out[job_name] = (
                    str(w.get("python-version")),
                    paths if w.get("cache") == "pip" else None,
                )
    return out


def test_setup_python_caches_pip():
    keys = _pip_cache_keys()
    assert keys, "no job installs .[dev]; the check reads nothing"
    bad = [j for j, (_, paths) in keys.items() if not paths or "pyproject.toml" not in paths]
    assert not bad, f"jobs installing .[dev] without a pip cache keyed on pyproject.toml: {bad}"
    # Caches are immutable and the first job to finish saves the key, so a job
    # that installs more (pyspark) than another on the same Python needs its
    # own key, or its extra packages are never cached.
    by_key: dict[tuple, list[str]] = {}
    for job, (py, paths) in keys.items():
        by_key.setdefault((py, tuple(paths or ())), []).append(job)
    for jobs in by_key.values():
        extras = {
            j: "pyspark"
            in " ".join(str(s.get("run", "")) for s in _load("ci.yml")["jobs"][j]["steps"])
            for j in jobs
        }
        assert len(set(extras.values())) == 1, (
            f"jobs share a pip cache key but install different packages: {extras}"
        )


def test_unit_step_prints_skips_and_does_not_stop_early():
    steps = _load("ci.yml")["jobs"]["test"]["steps"]
    run = next(str(s["run"]) for s in steps if s.get("name") == "Run unit tests")
    args = run.split()
    # -x hides every failure after the first; -rs prints each skip reason.
    assert "-x" not in args and "--exitfirst" not in args, run
    assert "-rs" in args, run


# -- QA-8: the AML statistics (slow) merge job --------------------------------


def _slow_job_runs(event: str, ref: str, base_ref: str = "") -> bool:
    cond = _load("ci.yml")["jobs"]["aml-slow"]["if"]
    ctx = {"github.event_name": event, "github.ref": ref, "github.base_ref": base_ref}
    return bool(_eval_expr(cond, ctx))


def test_aml_slow_job_runs_on_integrate_main_tags_and_prs_to_integrate_and_main():
    assert _slow_job_runs("push", "refs/heads/integrate/v1.5.0")
    assert _slow_job_runs("push", "refs/heads/main")
    assert _slow_job_runs("push", "refs/heads/train/1003-am")
    # release.yml calls ci.yml; inside the call event_name is the caller's push.
    assert _slow_job_runs("push", "refs/tags/v1.7.0")
    assert _slow_job_runs("pull_request", "refs/pull/9/merge", "integrate/v1.5.0")
    # A skipped job satisfies a required check, so the PR to main runs it too.
    assert _slow_job_runs("pull_request", "refs/pull/9/merge", "main")
    assert not _slow_job_runs("push", "refs/heads/lane/v17-x")
    assert not _slow_job_runs("pull_request", "refs/pull/9/merge", "lane/v17-x")


def test_aml_slow_job_selects_slow_tests_on_pinned_libraries():
    job = _load("ci.yml")["jobs"]["aml-slow"]
    assert job["name"] == "AML statistics (slow)"
    assert int(job["timeout-minutes"]) <= 45
    runs = [str(s.get("run", "")) for s in job["steps"]]
    assert any("tests/test_reference_pins.py" in r for r in runs)
    unit = next(r for r in runs if "--ignore=tests/spark" in r)
    assert '-m "slow and not e2e and not integration"' in unit
    assert "--ignore=tests/test_e2e.py" in unit and "--ignore=tests/test_integration.py" in unit
    assert any(r.startswith("pytest tests/spark") and "-m slow" in r for r in runs)
    # The power-simulation hash guard skips on the pins; its own step must not.
    ps = [s for s in job["steps"] if "test_power_sim_output_hash_is_recorded" in str(s.get("run"))]
    assert ps and ps[0]["env"]["LB_REQUIRE_POWER_SIM_HASH"] == "1"
    # A skipped needed job would skip the build, so nothing needs aml-slow.
    needs = [n for j in _load("ci.yml")["jobs"].values() for n in j.get("needs", [])]
    assert "aml-slow" not in needs


def _has_slow_mark(node) -> bool:
    # `@pytest.mark.slow`, or a module alias such as `SLOW = pytest.mark.slow`.
    return any("slow" in ast.unparse(d).lower() for d in getattr(node, "decorator_list", []))


def test_aml_statistics_tests_are_marked_slow():
    """The tests the slow job exists for carry the mark, so the job is not empty
    and the fast path (QA-6) can deselect them."""
    want = {
        "tests/test_aml_scale_invariance.py": None,  # the whole module
        "tests/test_aml_fidelity_gate.py": "test_real_preregistration_runs_end_to_end",
        "tests/spark/test_score_reference_gate_spark.py": "test_fidelity_gate_over_silver",
    }
    for rel, func in want.items():
        tree = ast.parse((ROOT / rel).read_text())
        if func is None:
            marks = [
                ast.unparse(n.value)
                for n in tree.body
                if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in n.targets)
            ]
            assert any("slow" in m for m in marks), rel
        else:
            fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == func)
            assert _has_slow_mark(fn), f"{rel}::{func}"


# -- OSS-2: the history scan job ----------------------------------------------


def test_secrets_history_job_scans_full_history_and_requires_gitleaks():
    jobs = _load("ci.yml")["jobs"]
    job = jobs["secrets-history"]
    assert job["steps"][0]["with"]["fetch-depth"] == 0
    scan = next(str(s["run"]) for s in job["steps"] if s.get("name") == "Scan history")
    # The shared scanner (history with --remerge-diff, messages, no inline
    # allow, fails on an empty scan) with a trusted ref's baseline.
    assert 'scan="$PWD/scripts/gitleaks_history.py"' in scan
    assert 'python "$scan" --repo . --config "$config" --ignore "$ignore"' in scan
    assert 'git show "$cfg_ref:.gitleaks.toml"' in scan
    tests = next(s for s in job["steps"] if "test_pre_push_hook.py" in str(s.get("run", "")))
    # Without this the gitleaks-backed tests would skip and the step pass.
    assert tests["env"]["LB_REQUIRE_GITLEAKS"] == "1"
    assert "secrets-history" in jobs["build"]["needs"]


def _history_trust_order(event: str, ref: str | None, base_ref: str = "") -> list[str]:
    """Run the trust-order block of the history scan step for one event,
    under ``set -euo pipefail`` as the step runs it; ``ref=None`` leaves REF unset."""
    import subprocess

    job = _load("ci.yml")["jobs"]["secrets-history"]
    scan = next(str(s["run"]) for s in job["steps"] if s.get("name") == "Scan history")
    m = re.search(r"# trust-order begin\n(.*?)# trust-order end", scan, re.S)
    assert m, "the Scan history step has no trust-order block"
    env = {"EVENT": event, "BASE_REF": base_ref, "PATH": "/usr/bin:/bin"}
    if ref is not None:
        env["REF"] = ref
    out = subprocess.run(
        ["bash", "-c", "set -euo pipefail\n" + m.group(1) + 'printf "%s" "$trust_order"'],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return out.split()


MAIN_FIRST = ["origin/main", "origin/integrate/v1.5.0"]
INTEGRATE_FIRST = ["origin/integrate/v1.5.0", "origin/main"]


def test_history_scan_trusts_main_for_what_goes_to_main():
    assert _history_trust_order("push", "refs/heads/main") == MAIN_FIRST
    assert _history_trust_order("pull_request", "refs/pull/7/merge", "main") == MAIN_FIRST
    # release.yml calls ci.yml on the tag push.
    assert _history_trust_order("push", "refs/tags/v1.7.0") == MAIN_FIRST


def test_history_scan_trusts_integrate_first_for_every_other_ref():
    for ref in (
        "refs/heads/integrate/v1.5.0",
        "refs/heads/train/1002-e",
        "refs/heads/lane/v17-platform-sd16",
        "refs/heads/maintained",  # a name that only starts like main
    ):
        assert _history_trust_order("push", ref) == INTEGRATE_FIRST, ref
    assert (
        _history_trust_order("pull_request", "refs/pull/8/merge", "integrate/v1.5.0")
        == INTEGRATE_FIRST
    )
    # REF unset (a caller that sets only EVENT and BASE_REF) is not an error.
    assert _history_trust_order("push", None) == INTEGRATE_FIRST


def test_history_scan_config_and_baseline_follow_the_trust_order():
    job = _load("ci.yml")["jobs"]["secrets-history"]
    scan = next(str(s["run"]) for s in job["steps"] if s.get("name") == "Scan history")
    assert scan.count("for ref in $trust_order; do") == 2
    assert "for ref in origin/" not in scan
    step = next(s for s in job["steps"] if s.get("name") == "Scan history")
    assert step["env"]["REF"] == "${{ github.ref }}"


# -- OSS-5: the wheel in a clean venv -----------------------------------------


def test_clean_venv_job_installs_the_wheel_alone_and_with_aml():
    jobs = _load("ci.yml")["jobs"]
    job = jobs["clean-venv"]
    assert job["needs"] == "build"
    assert job["strategy"]["matrix"]["python-version"] == ["3.10", "3.13"]
    runs = {s.get("name"): str(s.get("run", "")) for s in job["steps"]}
    plain, aml = runs["Wheel alone"], runs["Wheel with the aml extra"]
    assert "python -m venv" in plain and 'pip" install dist/*.whl' in plain
    assert "walk_packages" in plain and 'lakebench" version' in plain
    assert "python -m venv" in aml and '"$(ls dist/*.whl)[aml]"' in aml
    assert "REFERENCE_PY_DEPS" in aml and "lakebench.aml.reference_score" in aml
    # Neither step installs from the checkout.
    assert "-e " not in plain + aml and "pip install ." not in plain + aml


def test_clean_venv_wheel_artifact_name_is_its_own():
    # release.yml calls ci.yml in the same run, and artifact names are per run.
    build = _load("ci.yml")["jobs"]["build"]
    (upload,) = [s for s in build["steps"] if "upload-artifact" in str(s.get("uses", ""))]
    name = upload["with"]["name"]
    (download,) = [
        s
        for s in _load("ci.yml")["jobs"]["clean-venv"]["steps"]
        if "download-artifact" in str(s.get("uses", ""))
    ]
    assert download["with"]["name"] == name
    release_names = {
        str((s.get("with") or {}).get("name", ""))
        for j in _load("release.yml")["jobs"].values()
        for s in j.get("steps") or []
        if "upload-artifact" in str(s.get("uses", ""))
    }
    assert name not in release_names and not name.startswith("lakebench-")


def test_clean_venv_checks_the_reference_packages_test_reference_pins_checks():
    import importlib.util

    spec = importlib.util.spec_from_file_location("trp", ROOT / "tests" / "test_reference_pins.py")
    assert spec and spec.loader
    trp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(trp)
    job = _load("ci.yml")["jobs"]["clean-venv"]
    (aml,) = [str(s["run"]) for s in job["steps"] if s.get("name") == "Wheel with the aml extra"]
    m = re.search(r"for name in \(([^)]*)\):", aml)
    assert m, "the aml step no longer loops over a literal tuple of package names"
    assert set(ast.literal_eval("(" + m.group(1) + ")")) == trp.CHECKED
