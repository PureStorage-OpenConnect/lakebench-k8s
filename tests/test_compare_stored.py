"""``lakebench compare`` over stored records (metrics/compare.py, cli/_compare.py).

The failures that matter here are silent ones: a winner named inside noise
or across sides that are not comparable, a side that resolved to the wrong
runs, a number a Lakebench cap held shown as the system's. Every pinned
pair's verdict, exit code and hint is a reviewed golden string in
``tests/expected/pairs.json`` (``compare``).
"""

from __future__ import annotations

import ast
import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.metrics import comparability as cmp
from lakebench.metrics import compare as cm
from tests.fixtures import stored_records as sr

ROOT = Path(__file__).resolve().parents[1]
PAIRS = json.loads((ROOT / "tests" / "expected" / "pairs.json").read_text())["pairs"]


def _runs(tmp_path: Path, *records: dict, name: str = "runs") -> Path:
    d = tmp_path / name
    for r in records:
        p = d / f"run-{r['run_id']}"
        p.mkdir(parents=True, exist_ok=True)
        (p / "metrics.json").write_text(json.dumps(r))
    return d


def _with_id(rec: dict, run_id: str, **top) -> dict:
    out = copy.deepcopy(rec)
    out["run_id"] = run_id
    out.update(top)
    return out


def _invoke(*argv: str):
    return CliRunner().invoke(app, ["compare", *argv])


def _stderr(result) -> str:
    try:
        return result.stderr
    except ValueError:
        return result.output


def _stdout(result) -> str:
    try:
        return result.stdout
    except ValueError:
        return result.output


# ---------------------------------------------------------------------------
# The pinned pairs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pair", sorted(PAIRS, key=lambda p: int(p[1:])))
def test_pinned_pairs(pair: str, tmp_path: Path) -> None:
    """P1 to P12 through the command: the After verdict, its exit code and
    the reviewed missing condition, command and hint."""
    spec = PAIRS[pair]
    want = spec["compare"]
    a, b = sr.load_record(spec["a"]), sr.load_record(spec["b"])
    runs = _runs(tmp_path, a, b)
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "--format", "json")
    assert result.exit_code == want["exit_code"], _stderr(result)
    doc = json.loads(_stdout(result))
    assert doc["schema"] == "cmp2"
    assert doc["verdict"] == spec["after"]["verdict"] == want["verdict"]
    assert doc["exit_code"] == spec["after"]["code"]
    assert doc["missing"] == {
        "condition": want["condition"],
        "command": want["command"],
        "hint": want["hint"],
    }
    assert all(r["winner"] is None for r in doc["metrics"])


@pytest.mark.parametrize("pair", sorted(PAIRS, key=lambda p: int(p[1:])))
def test_no_winner_and_no_delta_where_withheld(pair: str) -> None:
    """No row of any pinned pair names a winner or reads as resolved; a NOT
    COMPARABLE or NOT ESTABLISHED pair shows no delta at all."""
    spec = PAIRS[pair]
    doc = cm.compare_records([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
    assert doc["winner_rule"] is False
    for r in doc["metrics"]:
        assert r["winner"] is None
        assert r["assessment"] in (
            cm.WITHHELD,
            cm.NOT_DIRECTIONAL,
            cm.CONFOUNDED_ROW,
            cm.NOT_ASSESSED,
            cm.CAPPED,
        )
        if doc["verdict"] in (cmp.NOT_COMPARABLE, cmp.NOT_ESTABLISHED):
            assert r["delta_pct"] is None and r["assessment"] == cm.WITHHELD


def test_p2_hint_never_says_repeat() -> None:
    """Continuous sides cannot ``--repeat``; P2's hint is about rounds."""
    spec = PAIRS["P2"]
    doc = cm.compare_records([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
    assert "--repeat" not in json.dumps(doc["missing"])
    assert "rounds" in doc["missing"]["hint"]


def test_p2_degraded_side_is_not_a_winner() -> None:
    """P2: qph_degradation_pct is lower-is-better; B degraded more. The row
    is shown but not assessed (the pair is not like-for-like) and names no
    side."""
    from lakebench.metrics import metric_registry as reg

    spec = PAIRS["P2"]
    a, b = sr.load_record(spec["a"]), sr.load_record(spec["b"])
    assert reg.lookup("qph_degradation_pct", "sustained").direction == "lower"
    doc = cm.compare_records([a], [b])
    row = next(r for r in doc["metrics"] if r["metric"] == "qph_degradation_pct")
    assert row["assessment"] == cm.NOT_ASSESSED and row["winner"] is None
    for metric in ("total_elapsed_seconds", "total_core_hours", "total_rows_processed"):
        row = next(r for r in doc["metrics"] if r["metric"] == metric)
        assert row["assessment"] == cm.NOT_DIRECTIONAL, metric


# ---------------------------------------------------------------------------
# Read-only
# ---------------------------------------------------------------------------


def test_zero_cluster_calls(tmp_path: Path, recording_k8s, monkeypatch) -> None:
    spec = PAIRS["P1"]
    runs = _runs(tmp_path, sr.load_record(spec["a"]), sr.load_record(spec["b"]))
    monkeypatch.chdir(tmp_path)
    before = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*"))
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs))
    assert result.exit_code == 0, _stderr(result)
    assert "LIKE-FOR-LIKE" in _stdout(result)
    recording_k8s.assert_no_calls()
    # No automatic comparisons/ file, nothing written at all.
    assert sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*")) == before


def test_comparison_module_imports_no_cluster_client(tmp_path: Path) -> None:
    """A fresh interpreter builds a comparison without importing kubernetes
    or any lakebench module that reaches a cluster."""
    spec = PAIRS["P1"]
    code = (
        "import sys, json\n"
        "from lakebench.metrics import compare as cm\n"
        f"a = json.load(open({str(sr.record_path(spec['a']))!r}))\n"
        f"b = json.load(open({str(sr.record_path(spec['b']))!r}))\n"
        "d = cm.compare_records([a], [b])\n"
        "assert d['verdict'] == 'LIKE-FOR-LIKE'\n"
        "bad = [m for m in sys.modules if m == 'kubernetes' or m.startswith('kubernetes.')\n"
        "       or m.startswith('lakebench.k8s') or m.startswith('lakebench.deploy')]\n"
        "assert not bad, bad\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_cli_module_imports_nothing_that_reaches_a_cluster() -> None:
    tree = ast.parse((ROOT / "src" / "lakebench" / "cli" / "_compare.py").read_text())
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names.append(node.module or "")
    for name in names:
        assert not name.startswith(("lakebench.k8s", "lakebench.deploy", "kubernetes")), name
        assert name not in ("subprocess", "os"), name


# ---------------------------------------------------------------------------
# Refused flags and usage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flag",
    [
        ["--keep"],
        ["--scale", "10"],
        ["--skip-benchmark"],
        ["--timeout", "600"],
        ["--local"],
        ["--generate"],
        ["--yes"],
        ["-y"],
    ],
)
def test_old_flags_refused(flag: list[str], tmp_path: Path) -> None:
    result = _invoke("a.yaml", "b.yaml", *flag)
    assert result.exit_code == 2
    text = " ".join(_stderr(result).split())
    assert "compare reads stored records and no longer runs configs" in text
    assert "`lakebench run a.yaml` and `lakebench run b.yaml`" in text
    assert "then `lakebench compare a.yaml b.yaml`" in text
    assert "for a winner" not in text


def test_old_flags_are_hidden_from_help() -> None:
    result = CliRunner().invoke(app, ["compare", "--help"])
    for flag in ("--keep", "--scale", "--skip-benchmark", "--timeout", "--local", "--generate"):
        assert flag not in result.output


@pytest.mark.parametrize("fmt", ["html", "htlm"])
def test_unsupported_format_refused(fmt: str) -> None:
    result = _invoke("a", "b", "--format", fmt)
    assert result.exit_code == 2
    assert "not supported" in _stderr(result)


def test_unknown_ref_exits_2(tmp_path: Path) -> None:
    runs = _runs(tmp_path, sr.load_record("5105a0"))
    result = _invoke("20260101-000000-abcdef", "5105a0x", "--runs-dir", str(runs))
    assert result.exit_code == 2
    assert "no record for 20260101-000000-abcdef" in " ".join(_stderr(result).split())


def test_unknown_config_ref_names_the_run_command(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "a.yaml").write_text("name: never-ran\n")
    runs = _runs(tmp_path, sr.load_record("5105a0"))
    result = _invoke("a.yaml", "20260929-212900-5105a0", "--runs-dir", str(runs))
    assert result.exit_code == 2
    assert "run it first: lakebench run a.yaml" in " ".join(_stderr(result).split())


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_same_runs_refused(tmp_path: Path) -> None:
    runs = _runs(tmp_path, sr.load_record("5105a0"))
    rid = "20260929-212900-5105a0"
    result = _invoke(rid, f"run-{rid}", "--runs-dir", str(runs))
    assert result.exit_code == 2
    assert "A and B resolve to the same runs" in _stderr(result)


def test_overlapping_sides_refused(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    r1, r2 = _with_id(base, "20260101-000000-aaaaaa"), _with_id(base, "20260101-000001-bbbbbb")
    runs = _runs(tmp_path, r1, r2)
    result = _invoke(f"{r1['run_id']},{r2['run_id']}", r2["run_id"], "--runs-dir", str(runs))
    assert result.exit_code == 2
    assert "share runs" in _stderr(result)


def test_two_records_for_one_run_refused(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    rid = base["run_id"]
    d1 = _runs(tmp_path, base, name="one")
    changed = copy.deepcopy(base)
    changed["total_elapsed_seconds"] = 1.0
    d2 = _runs(tmp_path, changed, name="two")
    other = _runs(tmp_path, _with_id(base, "20260101-000000-cccccc"), name="three")
    result = _invoke(
        rid,
        "20260101-000000-cccccc",
        "--runs-dir",
        str(d1),
        "--runs-dir",
        str(d2),
        "--runs-dir",
        str(other),
    )
    assert result.exit_code == 2
    assert "different records" in _stderr(result)


def test_run_dir_whose_record_names_another_run_refused(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    d = tmp_path / "runs" / "run-20260101-000000-dddddd"
    d.mkdir(parents=True)
    (d / "metrics.json").write_text(json.dumps(base))
    with pytest.raises(cm.CompareError, match="records run"):
        cm.resolve_side("A", str(d), [tmp_path / "runs"])


def test_a_failed_member_is_excluded_and_listed(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    ok1 = _with_id(base, "20260101-000000-a00001")
    bad = _with_id(base, "20260101-000000-a00002")
    bad["verdict"] = {"status": "FAILED", "reasons": ["silver did not keep pace"]}
    bad["pipeline_benchmark"]["scores"]["composite_qph"] = 1.0
    other = _with_id(base, "20260101-000000-b00001")
    runs = _runs(tmp_path, ok1, bad, other)
    a, b = cm.resolve(f"{ok1['run_id']},{bad['run_id']}", other["run_id"], [runs])
    line = cm.resolution_line(a)
    assert "20260101-000000-a00002 FAILED: silver did not keep pace (excluded)" in line
    assert [m.run_id for m in a.passed] == [ok1["run_id"]]
    doc = cm.build_comparison(a, b)
    assert doc["verdict"] == cmp.LIKE_FOR_LIKE
    assert doc["sides"]["a"]["n_passed"] == 1 and doc["sides"]["a"]["n_attempted"] == 2
    qph = next(r for r in doc["metrics"] if r["metric"] == "composite_qph")
    # The failed member's number is not in A's values.
    assert 1.0 not in qph["a"]["values"] and qph["a"]["n"] == 1


def test_a_side_with_no_passed_member_names_the_failure(tmp_path: Path) -> None:
    spec = PAIRS["P9"]
    doc = cm.compare_records([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
    assert doc["verdict"] == cmp.NOT_COMPARABLE and doc["step"] == "1"
    assert doc["missing"]["condition"] == "a passed run"


def _series(tmp_path: Path, base: dict, sid: str, n: int, *, start: str = "2026-10-01T00"):
    recs = []
    for i in range(1, n + 1):
        r = _with_id(base, f"20261001-00000{i}-5e{i:04d}")
        r["start_time"] = f"{start}:0{i}:00"
        r["series"] = {"id": sid, "index": i, "size": n}
        recs.append(r)
    return recs


def _manifest(out: Path, sid: str, recs: list[dict], deployment: str, extra=()) -> Path:
    d = out / "series"
    d.mkdir(parents=True, exist_ok=True)
    runs = [
        {
            "run_id": r["run_id"],
            "index": i + 1,
            "exit_code": 0,
            "verdict": "PASSED",
            "member": True,
            "corpus_changed": False,
        }
        for i, r in enumerate(recs)
    ]
    runs += list(extra)
    p = d / f"{sid}.json"
    p.write_text(
        json.dumps(
            {
                "schema": "lb-series/1",
                "series_id": sid,
                "deployment_name": deployment,
                "requested": len(runs),
                "runs": runs,
            }
        )
    )
    return p


def test_config_resolves_to_series(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    name = base["deployment_name"]
    sid = "s-20261001-000000-abc123"
    out = tmp_path / "lakebench-output"
    recs = _series(tmp_path, base, sid, 3)
    older = _with_id(base, "20260901-000000-0ld000")
    older["start_time"] = "2026-09-01T00:00:00"
    bench = _with_id(base, "20261002-000000-be0000")
    bench["start_time"] = "2026-10-02T00:00:00"
    bench["record_kind"] = "benchmark"
    runs = _runs(out, *recs, older, bench)
    _manifest(
        out,
        sid,
        recs,
        name,
        extra=[
            {
                "run_id": None,
                "index": 4,
                "exit_code": 3,
                "verdict": None,
                "member": False,
                "not_member_reason": "bronze changed during this repetition",
            }
        ],
    )
    (tmp_path / "a.yaml").write_text(f"name: {name}\n")
    side = cm.resolve_side("A", "a.yaml", [runs])
    assert side.series == sid
    assert [m.run_id for m in side.members] == [r["run_id"] for r in recs]
    line = cm.resolution_line(side)
    assert f"deployment {name}, series {sid}, 3 runs:" in line
    assert "not a member (bronze changed during this repetition)" in line
    # The same through series:<id>.
    by_id = cm.resolve_side("A", f"series:{sid}", [runs])
    assert [m.run_id for m in by_id.members] == [m.run_id for m in side.members]


def test_config_series_without_manifest_refused(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    recs = _series(tmp_path, base, "s-20261001-000000-abc124", 2)
    runs = _runs(tmp_path / "out", *recs)
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    with pytest.raises(cm.CompareError, match="no manifest for series") as e:
        cm.resolve_side("A", "a.yaml", [runs])
    assert e.value.path == "compare.bad_ref"


def test_manifest_member_with_another_series_stamp_refused(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    sid = "s-20261001-000000-abc125"
    recs = _series(tmp_path, base, sid, 2)
    recs[1]["series"]["id"] = "s-20261001-000000-ffffff"
    out = tmp_path / "out"
    runs = _runs(out, *recs)
    _manifest(out, sid, recs, base["deployment_name"])
    with pytest.raises(cm.CompareError, match="whose record says series"):
        cm.resolve_side("A", f"series:{sid}", [runs])


def test_config_picks_the_latest_run_of_its_deployment(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    old = _with_id(base, "20260101-000000-000001", start_time="2026-01-01T00:00:00")
    new = _with_id(base, "20260102-000000-000002", start_time="2026-01-02T00:00:00")
    foreign = _with_id(
        base, "20260103-000000-000003", start_time="2026-01-03T00:00:00", deployment_name="other"
    )
    runs = _runs(tmp_path, old, new, foreign)
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    side = cm.resolve_side("A", "a.yaml", [runs])
    assert [m.run_id for m in side.members] == [new["run_id"]]


def test_config_changed_since_the_run_warns(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    rec = _with_id(base, "20260101-000000-000001")
    rec.setdefault("config_snapshot", {})["config_sha256"] = "0" * 64
    runs = _runs(tmp_path, rec)
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    side = cm.resolve_side("A", "a.yaml", [runs])
    assert any("has changed since run" in w for w in side.warnings)


def test_equal_names_with_different_configs_refused(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    runs = _runs(tmp_path, _with_id(base, "20260101-000000-000001"))
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    (tmp_path / "b.yaml").write_text(f"name: {base['deployment_name']}\n# changed\n")
    result = _invoke("a.yaml", "b.yaml", "--runs-dir", str(runs))
    assert result.exit_code == 2
    assert f"A and B both resolve to {base['deployment_name']}" in _stderr(result)


def test_config_with_an_unset_variable_still_resolves_its_name(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LB_TEST_UNSET_KEY", raising=False)
    base = sr.load_record("5105a0")
    runs = _runs(tmp_path, _with_id(base, "20260101-000000-000001"))
    (tmp_path / "a.yaml").write_text(
        f"name: {base['deployment_name']}\nplatform:\n  storage:\n    s3:\n"
        "      access_key: ${LB_TEST_UNSET_KEY}\n"
    )
    side = cm.resolve_side("A", "a.yaml", [runs])
    assert side.deployment == base["deployment_name"]


# ---------------------------------------------------------------------------
# Hints
# ---------------------------------------------------------------------------


def _c360_batch() -> dict:
    return sr.load_record("5105a0")


def test_version_bump_hint() -> None:
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-c36002")
    b["experiment"]["workload"]["version"] = "c360-2"
    doc = cm.compare_records([a], [b])
    assert doc["verdict"] == cmp.NOT_COMPARABLE
    assert doc["missing"]["hint"].startswith("baseline predates c360-2; re-run it: ")
    doc = cm.compare_records([b], [a])
    assert doc["missing"]["hint"].startswith("candidate predates c360-2; re-run it: ")


def test_generation_mismatch_hint() -> None:
    from tests.test_comparability import _fresh

    v2 = _fresh().to_dict()
    v1 = _c360_batch()
    doc = cm.compare_records([v1], [v2])
    assert doc["exit_code"] == 10 and doc["step"] == "0"
    hint = doc["missing"]["hint"]
    assert hint.startswith("A was recorded with experiment identity v1")
    assert "and B with v2. Missing: one identity version." in hint
    assert doc["missing"]["command"].endswith("--regenerate")


def test_continuous_hint_names_run_ids() -> None:
    """A continuous side that is not one experiment cannot be repeated:
    the hint says run it again and pass the ids, never ``--repeat``."""
    a1 = sr.load_record("011043-e338c5")
    a2 = _with_id(a1, "20261001-000000-c0a002")
    a2["experiment"]["limits"]["benchmark_mode"] = "throughput"
    b = _with_id(a1, "20261001-000000-c0b001")
    doc = cm.compare_records([a1, a2], [b])
    assert doc["step"] == "2"
    hint = doc["missing"]["hint"]
    assert "--repeat" not in hint
    assert "--continuous` 2 more times and pass the run ids" in hint
    assert f"`lakebench compare {a1['run_id']},<new run id>,<new run id> {b['run_id']}`" in hint


def test_batch_side_not_one_experiment_says_repeat() -> None:
    a1 = _c360_batch()
    a2 = _with_id(a1, "20261001-000000-ba0002")
    a2["experiment"]["limits"]["benchmark_iterations"] = 1
    b = _with_id(a1, "20261001-000000-bb0001")
    doc = cm.compare_records([a1, a2], [b])
    hint = doc["missing"]["hint"]
    assert hint.startswith(
        f"A: run {a2['run_id']} differs from {a1['run_id']} in benchmark iterations (3 vs 1)"
    )
    assert doc["missing"]["command"].endswith("--repeat 3")


def test_within_side_rounds_are_an_outcome_hint() -> None:
    a1 = sr.load_record("011043-e338c5")
    a2 = _with_id(a1, "20261001-000000-r0a002")
    a2["experiment"]["limits"]["benchmark_rounds"] = 99
    b = _with_id(a1, "20261001-000000-r0b001")
    doc = cm.compare_records([a1, a2], [b])
    assert doc["verdict"] == cmp.NOT_COMPARABLE and doc["step"] == "2"
    assert "an outcome of speed" in doc["missing"]["hint"]
    assert "--duration" not in doc["missing"]["hint"]
    assert doc["missing"]["command"] is None


def test_newer_schema_says_upgrade() -> None:
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-5c4e9a")
    b["experiment"]["schema"] = "exp9"
    doc = cm.compare_records([a], [b])
    assert doc["verdict"] == cmp.NOT_COMPARABLE
    assert "newer than this lakebench" in doc["missing"]["hint"]
    assert doc["missing"]["command"] == "pip install --upgrade lakebench"


def test_like_for_like_names_no_command() -> None:
    spec = PAIRS["P1"]
    doc = cm.compare_records([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
    assert doc["missing"]["command"] is None
    assert "no winner" in doc["missing"]["hint"]


def test_config_path_names_the_config_in_commands() -> None:
    spec = PAIRS["P9"]
    b = sr.load_record(spec["b"])
    b.setdefault("provenance", {})["config_path"] = "configs/b.yaml"
    doc = cm.compare_records([sr.load_record(spec["a"])], [b])
    assert doc["missing"]["command"] == "lakebench run configs/b.yaml"


def test_protected_seed_never_printed(monkeypatch, tmp_path: Path) -> None:
    spec = PAIRS["P7"]
    a, b = sr.load_record(spec["a"]), sr.load_record(spec["b"])
    seed_b = b["experiment"]["corpus"]["seed"]
    monkeypatch.setattr(cm, "_hidden_seeds", lambda: frozenset({seed_b}))
    runs = _runs(tmp_path, a, b)
    for fmt in ("json", "table", "csv"):
        result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "--format", fmt)
        assert result.exit_code == 10
        assert str(seed_b) not in result.output, fmt
    doc = cm.compare_records([a], [b])
    assert "<protected seed>" in doc["missing"]["hint"]


def test_unreadable_seed_list_hides_every_integer_seed(monkeypatch) -> None:
    spec = PAIRS["P7"]
    a, b = sr.load_record(spec["a"]), sr.load_record(spec["b"])
    monkeypatch.setattr(cm, "_hidden_seeds", lambda: None)
    text = json.dumps(cm.compare_records([a], [b]))
    for rec in (a, b):
        assert str(rec["experiment"]["corpus"]["seed"]) not in text


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def test_capped_row_names_its_cap() -> None:
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-ca0001")
    for r in (a, b):
        r["experiment"]["limits"]["bound_kinds"] = ["silver-build: executor cap"]
    doc = cm.compare_records([a], [b])
    assert doc["verdict"] == cmp.LIKE_FOR_LIKE
    capped = [r for r in doc["metrics"] if r["assessment"] == cm.CAPPED]
    assert capped
    for r in capped:
        assert r["capped_by"] == ["silver-build: executor cap"]
        assert "this figure measures silver-build: executor cap" in r["hint"]
        assert r["winner"] is None


def test_trickle_held_continuous_rows_are_capped() -> None:
    """A continuous repeat: the rows the trickle bounds read capped by it,
    from each member's own trickle bound."""
    a = sr.load_record("011043-e338c5")
    b = _with_id(a, "20261001-000000-7c0001")
    doc = cm.compare_records([a], [b])
    assert doc["verdict"] == cmp.LIKE_FOR_LIKE
    capped = {r["metric"] for r in doc["metrics"] if r["assessment"] == cm.CAPPED}
    assert capped
    for r in doc["metrics"]:
        if r["metric"] in capped:
            assert "trickle" in r["capped_by"]


def test_mode_split_metric_without_a_mode_is_not_directional() -> None:
    """A metric whose meaning depends on the mode, read without one, has no
    better side (the registry raises ModeRequired)."""
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-0m0001")
    side_a, side_b = cm.side_of_records("A", [a]), cm.side_of_records("B", [b])
    pair = cmp.pair_verdict([a], [b])
    assert pair.verdict == cmp.LIKE_FOR_LIKE
    missing = cm.missing_condition(pair, side_a, side_b)
    assert cm.assess("total_core_hours", "batch", pair, missing, [], False).outcome == (
        cm.NOT_ASSESSED
    )
    got = cm.assess("total_core_hours", None, pair, missing, [], False)
    assert got.outcome == cm.NOT_DIRECTIONAL


def test_scores_skip_bools_and_text_and_apply_aliases() -> None:
    rec = _c360_batch()
    rec["pipeline_benchmark"]["scores"] = {
        "query_time_freshness_seconds": 5,
        "maintenance_settled": True,
        "maintenance_value_reason": "within noise",
        "composite_qph": 10,
        "qph_spread": {"low": 1},
    }
    assert cm._scores(rec) == {"query_time_event_age_seconds": 5.0, "composite_qph": 10.0}


def test_delta_has_no_value_when_median_a_is_zero() -> None:
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-0d0001")
    a["pipeline_benchmark"]["scores"] = {"composite_qph": 0.0}
    b["pipeline_benchmark"]["scores"] = {"composite_qph": 5.0}
    doc = cm.compare_records([a], [b])
    assert doc["metrics"][0]["delta_pct"] is None


def test_delta_is_relative_to_the_magnitude_of_a() -> None:
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-0d0002")
    a["pipeline_benchmark"]["scores"] = {"qph_degradation_pct": -10.0}
    b["pipeline_benchmark"]["scores"] = {"qph_degradation_pct": -5.0}
    doc = cm.compare_records([a], [b])
    assert doc["metrics"][0]["delta_pct"] == 50.0


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def test_json_and_csv_shapes(tmp_path: Path) -> None:
    spec = PAIRS["P1"]
    runs = _runs(tmp_path, sr.load_record(spec["a"]), sr.load_record(spec["b"]))
    j = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "--format", "json")
    doc = json.loads(_stdout(j))
    for key in (
        "schema",
        "verdict",
        "exit_code",
        "attribution",
        "missing",
        "sides",
        "groups",
        "warnings",
        "metrics",
    ):
        assert key in doc, key
    for side in ("a", "b"):
        s = doc["sides"][side]
        for key in ("refs", "members", "n_attempted", "n_passed", "experiment", "support", "bound"):
            assert key in s, key
    c = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "--format", "csv")
    lines = _stdout(c).splitlines()
    assert lines[0] == "# schema: cmp2"
    assert "# verdict: LIKE-FOR-LIKE" in lines
    header = next(line for line in lines if not line.startswith("#"))
    assert header.split(",") == list(cm.CSV_COLUMNS)
    # The resolution goes to stderr, never into the data.
    assert "A: " in _stderr(c) and not any(line.startswith("A: ") for line in lines)


def test_output_file_written_only_when_asked(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    spec = PAIRS["P2"]
    runs = _runs(tmp_path, sr.load_record(spec["a"]), sr.load_record(spec["b"]))
    out = tmp_path / "cmp.json"
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "-o", str(out))
    assert result.exit_code == 12
    assert json.loads(out.read_text())["verdict"] == cmp.NOT_LIKE_FOR_LIKE
    assert not (tmp_path / "lakebench-output").exists()


def test_output_over_an_input_refused(tmp_path: Path) -> None:
    spec = PAIRS["P1"]
    runs = _runs(tmp_path, sr.load_record(spec["a"]), sr.load_record(spec["b"]))
    target = runs / f"run-{spec['a']}" / "metrics.json"
    before = target.read_bytes()
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "-o", str(target))
    assert result.exit_code == 2
    assert target.read_bytes() == before


def test_table_prints_no_delta_when_withheld(tmp_path: Path) -> None:
    spec = PAIRS["P7"]
    runs = _runs(tmp_path, sr.load_record(spec["a"]), sr.load_record(spec["b"]))
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs))
    text = _stdout(result)
    assert "NOT COMPARABLE" in text and "Missing: the same corpus" in text
    assert "%" not in "".join(line for line in text.splitlines() if "withheld" in line)


# ---------------------------------------------------------------------------
# Review fixes: seeds, caps, commands, resolution
# ---------------------------------------------------------------------------


def _aml_batch() -> dict:
    return sr.load_record("825153")


def test_seed_difference_inside_a_side_is_hidden(monkeypatch) -> None:
    a1 = _aml_batch()
    a2 = _with_id(a1, "20261001-000000-5d0002")
    a2["experiment"]["corpus"]["seed"] = 987654321
    b = _with_id(a1, "20261001-000000-5d0003")
    monkeypatch.setattr(cm, "_hidden_seeds", lambda: frozenset({987654321}))
    doc = cm.compare_records([a1, a2], [b])
    assert doc["step"] == "2" and doc["cause"]["key"] == "seed"
    assert "987654321" not in json.dumps(doc)
    assert doc["cause"]["b"] == "<protected seed>"


def test_seed_only_difference_between_sides_is_hidden(monkeypatch) -> None:
    a = _aml_batch()
    b = _with_id(a, "20261001-000000-5d0004")
    b["experiment"]["corpus"]["seed"] = 987654321
    monkeypatch.setattr(cm, "_hidden_seeds", lambda: frozenset({987654321}))
    doc = cm.compare_records([a], [b])
    assert doc["step"] == "3"
    assert "987654321" not in json.dumps(doc)


def test_seed_inside_a_corpus_problem_is_hidden(monkeypatch) -> None:
    a = _aml_batch()
    b = _with_id(a, "20261001-000000-5d0005")
    b["experiment"]["corpus"]["problems"] = ["config seed 43 but the datagen pods ran 987654321"]
    monkeypatch.setattr(cm, "_hidden_seeds", lambda: frozenset({987654321}))
    doc = cm.compare_records([a], [b])
    assert doc["cause"]["kind"] == "corpus_problem"
    text = json.dumps(doc)
    assert "987654321" not in text and "config seed 43" in text


def test_customer360_seeds_are_not_hidden(monkeypatch) -> None:
    """The AML seed lists do not apply to another workload's corpus."""
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-5d0006")
    b["experiment"]["corpus"]["seed"] = 7
    monkeypatch.setattr(cm, "_hidden_seeds", lambda: None)
    doc = cm.compare_records([a], [b])
    assert "datagen.seed: <protected seed>" not in doc["missing"]["hint"]
    assert f"datagen.seed: {a['experiment']['corpus']['seed']}" in doc["missing"]["hint"]


def test_caps_are_labelled_on_a_pair_that_is_not_like_for_like(tmp_path: Path) -> None:
    """P2 is NOT LIKE-FOR-LIKE and trickle-held: the rows the trickle
    bounds still carry it, and the table says BOUNDED BY."""
    spec = PAIRS["P2"]
    a, b = sr.load_record(spec["a"]), sr.load_record(spec["b"])
    doc = cm.compare_records([a], [b])
    rows = {r["metric"]: r for r in doc["metrics"]}
    assert rows["sustained_throughput_rps"]["capped_by"] == ["trickle"]
    assert rows["sustained_throughput_rps"]["assessment"] == cm.NOT_ASSESSED
    runs = _runs(tmp_path, a, b)
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs))
    # The table wraps at the runner's width; the label is there.
    assert "BOUNDED" in _stdout(result)
    csv_text = _stdout(_invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "--format", "csv"))
    line = next(x for x in csv_text.splitlines() if x.startswith("sustained_throughput_rps,"))
    assert line.endswith(",trickle")


def test_a_cap_on_one_side_labels_its_rows() -> None:
    a = _c360_batch()
    b = _with_id(a, "20261001-000000-ca0002")
    b["experiment"]["limits"]["bound_kinds"] = ["silver-build: executor cap"]
    doc = cm.compare_records([a], [b])
    assert doc["verdict"] == cmp.NOT_LIKE_FOR_LIKE
    assert any(r["capped_by"] == ["silver-build: executor cap"] for r in doc["metrics"])


def test_regenerate_commands_are_ones_run_accepts() -> None:
    """A batch side regenerates with --generate --regenerate; a continuous
    side with --continuous (run refuses --regenerate there)."""
    from lakebench.cli._run_args import run_args_problems  # noqa: F401 -- the rule exists

    for pair, want in (("P7", "--generate --regenerate"), ("P8", "--continuous")):
        spec = PAIRS[pair]
        doc = cm.compare_records([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
        assert doc["missing"]["command"].endswith(want), pair
        assert doc["missing"]["command"] != "--regenerate"


def test_maintenance_across_table_formats_names_no_setting() -> None:
    for pair in ("P4", "P6"):
        spec = PAIRS[pair]
        doc = cm.compare_records([sr.load_record(spec["a"])], [sr.load_record(spec["b"])])
        assert doc["missing"]["command"] is None, pair
        assert "Iceberg and Delta run different maintenance operations" in doc["missing"]["hint"]


def test_latest_run_is_by_instant_not_by_string(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    early = _with_id(base, "20260101-000000-000011", start_time="2026-01-01T09:30:00+02:00")
    late = _with_id(base, "20260101-000000-000012", start_time="2026-01-01T08:00:00+00:00")
    runs = _runs(tmp_path, early, late)
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    side = cm.resolve_side("A", "a.yaml", [runs])
    assert [m.run_id for m in side.members] == [late["run_id"]]


def test_unreadable_record_on_the_config_route_warns(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    runs = _runs(tmp_path, _with_id(base, "20260101-000000-000021"))
    broken = runs / "run-20260101-000000-000022"
    broken.mkdir()
    (broken / "metrics.json").write_text("{trunc")
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    side = cm.resolve_side("A", "a.yaml", [runs])
    assert any("skipped unreadable record" in w for w in side.warnings)


def test_series_with_no_member_is_a_bad_ref(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    sid = "s-20261001-000000-abc126"
    out = tmp_path / "out"
    runs = out / "runs"
    runs.mkdir(parents=True)
    _manifest(
        out,
        sid,
        [],
        base["deployment_name"],
        extra=[{"run_id": None, "index": 1, "member": False, "not_member_reason": "x"}],
    )
    with pytest.raises(cm.CompareError, match="resolves to no run") as e:
        cm.resolve_side("A", f"series:{sid}", [runs])
    assert e.value.path == "compare.bad_ref"


def test_series_member_without_a_record_names_the_series(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    sid = "s-20261001-000000-abc127"
    recs = _series(tmp_path, base, sid, 2)
    out = tmp_path / "out"
    runs = _runs(out, recs[0])
    _manifest(out, sid, recs, base["deployment_name"])
    with pytest.raises(cm.CompareError, match=f"series {sid} lists run"):
        cm.resolve_side("A", f"series:{sid}", [runs])


def test_series_found_beside_a_relative_runs_dir(tmp_path: Path, monkeypatch) -> None:
    base = sr.load_record("5105a0")
    sid = "s-20261001-000000-abc128"
    recs = _series(tmp_path, base, sid, 2)
    out = tmp_path / "out"
    _runs(out, *recs)
    _manifest(out, sid, recs, base["deployment_name"])
    monkeypatch.chdir(out / "runs")
    side = cm.resolve_side("A", f"series:{sid}", [Path(".")])
    assert len(side.members) == 2


def test_config_ref_makes_no_cluster_call(tmp_path: Path, recording_k8s, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    base = sr.load_record("5105a0")
    other = _with_id(base, "20260101-000000-000031", deployment_name="other-dep")
    runs = _runs(tmp_path, base, other)
    (tmp_path / "a.yaml").write_text(f"name: {base['deployment_name']}\n")
    result = _invoke("a.yaml", other["run_id"], "--runs-dir", str(runs))
    assert result.exit_code in (0, 10, 12, 13), _stderr(result)
    recording_k8s.assert_no_calls()


@pytest.mark.parametrize("where", ["runs", "series", "metrics"])
def test_output_into_records_refused(where: str, tmp_path: Path) -> None:
    spec = PAIRS["P1"]
    runs = _runs(tmp_path, sr.load_record(spec["a"]), sr.load_record(spec["b"]))
    target = {
        "runs": runs / "cmp.json",
        "series": tmp_path / "series" / "cmp.json",
        "metrics": tmp_path / "elsewhere" / "metrics.json",
    }[where]
    result = _invoke(spec["a"], spec["b"], "--runs-dir", str(runs), "-o", str(target))
    assert result.exit_code == 2, _stderr(result)
    assert not target.exists()


def test_results_hint_names_a_non_default_runs_dir(tmp_path: Path) -> None:
    from tests.test_experiment import _cfg, _fp, _metrics

    a = _metrics(_cfg(), {"Q1": _fp(1)}).to_dict()
    b = _metrics(_cfg(), {"Q1": _fp(2)}).to_dict()
    b["run_id"] = "20260926-120000-bbbbbb"
    runs = _runs(tmp_path, a, b)
    sa = cm.resolve_side("A", a["run_id"], [runs])
    sb = cm.resolve_side("B", b["run_id"], [runs])
    doc = cm.build_comparison(sa, sb)
    assert doc["missing"]["command"] == f"lakebench report --run {b['run_id']} --metrics {runs}"


def test_step_two_digest_difference_names_the_digests() -> None:
    a1 = _c360_batch()
    a1["experiment"]["corpus"].setdefault("datagen", {})["digest"] = "sha256:" + "1" * 64
    a2 = _with_id(a1, "20261001-000000-d90002")
    a2["experiment"]["corpus"]["datagen"]["digest"] = "sha256:" + "2" * 64
    b = _with_id(a1, "20261001-000000-d90003")
    doc = cm.compare_records([a1, a2], [b])
    if doc["cause"].get("key") == "generator digest":
        assert "not recorded vs not recorded" not in doc["missing"]["hint"]
        assert "1111" in doc["missing"]["hint"]


def test_resolution_line_names_every_deployment(tmp_path: Path) -> None:
    base = sr.load_record("5105a0")
    r1 = _with_id(base, "20260101-000000-000041", deployment_name="dep-one")
    r2 = _with_id(base, "20260101-000000-000042", deployment_name="dep-two")
    runs = _runs(tmp_path, r1, r2)
    side = cm.resolve_side("A", f"{r1['run_id']},{r2['run_id']}", [runs])
    assert "deployments dep-one, dep-two" in cm.resolution_line(side)
