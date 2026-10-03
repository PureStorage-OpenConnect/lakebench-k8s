"""The expected-results writer (scripts/release/expected.py, ``harness.py
expected``) against the release gate's reader (release_record.load_expected
and record_problems): what the writer writes from reference runs the reader
accepts for those runs, a run whose results differ is refused, and the
writer refuses inputs that would make a wrong expected file."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakebench.metrics import release_record as rr
from lakebench.metrics import verdict as verdict_mod
from tests import test_release_record as trr
from tests.test_release_harness import H

X = H._expected

#: A well-formed alert set (metrics.alert_set.shape_problem's shape: rows and
#: h are the sums of by_rule's).
ALERTS = {
    "spec": "as1",
    "columns": ["rule_id", "entity_id", "alert_ts"],
    "cols_sha": "c0ffee",
    "rows": 3,
    "h": "10",
    "by_rule": {"R1": {"rows": 1, "h": "4"}, "R2": {"rows": 2, "h": "6"}},
}


@pytest.fixture
def ready(monkeypatch):
    monkeypatch.setattr(
        verdict_mod,
        "verdict_from_record",
        lambda record: SimpleNamespace(
            gates={"layer_rows": "PASS"}, qualifiers={rr.LAYER_ROWS_UNMEASURED: []}
        ),
        raising=False,
    )


def _ref(kind: str) -> dict:
    """A clean release record of *kind* that a reference run would leave:
    AML batch with its alert set, AML continuous with both query sets."""
    rec = trr._release(kind)
    exp = rec["experiment"]
    if kind == "aml_batch":
        exp["results"]["alert_set"] = copy.deepcopy(ALERTS)
    if kind == "aml_cont":
        rounds = rec.get("pipeline_benchmark", {}).get("benchmark_rounds") or rec.get(
            "benchmark_rounds"
        )
        post = copy.deepcopy(next(r for r in rounds if (r.get("qph") or 0) > 0))
        post["executed_query_set_id"] = post["query_set_id"] = "qs12-post"
        rounds.append(post)
    return rec


def _with(rec: dict, recipe: str | None = None, engine: str | None = None) -> dict:
    """A second reference run of the same corpus on another recipe and engine."""
    out = copy.deepcopy(rec)
    exp = out["experiment"]
    if recipe:
        exp["architecture"]["recipe"] = recipe
    if engine:
        for fp in exp["results"]["fingerprints"].values():
            fp["engine"] = engine
            fp["adapted_sql_sha"] = "0" * 16
    return out


def _dir(tmp_path: Path, recs: dict[str, dict]) -> Path:
    d = tmp_path / "runs"
    for rid, rec in recs.items():
        (d / rid).mkdir(parents=True)
        (d / rid / "metrics.json").write_text(json.dumps(rec))
    return d


def _write(tmp_path: Path, recs: dict[str, dict]) -> dict:
    """Write the file with the CLI's code path and read it with the gate's."""
    out = tmp_path / "uat" / X.file_name("9.9.9")
    rc = H.expected_command([_dir(tmp_path, recs)], "9.9.9", out)
    assert rc == 0
    return rr.load_expected(out)


def _gate(rec: dict, expected: dict) -> list[str]:
    return rr.record_problems(rec, trr.FREEZE, expected, release_digest=trr.DIGEST)


def _refusals(recs: dict[str, dict]) -> list[str]:
    with pytest.raises(X.ExpectedRefused) as e:
        X.build_expected(sorted(recs.items()), "9.9.9")
    return e.value.problems


# -- round trip ------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(trr.BASES))
def test_written_file_accepts_its_reference_run(ready, tmp_path, kind):
    rec = _ref(kind)
    expected = _write(tmp_path, {"run-1": rec})
    assert _gate(rec, expected) == []


def test_one_file_for_the_whole_matrix(ready, tmp_path):
    c360 = _ref("c360_batch")
    recs = {
        "run-1": c360,
        "run-2": _with(c360, "hive-iceberg-spark-trino", "trino"),
        "run-3": _with(c360, "hive-iceberg-spark-none"),
        "run-4": _ref("aml_batch"),
        "run-5": _ref("c360_cont"),
        "run-6": _ref("aml_cont"),
    }
    recs["run-3"]["experiment"]["results"] = {
        "query_set_id": None,
        "fingerprints": {},
        "not_checked": "no benchmark ran",
    }
    recs["run-3"]["experiment"]["stages"]["executed"] = ["bronze", "silver", "gold"]
    recs["run-3"]["experiment"]["stages"]["skipped"] = [rr.DECLARED_SKIP_NO_ENGINE]
    expected = _write(tmp_path, recs)
    assert [(e["workload"], e["from_runs"]) for e in expected["entries"]] == [
        ("customer360", ["run-1", "run-2", "run-3"]),
        ("financial", ["run-4"]),
    ]
    assert [c["workload"] for c in expected["continuous"]] == ["customer360", "financial"]
    aml_cont = expected["continuous"][1]
    assert len(aml_cont["query_set_ids"]) == 2 and "fingerprints" not in aml_cont
    assert expected["entries"][1]["alert_set"] == ALERTS
    assert "alert_set" not in expected["entries"][0]
    # Engine evidence is not part of the expected value.
    fp = next(iter(expected["entries"][0]["fingerprints"].values()))
    assert "engine" not in fp and "adapted_sql_sha" not in fp
    for rid, rec in recs.items():
        assert _gate(rec, expected) == [], rid


def test_output_names_no_seed(ready, tmp_path):
    rec = _ref("aml_batch")
    expected = _write(tmp_path, {"run-1": rec})
    text = json.dumps(expected)
    assert "seed" not in text
    assert set(expected["entries"][0]) == {
        "workload",
        "workload_version",
        "corpus_id_v2",
        "scale",
        "mode",
        "query_set_id",
        "fingerprints",
        "alert_set",
        "from_runs",
    }


# -- planted mismatches the gate must refuse -------------------------------------


def test_a_run_whose_result_differs_is_refused_by_the_gate(ready, tmp_path):
    rec = _ref("c360_batch")
    expected = _write(tmp_path, {"run-1": rec})
    bad = copy.deepcopy(rec)
    q = sorted(bad["experiment"]["results"]["fingerprints"])[0]
    bad["experiment"]["results"]["fingerprints"][q]["exact"] = "0" * 16
    assert any(f"query {q} result differs" in p for p in _gate(bad, expected))


def test_a_run_whose_alert_set_differs_is_refused_by_the_gate(ready, tmp_path):
    rec = _ref("aml_batch")
    expected = _write(tmp_path, {"run-1": rec})
    bad = copy.deepcopy(rec)
    bad["experiment"]["results"]["alert_set"]["by_rule"]["R1"]["h"] = "5"
    bad["experiment"]["results"]["alert_set"]["h"] = "11"
    assert any("alert set" in p for p in _gate(bad, expected))


def test_a_planted_wrong_file_is_refused_by_the_gate(ready, tmp_path):
    """A file edited after writing (a wrong fingerprint set) does not pass
    the run it claims to come from."""
    rec = _ref("c360_cont")
    expected = _write(tmp_path, {"run-1": rec})
    fps = expected["continuous"][0]["fingerprints"]
    fps[sorted(fps)[0]]["rows"] += 1
    assert any("result differs" in p for p in _gate(rec, expected))


# -- what the writer refuses -----------------------------------------------------


def test_references_that_disagree_are_refused(ready):
    a = _ref("c360_batch")
    b = _with(a, "hive-iceberg-spark-trino", "trino")
    q = sorted(b["experiment"]["results"]["fingerprints"])[0]
    b["experiment"]["results"]["fingerprints"][q]["exact"] = "1" * 16
    problems = _refusals({"run-1": a, "run-2": b})
    assert any(f"query {q} differs between run-2 and run-1" in p for p in problems)


def test_references_that_answered_other_queries_are_refused(ready):
    a = _ref("c360_batch")
    b = _with(a, "hive-iceberg-spark-trino")
    b["experiment"]["results"]["fingerprints"].popitem()
    assert any("answered other queries" in p for p in _refusals({"run-1": a, "run-2": b}))


def test_exp1_record_refused(ready):
    rec = _ref("c360_batch")
    rec["experiment"]["schema"] = "exp1"
    rec["experiment"].pop("identity_version", None)
    assert any("exp1 record" in p for p in _refusals({"run-1": rec}))


def test_record_that_did_not_pass_refused(ready, monkeypatch):
    monkeypatch.setattr(verdict_mod, "passed", lambda record: False)
    assert any("did not pass" in p for p in _refusals({"run-1": _ref("c360_batch")}))


def test_different_corpus_ids_in_one_entry_refused(ready):
    a = _ref("c360_batch")
    b = _with(a, "hive-iceberg-spark-trino")
    b["experiment"]["corpus"]["id_v2"] = "v2-other"
    assert any("runs differ in corpus id v2" in p for p in _refusals({"run-1": a, "run-2": b}))


def test_missing_fingerprint_refused(ready):
    rec = _ref("c360_batch")
    q = sorted(rec["experiment"]["results"]["fingerprints"])[0]
    rec["experiment"]["results"]["fingerprints"][q] = None
    assert any(f"query {q} has no usable fingerprint" in p for p in _refusals({"run-1": rec}))


def test_no_fingerprints_refused(ready):
    rec = _ref("c360_batch")
    rec["experiment"]["results"]["fingerprints"] = {}
    assert any("no result fingerprints" in p for p in _refusals({"run-1": rec}))


def test_aml_batch_without_alert_set_refused(ready):
    rec = _ref("aml_batch")
    del rec["experiment"]["results"]["alert_set"]
    rec["experiment"]["results"]["alert_set_unavailable"] = "the job printed no line"
    problems = _refusals({"run-1": rec})
    assert any("no alert set (the job printed no line)" in p for p in problems)


def test_aml_batch_alert_sets_that_differ_refused(ready):
    a = _ref("aml_batch")
    b = _with(a, "polaris-iceberg-spark-trino")
    b["experiment"]["results"]["alert_set"]["cols_sha"] = "other"
    assert any("runs differ in alert set" in p for p in _refusals({"run-1": a, "run-2": b}))


def test_held_out_corpus_refused_without_its_seed(ready):
    rec = _ref("aml_batch")
    rec["experiment"]["corpus"]["corpus_role"] = "evaluation"
    rec["experiment"]["corpus"]["seed"] = 987654321
    problems = _refusals({"run-1": rec})
    assert any("held-out evaluation corpus" in p for p in problems)
    assert "987654321" not in " ".join(problems)


def test_aml_continuous_with_only_one_query_set_refused(ready):
    rec = trr._release("aml_cont")
    assert any("pre-case and post-case" in p for p in _refusals({"run-1": rec}))


def test_continuous_references_with_different_query_sets_refused(ready):
    a = _ref("c360_cont")
    b = copy.deepcopy(a)
    for rounds in (
        b.get("benchmark_rounds") or [],
        (b.get("pipeline_benchmark") or {}).get("benchmark_rounds") or [],
    ):
        for r in rounds:
            r["executed_query_set_id"] = "qs8-other"
    assert any("executed query sets" in p for p in _refusals({"run-1": a, "run-2": b}))


def test_query_set_differs_refused(ready):
    a = _ref("c360_batch")
    b = _with(a, "hive-iceberg-spark-trino")
    b["experiment"]["results"]["query_set_id"] = "qs8-other"
    assert any("runs differ in query set" in p for p in _refusals({"run-1": a, "run-2": b}))


def test_writer_checks_its_file_through_the_reader(ready, monkeypatch):
    """A writer that drifted from the reader refuses instead of writing."""
    real = X._strip
    monkeypatch.setattr(X, "_strip", lambda fp: {**real(fp), "rows": -1})
    assert any(
        "does not accept its own reference run" in p
        for p in _refusals({"run-1": _ref("c360_batch")})
    )


# -- the command -----------------------------------------------------------------


def test_command_refuses_a_name_the_gate_does_not_read(tmp_path):
    with pytest.raises(H.Refused, match="must be named expected-results-9.9.9.json"):
        H.expected_command([tmp_path], "9.9.9", tmp_path / "expected.json")
    with pytest.raises(H.Refused, match="not X.Y.Z"):
        H.expected_command([tmp_path], "1.7", tmp_path / "expected-results-1.7.json")


def test_command_never_rewrites_a_file(ready, tmp_path, capsys):
    out = tmp_path / "uat" / X.file_name("9.9.9")
    out.parent.mkdir()
    out.write_text("{}")
    rc = H.expected_command([_dir(tmp_path, {"run-1": _ref("c360_batch")})], "9.9.9", out)
    assert rc == 1 and out.read_text() == "{}"
    assert "is not rewritten" in capsys.readouterr().err


def test_command_exit_1_writes_nothing(ready, tmp_path, capsys):
    rec = _ref("c360_batch")
    rec["experiment"]["schema"] = "exp1"
    out = tmp_path / "uat" / X.file_name("9.9.9")
    assert H.expected_command([_dir(tmp_path, {"run-1": rec})], "9.9.9", out) == 1
    assert not out.exists()
    assert "exp1 record" in capsys.readouterr().err


def test_parser_has_the_command():
    args = H.build_parser().parse_args(
        ["expected", "--from", "a", "--from", "b", "--version", "1.7.0", "--out", "o.json"]
    )
    assert args.from_dirs == [Path("a"), Path("b")] and args.version == "1.7.0"
