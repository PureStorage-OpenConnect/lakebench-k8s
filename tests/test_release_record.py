"""Release evidence: what a cited run record must show (QA-1, DESIGN ch03
section 19). One fixture per failure case, each a stored record with named
fields edited, and a clean fixture that passes."""

from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from lakebench.metrics import release_record as rr
from lakebench.metrics import verdict as verdict_mod
from tests.fixtures import stored_records as sr

FREEZE = "f" * 40
#: The v1.6 release image, a root row of config/datagen_lineage.yaml.
DIGEST = "sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a"
BASES = {
    "c360_batch": "102711-8387da",
    "aml_batch": "130953-f8a2cf",
    "c360_cont": "204941-1d17f4",
    "aml_cont": "205000-ebb26f",
}


@pytest.fixture
def ready(monkeypatch):
    """The layer_rows gate stubbed as passing (the edited records are not
    whole runs); the allowed rule skips are the verdict's real table."""
    monkeypatch.setattr(
        verdict_mod,
        "verdict_from_record",
        lambda record: SimpleNamespace(
            gates={"layer_rows": "PASS"}, qualifiers={rr.LAYER_ROWS_UNMEASURED: []}
        ),
        raising=False,
    )


def _release(kind: str) -> dict:
    """A stored record edited into clean release evidence: exp2 with corpus
    id v2 and the release image's lineage, from the freeze commit, clean."""
    rec = sr.load_record(BASES[kind])
    exp = rec["experiment"]
    exp["schema"], exp["identity_version"] = "exp2", 2
    exp["corpus"]["id_v2"] = "v2-" + BASES[kind]
    exp["corpus"]["lineage"] = DIGEST
    exp["corpus"]["datagen"]["digest"] = DIGEST
    rec["provenance"] = {
        "git_sha": FREEZE,
        "git_dirty": False,
        "end_sample": {"code_changed_during_run": False},
    }
    for rounds in (
        rec.get("benchmark_rounds") or [],
        (rec.get("pipeline_benchmark") or {}).get("benchmark_rounds") or [],
    ):
        for r in rounds:
            r["executed_query_set_id"] = r["query_set_id"]
    if kind == "aml_cont":
        exp["rules"]["executed"] = sorted(
            set(exp["rules"]["executed"]) | (_rule_targets() - set(_continuous_skipped()))
        )
    if kind == "aml_batch":
        exp["rules"]["executed"] = sorted(_rule_targets() - set(exp["rules"]["skipped"]))
        # 1.7 AML batch evidence carries its alert set (EVD-10).
        exp["results"]["alert_set"] = {
            "spec": "as1",
            "columns": ["rule_id", "entity_id", "alert_ts"],
            "cols_sha": "0123456789abcdef",
            "rows": 3,
            "h": "-5",
            "by_rule": {"W2_structuring": {"rows": 3, "h": "-5"}},
        }
    return rec


def _rule_targets() -> set[str]:
    from lakebench.benchmark.aml_queries import RULE_TARGETS

    return set(RULE_TARGETS)


def _continuous_skipped() -> tuple[str, ...]:
    from lakebench.config.support import AML_CONTINUOUS_SKIPPED_RULES

    return AML_CONTINUOUS_SKIPPED_RULES


def _expected(*recs: dict) -> dict:
    """An expected-results file written from the records themselves."""
    entries, continuous = [], []
    for rec in recs:
        exp = rec["experiment"]
        w = exp["workload"]
        if exp["mode"] in ("sustained", "continuous"):
            continuous.append(
                {
                    "workload": w["name"],
                    "workload_version": w["version"],
                    "query_set_ids": sorted(
                        {r["query_set_id"] for r in rec.get("benchmark_rounds") or []}
                    ),
                    **(
                        {"fingerprints": copy.deepcopy(exp["results"]["fingerprints"])}
                        if w["name"] == "customer360"
                        else {}
                    ),
                }
            )
        else:
            entries.append(
                {
                    "workload": w["name"],
                    "workload_version": w["version"],
                    "corpus_id_v2": exp["corpus"]["id_v2"],
                    "scale": exp["corpus"]["scale"],
                    "mode": "batch",
                    "query_set_id": exp["results"]["query_set_id"],
                    "fingerprints": copy.deepcopy(exp["results"]["fingerprints"]),
                }
            )
    return {"version": "9.9.9", "entries": entries, "continuous": continuous}


def _problems(rec: dict, expected: dict | None = None) -> list[str]:
    return rr.record_problems(
        rec, FREEZE, expected if expected is not None else _expected(rec), release_digest=DIGEST
    )


@pytest.mark.parametrize("kind", sorted(BASES))
def test_clean_record_passes(ready, kind):
    assert _problems(_release(kind)) == []


def test_aml_batch_without_alert_set_fails(ready):
    rec = _release("aml_batch")
    del rec["experiment"]["results"]["alert_set"]
    _fails(rec, "the alert-set fingerprint was not recorded")


def test_layer_rows_fails_closed_until_the_verdict_computes_it(monkeypatch):
    monkeypatch.delattr(verdict_mod, "verdict_from_record", raising=False)
    assert any("layer_rows" in p for p in _problems(_release("c360_batch")))


def test_aml_rules_fail_closed_until_the_skips_are_declared(ready, monkeypatch):
    monkeypatch.delattr(verdict_mod, "EXPECTED_SKIPS")
    assert any("allowed skips" in p for p in _problems(_release("aml_batch")))


def _fails(rec: dict, needle: str, expected: dict | None = None) -> None:
    problems = _problems(rec, expected)
    assert any(needle in p for p in problems), problems


def test_legacy_record(ready):
    legacy = sr.load_record("211343-978622")
    assert rr.record_problems(legacy, FREEZE, {"entries": []}) == ["no experiment block"]


def test_failed_record(ready):
    rec = _release("c360_batch")
    rec["verdict"]["status"], rec["success"] = "FAILED", False
    _fails(rec, "did not pass")


@pytest.mark.parametrize(
    ("gate", "qualifiers", "needle"),
    [
        ("FAIL", {rr.LAYER_ROWS_UNMEASURED: []}, "layer_rows gate is FAIL"),
        ("PASS", {rr.LAYER_ROWS_UNMEASURED: ["gold"]}, "rows not measured for gold"),
        ("PASS", {}, "does not say which layers were measured"),
    ],
)
def test_zero_or_unmeasured_rows(ready, monkeypatch, gate, qualifiers, needle):
    monkeypatch.setattr(
        verdict_mod,
        "verdict_from_record",
        lambda record: SimpleNamespace(gates={"layer_rows": gate}, qualifiers=qualifiers),
        raising=False,
    )
    _fails(_release("c360_batch"), needle)


def test_missing_stage(ready):
    rec = _release("c360_batch")
    rec["experiment"]["stages"]["executed"].remove("gold")
    _fails(rec, "expected stages did not run: gold")


def test_skipped_stage(ready):
    rec = _release("c360_batch")
    rec["experiment"]["stages"]["skipped"].append("table maintenance (--skip-maintenance)")
    _fails(rec, "stages skipped or failed")


def test_no_engine_recipe_may_skip_the_benchmark(ready):
    rec = _release("c360_batch")
    rec["experiment"]["architecture"]["recipe"] = "hive-iceberg-spark-none"
    rec["experiment"]["stages"]["executed"].remove("query")
    rec["experiment"]["stages"]["skipped"].append(rr.DECLARED_SKIP_NO_ENGINE)
    rec["experiment"]["results"] = {"query_set_id": None, "fingerprints": {}}
    # The expected file lists the engine recipes' fingerprints for this
    # corpus; a no-engine row is not held to them.
    assert _problems(rec, _expected(_release("c360_batch"))) == []


def test_errored_rule(ready):
    rec = _release("aml_batch")
    rec["experiment"]["rules"]["errored"] = {"W3_round_tripping": "boom"}
    _fails(rec, "rule W3_round_tripping errored")


def test_unallowed_rule_skip(ready):
    rec = _release("aml_batch")
    rec["experiment"]["rules"]["skipped"] = {"W3_round_tripping": "vertex-cap"}
    _fails(rec, "not an allowed skip")


def test_aml_scoring_missing(ready):
    rec = _release("aml_batch")
    rec["financial_scoring"] = None
    _fails(rec, "financial scoring did not run")


def test_fingerprint_mismatch(ready):
    rec = _release("c360_batch")
    expected = _expected(rec)
    q = sorted(expected["entries"][0]["fingerprints"])[0]
    expected["entries"][0]["fingerprints"][q]["rows"] = -1
    _fails(rec, f"query {q} result differs", expected)


def test_no_expected_entry(ready):
    rec = _release("c360_batch")
    expected = _expected(rec)
    expected["entries"][0]["corpus_id_v2"] = "other"
    _fails(rec, "no expected results", expected)


def test_continuous_result_check_missing(ready):
    rec = _release("c360_cont")
    rec["continuous"]["result_check"] = {"not_checked": "the corpus did not settle"}
    _fails(rec, "continuous result check did not run")


def test_aml_continuous_only_the_8_query_set_fails(ready):
    rec = _release("aml_cont")
    expected = _expected(rec)
    expected["continuous"][0]["query_set_ids"] = ["qs12-x", "qs8-32f521a57551"]
    _fails(rec, "never ran query set(s) qs12-x", expected)


def test_aml_continuous_only_the_12_query_set_fails(ready):
    rec = _release("aml_cont")
    for rounds in (rec["benchmark_rounds"], rec["pipeline_benchmark"]["benchmark_rounds"]):
        for r in rounds:
            r["query_set_id"] = r["executed_query_set_id"] = "qs12-x"
    expected = _expected(_release("aml_cont"))
    expected["continuous"][0]["query_set_ids"] = ["qs12-x", "qs8-32f521a57551"]
    _fails(rec, "never ran query set(s) qs8-32f521a57551", expected)


def test_dirty_tree(ready):
    rec = _release("c360_batch")
    rec["provenance"]["git_dirty"] = True
    _fails(rec, "modified tree")


def test_sha_not_the_freeze(ready):
    rec = _release("c360_batch")
    rec["provenance"]["git_sha"] = "a" * 40
    _fails(rec, "not from the freeze commit")


def test_code_changed_mid_run(ready):
    rec = _release("c360_batch")
    rec["provenance"]["end_sample"]["code_changed_during_run"] = True
    _fails(rec, "code changed during the run")


def test_record_without_an_end_sample(ready):
    rec = _release("c360_batch")
    rec["provenance"].pop("end_sample")
    _fails(rec, "run-end sample is missing")


def test_held_out_role(ready):
    rec = _release("aml_batch")
    rec["experiment"]["corpus"]["corpus_role"] = "evaluation"
    _fails(rec, "held-out evaluation corpus")


def test_exp1_record(ready):
    rec = _release("c360_batch")
    rec["experiment"]["schema"] = "exp1"
    _fails(rec, "exp1 record")


def test_release_record_needs_release_image(ready):
    """Valid markers, another image: refused; with the image conditions
    removed it would pass."""
    rec = _release("c360_batch")
    rec["experiment"]["corpus"]["datagen"]["digest"] = "sha256:" + "1" * 64
    _fails(rec, "not generated by the release datagen image (111111111111)")
    rec = _release("c360_batch")
    rec["experiment"]["corpus"]["lineage"] = "sha256:" + "2" * 64
    _fails(rec, "is not the release image's")


def test_unpinned_release_image_fails_closed(ready):
    assert rr.release_datagen_digest("docker.io/sillidata/lb-datagen:1.6.0") is None
    problems = rr.record_problems(
        _release("c360_batch"), FREEZE, _expected(_release("c360_batch")), release_digest=None
    )
    # The in-tree default is a tag today, so the image check refuses.
    if rr.release_datagen_digest() is None:
        assert any("not pinned by digest" in p for p in problems)


def test_evaluation_profile_record(ready):
    rec = _release("c360_batch")
    rec["experiment"]["limits"]["bound_kinds"] = [rr.EVALUATION_PROFILE_KIND]
    _fails(rec, "evaluation profile runs are not release evidence")


def test_executor_cap_bound(ready):
    rec = _release("c360_batch")
    rec["experiment"]["limits"]["bound_kinds"] = ["silver-build: executor cap"]
    _fails(rec, "silver-build: executor cap bound this run")


def test_path_cap_skip_is_allowed_but_its_bound_refuses_release(ready):
    """A W3 path-cap skip passes the rule set (owner, 10-03) and is labelled
    as a bound kind, which no release row allows: the record is refused on
    the bound alone."""
    rec = _release("aml_batch")
    rules = rec["experiment"]["rules"]
    rules["executed"].remove("W3_round_tripping")
    rules["skipped"]["W3_round_tripping"] = "path-cap"
    rec["experiment"]["limits"]["bound_kinds"] = ["rule W3_round_tripping cap"]
    problems = _problems(rec)
    assert not any("W3_round_tripping skipped" in p for p in problems), problems
    assert "rule W3_round_tripping cap bound this run; its numbers measure the cap" in problems


def test_display_list_alone_is_not_read(ready):
    """Only bound_kinds decides: a trickle line in the display list (every
    continuous record has one) does not refuse the record."""
    rec = _release("c360_cont")
    rec["experiment"]["limits"]["bound"] = ["trickle: max_files_per_trigger 2 (auto)"]
    assert _problems(rec) == []


def test_aml_record_with_an_ml_loop(ready):
    rec = _release("aml_batch")
    rec["ml_loop"] = {"ml_loop_ok": True}
    _fails(rec, "ML loop is C360 only")


def test_record_key():
    assert rr.record_key(_release("c360_cont")) == (
        "customer360",
        "continuous",
        "hive-iceberg-spark-trino",
        1.0,
    )
    assert len(rr.RELEASE_MATRIX) == 16


def test_eval_profile_baseline_refused():
    """The perf gate refuses what the release refuses: a run an evaluation
    profile or a cap bound never becomes or meets a baseline."""
    from lakebench.metrics.perf_gate import RunRecord, run_refusals

    raw = sr.load_record("212900-5105a0")
    raw["experiment"]["limits"]["bound_kinds"] = [rr.EVALUATION_PROFILE_KIND]
    run = RunRecord("x", sr.record_path("212900-5105a0"), raw, sr.load_metrics("212900-5105a0"))
    pinned = SimpleNamespace(
        mode=run.mode, fingerprint={}, fingerprint_hash="", file_sha256="", name="p"
    )
    reasons = run_refusals(run, pinned)  # type: ignore[arg-type]
    assert "evaluation profile runs are not release evidence" in reasons


def test_extra_rule_ran(ready):
    rec = _release("aml_cont")
    from lakebench.config.support import AML_CONTINUOUS_SKIPPED_RULES

    rec["experiment"]["rules"]["executed"].append(AML_CONTINUOUS_SKIPPED_RULES[0])
    _fails(rec, "rules outside the expected set ran")


def test_rounds_without_the_executed_query_set(ready):
    rec = _release("c360_cont")
    for r in rec["pipeline_benchmark"]["benchmark_rounds"]:
        r.pop("executed_query_set_id")
    _fails(rec, "rounds do not record the query set they executed")


def test_a_round_with_no_qph_is_left_out(ready):
    """A round whose queries all failed has QpH 0 and records no executed
    set; it is left out, as the composite QpH leaves it out."""
    rec = _release("c360_cont")
    rounds = rec["pipeline_benchmark"]["benchmark_rounds"]
    assert len(rounds) > 1 and all(r["qph"] > 0 for r in rounds)
    rounds[0]["qph"] = 0.0
    rounds[0]["executed_query_set_id"] = None
    assert _problems(rec) == []
    for r in rounds:
        r["qph"] = 0.0
    _fails(rec, "no in-stream round measured a QpH")


def test_c360_continuous_fingerprints_compared_when_listed(ready):
    rec = _release("c360_cont")
    expected = _expected(rec)
    fps = copy.deepcopy(rec["experiment"]["results"]["fingerprints"])
    q = sorted(fps)[0]
    fps[q]["rows"] = -1
    expected["continuous"][0]["fingerprints"] = fps
    _fails(rec, f"query {q} result differs", expected)
    expected["continuous"][0].pop("fingerprints")
    _fails(rec, "no expected fingerprints for this C360 continuous workload", expected)


def test_c360_continuous_failed_result_queries_refused(ready):
    rec = _release("c360_cont")
    check = rec["continuous"]["result_check"]
    q = sorted(check["fingerprints"])[0]
    check["fingerprints"][q] = None
    _fails(rec, f"continuous result check queries failed: {q}")


def test_mixed_datagen_images_refused(ready):
    """Pods on two images: the fleet names no single digest, and the series
    marker of the release image does not vouch for them."""
    rec = _release("c360_batch")
    rec["experiment"]["corpus"]["datagen"]["digest"] = None
    rec["datagen_fleet"] = {"image_ids": ["a@sha256:" + "1" * 64, "b@" + DIGEST]}
    rec["config_snapshot"].setdefault("experiment_inputs", {})["corpus_observation"] = {
        "series": {"generation": {"image_digest": DIGEST}}
    }
    _fails(rec, "not generated by the release datagen image")


def test_corpus_problems_refused(ready):
    rec = _release("c360_batch")
    rec["experiment"]["corpus"]["problems"] = ["datagen pods ran different images: a, b"]
    _fails(rec, "corpus problems")
