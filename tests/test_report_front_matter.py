"""RPT-1 and RPT-4: the front matter and the evidence-class stamp
(DESIGN-v1.7 ch03 section 13).

The page and the terminal ``report`` open with the verdict, the evidence
class, the support state and any binding cap, before any metric. The
evidence class is read from the registered-look record, never the config.
"""

from __future__ import annotations

import json
import re

from tests.fixtures.report_consistency_helpers import _render_dict
from tests.fixtures.report_goldens import golden_path, page_text
from tests.fixtures.stored_records import load_record


def _plain(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    from html import unescape

    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html)))


# ---------------------------------------------------------------------------
# Evidence class (RPT-4): one case per stamp state
# ---------------------------------------------------------------------------


def _with_looks(monkeypatch, looks):
    from lakebench.config import datagen_seed

    def fake(path=None):
        if isinstance(looks, Exception):
            raise looks
        return looks

    monkeypatch.setattr(datagen_seed, "load_looks", fake)


def test_stamp_no_look_record(monkeypatch):
    _with_looks(monkeypatch, FileNotFoundError("aml_registered_looks.json"))
    text = _plain(_render_dict(load_record("1320bd")))
    assert "Evidence class: development (look record unreadable: FileNotFoundError)" in text


def test_unreadable_look_record_never_prints_its_message(monkeypatch):
    """load_looks quotes a malformed entry, seed included; only the error
    class may reach the page."""
    _with_looks(monkeypatch, ValueError("looks.json: malformed look entry {'seed': 123456789}"))
    html = _render_dict(load_record("1320bd"))
    assert "123456789" not in html
    assert "look record unreadable: ValueError)" in _plain(html)


def test_stamp_look_record_without_this_run(monkeypatch):
    _with_looks(
        monkeypatch,
        [{"role": "evaluation", "state": "complete", "run_ids": ["20990101-000000-other1"]}],
    )
    text = _plain(_render_dict(load_record("1320bd")))
    assert "Evidence class: development (no look record names this run)" in text


def test_stamp_registered_look_names_this_run(monkeypatch):
    run_id = load_record("1320bd")["run_id"]
    _with_looks(
        monkeypatch,
        [
            {
                "role": "evaluation",
                "state": "complete",
                "run_ids": [run_id],
                "report_sha256": "ab" * 32,
            }
        ],
    )
    text = _plain(_render_dict(load_record("1320bd")))
    assert f"Evidence class: registered look: evaluation (report sha256 {'ab' * 6})" in text
    assert "Recall (registered look: evaluation)" in text


def test_stamp_started_look_is_not_a_look(monkeypatch):
    run_id = load_record("1320bd")["run_id"]
    _with_looks(monkeypatch, [{"role": "evaluation", "state": "started", "run_ids": [run_id]}])
    assert "Evidence class: development" in _plain(_render_dict(load_record("1320bd")))


def test_stamp_calibration_corpus(monkeypatch):
    _with_looks(monkeypatch, [])
    record = load_record("1320bd")
    record["experiment"]["corpus"]["corpus_role"] = "calibration"
    text = _plain(_render_dict(record))
    assert (
        "Evidence class: development (calibration corpus: in-sample, the corpus the rules "
        "were developed on)" in text
    )


def test_stamp_never_read_from_config(monkeypatch):
    """A config that claims a role changes nothing: only the look record
    can make a run a registered look."""
    _with_looks(monkeypatch, [])
    record = load_record("1320bd")
    record["config_snapshot"]["datagen"]["corpus_role"] = "evaluation"
    record["experiment"]["corpus"]["corpus_role"] = "evaluation"
    text = _plain(_render_dict(record))
    assert "Evidence class: development (no look record names this run)" in text


# ---------------------------------------------------------------------------
# Scale ratio warning (K20)
# ---------------------------------------------------------------------------


def test_scale_ratio_warning():
    """be2b70 (1.114) carries the warning; a passed record at 1.114 (825153)
    reads PASSED with the warning as its headline and an amber badge."""
    from lakebench.metrics.verdict import compute_badge_status
    from tests.fixtures.stored_records import load_metrics

    ratio = load_record("be2b70")["pipeline_benchmark"]["scores"]["scale_ratio"]
    warning = f"Scale ratio {ratio * 100:.1f}% > 105% (more data than the scale asks for)"
    assert warning in compute_badge_status(load_metrics("be2b70"))[2]
    text = _plain(golden_path("be2b70").read_text())
    assert warning in text
    passed = load_record("825153")
    assert passed["pipeline_benchmark"]["scores"]["scale_ratio"] > 1.05
    html = _render_dict(passed)
    assert f"Headline: {warning}" in _plain(html)
    assert re.search(r'class="status status-warning"[^>]*>\s*WARNING', html)


def test_scale_ratio_inside_band_has_no_warning():
    from lakebench.metrics.verdict import compute_badge_status
    from tests.fixtures.stored_records import load_metrics

    assert compute_badge_status(load_metrics("5105a0"))[2] == []


# ---------------------------------------------------------------------------
# Stored and recomputed verdicts
# ---------------------------------------------------------------------------


def test_stored_passed_recomputed_failed_reads_failed():
    """A record that stored PASSED but fails today (a gold job failed in
    the record) reads FAILED, and says both."""
    record = load_record("5105a0")
    assert record["verdict"]["status"] == "PASSED"
    gold = next(j for j in record["jobs"] if j["job_type"] == "gold-finalize")
    gold["success"] = False
    gold["error_message"] = "edited: failed"
    text = _plain(_render_dict(record))
    assert "Verdict: FAILED (stored PASSED; recomputed FAILED)" in text


def test_stored_failed_is_never_promoted():
    record = load_record("5105a0")
    record["verdict"] = {"status": "FAILED", "reasons": ["stored: gold rows 0"], "gates": {}}
    text = _plain(_render_dict(record))
    assert "Verdict: FAILED (stored FAILED; recomputed PASSED)" in text
    assert "Headline: stored: gold rows 0" in text
    assert re.search(r'class="status status-failed"', _render_dict(record))


# ---------------------------------------------------------------------------
# Verdict qualifiers (ER-3)
# ---------------------------------------------------------------------------


def test_rule_cap_qualifier():
    record = load_record("1320bd")
    gold = next(j for j in record["jobs"] if j["job_type"] == "gold-finalize")
    gold["rules_skipped"]["W3_round_tripping"] = "path-cap"
    gold["alerts_by_rule"].pop("W3_round_tripping", None)
    text = _plain(_render_dict(record))
    assert "rules skipped on a Lakebench cap:" in text
    assert "W3_round_tripping (path-cap)" in text


# ---------------------------------------------------------------------------
# Terminal report and end-of-run panel
# ---------------------------------------------------------------------------


def test_strictest_verdict_by_priority():
    """Stored FAILED against a recomputed INTERRUPTED reads FAILED, with
    both sides' reasons (FAILED > INTERRUPTED > REFUSED > PASSED)."""
    from lakebench.reports import front_matter as fmod

    class _V:
        status = "INTERRUPTED"
        reasons = ["Run interrupted (SIGINT during gold-finalize)"]

    import lakebench.metrics.verdict as verdict_mod

    m = type("M", (), {})()
    m.stored_verdict = {"status": "FAILED", "reasons": ["silver rows 0"]}
    orig = verdict_mod.compute_verdict
    verdict_mod.compute_verdict = lambda metrics: _V()
    try:
        status, reasons, note = fmod.page_verdict(m)
    finally:
        verdict_mod.compute_verdict = orig
    assert status == "FAILED"
    assert reasons == ["silver rows 0", "Run interrupted (SIGINT during gold-finalize)"]
    assert note == "stored FAILED; recomputed INTERRUPTED"


def test_stage_only_record_has_no_scale_warning():
    from lakebench.metrics.verdict import compute_badge_status
    from tests.fixtures.stored_records import load_metrics

    m = load_metrics("825153")
    assert compute_badge_status(m)[2]
    m.stage_only = "silver-build"
    assert compute_badge_status(m)[2] == []


def test_malformed_experiment_blocks_still_render():
    """A non-dict corpus, rules or support block reads empty in the front
    matter, and a stored FAILED is never shown PASSED."""
    from lakebench.reports.front_matter import front_matter

    record = load_record("5105a0")
    record["experiment"]["corpus"] = "bad"
    record["experiment"]["rules"] = ["bad"]
    record["experiment"]["support"] = 7
    record["verdict"] = {"status": "FAILED", "reasons": ["stored failure"], "gates": {}}
    fm = front_matter(record)
    assert (fm.verdict, fm.verdict_note) == ("FAILED", "stored FAILED; recomputed PASSED")
    assert fm.corpus == "" and fm.support_state == "unknown"


def test_print_front_matter_reads_back_the_saved_record(tmp_path):
    from io import StringIO

    from rich.console import Console

    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.front_matter import print_front_matter

    record = load_record("5105a0")
    record["verdict"] = {"status": "FAILED", "reasons": ["stored failure"], "gates": {}}
    run_dir = tmp_path / f"run-{record['run_id']}"
    run_dir.mkdir()
    (run_dir / "metrics.json").write_text(json.dumps(record))
    storage = MetricsStorage(tmp_path)
    buf = StringIO()
    console = Console(file=buf, width=200)
    print_front_matter(object(), console, storage=storage, run_id=record["run_id"])
    assert "Verdict: FAILED (stored FAILED; recomputed PASSED)" in buf.getvalue()

    class _Broken:
        def load_run(self, run_id):
            raise OSError("disk gone")

    buf2 = StringIO()
    print_front_matter(object(), Console(file=buf2), storage=_Broken(), run_id="x")
    assert buf2.getvalue().startswith("Verdict:")
