"""The scorer's recall.json sidecar lands at the key the held-out audit reads,
and a score the engine fails records the engine's error as its reason."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from lakebench.cli import _aml_post


def test_recall_json_is_put_under_the_gold_scoring_prefix(monkeypatch):
    calls = []
    raw = SimpleNamespace(put_object=lambda **kw: calls.append(kw))
    monkeypatch.setattr(_aml_post, "_s3_client", lambda cfg: SimpleNamespace(raw_client=raw))
    cfg = SimpleNamespace(
        platform=SimpleNamespace(
            storage=SimpleNamespace(s3=SimpleNamespace(buckets=SimpleNamespace(gold="lb-gold")))
        )
    )

    _aml_post._write_recall_json(cfg, "run-1", {"mode": "covered", "status": "scored"})

    assert len(calls) == 1
    assert calls[0]["Bucket"] == "lb-gold"
    assert calls[0]["Key"] == "scoring/run-1/recall.json"
    assert json.loads(calls[0]["Body"]) == {"mode": "covered", "status": "scored"}


@pytest.mark.parametrize("covered", [None, {"cycle": 3}])
def test_an_engine_failure_is_the_not_scored_reason(monkeypatch, covered):
    from lakebench.aml import scoring_queries as sq

    def _fail(*a, **k):
        raise RuntimeError("alerts.sql: Query exceeded distributed total memory limit")

    executor = SimpleNamespace(query_rows=None, catalog_name="lakehouse")
    monkeypatch.setattr("lakebench.benchmark.executor.get_executor", lambda cfg: executor)
    monkeypatch.setattr(_aml_post, "_rules_meta", lambda: None)
    monkeypatch.setattr(_aml_post, "COVERED_OPTIONS", ())
    monkeypatch.setattr(sq, "score", _fail)

    if covered is None:
        out = _aml_post.score_financial_via_queries(None, "run-1", covered=None, timeout=60)
        mode, head = "batch", "the scoring queries did not complete"
    else:
        out = _aml_post.continuous_scoring(None, "run-1", covered, "", None, None, 60)
        mode, head = "covered", "the score query did not complete"
    assert out == {
        "mode": mode,
        "status": "not_scored",
        "reason": f"{head}: alerts.sql: Query exceeded distributed total memory limit",
    }


def test_a_bracketed_error_is_printed_without_an_escape(monkeypatch, capsys):
    """print_warning prints its text verbatim: an escaped [x] would show
    a stray backslash."""

    def _fail(**kw):
        raise OSError("bucket [lb-gold] is read-only")

    raw = SimpleNamespace(put_object=_fail)
    monkeypatch.setattr(_aml_post, "_s3_client", lambda cfg: SimpleNamespace(raw_client=raw))
    cfg = SimpleNamespace(
        platform=SimpleNamespace(
            storage=SimpleNamespace(s3=SimpleNamespace(buckets=SimpleNamespace(gold="lb-gold")))
        )
    )

    _aml_post._write_recall_json(cfg, "run-1", {})

    err = capsys.readouterr().err
    assert "bucket [lb-gold] is read-only" in err
    assert "\\[" not in err
