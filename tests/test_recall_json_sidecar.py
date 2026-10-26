"""The scorer's recall.json sidecar lands at the key the held-out audit reads."""

from __future__ import annotations

import json
from types import SimpleNamespace

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
