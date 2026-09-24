"""Reference detector (D7): entity-level split and prevalence-weighted
precision. Self-scoring and a downsampled test prior both inflated the
reference numbers the leakage verdict relies on."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

from lakebench.faml.reference_score import train_reference_gbt  # noqa: E402


def _data(n_ent=400, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for e in range(n_ent):
        typ = "fan_in" if e % 10 == 0 else "baseline"
        for _ in range(5):
            x = rng.normal(3.0 if typ == "fan_in" else 0.0, 1.0)
            rows.append((e, typ, x, rng.normal()))
    df = pd.DataFrame(rows, columns=["entity", "label", "f1", "f2"])
    return df


def test_grouped_split_keeps_entities_on_one_side(monkeypatch):
    import sklearn.ensemble as ens

    seen = {}
    real = ens.GradientBoostingClassifier.fit

    def spy(self, X, y, *a, **k):
        seen["n_train"] = len(y)
        return real(self, X, y, *a, **k)

    monkeypatch.setattr(ens.GradientBoostingClassifier, "fit", spy)
    df = _data()
    rep = train_reference_gbt(df[["f1", "f2"]], df["label"], groups=df["entity"])
    assert rep.n_train + rep.n_test == len(df)
    # Every entity has 5 rows; a grouped split can only move whole entities.
    assert rep.n_test % 5 == 0 and rep.n_train % 5 == 0


def test_weights_restore_real_prevalence_in_precision():
    df = _data(seed=1)
    base = train_reference_gbt(df[["f1", "f2"]], df["label"], groups=df["entity"])
    # Pretend baseline was downsampled 1:20 -- every baseline row stands for 20.
    w = pd.Series(np.where(df["label"] == "baseline", 20.0, 1.0))
    weighted = train_reference_gbt(
        df[["f1", "f2"]], df["label"], groups=df["entity"], sample_weight=w
    )
    b = {r.typology_type: r for r in base.per_typology}["fan_in"]
    wt = {r.typology_type: r for r in weighted.per_typology}["fan_in"]
    assert wt.recall == pytest.approx(b.recall)  # positives are unweighted
    if b.fp:
        assert wt.fp == pytest.approx(20 * b.fp, abs=1)
        assert wt.precision < b.precision
