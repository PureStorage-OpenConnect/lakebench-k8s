"""The datagen calendar-leakage gate must catch a leak concentrated in a
rare bin (review of 7d1eb07: merging sparse bins pooled night hours, so all
planted night rows at 02:00 passed)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("numpy")

TOOL = Path(__file__).resolve().parents[1] / "datagen_rs/tools/calendar_leakage.py"


def _tool():
    pytest.importorskip("duckdb")
    spec = importlib.util.spec_from_file_location("calendar_leakage", TOOL)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _hours():
    # Background: most volume in business hours, a thin night tail 00-05.
    base = {h: (0.004 if h < 6 else 0.9 / 18) for h in range(24)}
    total = sum(base.values())
    return {h: v / total for h, v in base.items()}


def test_rare_bin_concentration_is_caught():
    t = _tool()
    base = _hours()
    n = 300
    cells = {h: n * base[h] for h in range(24)}
    night = sum(cells[h] for h in range(6))
    for h in range(6):
        cells[h] = 0.0
    cells[2] = night  # same night total, all of it at 02:00
    stat, p = t.chi2_sim(base, cells, n)
    assert p < 0.001


def test_matching_sample_passes():
    import numpy as np

    t = _tool()
    base = _hours()
    rng = np.random.default_rng(1)
    draw = rng.multinomial(300, [base[h] for h in range(24)])
    _, p = t.chi2_sim(base, {h: float(draw[h]) for h in range(24)}, 300)
    assert p > 0.01


def test_planted_mass_in_an_unused_bin_fails():
    t = _tool()
    stat, p = t.chi2_sim({0: 0.5, 1: 0.5}, {0: 10.0, 1: 10.0, 2: 1.0}, 21)
    assert p == 0.0
