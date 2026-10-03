"""Unit test for the Welford parallel update helper.

D-full-profiles maintains ``silver.entity_profiles.avg_amount_usd`` and
``stddev_amount_usd`` incrementally: each micro-batch computes the batch's
own (n_b, mean_b, M2_b) block over the originator-side amounts, then
merges into the target profile row's (n_a, mean_a, M2_a) via the parallel
Welford recurrence (Chan et al., 1979; Wikipedia
"Algorithms_for_calculating_variance#Parallel_algorithm"):

    n   = n_a + n_b
    d   = mean_b - mean_a
    mean = mean_a + d * n_b / n
    M2  = M2_a + M2_b + d^2 * n_a * n_b / n

Sample stddev is then ``sqrt(M2 / (n - 1))``.

This test proves ``welford_merge`` in ``common.py`` produces the same M2
(and thus the same sample variance) as a single-pass ``sum(x^2) - n*mean^2``
computation, across ten random splits of the same dataset. Any regression
that reintroduces a naive delta (``M2 += batch_M2``) fails one or more of
these because it drops the ``d^2 * n_a * n_b / n`` cross-term.
"""

from __future__ import annotations

import random
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
pytestmark = pytest.mark.usefixtures("load_script")


def _reference_M2(xs: list[float]) -> tuple[int, float, float]:
    """Single-pass reference: return (n, mean, M2 = sum((x - mean)^2))."""
    n = len(xs)
    if n == 0:
        return 0, 0.0, 0.0
    mean = sum(xs) / n
    M2 = sum((x - mean) ** 2 for x in xs)
    return n, mean, M2


def _welford_reduce_via_helper(xs: list[float], splits: list[int]):
    from common import welford_merge

    # Split ``xs`` into contiguous chunks at the given cut points; then
    # fold left with welford_merge starting from the empty block.
    chunks: list[list[float]] = []
    prev = 0
    for cut in splits + [len(xs)]:
        chunks.append(xs[prev:cut])
        prev = cut
    n, mean, M2 = 0, 0.0, 0.0
    for chunk in chunks:
        cn, cmean, cM2 = _reference_M2(chunk)
        n, mean, M2 = welford_merge(n, mean, M2, cn, cmean, cM2)
    return n, mean, M2


def test_welford_merge_matches_single_pass_across_random_splits():
    rng = random.Random(20260928)
    for case in range(10):
        # Populations chosen to span single-batch, small, and larger sizes.
        pop_size = rng.choice([5, 25, 100, 250, 1000])
        xs = [rng.uniform(0.01, 10000.0) for _ in range(pop_size)]
        # Pick 1--5 random split points inside the population.
        n_splits = rng.randint(1, 5)
        splits = sorted(rng.sample(range(1, pop_size), n_splits))
        ref_n, ref_mean, ref_M2 = _reference_M2(xs)
        got_n, got_mean, got_M2 = _welford_reduce_via_helper(xs, splits)
        assert got_n == ref_n, f"case {case}: n mismatch {got_n} vs {ref_n}"
        # tolerances chosen relative to magnitudes: mean/M2 are sums, so the
        # tolerance scales with population size.
        assert abs(got_mean - ref_mean) < 1e-9 * max(1.0, abs(ref_mean)), (
            f"case {case}: mean drifted; got {got_mean}, ref {ref_mean}, splits {splits}"
        )
        assert abs(got_M2 - ref_M2) < 1e-9 * max(1.0, abs(ref_M2)), (
            f"case {case}: M2 drifted; got {got_M2}, ref {ref_M2}, splits {splits}"
        )


def test_welford_merge_empty_left_identity():
    """Merging (0, 0, 0) with any block returns that block unchanged."""
    from common import welford_merge

    n, mean, M2 = welford_merge(0, 0.0, 0.0, 7, 3.5, 12.25)
    assert (n, mean, M2) == (7, 3.5, 12.25)


def test_welford_merge_empty_right_identity():
    from common import welford_merge

    n, mean, M2 = welford_merge(7, 3.5, 12.25, 0, 0.0, 0.0)
    assert (n, mean, M2) == (7, 3.5, 12.25)


def test_welford_merge_single_singletons_reproduces_variance():
    """Fold 8 singletons; assert M2 matches the direct formula. This catches
    a regression that would drop the delta^2 cross-term."""
    from common import welford_merge

    xs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    n, mean, M2 = 0, 0.0, 0.0
    for x in xs:
        n, mean, M2 = welford_merge(n, mean, M2, 1, x, 0.0)
    ref_n, ref_mean, ref_M2 = _reference_M2(xs)
    assert n == ref_n
    assert abs(mean - ref_mean) < 1e-12
    assert abs(M2 - ref_M2) < 1e-9


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
