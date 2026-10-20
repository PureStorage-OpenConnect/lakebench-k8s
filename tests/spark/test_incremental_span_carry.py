"""The per-tick cut never dilates to span real event-time gaps: when a rule's
accumulated cut holds more than MAX_SPANS disjoint day-runs (datagen pods drift
apart in event time), the newest MAX_SPANS are read this tick and the older
overflow is carried to a later tick, so the read is bounded and no span is
dropped.

These exercise only the pure-Python span bookkeeping (merge_spans,
cap_newest_spans, IncrementalDetection.begin_tick/cut/carry_spans); no Spark
session is built, so they are fast. The end-to-end "incremental == full
recompute" guarantee is test_incremental_detection_spark.py.
"""

from __future__ import annotations

import pytest

# incremental_detection imports pyspark at module scope, so the module cannot
# load without it even though the span bookkeeping under test needs no session.
pytest.importorskip("pyspark")

pytestmark = [pytest.mark.usefixtures("load_script_module")]

DAY = 24 * 3_600_000_000


def _spans(*days):
    return tuple((d * DAY, (d + 1) * DAY) for d in days)


def test_cap_newest_spans_keeps_budget_and_carries_middle(load_script_module):
    inc = load_script_module("incremental_detection")
    # Disjoint single-day runs, days 0..(2*n) step 2 (one apart, none coalesce).
    n = inc.MAX_SPANS + 10
    cut = _spans(*range(0, 2 * n, 2))
    kept, carry = inc.cap_newest_spans(cut)
    assert len(kept) == inc.MAX_SPANS
    assert len(carry) == len(cut) - inc.MAX_SPANS
    # Nothing dropped, nothing dilated across a gap: kept|carry == input,
    # disjoint.
    assert set(kept) | set(carry) == set(cut)
    assert not (set(kept) & set(carry))
    # Progress guarantee: the oldest CARRY_DRAIN_SPANS runs are kept this tick,
    # so a lagging span is never starved by newer runs.
    oldest = sorted(cut)[: inc.CARRY_DRAIN_SPANS]
    assert set(oldest) <= set(kept), "oldest runs must be drained, not starved"
    # The freshest runs are kept too.
    newest = sorted(cut)[-(inc.MAX_SPANS - inc.CARRY_DRAIN_SPANS) :]
    assert set(newest) <= set(kept)


def test_adjacent_days_coalesce_so_cap_only_bites_on_scatter(load_script_module):
    inc = load_script_module("incremental_detection")
    # A contiguous run of many days is ONE span: the cap does not bite, no carry.
    kept, carry = inc.cap_newest_spans(_spans(*range(inc.MAX_SPANS + 50)))
    assert len(kept) == 1
    assert carry == ()


def test_under_cap_is_unchanged_no_carry(load_script_module):
    inc = load_script_module("incremental_detection")
    cut = _spans(0, 2, 4)
    kept, carry = inc.cap_newest_spans(cut)
    assert kept == cut
    assert carry == ()


def test_begin_tick_bounds_cut_and_drains_carry(load_script_module):
    inc = load_script_module("incremental_detection")
    det = inc.IncrementalDetection(spark=None, root="/tmp/none")
    rule = "W4_risk_propagation"

    # Tick 1: more than MAX_SPANS disjoint day-runs arrive at once.
    n = inc.MAX_SPANS + 5
    det.begin_tick(_spans(*range(0, 2 * n, 2)), [rule])
    assert len(det.cut[rule]) == inc.MAX_SPANS, "cut must be bounded to MAX_SPANS"
    assert len(det.carry_spans[rule]) == 5, "overflow must be carried, not merged away"
    # The cut must be disjoint runs, NOT one dilated envelope across the gaps.
    assert det.cut[rule] == tuple(sorted(det.cut[rule]))
    envelope = det.cut[rule][-1][1] - det.cut[rule][0][0]
    assert envelope < n * 2 * DAY, "cut dilated to span the gaps (the bug)"

    # The rule reads + commits this tick's kept cut; carry must survive.
    carried_after_commit = det.carry_spans[rule]
    det.cut.pop(rule, None)  # committed() pops the cut; carry_spans is separate.
    assert det.carry_spans[rule] == carried_after_commit

    # Tick 2: nothing new arrives, but the carried overflow must become the cut.
    det.begin_tick(None, [rule])
    assert det.cut.get(rule) is not None, "carried spans must re-enter the cut"
    assert set(det.cut[rule]) == set(carried_after_commit)
    assert rule not in det.carry_spans or not det.carry_spans[rule]


def test_prior_combine_stays_disjoint_and_carries(load_script_module):
    inc = load_script_module("incremental_detection")
    det = inc.IncrementalDetection(spark=None, root="/tmp/none")
    rule = "W4_risk_propagation"
    # Tick 1: a few scattered runs, under the cap -> all kept, no carry.
    det.begin_tick(_spans(0, 10, 20), [rule])
    assert det.cut[rule] == _spans(0, 10, 20)
    assert not det.carry_spans.get(rule)
    # The rule does NOT commit this tick (a skip), so its cut persists as prior.
    # Tick 2: many more scattered runs arrive. The prior+cut combine must stay
    # DISJOINT (not merge_spans gap-filled), so the total exceeds MAX_SPANS and
    # carry fires. A gap-filling combine would collapse to <=MAX_SPANS and
    # silently carry nothing -- the bug the adversarial review caught.
    more = _spans(*range(100, 100 + 2 * (inc.MAX_SPANS + 5), 2))
    det.begin_tick(more, [rule])
    assert det.carry_spans.get(rule), "prior+cut combine gap-filled, carry defeated"
    assert len(det.cut[rule]) == inc.MAX_SPANS
    # Every original run is still accounted for across cut + carry (coverage).
    all_runs = set(_spans(0, 10, 20)) | set(more)
    assert all_runs <= set(det.cut[rule]) | set(det.carry_spans[rule])


def test_full_clears_carry(load_script_module):
    inc = load_script_module("incremental_detection")
    det = inc.IncrementalDetection(spark=None, root="/tmp/none")
    rule = "W2_structuring"
    det.begin_tick(_spans(*range(0, 2 * (inc.MAX_SPANS + 3), 2)), [rule])
    assert det.carry_spans.get(rule)
    # A FULL cut (position unknown) recomputes everything, so stale carry must go.
    det.begin_tick(inc.FULL, [rule])
    assert det.cut[rule] == inc.FULL
    assert not det.carry_spans.get(rule)
