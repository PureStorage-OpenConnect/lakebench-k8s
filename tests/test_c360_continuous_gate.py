"""c360 continuous runs that moved no data must fail
(2026-09-24 audit: the honest gate existed only for AML)."""

from lakebench.cli._sustained import _c360_continuous_gate_problems


def test_c360_continuous_gate_problems():
    cases = [
        # a stage that processed zero rows fails, naming it
        ("zero-rows", {"bronze-ingest": 10, "silver-stream": 0}, "silver-stream processed 0 rows"),
        # a stage with missing logs fails
        ("missing-logs", {"bronze-ingest": None, "silver-stream": 5}, True),
        # data moving everywhere passes
        ("data-moving", {"bronze-ingest": 10, "silver-stream": 7}, None),
        # an absent stage is ignored
        ("absent-stage", {"silver-stream": 7}, None),
    ]
    for label, stages, expect in cases:
        probs = _c360_continuous_gate_problems(stages)
        if expect is None:
            assert probs == [], label
        elif expect is True:
            assert probs, label
        else:
            assert len(probs) == 1 and expect in probs[0], label
