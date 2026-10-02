"""AML-1: common.rule_stage_profile never raises; a status store it cannot
read gives one ``unavailable`` line and None."""

from __future__ import annotations


class _NoStore:
    @property
    def sparkContext(self):  # noqa: N802
        raise RuntimeError("no py4j\nsecond line")


def test_unreadable_store_logs_unavailable(load_script, capsys):
    common = load_script("common")
    assert (
        common.rule_stage_profile(_NoStore(), "g", "W2_structuring", mark={"jobs": 3, "dropped": 0})
        is None
    )
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1, out
    assert out[0].endswith(
        "[stage-profile] rule=W2_structuring group=g unavailable "
        "reason=RuntimeError: no py4j second line"
    ), out
    assert common.rule_profile_mark(_NoStore()) is None
