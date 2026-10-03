"""AML-1: per-rule elapsed time and stage profile from the gold driver log.

The driver logs ``[detection] <rule>: ... elapsed=Ts`` per rule and, after
each rule, ``[stage-profile]`` lines (common.rule_stage_profile). The
collector keeps both on the gold-finalize job: ``rule_elapsed_s`` and
``stage_profile`` (with ``stage_profile_unavailable`` for a failed read).
"""

from __future__ import annotations

import json

from lakebench.metrics.collector import JobMetrics, MetricsCollector
from lakebench.metrics.stage_profile import parse_stage_profile

P = "[lb] 2026-10-02T10:00:00.000000 - "

DRIVER_LOG = "\n".join(
    [
        P + "[detection] W2_structuring: alerts=12 prior=0 elapsed=41.5s",
        P + "[stage-profile] rule=W2_structuring group=lb-rule-W2_structuring-aaaa1111 "
        "stage=7 attempt=0 status=COMPLETE tasks=88 wall_s=30.2 exec_s=2400.0 "
        "shuffle_read_mb=512.5 max_task_s=40.1 stages=4 truncated=false complete=true "
        "lossy=false profile_s=0.12 name=count at NativeMethodAccessorImpl.java:0",
        P + "[stage-profile] rule=W2_structuring group=lb-rule-W2_structuring-aaaa1111 "
        "stage=5 attempt=1 status=ACTIVE tasks=2 wall_s=None exec_s=3.0 shuffle_read_mb=0.0 "
        "max_task_s=None stages=4 truncated=false complete=true lossy=false "
        "profile_s=0.12 name=collect at x.py:1",
        P + "[detection] W1_layering: skipped=vertex-cap detail=8000001 > 8000000 elapsed=3.2s",
        P + "[stage-profile] rule=W1_layering group=lb-rule-W1_layering-bbbb2222 stages=0 "
        "truncated=false complete=true lossy=false profile_s=0.05",
        P + "[detection] W7_cross_border_high_risk: alerts=0 error=RuntimeError: boom elapsed=0.7s",
        P + "[stage-profile] rule=W7_cross_border_high_risk group=lb-rule-W7-cccc3333 "
        "unavailable reason=Py4JError: statusStore does not exist",
        P + "[detection] W8_dormant_reactivation: skipped=mode-excluded elapsed=0.0s",
        P + "[detection] W9_typo: alerts=0 error=unknown-rule elapsed=0.0s",
        P + "[detection] W3_round_tripping: alerts=4 prior=0 elapsed=9.0s",
        P + "[stage-profile] rule=W3_round_tripping group=lb-rule-W3-dddd4444 stages=0 "
        "truncated=true complete=false lossy=false profile_s=5.01",
    ]
)


def test_rule_elapsed_s_for_every_attempted_rule():
    job = MetricsCollector().parse_driver_logs(DRIVER_LOG, "gold-finalize")
    assert job.rule_elapsed_s == {
        "W2_structuring": 41.5,
        "W1_layering": 3.2,
        "W7_cross_border_high_risk": 0.7,
        "W3_round_tripping": 9.0,
    }
    # A mode-excluded or unknown rule never started: no time, still listed.
    assert job.rules_skipped["W8_dormant_reactivation"] == "mode-excluded"
    assert job.rule_errors["W9_typo"] == "unknown-rule"


def test_stage_profile_per_rule():
    job = MetricsCollector().parse_driver_logs(DRIVER_LOG, "gold-finalize")
    w2 = job.stage_profile["W2_structuring"]
    assert [s["stage"] for s in w2] == [7, 5]
    assert w2[0] == {
        "stage": 7,
        "attempt": 0,
        "status": "COMPLETE",
        "tasks": 88,
        "wall_s": 30.2,
        "exec_s": 2400.0,
        "shuffle_read_mb": 512.5,
        "max_task_s": 40.1,
        "stages": 4,
        "truncated": False,
        "complete": True,
        "lossy": False,
        "name": "count at NativeMethodAccessorImpl.java:0",
    }
    assert w2[1]["status"] == "ACTIVE"
    assert w2[1]["wall_s"] is None and w2[1]["max_task_s"] is None
    # Empty with clean flags: the rule ran no stage.
    assert job.stage_profile["W1_layering"] == []
    # Empty while stages may be missing: not "ran no stage", but unavailable.
    assert "W3_round_tripping" not in job.stage_profile
    assert "W7_cross_border_high_risk" not in job.stage_profile
    assert job.stage_profile_unavailable == {
        "W7_cross_border_high_risk": "Py4JError: statusStore does not exist",
        "W3_round_tripping": (
            "no stage in the status store: truncated=true complete=false lossy=false"
        ),
    }
    assert job.stage_profile_cost_s == {
        "W2_structuring": 0.12,
        "W1_layering": 0.05,
        "W3_round_tripping": 5.01,
    }


def test_last_group_of_a_rule_wins():
    """A rerun driver logs the rule again under a new group: only the last
    group's stages are kept, and an earlier unavailable read is cleared."""
    logs = "\n".join(
        [
            "[stage-profile] rule=W2 group=g1 unavailable reason=x",
            "[stage-profile] rule=W2 group=g2 stage=1 attempt=0 status=COMPLETE tasks=1 "
            "wall_s=1.0 exec_s=1.0 shuffle_read_mb=0.0 max_task_s=1.0 stages=1 "
            "truncated=true complete=true lossy=true profile_s=0.1 name=a",
        ]
    )
    profile, unavailable, cost = parse_stage_profile(logs)
    assert unavailable == {} and cost == {"W2": 0.1}
    assert [(s["stage"], s["truncated"], s["lossy"]) for s in profile["W2"]] == [(1, True, True)]
    profile, unavailable, cost = parse_stage_profile(
        logs + "\n[stage-profile] rule=W2 group=g3 unavailable reason=y"
    )
    assert profile == {} and unavailable == {"W2": "y"} and cost == {}


def test_fields_survive_the_metrics_json_round_trip(tmp_path):
    from lakebench.metrics.storage import _dataclass_from_dict

    job = MetricsCollector().parse_driver_logs(DRIVER_LOG, "gold-finalize")
    data = json.loads(json.dumps(job.to_dict()))
    back = _dataclass_from_dict(JobMetrics, data)
    assert back.rule_elapsed_s == job.rule_elapsed_s
    assert back.stage_profile == job.stage_profile
    assert back.stage_profile_unavailable == job.stage_profile_unavailable
    assert back.stage_profile_cost_s == job.stage_profile_cost_s
