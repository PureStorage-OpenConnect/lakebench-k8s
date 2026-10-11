"""RPT-1 report fixes R1, R2, R3, R4, R7 and R9.

Each test asserts the fixed number or label on a stored record against a
value the test reads from the record itself, not from the renderer.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from tests.fixtures.report_consistency_helpers import _render_dict
from tests.fixtures.report_goldens import page_text, render
from tests.fixtures.stored_records import load_record


def _flat(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def _text(run_id: str) -> str:
    return _flat(render(run_id))


# ---------------------------------------------------------------------------
# R1: platform CPU and memory double counted
# ---------------------------------------------------------------------------


class _FakePrometheus:
    """A Prometheus over cAdvisor-shaped series as OpenShift with a second
    kubelet ServiceMonitor exposes them: for each pod a pod-level total
    (``container=""``), the pause container (``POD``) and its containers,
    every series scraped twice (``service`` differs). It applies the
    ``container!=""`` and ``container!="POD"`` selectors, ``max by (pod,
    container)`` when the query asks for it, then ``sum by (pod)``."""

    _ONE_SCRAPE = {
        "container_cpu_usage_seconds_total": [
            # pod, container, values over three steps (cores after rate())
            ("exec-1", "", [3.0, 5.0, 4.0]),
            ("exec-1", "POD", [0.01, 0.01, 0.01]),
            ("exec-1", "spark-kubernetes-executor", [2.0, 4.0, 3.0]),
            ("exec-1", "sidecar", [1.0, 1.0, 1.0]),
            ("driver", "", [1.0, 1.0, 1.0]),
            ("driver", "POD", [0.01, 0.01, 0.01]),
            ("driver", "spark-kubernetes-driver", [1.0, 1.0, 1.0]),
        ],
        "container_memory_working_set_bytes": [
            ("exec-1", "", [8e9, 9e9, 8e9]),
            ("exec-1", "POD", [1e6, 1e6, 1e6]),
            ("exec-1", "spark-kubernetes-executor", [7e9, 8e9, 7e9]),
            ("exec-1", "sidecar", [1e9, 1e9, 1e9]),
        ],
    }
    SERIES = {
        metric: [(pod, c, svc, v) for pod, c, v in series for svc in ("kubelet", "lb-kubelet")]
        for metric, series in _ONE_SCRAPE.items()
    }

    def __init__(self) -> None:
        self.queries: list[str] = []

    def get(self, url, params=None):
        query = (params or {}).get("query", "")
        self.queries.append(query)
        result = []
        for metric, series in self.SERIES.items():
            if metric not in query:
                continue
            kept = [
                s
                for s in series
                if not ('container!=""' in query and s[1] == "")
                and not ('container!="POD"' in query and s[1] == "POD")
            ]
            if "max by (pod, container)" in query:
                deduped: dict[tuple[str, str], list[float]] = {}
                for pod, c, _svc, values in kept:
                    prev = deduped.get((pod, c))
                    deduped[(pod, c)] = (
                        list(values)
                        if prev is None
                        else [max(a, b) for a, b in zip(prev, values, strict=True)]
                    )
                kept = [(pod, c, "", v) for (pod, c), v in deduped.items()]
            by_pod: dict[str, list[float]] = {}
            for pod, _c, _svc, values in kept:
                acc = by_pod.setdefault(pod, [0.0] * len(values))
                for i, v in enumerate(values):
                    acc[i] += v
            result = [
                {"metric": {"pod": pod}, "values": [[i, str(v)] for i, v in enumerate(vals)]}
                for pod, vals in by_pod.items()
            ]
        return _Resp(result)

    def close(self) -> None:
        pass


class _Resp:
    status_code = 200
    text = ""

    def __init__(self, result):
        self._result = result

    def json(self):
        return {"data": {"result": self._result}}


def test_r1_platform_queries_sum_containers_only(monkeypatch):
    """Per-pod CPU and memory equal the sum of the pod's containers, not
    containers plus the pod-level total plus the pause container."""
    import httpx

    from lakebench.observability.platform_collector import PlatformCollector

    fake = _FakePrometheus()
    monkeypatch.setattr(httpx, "Client", lambda **kw: fake)
    t0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
    pm = PlatformCollector("http://prom:9090", "ns").collect(t0, t0)
    pods = {p.pod_name: p for p in pm.pods}
    # exec-1 containers: 2+1, 4+1, 3+1 cores; peak 5, average 4.
    assert pods["exec-1"].cpu_max_cores == 5.0
    assert pods["exec-1"].cpu_avg_cores == 4.0
    assert pods["driver"].cpu_max_cores == 1.0
    assert pods["exec-1"].memory_max_bytes == 9e9
    stamped = pm.to_dict()
    assert stamped["query_version"] >= 2
    record = load_record("1320bd")
    record["platform_metrics"] = stamped
    assert "count containers more than once" not in _render_dict(record)


def test_r1_older_platform_record_is_caveated():
    """1320bd was collected by the old queries (no query_version): its
    platform section says the figures count containers more than once."""
    record = load_record("1320bd")
    assert "query_version" not in record["platform_metrics"]
    assert "count containers more than once" in _text("1320bd")


def test_r1_current_platform_record_has_no_caveat():
    record = load_record("1320bd")
    record["platform_metrics"]["query_version"] = 2
    assert "count containers more than once" not in _render_dict(record)


def test_r1_stage_rows_labelled_sum_of_per_pod_peaks():
    """1320bd's platform stage table says its max columns are sums of
    per-pod peaks, and each stage cell equals that sum from the record."""
    text = _text("1320bd")
    assert "CPU Max (cores, sum of per-pod peaks)" in text
    assert "Mem Max (sum of per-pod peaks)" in text
    pods = load_record("1320bd")["platform_metrics"]["pods"]
    silver = [
        p
        for p in pods
        if "silver" in p["pod_name"]
        and (p.get("cpu_max_cores", 0) >= 0.01 or p.get("memory_max_bytes", 0) >= 10 * 1024**2)
    ]
    assert silver
    peak_sum = sum(p["cpu_max_cores"] for p in silver)
    avg_sum = sum(p["cpu_avg_cores"] for p in silver)
    assert f"silver {len(silver)} {avg_sum:.2f} {peak_sum:.2f}" in text


# ---------------------------------------------------------------------------
# R2: pre and post QpH over different query counts beside a paired change
# ---------------------------------------------------------------------------


def test_r2_paired_change_names_its_query_count():
    record = load_record("1320bd")
    pb = record["pipeline_benchmark"]
    scores = pb["scores"]
    paired = scores["maintenance_paired_queries"]
    value = scores["maintenance_value_pct"]
    pre_n = len(pb["pre_compaction_benchmark"]["queries"])
    post_n = len(pb["query_benchmark"]["queries"])
    text = _text("1320bd")
    assert f"QpH change, paired over {paired} queries {value:+.1f}%" in text
    assert f"Pre-compaction QpH (over {pre_n} queries) {scores['pre_compaction_qph']:.1f}" in text
    assert (
        f"Post-compaction QpH (over {post_n} queries) {scores['post_compaction_qph']:.1f}" in text
    )
    assert "unpaired: the two rounds' successful queries differ" in text
    assert "QpH improvement" not in text


def test_r2_counts_successful_queries_and_compares_sets():
    """A post round with two failed queries: QpH is over the 6 that
    succeeded, the label says 6, and the sets differ, so the note shows."""
    record = load_record("5105a0")
    post = record["pipeline_benchmark"]["query_benchmark"]["queries"]
    post[0]["success"] = False
    post[1]["success"] = False
    ok = sum(1 for q in post if q["success"])
    html = page_text(_render_dict(record))
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
    assert f"Post-compaction QpH (over {ok} queries)" in text
    assert "unpaired: the two rounds' successful queries differ" in text


# ---------------------------------------------------------------------------
# R3: samples and rounds shown as runs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "samples_override", [None, 2], ids=["recorded-iterations", "recorded-samples"]
)
def test_r3_batch_qph_samples_are_not_runs(samples_override):
    """Samples per query come from the record (the recorded samples win over
    iterations) and are never shown as runs."""
    record = load_record("1320bd")
    runs = record["experiment"]["repetitions"]["runs"]
    samples = record["pipeline_benchmark"]["query_benchmark"]["iterations"]
    if samples_override is not None:
        record["experiment"]["repetitions"]["benchmark_samples_per_query"] = samples_override
        samples = samples_override
    text = _flat(_render_dict(record))
    assert f"n={runs} run, {samples} samples/query" in text
    assert f"n={samples} " not in text


def test_r3_continuous_qph_rounds_are_not_runs():
    record = load_record("1d17f4")
    rounds = [r for r in record["pipeline_benchmark"]["benchmark_rounds"] if r["qph"] > 0]
    runs = record["experiment"]["repetitions"]["runs"]
    text = _text("1d17f4")
    assert f"n={runs} run, {len(rounds)} rounds" in text
    assert f"n={len(rounds)}" not in text


@pytest.mark.parametrize("run_id", ["ebb26f", "1d17f4"])
def test_a_recorded_composite_re_renders_as_recorded(run_id):
    """A record keeps the composite it was written with: a record from
    before AML's composite was over one fixed query set re-renders its
    stored median over every round, never a recomputed one."""
    record = load_record(run_id)
    stored = record["pipeline_benchmark"]["scores"]["composite_qph"]
    assert f"{stored:,.1f}" in _text(run_id)


# ---------------------------------------------------------------------------
# R4: the benchmark section names the engine that ran it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("run_id", "engine_edit", "title", "other_title"),
    [
        ("233b69", None, "Spark Thrift query benchmark", "Trino query benchmark"),
        # The engine the benchmark recorded wins over an experiment block
        # that names another.
        ("233b69", "trino", "Spark Thrift query benchmark", "Trino query benchmark"),
        ("5105a0", None, "Trino query benchmark", "Spark Thrift query benchmark"),
    ],
    ids=["spark-thrift", "benchmark-record-wins", "trino"],
)
def test_r4_benchmark_title_from_recorded_engine(run_id, engine_edit, title, other_title):
    record = load_record(run_id)
    if engine_edit:
        record["experiment"]["architecture"]["query_engine"]["type"] = engine_edit
    text = _flat(_render_dict(record))
    assert title in text
    assert other_title.lower() not in text.lower()


# ---------------------------------------------------------------------------
# R7: a scale ratio above 1.05 is not "Complete"
# ---------------------------------------------------------------------------


def test_r7_scale_ratio_above_scale_is_amber():
    ratio = load_record("be2b70")["pipeline_benchmark"]["scores"]["scale_ratio"]
    assert ratio > 1.05
    shown = f"{ratio * 100:.1f}%"
    text = _text("be2b70")
    assert f"Scale Ratio: {shown} above the scale" in text
    assert f"{shown} Complete" not in text


def test_r7_scale_card_above_scale_is_amber():
    """A passed batch record whose scale ratio is above 1.05 (5105a0 edited)
    shows the card amber; be2b70 failed, so its cards show no number."""
    record = load_record("5105a0")
    record["pipeline_benchmark"]["scores"]["scale_ratio"] = 1.10
    record["pipeline_benchmark"]["scorecard"]["scale_ratio"] = 1.10
    html = page_text(_render_dict(record))
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
    assert "110.0% ABOVE SCALE" in text


def test_r7_scale_ratio_inside_band_stays_complete():
    ratio = load_record("5105a0")["pipeline_benchmark"]["scores"]["scale_ratio"]
    assert 0.95 <= ratio <= 1.05
    assert f"Scale Ratio: {ratio * 100:.1f}% Complete" in _text("5105a0")


# ---------------------------------------------------------------------------
# R9: the page's own trend replaced by the recorded degradation
# ---------------------------------------------------------------------------


def test_r9_recorded_degradation_shown():
    value = load_record("1d17f4")["pipeline_benchmark"]["scores"]["qph_degradation_pct"]
    assert value == 20.9
    text = _text("1d17f4")
    assert f"QpH degradation, first-half to second-half median: {value:.1f}%" in text
    for local in ("QpH is stable", "QpH is declining", "QpH is improving", "Insufficient data"):
        assert local not in text


def test_unpriced_continuous_bytes_read_not_measured():
    """A continuous run whose datagen bytes per row are unknown has no
    stage-input bytes: the page says not measured, never 0.00 GiB."""
    record = load_record("1d17f4")
    pb = record["pipeline_benchmark"]
    for part in (pb, pb.get("scorecard") or {}, pb.get("scores") or {}):
        for key in (
            "total_data_processed_gb",
            "pipeline_throughput_gb_per_second",
            "compute_efficiency_gb_per_core_hour",
        ):
            if key in part:
                part[key] = 0.0
    text = _flat(_render_dict(record))
    assert "not measured (no datagen fleet record)" in text
    assert "0.00 GiB/core-hr" not in text


def test_rescreen_only_instances_left_out_are_counted_on_the_page():
    """Covered recall leaves out the sanctions instances only a rescreen
    finds (continuous W5 screens on arrival); the page says how many."""
    from lakebench.benchmark.aml_queries import RULE_TARGETS

    record = load_record("ebb26f")
    typ = RULE_TARGETS["W5_sanctions_match"]
    record["financial_scoring"] = {
        "mode": "covered",
        "status": "scored",
        "covered": {
            "rules": [{"rule_id": "W5_sanctions_match", "status": "ran", "reason": None}],
            "typologies": [
                {
                    "typology_type": typ,
                    "recall_covered": 0.5,
                    "covered_instances": 10,
                    "corpus_instances": 20,
                    "coverage": 0.5,
                    "rescreen_only_excluded": 3,
                    "detection_status": "scored",
                    "designated_rules": "W5_sanctions_match",
                }
            ],
        },
    }
    text = _flat(_render_dict(record))
    assert "3 rescreen-only instances left out" in text


@pytest.mark.parametrize(
    ("platform", "shown"),
    [
        (None, "Platform Metrics"),
        (
            {"pods": [], "collection_error": "no Prometheus service found"},
            "no Prometheus service found",
        ),
        ({"pods": []}, "Platform Metrics"),
    ],
)
def test_absent_platform_metrics_are_reported_not_hidden(platform, shown):
    """A run without platform metrics still shows the section, with the
    recorded reason when there is one, instead of silently leaving it out."""
    record = load_record("1320bd")
    record["platform_metrics"] = platform
    page = _render_dict(record)
    assert "<h2>Platform Metrics</h2>" in page
    assert shown in page


def test_platform_values_not_collected_are_never_shown_as_zero():
    """A pod whose memory query failed shows "not collected" for memory, in
    its row and its stage's sum, never 0.0 GiB."""
    record = load_record("1320bd")
    record["platform_metrics"] = {
        "duration_seconds": 300,
        "pods": [
            {
                "pod_name": "lakebench-bronze-verify-driver",
                "component": "spark-driver",
                "cpu_avg_cores": 1.5,
                "cpu_max_cores": 2.0,
                "memory_avg_bytes": None,
                "memory_max_bytes": None,
            }
        ],
        "collection_error": "memory query failed: HTTP 503: unavailable",
        "query_version": 2,
    }
    text = _flat(_render_dict(record))
    assert "memory query failed" in text
    assert "0.0 GiB" not in text
    assert "2.00" in text and text.count("not collected") >= 4


def test_a_stage_sum_with_a_pod_not_collected_sums_the_rest_and_says_so():
    """Two bronze pods, one without CPU: the stage CPU sum is the other pod's
    and says one pod was not collected."""
    record = load_record("1320bd")

    def pod(name, cpu):
        return {
            "pod_name": name,
            "component": "spark-driver",
            "cpu_avg_cores": cpu,
            "cpu_max_cores": cpu,
            "memory_avg_bytes": 2 * 1024**3,
            "memory_max_bytes": 2 * 1024**3,
        }

    record["platform_metrics"] = {
        "duration_seconds": 300,
        "pods": [
            pod("lakebench-bronze-verify-driver", 1.25),
            pod("lakebench-bronze-verify-exec-1", None),
        ],
        "collection_error": "1 of 2 pods returned no CPU series",
        "query_version": 2,
    }
    text = _flat(_render_dict(record))
    assert "1.25 (1 pod not collected)" in text
    assert "4.0 GiB" in text


def test_continuous_cards_without_a_benchmark_say_not_measured():
    """A continuous record whose pipeline benchmark was not built shows its
    throughput and CPU-hours as not measured, not as 0."""
    record = load_record("1d17f4")
    record["pipeline_benchmark"] = None
    text = _flat(_render_dict(record))
    assert "0 rows/s" not in text
    assert "not measured (the pipeline benchmark was not built)" in text
