"""RPT-1 report fixes R1, R2, R3, R4, R7 and R9 (DESIGN-v1.7 ch03 section 14).

Each test asserts the fixed number or label on a stored record against a
value the test reads from the record itself, not from the renderer.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from tests.fixtures.report_goldens import page_text, render
from tests.fixtures.stored_records import load_record


def _text(run_id: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(render(run_id)), flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


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
    assert all('container!=""' in q and 'container!="POD"' in q for q in fake.queries[:2])
    assert pm.to_dict()["query_version"] == 2


def test_r1_older_platform_record_is_caveated():
    """1320bd was collected by the old queries (no query_version): its
    platform section says the figures count containers more than once."""
    record = load_record("1320bd")
    assert "query_version" not in record["platform_metrics"]
    assert "count containers more than once" in _text("1320bd")


def test_r1_current_platform_record_has_no_caveat():
    from tests.fixtures.report_consistency_helpers import _render_dict

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
    assert (paired, pre_n, post_n) == (8, 8, 12)
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
    from tests.fixtures.report_consistency_helpers import _render_dict

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


def test_r3_batch_qph_samples_are_not_runs():
    record = load_record("1320bd")
    runs = record["experiment"]["repetitions"]["runs"]
    samples = record["pipeline_benchmark"]["query_benchmark"]["iterations"]
    assert (runs, samples) == (1, 3)
    text = _text("1320bd")
    assert f"n={runs} run, {samples} samples/query" in text
    assert "n=3" not in text


def test_r3_continuous_qph_rounds_are_not_runs():
    record = load_record("ebb26f")
    rounds = [r for r in record["pipeline_benchmark"]["benchmark_rounds"] if r["qph"] > 0]
    runs = record["experiment"]["repetitions"]["runs"]
    text = _text("ebb26f")
    assert f"n={runs} run, {len(rounds)} rounds" in text
    assert f"n={len(rounds)}" not in text


# ---------------------------------------------------------------------------
# R4: the benchmark section names the engine that ran it
# ---------------------------------------------------------------------------


def test_r4_benchmark_title_from_recorded_engine():
    engine = load_record("233b69")["experiment"]["architecture"]["query_engine"]["type"]
    assert engine == "spark-thrift"
    text = _text("233b69")
    assert "Spark Thrift query benchmark" in text
    assert "Trino" not in text.split("Spark Thrift query benchmark")[0][-200:]
    assert "Trino Query Benchmark" not in text


def test_r4_title_prefers_the_benchmark_record_engine():
    """The engine the benchmark recorded wins over an experiment block that
    names another (a benchmark run after the config changed)."""
    from tests.fixtures.report_consistency_helpers import _render_dict

    record = load_record("233b69")
    assert record["benchmark"]["engine"] == "spark-thrift"
    record["experiment"]["architecture"]["query_engine"]["type"] = "trino"
    assert "Spark Thrift query benchmark" in _render_dict(record)


def test_r3_batch_samples_come_from_the_record():
    """The recorded samples per query (repetitions) win over iterations."""
    from tests.fixtures.report_consistency_helpers import _render_dict

    record = load_record("1320bd")
    record["experiment"]["repetitions"]["benchmark_samples_per_query"] = 2
    assert "n=1 run, 2 samples/query" in page_text(_render_dict(record))


def test_r4_trino_record_keeps_trino_title():
    assert load_record("5105a0")["experiment"]["architecture"]["query_engine"]["type"] == "trino"
    assert "Trino query benchmark" in _text("5105a0")


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
    html = render("be2b70")
    assert re.search(
        r"background: #fef3c7; color: var\(--warning\);[^>]*><strong>Scale Ratio", html
    )


def test_r7_scale_card_above_scale_is_amber():
    """A passed batch record whose scale ratio is above 1.05 (5105a0 edited)
    shows the card amber; be2b70 failed, so its cards show no number."""
    from tests.fixtures.report_consistency_helpers import _render_dict

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
