"""The scorecard tile grid: each tile cites a recorded field or says NR.

``build_tiles`` reads the run record and the loaded run's stored scores. These
tests check each tile against the record by hand, that a missing field is NR
and never zero, and the rendered goldens. The rest of the file checks the
numbers and labels the page shows beside the tiles (platform metrics, query
rates, resources as run, ingest basis, throughput and the bottleneck table)
against values read from the stored record.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from lakebench.reports.tiles import (
    EVIDENCE_DERIVED,
    EVIDENCE_NR,
    EVIDENCE_RECORDED,
    build_tiles,
    render_tiles,
)
from tests.fixtures.report_consistency_helpers import _render_dict
from tests.fixtures.report_goldens import GOLDEN_RUNS, golden_path, page_text, render
from tests.fixtures.stored_records import load_record

_GIB = 1024**3


def _record() -> dict:
    return {
        "pipeline_benchmark": {
            "stages": [
                {"stage_name": "bronze", "output_rows": 2478560},
                {"stage_name": "silver", "output_rows": 2428561, "input_size_gb": 9.5},
                {"stage_name": "gold", "input_rows": 2428561, "input_size_gb": 9.0},
            ],
            "scores": {
                "time_to_value_seconds": 152.13,
                "pipeline_saturated": False,
                "pipeline_throughput_gb_per_second": 0.1946,
                "composite_qph": 936.2,
                "benchmark_samples_per_query": 3,
                "total_core_hours": 0.86,
                "compute_efficiency_gb_per_core_hour": 34.5642,
                "sustained_throughput_rps": 1032.7,
                "data_freshness_seconds": 101.0,
                "in_stream_composite_qph": 260.0,
            },
        },
        "experiment": {"support": {"state": "unverified"}},
        "io": {
            "totals": {"trino": {"read_bytes": 3 * _GIB, "write_bytes": 0}},
            "intervals": [],
            "not_measured": [{"source": "spark", "stage": "bronze", "direction": "read"}],
        },
    }


def _by_label(tiles):
    return {t.label: t for t in tiles}


def test_batch_tiles_cite_the_records_fields() -> None:
    t = _by_label(build_tiles(_record(), mode="batch", gates={"a": "PASS", "b": "PASS"}))
    assert list(t) == [
        "Pace",
        "Time to value",
        "Query rate",
        "Compute",
        "Efficiency",
        "Storage",
        "Gates",
        "Support",
    ]
    assert "152 s" in t["Time to value"].value
    assert "15,964 interactions/s" in t["Pace"].value
    assert t["Pace"].evidence == EVIDENCE_DERIVED
    assert "2,428,561 interactions cleaned in 152 s" in t["Pace"].sub
    assert "MiB/s" in t["Pace"].alt_value  # 9.5 GiB over 152 s is 64 MiB/s
    assert t["Time to value"].source == "pipeline_benchmark.scores.time_to_value_seconds"
    assert "936.2" in t["Query rate"].value and "3 samples per query" in t["Query rate"].sub
    assert t["Time to value"].evidence == EVIDENCE_RECORDED
    assert t["Storage"].evidence == EVIDENCE_DERIVED
    assert "3.00 GiB" in t["Storage"].value
    assert "spark not measured" in t["Storage"].sub


def test_continuous_tiles_swap_the_lead_and_the_query_rate_field() -> None:
    t = _by_label(build_tiles(_record(), mode="continuous", gates={"a": "PASS"}))
    assert list(t)[:3] == ["Pace", "Freshness", "Query rate"]
    assert "1,033 interactions/s" in t["Pace"].value and "kept pace" in t["Pace"].sub
    assert "260.0" in t["Query rate"].value
    assert t["Query rate"].source.endswith("in_stream_composite_qph")


def test_missing_fields_are_nr_never_zero() -> None:
    rec = {"pipeline_benchmark": {"scores": {}}}
    tiles = build_tiles(rec, mode="batch", gates=None)
    assert all(x.evidence == EVIDENCE_NR and x.value == "NR" for x in tiles)
    assert all(x.sub.startswith("not measured") for x in tiles)


def test_zero_is_measured_zero() -> None:
    rec = _record()
    rec["pipeline_benchmark"]["scores"]["total_core_hours"] = 0.0
    t = _by_label(build_tiles(rec, mode="batch", gates={"a": "PASS"}))
    assert t["Compute"].evidence == EVIDENCE_RECORDED
    assert "0.00" in t["Compute"].value


def test_gates_count_and_name_what_did_not_pass() -> None:
    t = _by_label(
        build_tiles(_record(), mode="batch", gates={"a": "PASS", "b": "FAIL", "c": "PASS"})
    )
    assert t["Gates"].value == "2 / 3"
    assert "b" in t["Gates"].sub


def test_stored_scores_win_over_the_recomputed_ones() -> None:
    class Run:
        stored_scores = {"composite_qph": 260.0, "time_to_value_seconds": 1.0}

    rec = _record()
    rec["pipeline_benchmark"]["scores"]["composite_qph"] = 260.1
    t = _by_label(build_tiles(rec, mode="batch", gates=None, metrics=Run()))
    assert "260.0" in t["Query rate"].value


def test_rendered_html_escapes_and_names_the_field() -> None:
    rec = _record()
    rec["experiment"]["support"]["state"] = "<b>x</b>"
    html = render_tiles(build_tiles(rec, mode="batch", gates=None))
    assert "<b>x</b>" not in html
    assert 'title="pipeline_benchmark.scores.time_to_value_seconds"' in html


@pytest.mark.parametrize("run_id", GOLDEN_RUNS)
def test_golden_tiles_match_the_stored_record(run_id: str) -> None:
    text = re.sub(r"\|+", "|", re.sub(r"<[^>]+>", "|", page_text(golden_path(run_id).read_text())))
    sc = load_record(run_id)["pipeline_benchmark"]["scores"]
    grid = text[text.index("lb-grid") :]
    if "failed-headline" in golden_path(run_id).read_text():
        # A run that did not pass shows no score figure, only the reason.
        assert grid.count("not shown: the run did not pass") == 5
        assert f"{sc['total_core_hours']:.2f}" not in grid
        return
    assert f"{sc['total_core_hours']:.2f}" in grid
    assert f"{sc['compute_efficiency_gb_per_core_hour']:.2f}" in grid
    key = "composite_qph" if "time_to_value_seconds" in sc else "in_stream_composite_qph"
    if sc.get(key) is None:
        # Not recorded: the tile says so rather than showing a number.
        assert "Query rate|NR|" in grid
    else:
        assert f"{sc[key]:,.1f}" in grid


def test_aml_pace_counts_transactions_screened_by_gold() -> None:
    rec = _record()
    rec["config_snapshot"] = {"workload_schema": "financial"}
    t = _by_label(build_tiles(rec, mode="batch", gates={"a": "PASS"}))["Pace"]
    assert "15,964 payments/s" in t.value
    assert "payments screened" in t.sub
    assert t.source.startswith("pipeline_benchmark.stages[2].input_rows")
    assert "gold input bytes" in t.alt_sub


def test_pace_without_its_stage_is_nr_not_zero() -> None:
    rec = _record()
    rec["pipeline_benchmark"]["stages"] = []
    t = _by_label(build_tiles(rec, mode="batch", gates=None))["Pace"]
    assert t.evidence == EVIDENCE_NR and t.value == "NR"


def test_the_bytes_view_steps_its_unit_with_the_size() -> None:
    from lakebench.reports import derived as dv

    assert "TiB/s" in dv._fmt(2048.0, dv.AUTO_GIB, "/s")
    assert "GiB/s" in dv._fmt(2.0, dv.AUTO_GIB, "/s")
    assert "MiB/s" in dv._fmt(0.5, dv.AUTO_GIB, "/s")


# ---------------------------------------------------------------------------
# The rest of the page, against the stored record
# ---------------------------------------------------------------------------


def _flat(html: str) -> str:
    html = re.sub(r"<style>.*?</style>", " ", page_text(html), flags=re.S)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))


def _text(run_id: str) -> str:
    return _flat(render(run_id))


def _section(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return text[i : text.index(end, i + len(start))]


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


def test_platform_queries_sum_containers_only(monkeypatch):
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


def test_older_platform_record_is_caveated():
    """1320bd was collected by the old queries (no query_version): its
    platform section says the figures count containers more than once."""
    record = load_record("1320bd")
    assert "query_version" not in record["platform_metrics"]
    assert "count containers more than once" in _text("1320bd")


def test_stage_rows_labelled_sum_of_per_pod_peaks():
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


@pytest.mark.parametrize(
    ("platform", "reason"),
    [
        (None, None),
        (
            {"pods": [], "collection_error": "no Prometheus service found"},
            "no Prometheus service found",
        ),
    ],
)
def test_absent_platform_metrics_are_reported_not_hidden(platform, reason):
    """A run without platform metrics still shows the section, with the
    recorded reason when there is one, instead of silently leaving it out."""
    record = load_record("1320bd")
    record["platform_metrics"] = platform
    text = _flat(_render_dict(record))
    assert "Platform Metrics" in text
    assert reason is None or reason in text


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


def test_paired_change_names_its_query_count():
    record = load_record("1320bd")
    pb = record["pipeline_benchmark"]
    scores = pb["scores"]
    pre_n = len(pb["pre_compaction_benchmark"]["queries"])
    post_n = len(pb["query_benchmark"]["queries"])
    text = _text("1320bd")
    assert f"Pre-compaction QpH (over {pre_n} queries) {scores['pre_compaction_qph']:.1f}" in text
    assert (
        f"Post-compaction QpH (over {post_n} queries) {scores['post_compaction_qph']:.1f}" in text
    )
    assert "unpaired: the two rounds' successful queries differ" in text
    assert "QpH improvement" not in text


def test_counts_successful_queries_and_compares_sets():
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


@pytest.mark.parametrize("run_id", ["ebb26f", "1d17f4"])
def test_a_recorded_composite_re_renders_as_recorded(run_id):
    """A record keeps the composite it was written with: a record from
    before AML's composite was over one fixed query set re-renders its
    stored median over every round, never a recomputed one."""
    record = load_record(run_id)
    stored = record["pipeline_benchmark"]["scores"]["composite_qph"]
    assert f"{stored:,.1f}" in _text(run_id)


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
def test_benchmark_title_from_recorded_engine(run_id, engine_edit, title, other_title):
    record = load_record(run_id)
    if engine_edit:
        record["experiment"]["architecture"]["query_engine"]["type"] = engine_edit
    text = _flat(_render_dict(record))
    assert title in text
    assert other_title.lower() not in text.lower()


def test_recorded_degradation_shown():
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


def test_continuous_cards_without_a_benchmark_say_not_measured():
    """A continuous record whose pipeline benchmark was not built shows its
    throughput and CPU-hours as not measured, not as 0."""
    record = load_record("1d17f4")
    record["pipeline_benchmark"] = None
    text = _flat(_render_dict(record))
    assert "0 rows/s" not in text
    assert "not measured (the pipeline benchmark was not built)" in text


def test_throughput_label_and_corpus_size():
    record = load_record("5105a0")
    bronze = record["bronze_size_gb"]
    total = record["pipeline_benchmark"]["scores"]["total_data_processed_gb"]
    assert total > 2 * bronze  # stage inputs count the corpus once per stage
    text = _text("5105a0")
    assert "stage inputs processed per second (bronze, silver and gold reads)" in text
    assert f"{total:.1f} GiB of stage inputs; corpus {bronze:.1f} GiB in bronze" in text
    assert "data volume / wall-clock time" not in text


def test_resources_as_run_from_the_jobs():
    record = load_record("1320bd")
    silver = next(j for j in record["jobs"] if j["job_type"] == "silver-build")
    text = _text("1320bd")
    resources = _section(text, "Resources as run", "Configuration")
    assert (
        f"silver-build {silver['executor_count']} {silver['executor_cores']} "
        f"{silver['executor_memory_gb']:.0f} GB not recorded (this record predates it)"
    ) in resources
    # The snapshot's spark.executor block sized nothing and is not shown.
    executor = record["config_snapshot"]["spark"]["executor"]
    assert executor["instances"] != silver["executor_count"]
    assert "Executors, " not in text


def test_scratch_as_ran_from_provenance():
    record = load_record("1320bd")
    record["provenance"]["scratch_as_ran"] = {
        "silver-build": {"size_limit": "300Gi", "storage_class": "px-csi-scratch"},
        "gold-finalize": {"size_limit": None, "storage_class": None},
        "bronze-verify": {"not_recorded": "the SparkApplication spec was not read"},
    }
    text = _flat(_render_dict(record))
    assert "silver-build 8 4 48 GB 300Gi on px-csi-scratch" in text
    assert "no scratch PVC" in text
    assert "not recorded (the SparkApplication spec was not read)" in text


def test_continuous_resources_from_the_stages():
    record = load_record("ebb26f")
    stage = next(s for s in record["pipeline_benchmark"]["stages"] if s["stage_name"] == "silver")
    resources = _section(_text("ebb26f"), "Resources as run", "Configuration")
    assert (
        f"silver-stream {stage['executor_count']} {stage['executor_cores']} "
        f"{stage['executor_memory_gb']:.0f} GB"
    ) in resources


def test_continuous_ingest_labels_and_coverage():
    record = load_record("ebb26f")
    scores = record["pipeline_benchmark"]["scores"]
    sustained = record["config_snapshot"]["sustained"]
    coverage = scores["corpus_ingest_ratio"]
    assert coverage == 0.4374
    text = _text("ebb26f")
    # The denominator is a mean-rate estimate of the rows released, and
    # the label says so; it is never the corpus.
    assert "Ingest Ratio (estimate)" in text
    assert "bronze rows / rows released by the window's end" in text
    assert "bronze rows / datagen rows" not in text
    assert f"Corpus coverage {coverage * 100:.1f}%" in text
    assert f"Window {scores['window_seconds']:,.0f}s" in text
    files, trigger = sustained["max_files_per_trigger"], sustained["bronze_trigger_interval"]
    assert f"Offered load trickle {files} file per {trigger} bronze trigger" in text
    assert "Excluded in continuous mode: AML continuous runs detection rules" in text


@pytest.mark.parametrize(
    ("run_id", "must_contain", "must_not_contain"),
    [
        pytest.param(
            "65567b",
            lambda r: "bronze rows / generated corpus rows (released rows not recorded)",
            lambda r: "rows released by the window's end",
            id="ingest-basis-without-released-rows",
        ),
        pytest.param(
            "ebb26f",
            lambda r: (
                f"bronze bucket {r['bronze_size_gb']:.1f} GiB at run end: "
                "landing files plus the bronze table"
            ),
            lambda r: f"corpus {r['bronze_size_gb']:.1f} GiB",
            id="continuous-names-window-intake-not-corpus",
        ),
        pytest.param(
            "ebb26f",
            lambda r: f"commit {r['provenance']['git_sha'][:7]}, clean",
            lambda r: "uncommitted changes (dirty)",
            id="clean-tree-is-not-dirty",
        ),
    ],
)
def test_basis_labels_distinguish_what_the_number_divides(run_id, must_contain, must_not_contain):
    """A label names its basis and never the misleading neighbour."""
    record = load_record(run_id)
    text = _text(run_id)
    assert must_contain(record) in text
    assert must_not_contain(record) not in text


def _bottleneck_rows(run_id: str) -> dict[str, list[str]]:
    html = page_text(render(run_id))
    start = html.index("<h2>Bottleneck Identification</h2>")
    section = html[start : html.index("</section>", start)]
    rows = {}
    for m in re.finditer(r"<tr[^>]*>\s*<td[^>]*>(\w+)</td>(.*?)</tr>", section, flags=re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", m.group(2), flags=re.S)
        rows[m.group(1)] = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
    return rows


def test_bottleneck_continuous_query_stage_has_no_latency_share():
    """The query stage has no micro-batch latency: its latency and share
    cells are empty and the latency shares are over the stages that have
    one."""
    stages = load_record("ebb26f")["pipeline_benchmark"]["stages"]
    timed = [s["stage_name"] for s in stages if (s.get("latency_ms") or 0) > 0]
    assert "query" not in timed
    rows = _bottleneck_rows("ebb26f")
    assert rows["query"][:2] == ["-", "-"]
    latency = {s["stage_name"]: s["latency_ms"] for s in stages if s["stage_name"] in timed}
    total = sum(latency.values())
    for name, ms in latency.items():
        assert rows[name][1] == f"{ms / total * 100:.1f}%"


def test_bottleneck_thrift_query_stage_not_costed_with_trino_cores():
    """233b69 ran its queries on Spark Thrift: no Trino cores are charged
    to its query stage, and the shares are over the Spark stages."""
    record = load_record("233b69")
    assert record["config_snapshot"]["query_engine"] == "spark-thrift"
    stages = record["pipeline_benchmark"]["stages"]
    spark = [s for s in stages if s["stage_name"] != "query"]
    total = sum(s["executor_count"] * s["executor_cores"] * s["elapsed_seconds"] for s in spark)
    silver = next(s for s in spark if s["stage_name"] == "silver")
    share = silver["executor_count"] * silver["executor_cores"] * silver["elapsed_seconds"] / total
    section = _section(_text("233b69"), "Bottleneck Identification", "Data Validity")
    assert f"{share * 100:.1f}%" in section
    assert "Not in the bar: query (the spark-thrift query engine records no cores)" in section
    query_row = re.search(r"query (\S+) (\S+) (\S+) (\S+)", section.split("% of requested")[-1])
    assert query_row and query_row.group(3) == "-" and query_row.group(4) == "-"
